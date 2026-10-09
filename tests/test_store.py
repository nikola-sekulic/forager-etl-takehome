"""Exercise feed validation and the real compressed on-disk organization store."""

import gzip
import json
import os
import random
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from pipeline.store import OrganizationStore, _compressed_blocks, build_store, iter_records


class FeedAndStoreTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        # Limit fixture map allocation on Windows; never mock storage operations.
        allocator = patch("pipeline.store.STORE_MAP_BYTES", 8 * 1024 ** 2)
        allocator.start()
        self.addCleanup(allocator.stop)

    def feed(self, name, records):
        path = self.root / f"{name}.json.gz"
        with gzip.open(path, "wt", encoding="utf-8") as handle:
            for record in records:
                handle.write(json.dumps(record) + "\n")
        return path

    def envelope(self, identifier, name="Organization", **fields):
        return {"id": identifier, "serialized_data": {"forager_id": identifier, "name": name, **fields}}

    def test_stream_yields_source_line_numbers_and_full_envelopes(self):
        records = [self.envelope(3), self.envelope(8)]
        feed = self.feed("valid", records)
        self.assertEqual(list(iter_records(feed)), [(1, records[0]), (2, records[1])])

    def test_malformed_record_identifies_line_without_exposing_contents(self):
        path = self.root / "bad.json.gz"
        with gzip.open(path, "wt", encoding="utf-8") as handle:
            handle.write(json.dumps(self.envelope(3)) + "\n")
            handle.write('{"private_field":"sensitive-fixture-value"\n')
        with self.assertRaises(ValueError) as raised:
            list(iter_records(path))
        self.assertIn(f"{path}:2:", str(raised.exception))
        self.assertNotIn("sensitive-fixture-value", str(raised.exception))

    def test_blank_line_is_a_malformed_record(self):
        path = self.root / "blank.json.gz"
        with gzip.open(path, "wb") as handle:
            handle.write(b"\n")
        with self.assertRaises(ValueError) as raised:
            list(iter_records(path))
        self.assertIn(f"{path}:1:", str(raised.exception))

    def test_invalid_envelope_is_rejected(self):
        invalid_envelopes = [
            [], {}, {"id": 1}, {"id": 1, "serialized_data": []},
            {"id": True, "serialized_data": {}},
            {"id": "1", "serialized_data": {}},
            {"id": -1, "serialized_data": {}},
            {"id": 1, "serialized_data": {"forager_id": 2}},
        ]
        for index, record in enumerate(invalid_envelopes):
            with self.subTest(record=record):
                path = self.feed(f"invalid-{index}", [record])
                with self.assertRaises(ValueError) as raised:
                    list(iter_records(path))
                self.assertIn(f"{path}:1:", str(raised.exception))

    def test_truncated_gzip_is_rejected(self):
        path = self.feed("truncated", [self.envelope(8)])
        path.write_bytes(path.read_bytes()[:-8])
        with self.assertRaises(ValueError) as raised:
            list(iter_records(path))
        self.assertIn(str(path), str(raised.exception))

    def test_full_record_is_retrievable_and_missing_ids_are_none(self):
        records = [self.envelope(8, addresses=[{"city": "Budapest"}], arbitrary=[{"kept": True}]),
                   self.envelope(3, name="Another")]
        path = self.feed("organizations", records)
        destination = self.root / "org-store"
        metrics = build_store([path], destination)
        self.assertEqual(metrics["organizations"], 2)
        self.assertGreater(metrics["organization_raw_bytes"], 0)
        self.assertGreater(metrics["organization_store_bytes"], 0)
        self.assertGreaterEqual(metrics["organization_stage_seconds"], 0)
        with OrganizationStore(destination) as store:
            self.assertEqual(json.loads(store.get(8)), records[0]["serialized_data"])
            self.assertEqual(json.loads(store.get(3)), records[1]["serialized_data"])
            self.assertIsNone(store.get(999))
        with self.assertRaises(RuntimeError):
            store.get(8)

    def test_duplicate_org_ids_across_files_are_rejected(self):
        first = self.feed("first", [self.envelope(8)])
        second = self.feed("second", [self.envelope(8, name="Conflicting")])
        # This assertion checks serial source-order diagnostics. In parallel
        # either producer may reach the writer first, so either file can conflict.
        with patch.dict(os.environ, {"STAGE_WORKERS": "1"}), self.assertRaises(ValueError) as raised:
            build_store([first, second], self.root / "org-store")
        self.assertIn(f"{second}:1:", str(raised.exception))
        self.assertIn("duplicate", str(raised.exception))

    def test_existing_staging_directory_is_not_silently_reused(self):
        destination = self.root / "org-store"
        destination.mkdir()
        sentinel = destination / "existing"
        sentinel.write_text("keep", encoding="utf-8")
        with self.assertRaises(FileExistsError):
            build_store([], destination)
        self.assertEqual(sentinel.read_text(encoding="utf-8"), "keep")

    def test_zero_cache_still_returns_complete_records(self):
        body = self.envelope(8, details="x" * 1000)
        path = self.feed("large", [body])
        destination = self.root / "org-store"
        build_store([path], destination)
        with OrganizationStore(destination, cache_bytes=0) as store:
            for _attempt in range(3):
                self.assertEqual(json.loads(store.get(8)), body["serialized_data"])
            self.assertEqual(store._cached_bytes, 0)

    def test_cache_evicts_to_respect_budget_and_never_caches_oversized_record(self):
        records = [self.envelope(identifier) for identifier in range(1, 8)]
        records.append(self.envelope(100, details="x" * 1000))
        path = self.feed("cache", records)
        destination = self.root / "org-store"
        build_store([path], destination)
        with OrganizationStore(destination, cache_bytes=700) as store:
            for identifier in range(1, 8):
                self.assertEqual(json.loads(store.get(identifier)), records[identifier - 1]["serialized_data"])
                self.assertLessEqual(store._cached_bytes, 700)
            self.assertNotIn(1, store._cache)
            self.assertEqual(json.loads(store.get(100)), records[-1]["serialized_data"])
            self.assertNotIn(100, store._cache)
            self.assertLessEqual(store._cached_bytes, 700)

    def build_parallel_in_subprocess(self, paths, destination):
        """Bound failure duration so a broken producer cannot hang the suite."""
        command = (
            "import json, sys; from pathlib import Path; import pipeline.store as store; "
            "store.STORE_MAP_BYTES = 8 * 1024 ** 2; "
            "print(json.dumps(store.build_store([Path(path) for path in sys.argv[2:]], Path(sys.argv[1]))))"
        )
        environment = dict(os.environ, STAGE_WORKERS="2")
        return subprocess.run(
            [sys.executable, "-c", command, str(destination), *map(str, paths)],
            cwd=Path(__file__).resolve().parents[1], env=environment,
            capture_output=True, text=True, timeout=20,
        )

    def test_parallel_multi_file_stage_preserves_all_complete_bodies(self):
        first_records = [self.envelope(identifier, details={"file": "first"}) for identifier in range(1, 6)]
        second_records = [self.envelope(identifier, addresses=[{"country": "HU"}]) for identifier in range(8, 14)]
        paths = [self.feed("first", first_records), self.feed("second", second_records)]
        destination = self.root / "parallel-store"
        result = self.build_parallel_in_subprocess(paths, destination)
        self.assertEqual(result.returncode, 0, result.stderr)
        metrics = json.loads(result.stdout)
        self.assertEqual(metrics["organizations"], 11)
        self.assertEqual(metrics["organization_stage_workers"], 2)
        with OrganizationStore(destination) as store:
            for envelope in first_records + second_records:
                self.assertEqual(json.loads(store.get(envelope["id"])), envelope["serialized_data"])
            self.assertIsNone(store.get(7))

    def test_parallel_stage_malformed_producer_fails_and_cancels_peers(self):
        first = self.feed("valid", [self.envelope(identifier) for identifier in range(1000)])
        broken = self.root / "malformed.json.gz"
        with gzip.open(broken, "wb") as handle:
            handle.write(b'{"invalid":\n')
        result = self.build_parallel_in_subprocess([first, broken], self.root / "parallel-store")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn(f"{broken}:1:", result.stderr)
        self.assertIn("malformed", result.stderr)

    def test_parallel_stage_truncated_producer_fails_without_hanging(self):
        first = self.feed("valid", [self.envelope(3)])
        broken = self.feed("truncated", [self.envelope(8)])
        broken.write_bytes(broken.read_bytes()[:-8])
        result = self.build_parallel_in_subprocess([first, broken], self.root / "parallel-store")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn(str(broken), result.stderr)

    def test_parallel_stage_duplicate_ids_fail_and_cancel_peers(self):
        first = self.feed("first", [self.envelope(8)])
        second = self.feed("second", [self.envelope(8, name="Conflicting")])
        result = self.build_parallel_in_subprocess([first, second], self.root / "parallel-store")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("duplicate organization identifier", result.stderr)

    def test_producer_record_blocks_are_bounded_and_preserve_every_row(self):
        path = self.feed("many", [self.envelope(identifier) for identifier in range(600)])
        blocks = list(_compressed_blocks([path]))
        self.assertGreater(len(blocks), 1)
        line_numbers = []
        for source, block in blocks:
            self.assertEqual(source, str(path))
            self.assertLessEqual(len(block), 256)
            self.assertLessEqual(sum(len(record[2]) for record in block), 1024 ** 2)
            line_numbers.extend(record[0] for record in block)
        self.assertEqual(line_numbers, list(range(1, 601)))

    def test_producer_byte_limit_splits_large_compressed_blocks(self):
        generator = random.Random(8)
        records = [self.envelope(identifier, details=generator.randbytes(8192).hex())
                   for identifier in range(200)]
        path = self.feed("large-blocks", records)
        blocks = list(_compressed_blocks([path]))
        self.assertGreater(len(blocks), 1)
        self.assertEqual(sum(len(block) for _source, block in blocks), 200)
        for _source, block in blocks:
            self.assertLessEqual(len(block), 256)
            self.assertLessEqual(sum(len(record[2]) for record in block), 1024 ** 2)


if __name__ == "__main__":
    unittest.main()
