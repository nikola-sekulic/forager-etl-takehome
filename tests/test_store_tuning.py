"""Exercise LMDB tuning against real transactions and owned read values."""

import gzip
import importlib.util
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import lmdb
import orjson
from isal import isal_zlib

from pipeline.store import OrganizationStore, build_store, enrich_person_json


HAS_ZSTD = importlib.util.find_spec("zstandard") is not None


class StoreTuningTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        settings = patch.dict(os.environ, {
            "STAGE_WORKERS": "1", "ORG_WRITE_RECORDS": "1000",
            "ORG_WRITE_BYTES": str(64 * 1024**2),
            "ORG_PUTMULTI": "0", "ORG_READ_BUFFERS": "0",
            "ORG_CODEC": "isal", "ORG_ZSTD_DICT_BYTES": "0",
        })
        settings.start()
        self.addCleanup(settings.stop)
        allocator = patch("pipeline.store.STORE_MAP_BYTES", 8 * 1024**2)
        allocator.start()
        self.addCleanup(allocator.stop)

    @staticmethod
    def envelope(identifier, **fields):
        return {"id": identifier, "serialized_data": {
            "forager_id": identifier, "name": "Organization", **fields}}

    def feed(self, name, records):
        path = self.root / (name + ".json.gz")
        with gzip.open(path, "wt", encoding="utf-8") as stream:
            for envelope in records:
                stream.write(json.dumps(envelope) + "\n")
        return path

    @staticmethod
    def committed_ids(destination):
        # A failed build is never used by ingestion. Inspect its committed
        # prefix here to verify real transaction boundaries and abort behavior.
        environment = lmdb.open(str(destination), readonly=True, lock=False,
                                create=False, readahead=False)
        try:
            with environment.begin() as transaction:
                return {int(key) for key, _value in transaction.cursor()}
        finally:
            environment.close()

    def test_putmulti_preserves_unsorted_keys_and_full_bodies_across_transactions(self):
        rows = [self.envelope(identifier, arbitrary={"keep": [identifier, "é"]})
                for identifier in (100, 2, 9, 11, 1, 200)]
        feed = self.feed("unordered", rows)
        for putmulti in ("0", "1"):
            with self.subTest(putmulti=putmulti), patch.dict(os.environ, {
                    "ORG_PUTMULTI": putmulti, "ORG_WRITE_RECORDS": "2"}):
                destination = self.root / ("store-" + putmulti)
                metrics = build_store([feed], destination)
                self.assertEqual(metrics["organizations"], len(rows))
                self.assertEqual(metrics["organization_write_records"], 2)
                self.assertEqual(metrics["organization_write_bytes"], 64 * 1024**2)
                self.assertEqual(metrics["organization_putmulti"], putmulti == "1")
                with OrganizationStore(destination, cache_bytes=0) as store:
                    for row in rows:
                        self.assertEqual(orjson.loads(store.get(row["id"])), row["serialized_data"])
                    self.assertIsNone(store.get(999))

    def test_putmulti_duplicate_within_block_aborts_partial_insertions(self):
        marker = "sensitive-fixture-value"
        rows = [self.envelope(1), self.envelope(2), self.envelope(1, secret=marker)]
        feed = self.feed("duplicate", rows)
        destination = self.root / "duplicates"
        with patch.dict(os.environ, {"ORG_PUTMULTI": "1"}), self.assertRaises(ValueError) as raised:
            build_store([feed], destination)
        self.assertIn(f"{feed}:3:", str(raised.exception))
        self.assertIn("duplicate organization identifier", str(raised.exception))
        self.assertNotIn(marker, str(raised.exception))
        self.assertEqual(self.committed_ids(destination), set())

    def test_putmulti_duplicate_across_producer_blocks_aborts_open_transaction(self):
        feed = self.feed("blocks", [self.envelope(1), self.envelope(2), self.envelope(1)])
        destination = self.root / "blocks"
        with patch.dict(os.environ, {"ORG_PUTMULTI": "1"}), \
                patch("pipeline.store.STAGE_BLOCK_RECORDS", 2), self.assertRaises(ValueError) as raised:
            build_store([feed], destination)
        self.assertIn(f"{feed}:3:", str(raised.exception))
        self.assertEqual(self.committed_ids(destination), set())

    def test_duplicate_after_record_commit_fails_and_retains_only_committed_prefix(self):
        first = self.feed("first", [self.envelope(1), self.envelope(2)])
        conflicting = self.feed("second", [self.envelope(1)])
        for putmulti in ("0", "1"):
            with self.subTest(putmulti=putmulti), patch.dict(os.environ, {
                    "ORG_PUTMULTI": putmulti, "ORG_WRITE_RECORDS": "2"}):
                destination = self.root / ("record-boundary-" + putmulti)
                with self.assertRaises(ValueError) as raised:
                    build_store([first, conflicting], destination)
                self.assertIn(f"{conflicting}:1:", str(raised.exception))
                self.assertEqual(self.committed_ids(destination), {1, 2})

    def test_byte_boundary_commits_before_next_record_and_duplicate_still_fails(self):
        rows = [self.envelope(1, arbitrary="a" * 1024), self.envelope(2, arbitrary="b" * 1024)]
        sizes = [len(isal_zlib.compress(orjson.dumps(row["serialized_data"]), level=1)) for row in rows]
        # Either record fits independently, but no pair fits in one transaction.
        budget = max(sizes)
        feed = self.feed("bytes", rows + [rows[0]])
        for putmulti in ("0", "1"):
            with self.subTest(putmulti=putmulti), patch.dict(os.environ, {
                    "ORG_PUTMULTI": putmulti, "ORG_WRITE_BYTES": str(budget)}):
                destination = self.root / ("byte-boundary-" + putmulti)
                with self.assertRaises(ValueError) as raised:
                    build_store([feed], destination)
                self.assertIn(f"{feed}:3:", str(raised.exception))
                self.assertEqual(self.committed_ids(destination), {1, 2})

    def test_compressed_record_larger_than_transaction_byte_budget_fails(self):
        row = self.envelope(1, arbitrary="kept")
        size = len(isal_zlib.compress(orjson.dumps(row["serialized_data"]), level=1))
        feed = self.feed("oversized", [row])
        for putmulti in ("0", "1"):
            with self.subTest(putmulti=putmulti), patch.dict(os.environ, {
                    "ORG_PUTMULTI": putmulti, "ORG_WRITE_BYTES": str(size - 1)}):
                destination = self.root / ("oversized-" + putmulti)
                with self.assertRaises(ValueError) as raised:
                    build_store([feed], destination)
                self.assertIn(f"{feed}:1:", str(raised.exception))
                self.assertEqual(self.committed_ids(destination), set())

    def test_invalid_writer_settings_fail_before_creating_stage(self):
        invalid = {
            "ORG_WRITE_RECORDS": ("0", "-1", "100001", "not-an-int", "1.5"),
            "ORG_WRITE_BYTES": ("0", "-1", str(256 * 1024**2 + 1), "not-an-int"),
            "ORG_PUTMULTI": ("2", "true", ""),
            "ORG_READ_BUFFERS": ("2", "true", ""),
        }
        for name, values in invalid.items():
            for number, value in enumerate(values):
                destination = self.root / f"invalid-{name}-{number}"
                with self.subTest(setting=name, value=value), patch.dict(os.environ, {name: value}):
                    with self.assertRaises(ValueError):
                        build_store([], destination)
                    self.assertFalse(destination.exists())

    def test_invalid_buffer_setting_rejects_reader_without_leaking_environment(self):
        feed = self.feed("reader", [self.envelope(1)])
        destination = self.root / "reader"
        build_store([feed], destination)
        with patch.dict(os.environ, {"ORG_READ_BUFFERS": "invalid"}), self.assertRaises(ValueError):
            OrganizationStore(destination)
        with OrganizationStore(destination) as store:
            self.assertIsInstance(store.get(1), bytes)

    def test_buffer_reader_returns_owned_values_with_and_without_cache(self):
        rows = [self.envelope(1, details={"complete": [1, 2, "kept"]}), self.envelope(2)]
        feed = self.feed("buffers", rows)
        destination = self.root / "buffers"
        build_store([feed], destination)
        for buffers in ("0", "1"):
            for cache_bytes in (0, 4096):
                with self.subTest(buffers=buffers, cache_bytes=cache_bytes), patch.dict(os.environ, {
                        "ORG_READ_BUFFERS": buffers}):
                    with OrganizationStore(destination, cache_bytes=cache_bytes) as store:
                        first = store.get(1)
                        self.assertIs(type(first), bytes)
                        self.assertEqual(orjson.loads(first), rows[0]["serialized_data"])
                        self.assertEqual(store.get(1), first)
                        self.assertEqual(orjson.loads(store.get(2)), rows[1]["serialized_data"])
                        self.assertIsNone(store.get(999))
                        self.assertLessEqual(store._cached_bytes, cache_bytes)
                        if cache_bytes:
                            self.assertIs(type(store._cache[1][0]), bytes)
                    # No returned value depends on a closed transaction or mmap.
                    self.assertEqual(orjson.loads(first), rows[0]["serialized_data"])
                    with self.assertRaises(RuntimeError):
                        store.get(1)

    def test_buffer_reader_has_identical_fragment_join_bytes_and_statistics(self):
        feed = self.feed("join", [self.envelope(1, arbitrary=[{"kept": True}])])
        destination = self.root / "join"
        build_store([feed], destination)
        person = {"id": 10, "serialized_data": {
            "forager_id": 10, "arbitrary": {"preserved": [1, 2]},
            "roles": [{"organization_id": 1}, {"organization_id": 999},
                      {"organization_id": 1}, {"organization_id": None}],
        }}
        results = []
        for buffers in ("0", "1"):
            with patch.dict(os.environ, {"ORG_READ_BUFFERS": buffers}):
                with OrganizationStore(destination, cache_bytes=1024) as store:
                    results.append(enrich_person_json(person, store))
        self.assertEqual(results[0], results[1])
        result, statistics = results[1]
        self.assertEqual(len(orjson.loads(result)["roles"]), 4)
        self.assertEqual(statistics["resolved_org_refs"], 2)
        self.assertEqual(statistics["unresolved_org_refs"], 1)


@unittest.skipUnless(HAS_ZSTD, "Zstd runtime dependency unavailable locally")
class StoreCodecIntegrationTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        settings = patch.dict(os.environ, {
            "STAGE_WORKERS": "1", "ORG_WRITE_RECORDS": "1000",
            "ORG_WRITE_BYTES": str(64 * 1024**2),
            "ORG_PUTMULTI": "1", "ORG_READ_BUFFERS": "1",
            "ORG_CODEC": "isal", "ORG_ZSTD_DICT_BYTES": "0",
        })
        settings.start()
        self.addCleanup(settings.stop)
        allocator = patch("pipeline.store.STORE_MAP_BYTES", 8 * 1024**2)
        allocator.start()
        self.addCleanup(allocator.stop)

    def feed(self, name, rows):
        path = self.root / (name + ".json.gz")
        with gzip.open(path, "wt", encoding="utf-8") as stream:
            for row in rows:
                stream.write(json.dumps(row) + "\n")
        return path

    def training_feeds(self):
        rows = [{"id": identifier, "serialized_data": {
            "forager_id": identifier,
            "name": f"Synthetic organization {identifier}",
            "description": "Synthetic organization activity. " * 160,
            "history": [{"period": period, "label": f"fixture {identifier + period}"}
                        for period in range(20)],
            "arbitrary": {"retained": [identifier, "é", True]},
        }} for identifier in range(1, 257)]
        return [self.feed("one", rows[:128]), self.feed("two", rows[128:])], rows

    @staticmethod
    def person():
        return {"id": 1000, "serialized_data": {
            "forager_id": 1000, "arbitrary_person_field": {"kept": [1, 2]},
            "roles": [{"organization_id": 1}, {"organization_id": 256},
                      {"organization_id": 1}, {"organization_id": 999},
                      {"organization_id": None}, {"role_title": "Preserved"}],
        }}

    def build_dictionary_store(self, paths, destination, workers="1"):
        # Dense sampling keeps this fixture small while retaining real dictionary
        # training and the production stage/child/reader implementations.
        with patch.dict(os.environ, {"ORG_CODEC": "zstd", "ORG_ZSTD_DICT_BYTES": "32768",
                                     "STAGE_WORKERS": workers}), \
                patch("pipeline.codec.SAMPLE_STRIDE", 1):
            metrics = build_store(paths, destination)
        self.assertGreater(metrics["organization_dictionary_bytes"], 0)
        self.assertLessEqual(metrics["organization_dictionary_bytes"], 32768)
        self.assertIsNone(metrics["organization_dictionary_fallback_reason"])
        self.assertEqual(metrics["organization_codec"], "zstd")
        return metrics

    def test_dictionary_store_matches_isa_full_join_with_owned_buffer_values(self):
        paths, rows = self.training_feeds()
        isa_stage, zstd_stage = self.root / "isa", self.root / "zstd"
        build_store(paths, isa_stage)
        metrics = self.build_dictionary_store(paths, zstd_stage)
        self.assertEqual(metrics["organizations"], len(rows))
        expected = None
        for stage in (isa_stage, zstd_stage):
            for buffers in ("0", "1"):
                for cache_bytes in (0, 16 * 1024):
                    with self.subTest(stage=stage.name, buffers=buffers, cache_bytes=cache_bytes), \
                            patch.dict(os.environ, {"ORG_READ_BUFFERS": buffers}):
                        with OrganizationStore(stage, cache_bytes=cache_bytes) as store:
                            actual = enrich_person_json(self.person(), store)
                            owned = store.get(1)
                            self.assertIs(type(owned), bytes)
                            self.assertEqual(orjson.loads(owned), rows[0]["serialized_data"])
                            self.assertIsNone(store.get(999))
                            self.assertLessEqual(store._cached_bytes, cache_bytes)
                            if cache_bytes:
                                self.assertIs(type(store._cache[1][0]), bytes)
                        self.assertEqual(orjson.loads(owned), rows[0]["serialized_data"])
                        if expected is None:
                            expected = actual
                        self.assertEqual(actual, expected)
        body, statistics = expected
        self.assertEqual(len(orjson.loads(body)["roles"]), 6)
        self.assertEqual(statistics["resolved_org_refs"], 3)
        self.assertEqual(statistics["unresolved_org_refs"], 1)

    def test_tiny_feed_dictionary_fallback_is_readable_and_preserves_full_join(self):
        row = {"id": 1, "serialized_data": {
            "forager_id": 1, "name": "Synthetic organization", "arbitrary": [1, 2]}}
        path = self.feed("tiny", [row])
        isa_stage, zstd_stage = self.root / "tiny-isa", self.root / "tiny-zstd"
        build_store([path], isa_stage)
        with patch.dict(os.environ, {"ORG_CODEC": "zstd", "ORG_ZSTD_DICT_BYTES": "32768"}):
            metrics = build_store([path], zstd_stage)
        self.assertEqual(metrics["organization_dictionary_bytes"], 0)
        self.assertEqual(metrics["organization_dictionary_fallback_reason"],
                         "insufficient_dictionary_training_samples")
        self.assertFalse((zstd_stage / "dictionary.bin").exists())
        results = []
        for stage in (isa_stage, zstd_stage):
            with OrganizationStore(stage, cache_bytes=0) as store:
                self.assertEqual(orjson.loads(store.get(1)), row["serialized_data"])
                results.append(enrich_person_json(self.person(), store))
        self.assertEqual(results[0], results[1])

    def test_missing_or_corrupt_dictionary_fails_reader_and_restoration_reopens_cleanly(self):
        paths, rows = self.training_feeds()
        stage = self.root / "integrity"
        self.build_dictionary_store(paths, stage)
        dictionary = stage / "dictionary.bin"
        original = dictionary.read_bytes()
        dictionary.unlink()
        with self.assertRaisesRegex(ValueError, "missing or unreadable"):
            OrganizationStore(stage)
        altered = bytearray(original)
        altered[-1] ^= 1
        dictionary.write_bytes(altered)
        with self.assertRaisesRegex(ValueError, "integrity"):
            OrganizationStore(stage)
        dictionary.write_bytes(original)
        with OrganizationStore(stage) as store:
            self.assertEqual(orjson.loads(store.get(1)), rows[0]["serialized_data"])

    def test_spawned_stage_workers_reopen_dictionary_and_preserve_every_organization(self):
        paths, rows = self.training_feeds()
        stage = self.root / "spawned"
        metrics = self.build_dictionary_store(paths, stage, workers="2")
        self.assertEqual(metrics["organization_stage_workers"], 2)
        self.assertEqual(metrics["organizations"], len(rows))
        with OrganizationStore(stage, cache_bytes=0) as store:
            for row in rows:
                self.assertEqual(orjson.loads(store.get(row["id"])), row["serialized_data"])
            actual = enrich_person_json(self.person(), store)
        isa_stage = self.root / "spawned-isa-control"
        build_store(paths, isa_stage)
        with OrganizationStore(isa_stage, cache_bytes=0) as store:
            self.assertEqual(actual, enrich_person_json(self.person(), store))


if __name__ == "__main__":
    unittest.main()
