"""Validate bounded sample selection and independent codec roundtrip checks."""

import gzip
import json
from pathlib import Path
import tempfile
import unittest
import zlib

from bench.profile_compression import (
    RETAINED_PAYLOAD_MAX_BYTES, collect_sample, measure_codec, payload_digest,
)


class CompressionProbeTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)

    def feed(self, name, start, records=16, padding=0):
        path = self.root / f"{name}.json.gz"
        with gzip.open(path, "wt", encoding="utf-8") as output:
            for identifier in range(start, start + records):
                output.write(json.dumps({
                    "id": identifier,
                    "serialized_data": {"forager_id": identifier, "name": "fixture", "padding": "a" * padding},
                }) + "\n")
        return path

    def test_selection_is_deterministic_disjoint_and_includes_every_file(self):
        paths = [self.feed("second", 100), self.feed("first", 0)]
        train, heldout, metadata = collect_sample(paths, records=16, stride=2)
        train_again, heldout_again, metadata_again = collect_sample(list(reversed(paths)), records=16, stride=2)
        self.assertEqual((train, heldout, metadata), (train_again, heldout_again, metadata_again))
        self.assertEqual(metadata["per_file_selected_counts"], [8, 8])
        self.assertEqual((len(train), len(heldout)), (4, 12))
        self.assertFalse(set(train) & set(heldout))
        identifiers = {json.loads(value)["forager_id"] for value in train + heldout}
        self.assertEqual(identifiers, set(range(0, 16, 2)) | set(range(100, 116, 2)))

    def test_sample_payload_remains_within_byte_budget(self):
        paths = [self.feed("one", 0, padding=100), self.feed("two", 100, padding=100)]
        train, heldout, metadata = collect_sample(paths, records=16, stride=1, sample_bytes=1024)
        self.assertLessEqual(sum(map(len, train + heldout)), 1024)
        self.assertGreater(metadata["omitted_large_or_over_budget_rows"], 0)

    def test_invalid_limits_are_rejected(self):
        path = self.feed("one", 0)
        for kwargs in ({"records": 10000}, {"stride": 0}, {"sample_bytes": 64 * 1024 * 1024}):
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                collect_sample([path], **kwargs)

    def test_full_roundtrip_digest_and_byte_count(self):
        values = [b'{"forager_id":1,"name":"fixture"}', b'{"forager_id":2,"extra":true}']
        measured = measure_codec(values, zlib.compress, zlib.decompress, sum(map(len, values)))
        self.assertEqual(measured["roundtrip_sha256"], payload_digest(values))
        self.assertEqual(measured["decompressed_bytes"], sum(map(len, values)))
        self.assertTrue(measured["all_records_equal"])
        self.assertGreater(measured["compressed_bytes"], 0)

    def test_roundtrip_mismatch_fails_without_echoing_content(self):
        with self.assertRaises(ValueError) as error:
            measure_codec([b"private-fixture-marker"], lambda value: value, lambda value: b"changed", 64)
        self.assertNotIn("private-fixture-marker", str(error.exception))

    def test_retained_compressed_payload_is_bounded(self):
        with self.assertRaisesRegex(ValueError, "64 MiB"):
            measure_codec([b"one"], lambda value: value, lambda value: value, RETAINED_PAYLOAD_MAX_BYTES)

    def test_digest_includes_record_boundaries(self):
        self.assertNotEqual(payload_digest([b"ab", b"c"]), payload_digest([b"a", b"bc"]))


if __name__ == "__main__":
    unittest.main()
