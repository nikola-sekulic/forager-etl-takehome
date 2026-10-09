"""Exercise runtime codec bounds, immutable metadata and dictionary integrity."""

import gzip
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from isal import isal_zlib
from pipeline.codec import OrganizationCodec, _sample_organizations, open_codec, prepare_codec


HAS_ZSTD = importlib.util.find_spec("zstandard") is not None


class OrganizationCodecTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.stage = self.root / "stage"
        self.stage.mkdir()
        environment = patch.dict(os.environ, {"ORG_CODEC": "isal", "ORG_ZSTD_DICT_BYTES": "0"})
        environment.start()
        self.addCleanup(environment.stop)

    def feed(self, name, start=0, records=32, padding=0):
        path = self.root / f"{name}.json.gz"
        with gzip.open(path, "wt", encoding="utf-8") as output:
            for identifier in range(start, start + records):
                body = {
                    "forager_id": identifier, "name": f"fixture {identifier}",
                    "description": "fixture information " * padding,
                    "history": [{"period": value, "label": f"fixture {identifier + value}"}
                                for value in range(20)],
                }
                output.write(json.dumps({"id": identifier, "serialized_data": body}) + "\n")
        return path

    def metadata(self, **overrides):
        metadata = {"version": 1, "codec": "isal", "level": 1,
                    "dictionary_bytes": 0, "dictionary_sha256": None}
        metadata.update(overrides)
        (self.stage / "codec.json").write_text(json.dumps(metadata), encoding="utf-8")

    def test_legacy_isa_store_and_borrowed_buffer_roundtrip(self):
        codec = open_codec(self.stage)
        raw = b'{"forager_id":1,"name":"fixture"}'
        frame = codec.compress(raw)
        self.assertEqual(codec.name, "isal")
        self.assertEqual(codec.decompress(memoryview(frame)), raw)
        self.assertEqual(isal_zlib.decompress(frame), raw)

    def test_fresh_isa_metadata_roundtrip_and_no_dictionary(self):
        metrics = prepare_codec([], self.stage)
        self.assertEqual(metrics["organization_codec"], "isal")
        self.assertEqual(metrics["organization_dictionary_sample_records"], 0)
        self.assertFalse((self.stage / "dictionary.bin").exists())
        self.assertEqual(open_codec(self.stage).decompress(open_codec(self.stage).compress(b"{}")), b"{}")
        with self.assertRaisesRegex(ValueError, "already exists"):
            prepare_codec([], self.stage)

    def test_invalid_configuration_fails(self):
        for settings in ({"ORG_CODEC": "unknown"}, {"ORG_ZSTD_DICT_BYTES": "large"},
                         {"ORG_ZSTD_DICT_BYTES": "1"}, {"ORG_ZSTD_DICT_BYTES": "32768"}):
            with self.subTest(settings=settings), patch.dict(os.environ, settings), self.assertRaises(ValueError):
                prepare_codec([], self.stage)

    def test_metadata_unknown_missing_fields_and_oversize_fail(self):
        for metadata in ([], {}, {"version": True}, {"codec": "other"}, {"level": True}):
            (self.stage / "codec.json").write_text(json.dumps(metadata), encoding="utf-8")
            with self.subTest(metadata=metadata), self.assertRaises(ValueError):
                open_codec(self.stage)
        (self.stage / "codec.json").write_bytes(b" " * 4097)
        with self.assertRaises(ValueError):
            open_codec(self.stage)

    def test_missing_metadata_with_dictionary_fails(self):
        (self.stage / "dictionary.bin").write_bytes(b"fixture")
        with self.assertRaisesRegex(ValueError, "metadata is missing"):
            open_codec(self.stage)

    def test_missing_or_corrupt_dictionary_fails_before_native_context(self):
        self.metadata(codec="zstd", dictionary_bytes=32768,
                      dictionary_sha256=hashlib.sha256(b"fixture").hexdigest())
        with self.assertRaisesRegex(ValueError, "missing or unreadable"):
            open_codec(self.stage)
        (self.stage / "dictionary.bin").write_bytes(b"corrupt fixture")
        with self.assertRaisesRegex(ValueError, "integrity"):
            open_codec(self.stage)

    def test_unexpected_dictionary_fails(self):
        self.metadata()
        (self.stage / "dictionary.bin").write_bytes(b"fixture")
        with self.assertRaisesRegex(ValueError, "unexpected"):
            open_codec(self.stage)

    def test_isa_corruption_and_truncation_fail(self):
        codec = OrganizationCodec("isal")
        frame = codec.compress(b"private-fixture-marker")
        for corrupted in (b"invalid", frame[:-1]):
            with self.subTest(size=len(corrupted)), self.assertRaises(ValueError) as raised:
                codec.decompress(corrupted)
            self.assertNotIn("private-fixture-marker", str(raised.exception))

    def test_codec_input_and_expansion_limits(self):
        codec = OrganizationCodec("isal")
        oversized_frame = isal_zlib.compress(b"a" * 17)
        with patch("pipeline.codec.MAX_RECORD_BYTES", 16):
            with self.assertRaises(ValueError):
                codec.compress(b"a" * 17)
            with self.assertRaises(ValueError):
                codec.decompress(oversized_frame)
            self.assertEqual(codec.decompress(codec.compress(b"a" * 16)), b"a" * 16)

    def test_dictionary_sample_byte_and_scan_bounds(self):
        paths = [self.feed("one", 0), self.feed("two", 100)]
        with patch("pipeline.codec.SAMPLE_MAX_BYTES", 4096), patch("pipeline.codec.SAMPLE_RECORDS_PER_FILE", 8), \
                patch("pipeline.codec.SAMPLE_STRIDE", 2):
            samples, size = _sample_organizations(paths)
        self.assertLessEqual(size, 4096)
        self.assertEqual(size, sum(map(len, samples)))
        identifiers = [json.loads(value)["forager_id"] for value in samples]
        self.assertTrue(any(identifier < 100 for identifier in identifiers))
        self.assertTrue(any(identifier >= 100 for identifier in identifiers))
        self.assertTrue(all(identifier % 2 == 0 for identifier in identifiers))

    @unittest.skipUnless(HAS_ZSTD, "optional profiling/production Zstd dependency unavailable")
    def test_zstd_without_dictionary_and_strict_decode(self):
        with patch.dict(os.environ, {"ORG_CODEC": "zstd"}):
            metrics = prepare_codec([], self.stage)
        self.assertEqual(metrics["organization_dictionary_bytes"], 0)
        codec = open_codec(self.stage)
        raw = b"fixture " * 128
        frame = codec.compress(raw)
        self.assertEqual(codec.decompress(memoryview(frame)), raw)
        for corrupted in (b"invalid", frame[:-1], frame + b"trailing"):
            with self.subTest(size=len(corrupted)), self.assertRaises(ValueError):
                codec.decompress(corrupted)
        with patch("pipeline.codec.MAX_RECORD_BYTES", 16), self.assertRaises(ValueError):
            codec.decompress(frame)

    @unittest.skipUnless(HAS_ZSTD, "optional profiling/production Zstd dependency unavailable")
    def test_tiny_valid_feeds_fall_back_to_plain_zstd(self):
        with patch.dict(os.environ, {"ORG_CODEC": "zstd", "ORG_ZSTD_DICT_BYTES": "32768"}):
            metrics = prepare_codec([self.feed("tiny", records=1)], self.stage)
        self.assertEqual(metrics["organization_dictionary_requested_bytes"], 32768)
        self.assertEqual(metrics["organization_dictionary_bytes"], 0)
        self.assertEqual(metrics["organization_dictionary_fallback_reason"],
                         "insufficient_dictionary_training_samples")
        self.assertFalse((self.stage / "dictionary.bin").exists())
        codec = open_codec(self.stage)
        self.assertEqual(codec.decompress(codec.compress(b"{}")), b"{}")

    @unittest.skipUnless(HAS_ZSTD, "optional profiling/production Zstd dependency unavailable")
    def test_dictionary_fallback_does_not_swallow_malformed_source(self):
        path = self.root / "malformed.json.gz"
        with gzip.open(path, "wb") as output:
            output.write(b'{"private-fixture-marker"\n')
        with patch.dict(os.environ, {"ORG_CODEC": "zstd", "ORG_ZSTD_DICT_BYTES": "32768"}), \
                self.assertRaises(ValueError) as raised:
            prepare_codec([path], self.stage)
        self.assertNotIn("private-fixture-marker", str(raised.exception))
        self.assertFalse((self.stage / "codec.json").exists())

    @unittest.skipUnless(HAS_ZSTD, "optional profiling/production Zstd dependency unavailable")
    def test_runtime_trained_dictionaries_roundtrip_and_hash_check(self):
        paths = [self.feed("one", 0, records=128, padding=200),
                 self.feed("two", 1000, records=128, padding=200)]
        for size in (32768, 65536):
            stage = self.root / f"stage-{size}"
            stage.mkdir()
            with patch.dict(os.environ, {"ORG_CODEC": "zstd", "ORG_ZSTD_DICT_BYTES": str(size)}), \
                    patch("pipeline.codec.SAMPLE_STRIDE", 1):
                metrics = prepare_codec(paths, stage)
            self.assertEqual(metrics["organization_dictionary_sample_records"], 256)
            self.assertLessEqual(metrics["organization_dictionary_sample_bytes"], 4 * 1024 * 1024)
            self.assertLessEqual(metrics["organization_dictionary_bytes"], size)
            codec = open_codec(stage)
            raw = b'{"forager_id":9000,"name":"held-out fixture"}'
            self.assertEqual(codec.decompress(codec.compress(raw)), raw)
            dictionary = stage / "dictionary.bin"
            altered = bytearray(dictionary.read_bytes())
            altered[-1] ^= 1
            dictionary.write_bytes(altered)
            with self.assertRaisesRegex(ValueError, "integrity"):
                open_codec(stage)


if __name__ == "__main__":
    unittest.main()
