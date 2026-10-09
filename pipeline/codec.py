"""Per-process organization codecs with bounded, fresh runtime dictionaries.

Only staging owns codec metadata. Read workers reopen immutable metadata and
validate dictionary bytes before constructing their private native contexts.
No source rows, compression samples or dictionaries survive staging cleanup.
"""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import time

import orjson
from isal import isal_zlib


MAX_RECORD_BYTES = 16 * 1024 * 1024
SAMPLE_MAX_BYTES = 4 * 1024 * 1024
SAMPLE_RECORDS_PER_FILE = 128
SAMPLE_STRIDE = 16
SAMPLE_RECORD_MAX_BYTES = 64 * 1024
DICTIONARY_SIZES = (0, 32768, 65536)


def _sample_organizations(paths: list[Path]) -> tuple[list[bytes], int]:
    # Delay this import: store.py calls prepare_codec after its definitions exist.
    if __package__:
        from .store import iter_records
    else:
        from store import iter_records
    samples = []
    total_bytes = 0
    if not paths:
        return samples, total_bytes
    file_budget = SAMPLE_MAX_BYTES // len(paths)
    for path in sorted(paths):
        file_bytes = 0
        selected = 0
        stream = iter_records(path)
        try:
            for line_number, envelope in stream:
                if line_number > SAMPLE_RECORDS_PER_FILE * SAMPLE_STRIDE:
                    break
                if (line_number - 1) % SAMPLE_STRIDE:
                    continue
                raw = orjson.dumps(envelope["serialized_data"])
                if len(raw) > SAMPLE_RECORD_MAX_BYTES or file_bytes + len(raw) > file_budget:
                    continue
                samples.append(raw)
                file_bytes += len(raw)
                total_bytes += len(raw)
                selected += 1
                if selected == SAMPLE_RECORDS_PER_FILE:
                    break
        finally:
            stream.close()
    return samples, total_bytes


def prepare_codec(paths: list[Path], destination: Path) -> dict:
    """Write fresh runtime metadata; include sample and training cost in staging."""
    started = time.perf_counter()
    name = os.environ.get("ORG_CODEC", "isal")
    try:
        dictionary_size = int(os.environ.get("ORG_ZSTD_DICT_BYTES", "0"))
    except ValueError:
        raise ValueError("ORG_ZSTD_DICT_BYTES must be 0, 32768 or 65536") from None
    if name not in ("isal", "zstd"):
        raise ValueError("ORG_CODEC must be isal or zstd")
    if dictionary_size not in DICTIONARY_SIZES:
        raise ValueError("ORG_ZSTD_DICT_BYTES must be 0, 32768 or 65536")
    if name == "isal" and dictionary_size:
        raise ValueError("ISA-L cannot use a Zstd dictionary")
    destination = Path(destination)
    if not destination.is_dir():
        raise ValueError("codec destination must be a fresh staging directory")
    if (destination / "codec.json").exists() or (destination / "dictionary.bin").exists():
        raise ValueError("codec metadata already exists")
    metadata = {"version": 1, "codec": name, "level": 1,
                "dictionary_requested_bytes": dictionary_size,
                "dictionary_bytes": 0, "dictionary_sha256": None}
    sample_records = sample_bytes = 0
    sampling_seconds = training_seconds = 0.0
    fallback_reason = None
    if name == "zstd":
        import zstandard
        if dictionary_size:
            sampled_at = time.perf_counter()
            samples, sample_bytes = _sample_organizations(paths)
            sample_records = len(samples)
            sampling_seconds = time.perf_counter() - sampled_at
            dictionary = None
            if sample_records < 64 or sample_bytes < dictionary_size * 8:
                fallback_reason = "insufficient_dictionary_training_samples"
            else:
                trained_at = time.perf_counter()
                try:
                    dictionary = zstandard.train_dictionary(dictionary_size, samples, dict_id=dictionary_size)
                except zstandard.ZstdError:
                    fallback_reason = "dictionary_training_unavailable"
                training_seconds = time.perf_counter() - trained_at
            del samples
            if dictionary is not None:
                raw_dictionary = dictionary.as_bytes()
                metadata["dictionary_bytes"] = len(raw_dictionary)
                metadata["dictionary_sha256"] = hashlib.sha256(raw_dictionary).hexdigest()
                with (destination / "dictionary.bin").open("xb") as output:
                    output.write(raw_dictionary)
    with (destination / "codec.json").open("x", encoding="utf-8") as output:
        json.dump(metadata, output, sort_keys=True)
    return {
        "organization_codec": name,
        "organization_codec_level": 1,
        "organization_dictionary_requested_bytes": dictionary_size,
        "organization_dictionary_bytes": metadata["dictionary_bytes"],
        "organization_dictionary_sha256": metadata["dictionary_sha256"],
        "organization_dictionary_fallback_reason": fallback_reason,
        "organization_dictionary_sample_records": sample_records,
        "organization_dictionary_sample_bytes": sample_bytes,
        "organization_dictionary_sampling_seconds": sampling_seconds,
        "organization_dictionary_training_seconds": training_seconds,
        "organization_codec_prepare_seconds": time.perf_counter() - started,
    }


class OrganizationCodec:
    """One native compressor/decompressor pair, private to its owning process."""

    def __init__(self, name: str, dictionary: bytes | None = None):
        if name not in ("isal", "zstd") or (name == "isal" and dictionary):
            raise ValueError("invalid organization codec")
        if dictionary and len(dictionary) > 65536:
            raise ValueError("organization dictionary exceeds 64 KiB")
        self.name = name
        self.dictionary_bytes = len(dictionary) if dictionary else 0
        if name == "zstd":
            import zstandard
            try:
                data = zstandard.ZstdCompressionDict(
                    dictionary, dict_type=zstandard.DICT_TYPE_FULLDICT) if dictionary else None
                self._compressor = zstandard.ZstdCompressor(level=1, dict_data=data, threads=0)
                self._decompressor = zstandard.ZstdDecompressor(dict_data=data)
            except zstandard.ZstdError:
                raise ValueError("invalid organization dictionary") from None

    def compress(self, raw: bytes) -> bytes:
        if len(raw) > MAX_RECORD_BYTES:
            raise ValueError("organization JSON exceeds 16 MiB")
        if self.name == "isal":
            return isal_zlib.compress(raw, level=1)
        return self._compressor.compress(raw)

    def decompress(self, compressed: bytes | memoryview) -> bytes:
        if self.name == "isal":
            try:
                raw = isal_zlib.decompress(compressed)
            except isal_zlib.error:
                raise ValueError("invalid compressed organization record") from None
            # Preserve the existing native ISA-L fast path. Raw feed validation
            # bounds produced values; this check detects an invalid stage value.
            if len(raw) > MAX_RECORD_BYTES:
                raise ValueError("organization JSON exceeds 16 MiB")
            return raw
        import zstandard
        try:
            size = zstandard.frame_content_size(compressed)
            if size < 0 or size > MAX_RECORD_BYTES:
                raise ValueError("invalid organization decompression size")
            # Declared size is checked before allocation; generated frames always
            # include it. Disallow trailing data, so corruption cannot hide there.
            raw = self._decompressor.decompress(compressed, max_output_size=MAX_RECORD_BYTES,
                                                allow_extra_data=False)
        except zstandard.ZstdError:
            raise ValueError("invalid compressed organization record") from None
        if len(raw) > MAX_RECORD_BYTES:
            raise ValueError("organization JSON exceeds 16 MiB")
        return raw


def open_codec(destination: Path) -> OrganizationCodec:
    """Validate immutable metadata and dictionary; legacy stores use ISA-L."""
    destination = Path(destination)
    metadata_path = destination / "codec.json"
    dictionary_path = destination / "dictionary.bin"
    if not metadata_path.exists():
        if dictionary_path.exists():
            raise ValueError("organization codec metadata is missing")
        return OrganizationCodec("isal")
    try:
        with metadata_path.open("rb") as source:
            encoded = source.read(4097)
        if len(encoded) > 4096:
            raise ValueError("invalid organization codec metadata")
        metadata = json.loads(encoded)
    except (OSError, json.JSONDecodeError, UnicodeError):
        raise ValueError("invalid organization codec metadata") from None
    if (not isinstance(metadata, dict) or type(metadata.get("version")) is not int
            or metadata["version"] != 1 or metadata.get("codec") not in ("isal", "zstd")
            or type(metadata.get("level")) is not int or metadata["level"] != 1
            or type(metadata.get("dictionary_bytes")) is not int
            or not 0 <= metadata["dictionary_bytes"] <= 65536):
        raise ValueError("invalid organization codec metadata")
    size = metadata["dictionary_bytes"]
    requested = metadata.get("dictionary_requested_bytes")
    if "dictionary_requested_bytes" in metadata:
        if (type(requested) is not int or requested not in DICTIONARY_SIZES
                or size > requested or (metadata["codec"] == "isal" and requested)):
            raise ValueError("invalid organization codec metadata")
    if metadata["codec"] == "isal" and size:
        raise ValueError("invalid organization codec metadata")
    dictionary = None
    if size:
        try:
            with dictionary_path.open("rb") as source:
                dictionary = source.read(65537)
        except OSError:
            raise ValueError("organization dictionary is missing or unreadable") from None
        if len(dictionary) != size or hashlib.sha256(dictionary).hexdigest() != metadata.get("dictionary_sha256"):
            raise ValueError("organization dictionary integrity check failed")
    elif dictionary_path.exists() or metadata.get("dictionary_sha256") is not None:
        raise ValueError("unexpected organization dictionary")
    return OrganizationCodec(metadata["codec"], dictionary)
