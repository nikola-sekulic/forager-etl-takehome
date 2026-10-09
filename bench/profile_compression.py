#!/usr/bin/env python3
"""Bounded codec probe; aggregates only, never ingestion input or saved source data.

The optional zstandard dependency is deliberately absent from production
requirements. Install zstandard==0.25.0 only in an ephemeral profiling container.
Run from /app: python bench/profile_compression.py --data-dir /data

Sources for the binding API:
https://python-zstandard.readthedocs.io/en/latest/dictionaries.html
https://python-zstandard.readthedocs.io/en/latest/compressor.html
https://python-zstandard.readthedocs.io/en/latest/decompressor.html
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import importlib.metadata
import json
from pathlib import Path
import platform
import statistics
import sys
import time
from typing import Callable

import orjson
from isal import isal_zlib

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "pipeline"))
from store import iter_records


SAMPLE_MAX_BYTES = 16 * 1024 * 1024
RETAINED_PAYLOAD_MAX_BYTES = 64 * 1024 * 1024


def payload_digest(values: list[bytes]) -> str:
    """Length-prefix entries so boundaries contribute to the aggregate digest."""
    digest = hashlib.sha256()
    for value in values:
        digest.update(len(value).to_bytes(8, "big"))
        digest.update(value)
    return digest.hexdigest()


def collect_sample(
    paths: list[Path], records: int = 4096, stride: int = 97,
    sample_bytes: int = SAMPLE_MAX_BYTES,
) -> tuple[list[bytes], list[bytes], dict]:
    """Select strided prefixes per file, not a claim of statistical randomness.

    Every fourth selected row trains dictionaries; all remaining rows are held
    out. Per-file budgets ensure every file can contribute without retaining
    arbitrary decoded bodies. Large rows are omitted from this small-row probe.
    """
    if not paths or len(paths) > records or not 8 <= records <= 8192 or not 1 <= stride <= 4096:
        raise ValueError("invalid bounded sample configuration")
    if not 1024 <= sample_bytes <= SAMPLE_MAX_BYTES:
        raise ValueError("sample byte budget must be between 1024 and 16 MiB")
    training, heldout = [], []
    seen, selected, omitted = 0, 0, 0
    per_file_counts = []
    per_file_records = max(1, records // len(paths))
    per_file_bytes = sample_bytes // len(paths)
    for path in sorted(paths):
        count, stored_bytes = 0, 0
        stream = iter_records(path)
        try:
            for line_number, envelope in stream:
                seen += 1
                if (line_number - 1) % stride:
                    continue
                raw = orjson.dumps(envelope["serialized_data"])
                if len(raw) > 64 * 1024 or stored_bytes + len(raw) > per_file_bytes:
                    omitted += 1
                    continue
                (training if count % 4 == 0 else heldout).append(raw)
                count += 1
                selected += 1
                stored_bytes += len(raw)
                if count == per_file_records:
                    break
        finally:
            stream.close()
        per_file_counts.append(count)
    if not training or not heldout:
        raise ValueError("not enough selected rows for training and held-out sets")
    metadata = {
        "selection": "sorted file order; every stride-th row; per-file count/byte caps",
        "stride": stride,
        "maximum_records": records,
        "sample_payload_byte_limit": sample_bytes,
        "maximum_selected_record_bytes": 64 * 1024,
        "files": len(paths),
        "scanned_records": seen,
        "selected_records": selected,
        "per_file_selected_counts": per_file_counts,
        "omitted_large_or_over_budget_rows": omitted,
        "training_records": len(training),
        "training_bytes": sum(map(len, training)),
        "heldout_records": len(heldout),
        "heldout_bytes": sum(map(len, heldout)),
        "training_sha256": payload_digest(training),
        "heldout_sha256": payload_digest(heldout),
    }
    return training, heldout, metadata


def measure_codec(
    raw: list[bytes], compress: Callable[[bytes], bytes],
    decompress: Callable[[bytes], bytes], sample_retained_bytes: int,
) -> dict:
    """Time native operations separately from full equality/hash validation."""
    if not raw:
        raise ValueError("codec measurement requires a nonempty held-out sample")
    frames = []
    compressed_bytes = 0
    started = time.perf_counter()
    for value in raw:
        frame = compress(value)
        compressed_bytes += len(frame)
        if sample_retained_bytes + compressed_bytes > RETAINED_PAYLOAD_MAX_BYTES:
            raise ValueError("codec payload exceeded the 64 MiB retained-payload ceiling")
        frames.append(frame)
    compression_seconds = time.perf_counter() - started

    # Discard each output immediately; no second uncompressed sample is retained.
    started = time.perf_counter()
    decompressed_bytes = 0
    for frame in frames:
        output = decompress(frame)
        decompressed_bytes += len(output)
    decompression_seconds = time.perf_counter() - started
    del output
    digest = hashlib.sha256()
    for expected, frame in zip(raw, frames, strict=True):
        actual = decompress(frame)
        if actual != expected:
            raise ValueError("codec roundtrip changed organization JSON bytes")
        digest.update(len(actual).to_bytes(8, "big"))
        digest.update(actual)
    return {
        "compression_seconds": compression_seconds,
        "decompression_seconds": decompression_seconds,
        "compressed_bytes": compressed_bytes,
        "decompressed_bytes": decompressed_bytes,
        "retained_payload_bytes": sample_retained_bytes + compressed_bytes,
        "roundtrip_sha256": digest.hexdigest(),
        "all_records_equal": True,
    }


def benchmark(training: list[bytes], heldout: list[bytes], passes: int = 5) -> dict:
    import zstandard

    if not 2 <= passes <= 20:
        raise ValueError("passes must be between 2 and 20")
    dictionaries = {}
    training_stats = {}
    for size in (32 * 1024, 64 * 1024):
        started = time.perf_counter()
        # Fixed ID removes the API's randomized default. Single-thread training.
        dictionary = zstandard.train_dictionary(size, training, dict_id=size)
        elapsed = time.perf_counter() - started
        dictionaries[size] = dictionary
        training_stats[str(size)] = {
            "training_seconds": elapsed, "dictionary_bytes": len(dictionary),
            "dictionary_sha256": hashlib.sha256(dictionary.as_bytes()).hexdigest(),
        }

    codecs = {"isal_deflate_1": {
        "compress": lambda value: isal_zlib.compress(value, level=1),
        "decompress": isal_zlib.decompress, "dictionary_bytes": 0,
    }}
    for level in (1, 3):
        for size, dictionary in [(0, None), *dictionaries.items()]:
            compressor = zstandard.ZstdCompressor(level=level, dict_data=dictionary)
            decompressor = zstandard.ZstdDecompressor(dict_data=dictionary)
            codecs[f"zstd_{level}_dict_{size}"] = {
                "compress": compressor.compress, "decompress": decompressor.decompress,
                "dictionary_bytes": size,
            }
    sample_bytes = sum(map(len, training)) + sum(map(len, heldout))
    trials = {name: [] for name in codecs}
    orders = []
    names = list(codecs)
    expected_digest = payload_digest(heldout)
    for iteration in range(passes):
        # Alternate direction and rotate to reduce systematic order bias.
        order = names[iteration % len(names):] + names[:iteration % len(names)]
        if iteration % 2:
            order.reverse()
        orders.append(order)
        for name in order:
            codec = codecs[name]
            trial = measure_codec(heldout, codec["compress"], codec["decompress"],
                                  sample_bytes + 96 * 1024)
            if trial["roundtrip_sha256"] != expected_digest:
                raise ValueError("codec aggregate roundtrip digest mismatch")
            trials[name].append(trial)
    results = {}
    for name, measurements in trials.items():
        dictionary_bytes = codecs[name]["dictionary_bytes"]
        results[name] = {
            "dictionary_bytes": dictionary_bytes,
            "training_seconds": (training_stats[str(dictionary_bytes)]["training_seconds"]
                                 if dictionary_bytes else 0),
            "median_compression_seconds": statistics.median(
                measurement["compression_seconds"] for measurement in measurements),
            "median_decompression_seconds": statistics.median(
                measurement["decompression_seconds"] for measurement in measurements),
            "compressed_payload_bytes": measurements[0]["compressed_bytes"],
            "payload_plus_dictionary_bytes": measurements[0]["compressed_bytes"] + dictionary_bytes,
            "compressed_fraction": measurements[0]["compressed_bytes"] / sum(map(len, heldout)),
            "passes": measurements,
        }
    return {
        "zstandard_version": zstandard.__version__,
        "zstd_library_version": list(zstandard.ZSTD_VERSION),
        "dictionary_training": training_stats,
        "codec_order_by_pass": orders,
        "retained_payload_ceiling_bytes": RETAINED_PAYLOAD_MAX_BYTES,
        "codecs": results,
        "limitations": [
            "Codec-only held-out sample; no LMDB, multiprocessing, cache or Elasticsearch timing.",
            "All JSON stays in memory for codec comparison; this is not whole-pipeline performance.",
            "Byte ceiling covers retained sample/frame/dictionary payloads, not native trainer workspace or process RSS.",
            "Training and dictionary setup costs must be included in any full ingestion comparison.",
        ],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, default=Path("data"))
    parser.add_argument("--output", type=Path)
    parser.add_argument("--records", type=int, default=4096)
    parser.add_argument("--stride", type=int, default=97)
    parser.add_argument("--passes", type=int, default=5)
    args = parser.parse_args()
    started = time.perf_counter()
    training, heldout, sample = collect_sample(
        list((args.data_dir / "organization").glob("*.gz")), args.records, args.stride)
    sampling_seconds = time.perf_counter() - started
    results = benchmark(training, heldout, args.passes)
    result = {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "python_version": platform.python_version(),
        "isal_version": importlib.metadata.version("isal"),
        "orjson_version": orjson.__version__,
        "sample": sample,
        "sampling_seconds": sampling_seconds,
        "total_probe_seconds": time.perf_counter() - started,
        **results,
    }
    encoded = json.dumps(result, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.write_text(encoded, encoding="utf-8")
    print(encoded, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
