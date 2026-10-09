#!/usr/bin/env python3
"""Compare organization staging and bounded joins without Elasticsearch writes.

One invocation builds the full production LMDB from the raw organization feeds,
checks a bounded sample against raw bodies, and streams the first N persons in
each file through the production JSON join. Only counts, timings and digests are
written. The temporary stage is removed, never reused by ingestion.

Run each configuration in a fresh pipeline container under its 2 GiB / four CPU
limits. ORG_WRITE_RECORDS, ORG_WRITE_BYTES, ORG_PUTMULTI and ORG_READ_BUFFERS are
read by production code; the harness does not implement alternative writers.
This serial, no-HTTP sample is a candidate screen, not full-ingestion throughput.
"""

from __future__ import annotations

import argparse
import hashlib
import itertools
import os
import platform
from pathlib import Path
import sys
import tempfile
import time


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "pipeline"))

import orjson

from main import MemoryMonitor, write_metrics, person_records
from prefetch import open_organization_store, prefetch_settings
from resources import cpu_delta, read_cpu_stats
import store as production


def _update_digest(digest, identifier, raw):
    """Length-prefix fields so concatenated documents cannot be ambiguous."""
    key = str(identifier).encode("ascii")
    digest.update(len(key).to_bytes(8, "big"))
    digest.update(key)
    digest.update(len(raw).to_bytes(8, "big"))
    digest.update(raw)


def check_organization_bodies(paths, stage, records_per_file, cache_bytes):
    """Compare raw source bodies and both reader modes without retaining rows."""
    digest = hashlib.sha256()
    checked = 0
    original_buffers = os.environ.get("ORG_READ_BUFFERS")
    try:
        # Environments must not be opened twice for the same path in a process.
        # Check sequentially, and compare both reader passes to raw source bytes.
        for buffers in ("0", "1"):
            os.environ["ORG_READ_BUFFERS"] = buffers
            mode_digest = hashlib.sha256()
            with production.OrganizationStore(stage, cache_bytes=cache_bytes) as lookup:
                for path in paths:
                    rows = production.iter_records(path)
                    try:
                        for _line, envelope in itertools.islice(rows, records_per_file):
                            expected = orjson.dumps(envelope["serialized_data"])
                            actual = lookup.get(envelope["id"])
                            if type(actual) is not bytes or actual != expected:
                                raise ValueError("sampled organization body differs from raw feed")
                            _update_digest(mode_digest, envelope["id"], actual)
                            if buffers == "0":
                                checked += 1
                    finally:
                        rows.close()
            if buffers == "0":
                digest = mode_digest
            elif mode_digest.digest() != digest.digest():
                raise ValueError("organization reader modes produced different bodies")
    finally:
        if original_buffers is None:
            os.environ.pop("ORG_READ_BUFFERS", None)
        else:
            os.environ["ORG_READ_BUFFERS"] = original_buffers
    return {
        "organization_bodies_checked": checked,
        "organization_reader_modes_checked": 2,
        "organization_sample_sha256": digest.hexdigest(),
    }


def transform_person_sample(paths, stage, records_per_file, cache_bytes):
    """Use the same fresh per-file reader/cache lifecycle as ingest_file()."""
    digest = hashlib.sha256()
    statistics = {
        "persons": 0,
        "roles": 0,
        "resolved_org_refs": 0,
        "unresolved_org_refs": 0,
        "persons_with_unresolved_orgs": 0,
        "enriched_document_bytes": 0,
    }
    per_file = []
    for path in paths:
        started = time.perf_counter()
        count = 0
        with open_organization_store(stage, cache_bytes) as lookup:
            rows = person_records(path, lookup)
            try:
                for _line, envelope in itertools.islice(rows, records_per_file):
                    raw, counts = production.enrich_person_json(envelope, lookup)
                    _update_digest(digest, envelope["id"], raw)
                    count += 1
                    statistics["persons"] += 1
                    statistics["enriched_document_bytes"] += len(raw)
                    for name, value in counts.items():
                        statistics[name] += value
            finally:
                rows.close()
            if hasattr(lookup, "prefetch_metrics"):
                for name, value in lookup.prefetch_metrics.items():
                    statistics[name] = statistics.get(name, 0) + value
        per_file.append({"file": path.name, "persons": count,
                         "seconds": time.perf_counter() - started})
    return {**statistics, "person_sample_sha256": digest.hexdigest(),
            "person_sample_files": per_file}


def run(args):
    organizations = sorted((args.data_dir / "organization").glob("*.json.gz"))
    persons = sorted((args.data_dir / "person").glob("*.json.gz"))
    if not organizations or not persons:
        raise ValueError("raw organization and person gzip feeds are required")
    cache_bytes = int(os.environ.get("ORG_CACHE_MIB", "4")) * 1024**2
    if not 0 <= cache_bytes <= 128 * 1024**2:
        raise ValueError("ORG_CACHE_MIB must be between 0 and 128")
    started = time.perf_counter()
    codec_source = Path(production.__file__).with_name("codec.py")
    prefetch_source = Path(production.__file__).with_name("prefetch.py")
    report = {
        "status": "running",
        "execution": "Full production organization stage; serial bounded person sample; no Elasticsearch.",
        "platform": platform.platform(),
        "python": platform.python_version(),
        "store_source_sha256": hashlib.sha256(Path(production.__file__).read_bytes()).hexdigest(),
        "codec_source_sha256": (hashlib.sha256(codec_source.read_bytes()).hexdigest()
                                if codec_source.is_file() else None),
        "prefetch_source_sha256": (hashlib.sha256(prefetch_source.read_bytes()).hexdigest()
                                   if prefetch_source.is_file() else None),
        "profile_source_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "organization_source_files": len(organizations),
        "person_source_files": len(persons),
        "person_records_per_file_limit": args.person_records_per_file,
        "organization_checks_per_file_limit": args.organization_checks_per_file,
        "organization_cache_bytes": cache_bytes,
        "prefetch_settings": prefetch_settings(),
        "organization_read_buffers": os.environ.get("ORG_READ_BUFFERS", "0"),
        "requested_settings": {name: os.environ.get(name) for name in (
            "STAGE_WORKERS", "ORG_WRITE_RECORDS", "ORG_WRITE_BYTES",
            "ORG_PUTMULTI", "ORG_READ_BUFFERS", "ORG_CODEC", "ORG_ZSTD_DICT_BYTES")},
    }
    args.stage_dir.mkdir(parents=True, exist_ok=True)
    stage_base = args.stage_dir.resolve()
    with MemoryMonitor() as monitor:
        with tempfile.TemporaryDirectory(prefix="store-profile-", dir=stage_base) as temporary:
            stage_parent = Path(temporary).resolve()
            stage_parent.relative_to(stage_base)
            stage = stage_parent / "organizations"
            phase_started, cpu_started = time.perf_counter(), read_cpu_stats()
            report.update(production.build_store(organizations, stage))
            report["organization_stage_cpu"] = cpu_delta(
                cpu_started, read_cpu_stats(), time.perf_counter() - phase_started)
            print(orjson.dumps({"event": "store_profile_stage_complete",
                                "organizations": report["organizations"],
                                "seconds": report["organization_stage_seconds"]}).decode(), flush=True)

            checking_started = time.perf_counter()
            report.update(check_organization_bodies(
                organizations, stage, args.organization_checks_per_file, cache_bytes))
            report["organization_sample_verification_seconds"] = time.perf_counter() - checking_started

            phase_started, cpu_started = time.perf_counter(), read_cpu_stats()
            report.update(transform_person_sample(
                persons, stage, args.person_records_per_file, cache_bytes))
            report["person_sample_seconds"] = time.perf_counter() - phase_started
            report["person_sample_cpu"] = cpu_delta(
                cpu_started, read_cpu_stats(), report["person_sample_seconds"])
            if args.expected_digest and report["person_sample_sha256"] != args.expected_digest:
                raise ValueError("person sample differs from the expected variant digest")
    report.update(monitor.cpu_metrics)
    report["peak_container_memory_bytes"] = monitor.peak
    report["elapsed_seconds"] = time.perf_counter() - started
    report["status"] = "complete"
    report["limitations"] = [
        "Person sample uses the first N rows in every input file, not a random or exhaustive sample.",
        "Prefetch may read/fetch to the end of its window beyond the sample cutoff; full ingestion has no sample cutoff.",
        "Person processing is serial; full ingestion uses multiple workers and asynchronous Elasticsearch writes.",
        "Transform timing includes gzip, JSON decoding, join/encoding and SHA256; there is no HTTP or Elasticsearch.",
        "Organization checks verify bounded raw-source bodies and both LMDB reader modes, not every staged value.",
        "Filesystem and host cache state are uncontrolled; compare repeated runs before selecting defaults.",
        "Cgroup peak memory covers the container lifetime; use a fresh container for each invocation.",
    ]
    write_metrics(args.output, report)
    print(orjson.dumps({"event": "store_profile_complete", "output": str(args.output),
                        "organizations": report["organizations"], "persons": report["persons"],
                        "stage_seconds": report["organization_stage_seconds"],
                        "person_sample_seconds": report["person_sample_seconds"],
                        "person_sample_sha256": report["person_sample_sha256"],
                        "peak_container_memory_bytes": report["peak_container_memory_bytes"]}).decode(), flush=True)
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, default=Path("/data"))
    parser.add_argument("--stage-dir", type=Path, default=Path("/stage"))
    parser.add_argument("--output", type=Path, default=Path("/metrics/store_profile.json"))
    parser.add_argument("--person-records-per-file", type=int, default=20_000)
    parser.add_argument("--organization-checks-per-file", type=int, default=16)
    parser.add_argument("--expected-digest")
    args = parser.parse_args()
    if not 1 <= args.person_records_per_file <= 50_000:
        parser.error("person-records-per-file must be between 1 and 50000")
    if not 1 <= args.organization_checks_per_file <= 128:
        parser.error("organization-checks-per-file must be between 1 and 128")
    if args.expected_digest and (len(args.expected_digest) != 64 or any(
            character not in "0123456789abcdef" for character in args.expected_digest)):
        parser.error("expected-digest must be a lowercase SHA256 hex digest")
    run(args)
