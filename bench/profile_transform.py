#!/usr/bin/env python3
"""Profile a bounded real-feed sample without Elasticsearch writes.

The temporary LMDB contains only organizations referenced by the sampled
persons, discovered by streaming every organization feed. It is deleted after
the experiment and never used by ingestion. Reports contain aggregates only.
Requires the pipeline requirements; run from the repository root.
"""

from __future__ import annotations

import argparse
import cProfile
import ctypes
import hashlib
import itertools
import json
from pathlib import Path
import platform
import pstats
import statistics
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "pipeline"))

import lmdb
import orjson
from isal import igzip, isal_zlib
import store as production


def measure(operation, repeats):
    seconds = []
    result = None
    for _ in range(repeats):
        started = time.perf_counter()
        result = operation()
        seconds.append(time.perf_counter() - started)
    return {"seconds": seconds, "median_seconds": statistics.median(seconds)}, result


def measure_pair(first, second, repeats):
    """Alternate order to reduce allocator/cache and competing-work bias."""
    timings = [[], []]
    results = [None, None]
    for repeat in range(repeats):
        for index in ((0, 1) if repeat % 2 == 0 else (1, 0)):
            started = time.perf_counter()
            results[index] = (first if index == 0 else second)()
            timings[index].append(time.perf_counter() - started)
    return [({"seconds": values, "median_seconds": statistics.median(values)}, result)
            for values, result in zip(timings, results)]


def peak_rss_bytes():
    if sys.platform == "win32":
        class Counters(ctypes.Structure):
            _fields_ = [("cb", ctypes.c_ulong), ("PageFaultCount", ctypes.c_ulong)] + [
                (name, ctypes.c_size_t) for name in (
                    "PeakWorkingSetSize", "WorkingSetSize", "QuotaPeakPagedPoolUsage",
                    "QuotaPagedPoolUsage", "QuotaPeakNonPagedPoolUsage", "QuotaNonPagedPoolUsage",
                    "PagefileUsage", "PeakPagefileUsage")]
        counters = Counters()
        counters.cb = ctypes.sizeof(counters)
        kernel = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel.GetCurrentProcess.restype = ctypes.c_void_p
        psapi = ctypes.WinDLL("psapi", use_last_error=True)
        psapi.GetProcessMemoryInfo.argtypes = [ctypes.c_void_p, ctypes.POINTER(Counters), ctypes.c_ulong]
        if psapi.GetProcessMemoryInfo(kernel.GetCurrentProcess(), ctypes.byref(counters), counters.cb):
            return counters.PeakWorkingSetSize
        return None
    import resource
    size = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return size if sys.platform == "darwin" else size * 1024


def read_lines(path, limit):
    with igzip.open(path, "rb") as source:
        return list(itertools.islice(source, limit))


def make_sample_store(data_dir, lines, destination):
    needed = set()
    lookup_sequence = []
    roles = 0
    for line in lines:
        envelope = orjson.loads(line)
        person_ids = dict.fromkeys(
            role["organization_id"] for role in envelope["serialized_data"].get("roles", [])
            if role.get("organization_id") is not None
        )
        roles += len(envelope["serialized_data"].get("roles", []))
        needed.update(person_ids)
        lookup_sequence.extend(person_ids)
    paths = sorted((data_dir / "organization").glob("*.json.gz"))
    if not paths:
        raise ValueError("No organization files found")
    destination.mkdir()
    environment = lmdb.open(str(destination), map_size=2 * 1024**3, sync=False, metasync=False)
    transaction = environment.begin(write=True)
    found = 0
    raw_bytes = compressed_bytes = 0
    try:
        for path in paths:
            for _, envelope in production.iter_records(path):
                ident = envelope["id"]
                if ident not in needed:
                    continue
                raw = orjson.dumps(envelope["serialized_data"])
                compressed = isal_zlib.compress(raw, level=1)
                if not transaction.put(str(ident).encode("ascii"), compressed, overwrite=False):
                    raise ValueError("Duplicate sampled organization ID")
                found += 1
                raw_bytes += len(raw)
                compressed_bytes += len(compressed)
                if found % 1000 == 0:
                    transaction.commit()
                    transaction = environment.begin(write=True)
        transaction.commit()
        transaction = None
        environment.sync(True)
    finally:
        if transaction is not None:
            transaction.abort()
        environment.close()
    return lookup_sequence, {
        "sample_roles": roles,
        "sample_unique_referenced_orgs": len(needed),
        "sample_resolved_unique_orgs": found,
        "sample_unresolved_unique_orgs": len(needed) - found,
        "sample_lookup_calls_deduplicated_per_person": len(lookup_sequence),
        "sample_org_raw_bytes": raw_bytes,
        "sample_org_compressed_bytes": compressed_bytes,
    }


def mutating_candidate(envelope, lookup):
    """Experiment only: freshly decoded persons can reuse their dictionaries."""
    production._validate_envelope(envelope)
    body = envelope["serialized_data"]
    roles = body.get("roles", [])
    if not isinstance(roles, list):
        raise ValueError("person roles must be an array")
    organizations, missing, seen = [], [], {}
    resolved_refs = unresolved_refs = 0
    for role in roles:
        if not isinstance(role, dict):
            raise ValueError("person role must be an object")
        ident = role.get("organization_id")
        if ident is None:
            role["organization_unresolved"] = False
            continue
        if not production._valid_id(ident):
            raise ValueError("invalid role organization ID")
        if ident not in seen:
            raw = lookup.get(ident)
            seen[ident] = raw is not None
            if raw is None:
                missing.append(ident)
            else:
                organizations.append(orjson.Fragment(raw))
        resolved = seen[ident]
        role["organization_unresolved"] = not resolved
        if resolved:
            resolved_refs += 1
        else:
            unresolved_refs += 1
    body["roles"] = roles
    body["organizations"] = organizations
    body["unresolved_organization_ids"] = missing
    body["has_unresolved_organizations"] = bool(missing)
    return orjson.dumps(body), {
        "roles": len(roles), "resolved_org_refs": resolved_refs,
        "unresolved_org_refs": unresolved_refs,
        "persons_with_unresolved_orgs": int(bool(missing)),
    }


def transform(lines, store_path, candidate=False):
    bytes_out = 0
    join = mutating_candidate if candidate else production.enrich_person_json
    with production.OrganizationStore(store_path) as lookup:
        for line in lines:
            body, _ = join(orjson.loads(line), lookup)
            bytes_out += len(body)
    return bytes_out


def detailed_transform(lines, store_path):
    times = {"decode": 0.0, "join_including_lookup_and_copies": 0.0, "fragment_encode": 0.0}
    documents = []
    with production.OrganizationStore(store_path) as lookup:
        for line in lines:
            started = time.perf_counter()
            envelope = orjson.loads(line)
            decoded = time.perf_counter()
            body, _ = production._join_person(envelope, lookup, fragments=True)
            joined = time.perf_counter()
            encoded = orjson.dumps(body)
            finished = time.perf_counter()
            times["decode"] += decoded - started
            times["join_including_lookup_and_copies"] += joined - decoded
            times["fragment_encode"] += finished - joined
            documents.append((str(envelope["id"]), encoded))
    return times, documents


def batches(documents):
    pending, size = [], 0
    for document in documents:
        cost = len(document[1]) + 96
        if pending and (size + cost > 4 * 1024**2 or len(pending) >= 1000):
            yield pending
            pending, size = [], 0
        pending.append(document)
        size += cost
    if pending:
        yield pending


def original_payload(batch):
    return b"".join(
        orjson.dumps({"index": {"_index": "persons", "_id": ident}}) + b"\n" + body + b"\n"
        for ident, body in batch
    )


def parts_payload(batch):
    pieces = []
    for ident, body in batch:
        pieces.extend((orjson.dumps({"index": {"_index": "persons", "_id": ident}}), b"\n", body, b"\n"))
    return b"".join(pieces)


def assemble(documents, builder):
    return sum(len(builder(batch)) for batch in batches(documents))


def profile_functions(lines, stage):
    profiler = cProfile.Profile()
    profiler.enable()
    transform(lines, stage)
    profiler.disable()
    statistics_ = pstats.Stats(profiler)
    functions = []
    for (filename, line, name), values in statistics_.stats.items():
        primitive, calls, own, cumulative, _ = values
        if name in ("_join_person", "get", "_validate_envelope", "_valid_id", "_key") or any(
            term in name for term in ("loads", "dumps", "decompress", "getsizeof", "copy", "move_to_end", "popitem", "encode", "'get'")
        ):
            functions.append({"function": name, "module": Path(filename).name,
                              "calls": calls, "self_seconds": own, "cumulative_seconds": cumulative})
    return sorted(functions, key=lambda item: item["cumulative_seconds"], reverse=True)


def run(args):
    started = time.perf_counter()
    person_path = sorted((args.data_dir / "person").glob("*.json.gz"))[args.file_number]
    report = {"platform": platform.platform(), "python": platform.python_version(),
              "execution": "One-process microprofile; not Elasticsearch ingestion throughput or a constrained-container benchmark.",
              "sample_person_file": person_path.name, "repeats": args.repeats,
              "store_source_sha256": hashlib.sha256(Path(production.__file__).read_bytes()).hexdigest()}
    read_timing, lines = measure(lambda: read_lines(person_path, args.sample), args.repeats)
    report.update(sample_persons=len(lines), sample_person_raw_bytes=sum(map(len, lines)),
                  gzip_read=read_timing)
    report["decode_only"], _ = measure(
        lambda: sum(len(orjson.loads(line)["serialized_data"]) for line in lines), args.repeats)
    args.stage_dir.mkdir(parents=True, exist_ok=True)
    stage_base = args.stage_dir.resolve()
    with tempfile.TemporaryDirectory(prefix="profile-", dir=stage_base) as temporary:
        # Verify the absolute cleanup target remains inside the intended stage.
        stage_parent = Path(temporary).resolve()
        stage_parent.relative_to(stage_base)
        stage = stage_parent / "organizations"
        preparing = time.perf_counter()
        lookup_sequence, subset = make_sample_store(args.data_dir, lines, stage)
        report.update(subset)
        report["sample_store_preparation_seconds"] = time.perf_counter() - preparing
        print(json.dumps({"event": "sample_store_ready", **subset}), flush=True)
        baseline, candidate = measure_pair(lambda: transform(lines, stage),
                                           lambda: transform(lines, stage, True), args.repeats)
        report["actual_decode_join_encode"] = baseline[0]
        report["mutating_candidate_decode_join_encode"] = candidate[0]
        with production.OrganizationStore(stage) as lookup:
            for line in lines:
                baseline, baseline_stats = production.enrich_person_json(orjson.loads(line), lookup)
                candidate, candidate_stats = mutating_candidate(orjson.loads(line), lookup)
                if baseline != candidate or baseline_stats != candidate_stats:
                    raise ValueError("Mutation candidate differs from production output")
        report["mutation_candidate_exact_body_and_stats_matches"] = len(lines)
        report["component_seconds_with_per_record_timers"], documents = detailed_transform(lines, stage)

        def lookup_only(cache_bytes):
            with production.OrganizationStore(stage, cache_bytes=cache_bytes) as lookup:
                hits = misses = absent = bytes_out = 0
                for ident in lookup_sequence:
                    cached = ident in lookup._cache
                    raw = lookup.get(ident)
                    hits += cached
                    misses += not cached
                    absent += raw is None
                    bytes_out += len(raw) if raw is not None else 0
                return {"hits": hits, "misses": misses, "absent": absent, "raw_bytes_returned": bytes_out}

        cache_results = measure_pair(lambda: lookup_only(16 * 1024**2), lambda: lookup_only(0), args.repeats)
        for label, result in zip(("lookup_16m_cache", "lookup_no_cache"), cache_results):
            report[label], report[label + "_counts"] = result
        report["cprofile_functions"] = profile_functions(lines, stage)
        report["metadata_orjson_encode"], _ = measure(lambda: sum(
            len(orjson.dumps({"index": {"_index": "persons", "_id": ident}})) for ident, _ in documents
        ), args.repeats)
        payload_results = measure_pair(lambda: assemble(documents, original_payload),
                                       lambda: assemble(documents, parts_payload), args.repeats)
        for label, result in zip(("bulk_original_copying_payload", "bulk_parts_payload"), payload_results):
            report[label], byte_count = result
            report[label]["payload_bytes"] = byte_count
        for batch in batches(documents):
            if original_payload(batch) != parts_payload(batch):
                raise ValueError("Parts payload differs from production payload")
        report["parts_payload_exact_matches"] = len(documents)
        report["enriched_document_bytes"] = sum(len(body) for _, body in documents)
    report["elapsed_seconds"] = time.perf_counter() - started
    report["peak_process_rss_bytes"] = peak_rss_bytes()
    report["limitations"] = [
        "Samples first N persons of one selected input file; repeated runs have warm filesystem cache.",
        "Sample LMDB contains all available referenced orgs, but is smaller than production and more cache-friendly.",
        "Per-record timers and cProfile add overhead; repeated whole-pass medians are better for candidate comparisons.",
        "Mutation experiment applies only to freshly decoded disposable envelopes; production currently preserves input objects.",
        "No HTTP, Elasticsearch, bulk response parsing, multiworker scaling, or staging-throughput conclusion is measured.",
    ]
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_bytes(orjson.dumps(report, option=orjson.OPT_INDENT_2) + b"\n")
    print(json.dumps({"event": "profile_complete", "output": str(args.output),
                      "sample_persons": len(lines), "elapsed_seconds": report["elapsed_seconds"],
                      "actual_transform_median_seconds": report["actual_decode_join_encode"]["median_seconds"],
                      "mutation_transform_median_seconds": report["mutating_candidate_decode_join_encode"]["median_seconds"],
                      "original_payload_median_seconds": report["bulk_original_copying_payload"]["median_seconds"],
                      "parts_payload_median_seconds": report["bulk_parts_payload"]["median_seconds"]}), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, default=Path("data"))
    parser.add_argument("--stage-dir", type=Path, default=Path(".stage"))
    parser.add_argument("--output", type=Path, default=Path("metrics/transform_profile.json"))
    parser.add_argument("--sample", type=int, default=20_000)
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--file-number", type=int, default=0)
    args = parser.parse_args()
    if not 1 <= args.sample <= 50_000 or not 1 <= args.repeats <= 20 or not 0 <= args.file_number < 8:
        parser.error("sample must be 1..50000, repeats 1..20, and file-number 0..7")
    run(args)
