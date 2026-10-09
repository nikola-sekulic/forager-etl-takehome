#!/usr/bin/env python3
"""Stream the raw feeds and write aggregate evidence without personal data.

Run: python bench/audit_data.py --data-dir data --output bench/data_audit.json
This is an audit, never a prerequisite or preprocessed input for ingestion.
Only ID sets are retained; decoded feed records are discarded after each row.
orjson is used when installed, with a standard-library fallback.
"""

from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from datetime import datetime, timezone
import gzip
import json
from pathlib import Path
import platform
import re
import sys
import time
from typing import Any

try:
    import orjson

    def loads(value: bytes) -> Any:
        return orjson.loads(value)

    def compact(value: Any) -> bytes:
        return orjson.dumps(value)

    PARSER = f"orjson {orjson.__version__}"
except ImportError:
    loads = json.loads

    def compact(value: Any) -> bytes:
        return json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode()

    PARSER = "stdlib json"


def canonical_id(value: Any) -> int | None:
    """Numeric-string IDs have the same join meaning as integer IDs."""
    if type(value) is int:
        return value
    if isinstance(value, str) and re.fullmatch(r"[0-9]+", value):
        return int(value)
    return None


def deep_size(value: Any) -> int:
    """Approximate decoded object memory, counting each distinct object once."""
    seen: set[int] = set()

    def size(item: Any) -> int:
        address = id(item)
        if address in seen:
            return 0
        seen.add(address)
        result = sys.getsizeof(item)
        if isinstance(item, dict):
            result += sum(size(k) + size(v) for k, v in item.items())
        elif isinstance(item, (list, tuple)):
            result += sum(size(v) for v in item)
        return result

    return size(value)


def observe_schema(value: Any, path: str, types: dict, arrays: dict) -> None:
    types[path][type(value).__name__] += 1
    if isinstance(value, dict):
        for key, item in value.items():
            observe_schema(item, f"{path}.{key}" if path else key, types, arrays)
    elif isinstance(value, list):
        arrays[path] = max(arrays.get(path, 0), len(value))
        # Sampling all list elements captures mixed scalar/object array shapes.
        for item in value:
            observe_schema(item, f"{path}[]", types, arrays)


def date_shape(value: Any) -> str:
    if not isinstance(value, str):
        return type(value).__name__
    if re.fullmatch(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+ Z", value):
        return "yyyy-MM-dd HH:mm:ss.fraction Z"
    if re.fullmatch(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+ [+-]\d{4}", value):
        return "yyyy-MM-dd HH:mm:ss.fraction +/-HHmm"
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        return "yyyy-MM-dd"
    if re.fullmatch(r"\d{4}-\d{2}", value):
        return "yyyy-MM"
    if re.fullmatch(r"\d{4}", value):
        return "yyyy"
    return "other_string"


def scan(args: argparse.Namespace) -> dict:
    started = time.perf_counter()
    report: dict[str, Any] = {
        "started_at_utc": datetime.now(timezone.utc).isoformat(),
        "python_version": platform.python_version(),
        "platform": platform.platform(),
        "parser": PARSER,
        "purpose": "Aggregate raw-input audit; elapsed times are not Elasticsearch ingestion throughput.",
        "schema_sample": {"records_per_file": args.schema_sample},
        "feeds": {},
    }
    org_ids: set[int] = set()
    target_org_ids: set[int] = set()
    person_ids: set[int] = set()
    referenced_ids: set[int] = set()
    unresolved_ids: set[int] = set()
    target_role_person_ids: set[int] = set()
    target_org_person_ids: set[int] = set()

    for kind in ("organization", "person"):
        feed_started = time.perf_counter()
        files = sorted((args.data_dir / kind).glob("*.json.gz"))
        if not files:
            raise RuntimeError(f"No {kind} input files found under {args.data_dir}")
        counts: Counter = Counter({key: 0 for key in (
            "rows", "blank_lines", "malformed_json_rows", "invalid_envelope_rows",
            "invalid_envelope_ids", "envelope_source_id_mismatches", "duplicate_ids",
        )})
        envelope_types: Counter = Counter()
        source_id_types: Counter = Counter()
        role_id_types: Counter = Counter()
        top_types: dict = defaultdict(Counter)
        role_types: dict = defaultdict(Counter)
        sample_types: dict = defaultdict(Counter)
        max_arrays: dict = {}
        dates: dict = defaultdict(Counter)
        input_files = []
        compact_bytes = 0
        sample_memory_bytes = 0
        memory_sample_rows = 0
        max_source_bytes = 0

        for path in files:
            file_started = time.perf_counter()
            file_rows = 0
            raw_bytes = 0
            with gzip.open(path, "rb") as source:
                for line in source:
                    raw_bytes += len(line)
                    if not line.strip():
                        counts["blank_lines"] += 1
                        continue
                    counts["rows"] += 1
                    file_rows += 1
                    try:
                        envelope = loads(line)
                    except (ValueError, UnicodeError):
                        counts["malformed_json_rows"] += 1
                        continue
                    if not isinstance(envelope, dict) or not isinstance(envelope.get("serialized_data"), dict):
                        counts["invalid_envelope_rows"] += 1
                        continue
                    record = envelope["serialized_data"]
                    ident = canonical_id(envelope.get("id"))
                    envelope_types[type(envelope.get("id")).__name__] += 1
                    source_id_types[type(record.get("forager_id")).__name__] += 1
                    if ident is None:
                        counts["invalid_envelope_ids"] += 1
                    if ident != canonical_id(record.get("forager_id")):
                        counts["envelope_source_id_mismatches"] += 1
                    for key, value in record.items():
                        top_types[key][type(value).__name__] += 1
                    for key in ("date_updated", "founded_date"):
                        if key in record:
                            dates[key][date_shape(record[key])] += 1
                    if file_rows <= args.schema_sample:
                        observe_schema(record, "serialized_data", sample_types, max_arrays)

                    if kind == "organization":
                        if ident is not None:
                            if ident in org_ids:
                                counts["duplicate_ids"] += 1
                            org_ids.add(ident)
                            if record.get("name") == args.organization_name:
                                target_org_ids.add(ident)
                        encoded = compact(record)
                        compact_bytes += len(encoded)
                        max_source_bytes = max(max_source_bytes, len(encoded))
                        if memory_sample_rows < args.memory_sample:
                            sample_memory_bytes += deep_size(record)
                            memory_sample_rows += 1
                        continue

                    if ident is not None:
                        if ident in person_ids:
                            counts["duplicate_ids"] += 1
                        person_ids.add(ident)
                    existing_orgs = record.get("organizations")
                    if isinstance(existing_orgs, list) and existing_orgs:
                        counts["persons_with_existing_organizations"] += 1
                        counts["existing_organization_objects"] += len(existing_orgs)
                    roles = record.get("roles", [])
                    if not isinstance(roles, list):
                        counts["persons_with_invalid_roles_array"] += 1
                        continue
                    counts["roles"] += len(roles)
                    counts["persons_with_roles"] += bool(roles)
                    resolved_for_person: set[int] = set()
                    has_unresolved = has_missing = has_title = False
                    has_role_org_name = False
                    for role in roles:
                        if not isinstance(role, dict):
                            counts["invalid_role_objects"] += 1
                            continue
                        for key, value in role.items():
                            role_types[key][type(value).__name__] += 1
                        for key in ("date_updated", "start_date", "end_date"):
                            if key in role:
                                dates[f"roles.{key}"][date_shape(role[key])] += 1
                        role_id = canonical_id(role.get("organization_id"))
                        role_id_types[type(role.get("organization_id")).__name__] += 1
                        has_title |= role.get("role_title") == args.role_title
                        has_role_org_name |= role.get("organization_name") == args.organization_name
                        if role_id is None:
                            counts["roles_without_usable_organization_id"] += 1
                            has_missing = True
                            continue
                        referenced_ids.add(role_id)
                        counts["roles_with_organization_id"] += 1
                        if role_id in org_ids:
                            counts["resolved_role_references"] += 1
                            resolved_for_person.add(role_id)
                        else:
                            counts["unresolved_role_references"] += 1
                            unresolved_ids.add(role_id)
                            has_unresolved = True
                    counts["persons_with_unresolved_references"] += has_unresolved
                    counts["persons_with_missing_organization_ids"] += has_missing
                    counts["joined_organizations_deduplicated_per_person"] += len(resolved_for_person)
                    counts["persons_with_joined_organizations"] += bool(resolved_for_person)
                    counts["persons_with_target_role_title"] += has_title
                    has_target_org = bool(resolved_for_person & target_org_ids)
                    counts["persons_joined_at_target_organization"] += has_target_org
                    counts["persons_with_target_organization_role_name"] += has_role_org_name
                    if ident is not None and has_title:
                        target_role_person_ids.add(ident)
                    if ident is not None and has_target_org:
                        target_org_person_ids.add(ident)

            file_report = {
                "path": str(path.relative_to(args.data_dir)).replace("\\", "/"),
                "compressed_bytes": path.stat().st_size,
                "decompressed_bytes": raw_bytes,
                "rows": file_rows,
                "elapsed_seconds": round(time.perf_counter() - file_started, 3),
            }
            input_files.append(file_report)
            print(json.dumps({"event": "audited_file", **file_report}), flush=True)

        feed: dict[str, Any] = {
            "counts": dict(sorted(counts.items())),
            "distinct_ids": len(org_ids if kind == "organization" else person_ids),
            "files": input_files,
            "elapsed_seconds": round(time.perf_counter() - feed_started, 3),
            "envelope_id_types": dict(envelope_types),
            "source_forager_id_types": dict(source_id_types),
            "top_level_field_types_full_feed": {key: dict(val) for key, val in sorted(top_types.items())},
            "date_shapes_full_feed": {key: dict(val) for key, val in sorted(dates.items())},
            "sampled_schema_types": {key: dict(val) for key, val in sorted(sample_types.items())},
            "sampled_max_array_lengths": dict(sorted(max_arrays.items())),
        }
        if kind == "organization":
            feed["source_compact_json_bytes"] = compact_bytes
            feed["largest_source_compact_json_bytes"] = max_source_bytes
            feed["decoded_memory_estimate"] = {
                "method": "Mean recursive sys.getsizeof of first N decoded organization rows multiplied by all rows; excludes ID-map overhead and allocator overhead.",
                "sample_rows": memory_sample_rows,
                "sample_decoded_bytes": sample_memory_bytes,
                "estimated_all_decoded_bytes": round(sample_memory_bytes / max(memory_sample_rows, 1) * counts["rows"]),
                "raw_json_bytes_plus_object_overhead_lower_bound": compact_bytes + counts["rows"] * sys.getsizeof(b""),
            }
            feed["target_organization_matching_ids_count"] = len(target_org_ids)
        else:
            feed["role_organization_id_types"] = dict(role_id_types)
            feed["role_field_types_full_feed"] = {key: dict(val) for key, val in sorted(role_types.items())}
            feed["distinct_referenced_organization_ids"] = len(referenced_ids)
            feed["distinct_unresolved_organization_ids"] = len(unresolved_ids)
            feed["distinct_resolved_organization_ids"] = len(referenced_ids & org_ids)
        report["feeds"][kind] = feed

    measured = {
        "index_name": "persons",
        "total_persons": len(person_ids),
        "persons_with_role_title": {"role_title": args.role_title, "count": len(target_role_person_ids)},
        "persons_at_organization": {"name": args.organization_name, "count": len(target_org_person_ids)},
    }
    report["measured_correctness_counts"] = measured
    expected_path = Path(__file__).with_name("expected.json")
    if expected_path.exists():
        frozen = json.loads(expected_path.read_text(encoding="utf-8"))
        report["frozen_fixture_comparison"] = {
            "path": "bench/expected.json",
            "fixture_note": frozen.get("_note"),
            "total_persons_matches": frozen.get("total_persons") == measured["total_persons"],
            "role_count_matches": frozen["persons_with_role_title"]["count"] == measured["persons_with_role_title"]["count"],
            "organization_count_matches": frozen["persons_at_organization"]["count"] == measured["persons_at_organization"]["count"],
            "file_modified": False,
        }
    report["limitations"] = [
        "Schema nested-type inspection samples first N rows of each file; top-level and role field types cover the full feed.",
        "Decoded organization-memory estimate is a sample extrapolation, not a container RSS measurement.",
        "Rows stream through memory, but ID sets grow with distinct IDs; this utility is not the production pipeline.",
        "Join counts replace the input organizations array using resolved role IDs and deduplicate organizations per person.",
        "Audit timing includes gzip decoding, JSON parsing, counting and schema sampling on the stated host.",
    ]
    report["elapsed_seconds"] = round(time.perf_counter() - started, 3)
    report["completed_at_utc"] = datetime.now(timezone.utc).isoformat()
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, default=Path("data"))
    parser.add_argument("--output", type=Path, default=Path("bench/data_audit.json"))
    parser.add_argument("--schema-sample", type=int, default=100)
    parser.add_argument("--memory-sample", type=int, default=1000)
    parser.add_argument("--role-title", default="Project Manager")
    parser.add_argument("--organization-name", default="Dell Technologies")
    args = parser.parse_args()
    if args.schema_sample < 0 or args.memory_sample < 1:
        parser.error("schema-sample must be nonnegative and memory-sample must be positive")
    report = scan(args)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(json.dumps({"event": "audit_complete", "output": str(args.output), "elapsed_seconds": report["elapsed_seconds"], "measured_correctness_counts": report["measured_correctness_counts"]}), flush=True)


if __name__ == "__main__":
    main()
