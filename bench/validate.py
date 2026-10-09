#!/usr/bin/env python3
"""Validate the indexed feed against the raw-data audit and independent joins.

The supplied correctness.py and expected.json are intentionally unchanged;
their role/organization counts are placeholders. This additional validator
uses measured counts from data_audit.json and compares complete _source bodies
for bounded deterministic reservoir samples throughout each input file, plus
examples with missing and unresolved organization IDs. No personal data is
printed. HTTP requests use only the Python standard library.

Run after ingestion: python bench/validate.py
Environment: ES_URL (http://localhost:9200), DATA_DIR (./data), INDEX_NAME (persons)
"""

from __future__ import annotations

import gzip
import json
import os
from pathlib import Path
import random
import sys
import urllib.error
import urllib.parse
import urllib.request

try:
    from orjson import loads as decode
except ImportError:
    decode = json.loads


AUDIT_PATH = Path(__file__).with_name("data_audit.json")
SAMPLES_PER_FILE = 32


def request(url: str, path: str, body: dict) -> dict:
    encoded = json.dumps(body, separators=(",", ":")).encode()
    req = urllib.request.Request(
        url + path, data=encoded, method="POST",
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=120) as response:
        result = json.load(response)
    if result.get("_shards", {}).get("failed", 0):
        raise ValueError("Elasticsearch returned partial shard results")
    return result


def envelopes(path: Path):
    """Read one record at a time, never retaining a complete input file."""
    with gzip.open(path, "rb") as stream:
        for line_number, line in enumerate(stream, 1):
            try:
                envelope = decode(line)
            except ValueError:
                raise ValueError(f"Malformed JSON at {path.name}:{line_number}") from None
            if not isinstance(envelope, dict) or not isinstance(envelope.get("serialized_data"), dict):
                raise ValueError(f"Invalid envelope at {path.name}:{line_number}")
            yield envelope


def organization_ids(data_dir: Path) -> set[int]:
    """Retain only identifiers for independent dangling-reference detection."""
    paths = sorted((data_dir / "organization").glob("*.json.gz"))
    if not paths:
        raise ValueError("No organization input files found")
    identifiers = set()
    for path in paths:
        for envelope in envelopes(path):
            ident = envelope["id"]
            if type(ident) is not int or ident < 0 or ident in identifiers:
                raise ValueError("Invalid or duplicate organization identifier")
            identifiers.add(ident)
    return identifiers


def sample_persons(data_dir: Path, known_organizations: set[int],
                   samples_per_file: int = SAMPLES_PER_FILE) -> list[dict]:
    paths = sorted((data_dir / "person").glob("*.json.gz"))
    if not paths:
        raise ValueError("No person input files found")
    samples = []
    if samples_per_file < 1:
        raise ValueError("Sample count must be positive")
    for file_number, path in enumerate(paths):
        generator = random.Random(20261007 + file_number)
        reservoir = []
        examples = {}
        found = 0
        for found, envelope in enumerate(envelopes(path), 1):
            entry = (found, envelope)
            if found <= samples_per_file:
                reservoir.append(entry)
            else:
                slot = generator.randrange(found)
                if slot < samples_per_file:
                    reservoir[slot] = entry
            if len(examples) < 2:
                for role in envelope["serialized_data"].get("roles", []):
                    ident = role.get("organization_id")
                    if ident is None:
                        examples.setdefault("missing_id", entry)
                    elif ident not in known_organizations:
                        examples.setdefault("unresolved_id", entry)
        if found < samples_per_file:
            raise ValueError(f"Insufficient sample records in {path.name}")
        # A targeted example may already be in the reservoir. Preserve one
        # comparison per source line and stable file/line ordering.
        selected = dict(reservoir + list(examples.values()))
        samples.extend(selected[line] for line in sorted(selected))
    return samples


def load_sample_organizations(data_dir: Path, persons: list[dict]) -> dict[int, dict]:
    needed = {
        role["organization_id"]
        for person in persons
        for role in person["serialized_data"].get("roles", [])
        if role.get("organization_id") is not None
    }
    paths = sorted((data_dir / "organization").glob("*.json.gz"))
    if not paths:
        raise ValueError("No organization input files found")
    organizations = {}
    for path in paths:
        for envelope in envelopes(path):
            ident = envelope["id"]
            if ident in needed:
                if ident in organizations:
                    raise ValueError("Duplicate sampled organization identifier")
                organizations[ident] = envelope["serialized_data"]
    return organizations


def expected_body(envelope: dict, organizations_by_id: dict[int, dict]) -> dict:
    """A simple reference join independent of every production-pipeline module."""
    source = envelope["serialized_data"]
    roles = []
    organizations = []
    missing = []
    seen = set()
    for original in source.get("roles", []):
        role = dict(original)
        ident = role.get("organization_id")
        unresolved = ident is not None and ident not in organizations_by_id
        role["organization_unresolved"] = unresolved
        roles.append(role)
        if ident is None or ident in seen:
            continue
        seen.add(ident)
        if unresolved:
            missing.append(ident)
        else:
            organizations.append(organizations_by_id[ident])
    return {
        **source,
        "roles": roles,
        "organizations": organizations,
        "unresolved_organization_ids": missing,
        "has_unresolved_organizations": bool(missing),
    }


def check(name: str, actual: int, expected: int) -> bool:
    ok = actual == expected
    print(f"[{'PASS' if ok else 'FAIL'}] {name}: {actual:,} / expected {expected:,}")
    return ok


def canonical(body: dict) -> str:
    # Canonical JSON also distinguishes booleans from numeric values: Python
    # dict equality alone would incorrectly equate True and 1.
    return json.dumps(body, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def run() -> bool:
    audit = json.loads(AUDIT_PATH.read_text(encoding="utf-8"))
    measured = audit["measured_correctness_counts"]
    data_dir = Path(os.environ.get("DATA_DIR", "./data"))
    url = os.environ.get("ES_URL", "http://localhost:9200").rstrip("/")
    index = urllib.parse.quote(os.environ.get("INDEX_NAME", measured["index_name"]), safe="")
    checks = [
        ("total persons", {"match_all": {}}, measured["total_persons"]),
        ("persons with target role title", {"term": {
            "roles.role_title.keyword": measured["persons_with_role_title"]["role_title"]
        }}, measured["persons_with_role_title"]["count"]),
        ("persons at target joined organization", {"term": {
            "organizations.name.keyword": measured["persons_at_organization"]["name"]
        }}, measured["persons_at_organization"]["count"]),
        ("persons with unresolved organization references", {"term": {
            "has_unresolved_organizations": True
        }}, audit["feeds"]["person"]["counts"]["persons_with_unresolved_references"]),
    ]
    print("Validating with raw-data audit counts; original placeholder fixtures remain unchanged.")
    ok = True
    for name, query, expected in checks:
        actual = request(url, f"/{index}/_count", {"query": query})["count"]
        ok = check(name, actual, expected) and ok

    known_organizations = organization_ids(data_dir)
    samples = sample_persons(data_dir, known_organizations)
    del known_organizations
    print(f"Reconstructing {len(samples)} deterministic sample documents from raw feeds.", flush=True)
    organizations = load_sample_organizations(data_dir, samples)
    response = request(url, f"/{index}/_mget", {"ids": [str(person["id"]) for person in samples]})
    documents = {document["_id"]: document for document in response["docs"]}
    mismatches = 0
    for person in samples:
        ident = str(person["id"])
        document = documents.get(ident, {})
        expected = expected_body(person, organizations)
        if not document.get("found") or canonical(document.get("_source", {})) != canonical(expected):
            mismatches += 1
            print(f"[FAIL] Complete source comparison for sampled document ID {ident}")
    ok = check("complete raw-source sample matches", len(samples) - mismatches, len(samples)) and ok
    return ok


def main() -> int:
    try:
        ok = run()
    except urllib.error.HTTPError as error:
        print(f"Validation request failed: HTTP {error.code}", file=sys.stderr)
        return 2
    except urllib.error.URLError:
        print("Validation request failed: Elasticsearch is unreachable", file=sys.stderr)
        return 2
    except (OSError, ValueError, KeyError, TypeError):
        print("Validation failed: unreadable/invalid audit, source input, or response structure", file=sys.stderr)
        return 2
    print("All audit-based checks passed." if ok else "Audit-based validation failed.")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
