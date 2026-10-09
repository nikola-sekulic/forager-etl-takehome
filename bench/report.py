#!/usr/bin/env python3
"""Print measurements produced by an actual completed ingestion run."""

import argparse
import json
import math
import sys
from pathlib import Path


def number(metrics: dict, key: str) -> float:
    value = metrics.get(key)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"missing or invalid measurement: {key}")
    if not math.isfinite(value) or value < 0:
        raise ValueError(f"missing or invalid measurement: {key}")
    return value


def print_report(path: Path, es_peak_bytes: int | None = None) -> None:
    metrics = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(metrics, dict):
        raise ValueError("metrics must be a JSON object")
    if metrics.get("status") != "completed":
        raise ValueError(f"latest run is not completed (status={metrics.get('status')!r})")
    if metrics.get("worker_metrics_complete") is False:
        raise ValueError("distributed run completed, but final worker resource measurements are incomplete")

    # Optional Docker resource sampler results belong to one run. A previous
    # run's peaks must never be attributed to the current ingestion.
    resources_path = path.with_name("resource-peaks.json")
    if resources_path.exists():
        resources = json.loads(resources_path.read_text(encoding="utf-8"))
        if not isinstance(resources, dict):
            raise ValueError("resource measurements must be a JSON object")
        if metrics.get("run_id") and resources.get("run_id") == metrics["run_id"]:
            metrics = metrics.copy()
            for key in ("peak_pipeline_memory_bytes", "peak_elasticsearch_memory_bytes"):
                if resources.get(key) is not None:
                    sampled_peak = number(resources, key)
                    recorded_peak = number(metrics, key) if metrics.get(key) is not None else 0
                    metrics[key] = max(sampled_peak, recorded_peak)

    persons = number(metrics, "persons_indexed")
    wall = number(metrics, "wall_seconds")
    throughput = number(metrics, "persons_per_second")
    memory = number(metrics, "peak_pipeline_memory_bytes")
    if persons <= 0 or wall <= 0 or throughput <= 0 or memory <= 0:
        raise ValueError("completed run must have positive person count, duration, throughput, and peak memory")

    print("Performance measurements for the most recent completed run")
    print(f"Persons indexed: {persons:,.0f}")
    print(f"Persons indexed per second (full-run average): {throughput:,.2f}")
    print(f"Wall-clock total: {wall:,.2f} seconds")
    label = "Largest pipeline container peak" if metrics.get("pipeline_containers", 1) > 1 else "Peak pipeline container memory"
    print(f"{label}: {memory / (1024 ** 2):,.2f} MiB")
    if "average_cpu_cores" in metrics:
        print(f"Average pipeline CPU cores (entire run): {number(metrics, 'average_cpu_cores'):,.2f} / 4")
        print(f"Average pipeline CPU quota use: {number(metrics, 'cpu_quota_utilization_percent'):,.2f}%")
    if isinstance(metrics.get("person_ingest_cpu"), dict):
        person_cpu = metrics["person_ingest_cpu"]
        if "average_cpu_cores" in person_cpu:
            print(f"Average pipeline CPU cores (person phase): {number(person_cpu, 'average_cpu_cores'):,.2f} / 4")
    if "pipeline_containers" in metrics:
        print(f"Pipeline containers: {number(metrics, 'pipeline_containers'):,.0f} (each capped at 2 GiB / 4 CPUs)")
        if "redis_memory_after_stage_bytes" in metrics:
            print(f"Redis allocated memory after staging: {number(metrics, 'redis_memory_after_stage_bytes') / (1024 ** 2):,.2f} MiB")
        if "peak_coordinator_memory_bytes" in metrics:
            print(f"Peak coordinator memory: {number(metrics, 'peak_coordinator_memory_bytes') / (1024 ** 2):,.2f} MiB")
    if es_peak_bytes is not None:
        es_peak_bytes = number({"es_peak_bytes": es_peak_bytes}, "es_peak_bytes")
        if es_peak_bytes <= 0:
            raise ValueError("Elasticsearch lifetime peak must be positive")
    if metrics.get("peak_elasticsearch_memory_bytes") is None and es_peak_bytes is None:
        print("Peak Elasticsearch container memory: not recorded")
    elif metrics.get("peak_elasticsearch_memory_bytes") is not None:
        es_memory = number(metrics, "peak_elasticsearch_memory_bytes")
        print(f"Peak Elasticsearch container memory: {es_memory / (1024 ** 2):,.2f} MiB")
    if es_peak_bytes is not None:
        print(f"Elasticsearch container lifetime peak: {es_peak_bytes / (1024 ** 2):,.2f} MiB")

    if "roles" in metrics:
        roles = number(metrics, "roles")
        print(f"Roles preserved: {roles:,.0f}")
        if "unresolved_org_refs" in metrics:
            unresolved = number(metrics, "unresolved_org_refs")
            percentage = 100 * unresolved / roles if roles else 0
            print(f"Unresolved organization references: {unresolved:,.0f} ({percentage:.2f}% of roles)")
    if "organizations" in metrics:
        print(f"Organizations staged: {number(metrics, 'organizations'):,.0f}")
    if "bulk_requests" in metrics:
        print(f"Bulk HTTP requests: {number(metrics, 'bulk_requests'):,.0f}")
    if "retried_documents" in metrics:
        print(f"Retried document attempts: {number(metrics, 'retried_documents'):,.0f}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--metrics", type=Path, default=Path("/metrics/latest.json"))
    parser.add_argument("--es-peak-bytes", type=int, help="Elasticsearch cgroup container lifetime peak")
    args = parser.parse_args()
    try:
        print_report(args.metrics, es_peak_bytes=args.es_peak_bytes)
    except (OSError, ValueError, TypeError) as error:
        print(f"Cannot report performance: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
