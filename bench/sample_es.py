#!/usr/bin/env python3
"""Sample read-only Elasticsearch resource counters during one ingest.

Start before the ingest, with --watch-metrics pointing at its metrics file. The
sampler stops when a new run completes/fails, or at the bounded duration. Stats
are approximate at the sampling interval; indexing and merge times are summed
operation times, not CPU time. No document contents are requested.
"""

import argparse
import json
import math
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import urlopen


FIELDS = {
    "cpu_millis": "process.cpu.total_in_millis",
    "heap_bytes": "jvm.mem.heap_used_in_bytes",
    "heap_percent": "jvm.mem.heap_used_percent",
    "young_gc_millis": "jvm.gc.collectors.young.collection_time_in_millis",
    "old_gc_millis": "jvm.gc.collectors.old.collection_time_in_millis",
    "write_active": "thread_pool.write.active",
    "write_queue": "thread_pool.write.queue",
    "write_rejected": "thread_pool.write.rejected",
    "index_total": "indices.indexing.index_total",
    "index_millis": "indices.indexing.index_time_in_millis",
    "index_current": "indices.indexing.index_current",
    "index_throttle_millis": "indices.indexing.throttle_time_in_millis",
    "merge_current": "indices.merges.current",
    "merge_millis": "indices.merges.total_time_in_millis",
    "merge_throttle_millis": "indices.merges.total_throttled_time_in_millis",
}
COUNTERS = tuple(key for key in FIELDS if key.endswith("_millis") or key in ("write_rejected", "index_total"))


def value_at(body, path):
    value = body
    for part in path.split("."):
        value = value.get(part) if isinstance(value, dict) else None
    return value if type(value) in (int, float) and math.isfinite(value) and value >= 0 else None


def snapshot(es_url):
    filters = ["_nodes.*", "nodes.*.timestamp"] + [f"nodes.*.{path}" for path in FIELDS.values()]
    query = urlencode({"filter_path": ",".join(filters)})
    url = es_url.rstrip("/") + "/_nodes/stats/process,jvm,thread_pool,indices/indexing,merge?" + query
    with urlopen(url, timeout=5) as response:
        body = json.load(response)
    if body.get("_nodes", {}).get("failed", 0) or not body.get("nodes"):
        raise ValueError("Elasticsearch node stats are incomplete")
    return {"timestamp": time.time(), "nodes": {
        node_id: {key: value_at(node, path) for key, path in FIELDS.items()}
        for node_id, node in body["nodes"].items()
    }}


def read_metrics(path):
    if path is None:
        return {}
    try:
        body = json.loads(path.read_text(encoding="utf-8"))
        return body if isinstance(body, dict) else {}
    except (OSError, ValueError):
        return {}


def summarize(samples):
    result = {"sample_count": len(samples)}
    if len(samples) < 2:
        return result
    wall = samples[-1]["timestamp"] - samples[0]["timestamp"]
    result["sampled_wall_seconds"] = wall
    nodes = set(samples[0]["nodes"]) & set(samples[-1]["nodes"])
    result["node_set_changed"] = any(set(sample["nodes"]) != nodes for sample in samples)
    for key in COUNTERS:
        delta = 0
        valid = bool(nodes) and not result["node_set_changed"]
        for node_id in nodes:
            values = [sample["nodes"][node_id].get(key) for sample in samples]
            # Index counters can reset when the previous index is deleted. Do
            # not present a negative or silently adjusted lifetime delta.
            if any(value is None for value in values) or any(b < a for a, b in zip(values, values[1:])):
                valid = False
                break
            delta += values[-1] - values[0]
        result[f"delta_{key}"] = delta if valid else None
    cpu = result["delta_cpu_millis"]
    result["average_es_cpu_cores"] = cpu / (wall * 1000) if cpu is not None and wall > 0 else None
    for key in FIELDS.keys() - set(COUNTERS):
        totals = [sum(node[key] for node in sample["nodes"].values() if node[key] is not None)
                  for sample in samples]
        result[f"sampled_peak_{key}"] = max(totals)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--es-url", default="http://localhost:9200")
    parser.add_argument("--output", type=Path, default=Path("metrics/es-samples.json"))
    parser.add_argument("--watch-metrics", type=Path)
    parser.add_argument("--duration", type=float, default=180)
    parser.add_argument("--interval", type=float, default=1)
    args = parser.parse_args()
    if not (0.2 <= args.interval <= 10 and args.interval <= args.duration <= 3600):
        parser.error("interval must be 0.2..10 seconds; duration must be interval..3600 seconds")
    initial_id = read_metrics(args.watch_metrics).get("run_id")
    run, samples, failures = {}, [], []
    deadline = time.monotonic() + args.duration
    while time.monotonic() < deadline:
        started = time.monotonic()
        try:
            samples.append(snapshot(args.es_url))
        except (OSError, ValueError) as error:
            failures.append({"timestamp": time.time(), "error_type": type(error).__name__})
        metrics = read_metrics(args.watch_metrics)
        if metrics.get("run_id") and metrics["run_id"] != initial_id:
            if not run:
                run = metrics
            elif metrics["run_id"] != run["run_id"]:
                failures.append({"timestamp": time.time(), "error_type": "RunChanged"})
                break
            run = metrics
            if run.get("status") in ("completed", "failed"):
                break
        time.sleep(max(0, args.interval - (time.monotonic() - started)))
    selected = samples
    if run.get("started_at") and samples:
        start = datetime.fromisoformat(run["started_at"]).timestamp()
        before = [i for i, sample in enumerate(samples) if sample["timestamp"] <= start]
        selected = samples[before[-1]:] if before else samples
    output = {"run_id": run.get("run_id"), "run_status": run.get("status"),
              "interval_seconds": args.interval, "summary": summarize(selected),
              "failures": failures, "samples": selected,
              "note": "Sample boundaries may extend up to one interval around the run; peaks are sampled, and null deltas mean missing/reset counters."}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(output, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({key: output[key] for key in ("run_id", "run_status", "summary", "failures")}))
    return 1 if failures or (args.watch_metrics and run.get("status") != "completed") else 0


if __name__ == "__main__":
    raise SystemExit(main())
