"""Stream, join and index the raw feeds under the pipeline's 2 GiB / 4 CPU cap."""

import multiprocessing
import os
import shutil
import sys
import tempfile
import threading
import time
import uuid
from collections import deque
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

import orjson

from bulk import BulkClient, es_request, require_success
from mapping import index_definition
from resources import _read_counter, _read_stat, cpu_delta, read_cpu_stats
from store import OrganizationStore, build_store, enrich_person_json, iter_records
from prefetch import open_organization_store, prefetch_settings


_cancel = None


def initialize_worker(cancel):
    global _cancel
    _cancel = cancel


def log(event, **values):
    print(orjson.dumps({"event": event, **values}).decode(), flush=True)


def positive_int(name, default, maximum=None):
    value = int(os.environ.get(name, default))
    if value < 1 or (maximum is not None and value > maximum):
        raise ValueError(f"{name} must be positive and at most {maximum}")
    return value


def read_memory():
    """Cgroup accounting includes workers and resident mmap/file page cache."""
    for path in ("/sys/fs/cgroup/memory.current", "/sys/fs/cgroup/memory/memory.usage_in_bytes"):
        try:
            return int(Path(path).read_text())
        except (OSError, ValueError):
            pass
    return None


class MemoryMonitor:
    def __init__(self):
        self.peak = None
        self.stop = threading.Event()
        self.thread = threading.Thread(target=self.sample, daemon=True)
        self.cpu_start = None
        self.cpu_end = None
        self.cpu_metrics = {}
        self.max_sampled_cpu_cores = None
        self.memory_events_start = {}
        self.peak_swap = None

    def sample(self):
        previous_cpu, previous_time = read_cpu_stats(), time.perf_counter()
        while not self.stop.is_set():
            current = read_memory()
            if current is not None:
                self.peak = max(self.peak or 0, current)
            swap = _read_counter("/sys/fs/cgroup/memory.swap.current")
            if swap is not None:
                self.peak_swap = max(self.peak_swap or 0, swap)
            current_cpu, current_time = read_cpu_stats(), time.perf_counter()
            if current_time - previous_time >= 0.1:
                interval = cpu_delta(previous_cpu, current_cpu, current_time - previous_time)
                if interval:
                    self.max_sampled_cpu_cores = max(self.max_sampled_cpu_cores or 0, interval["average_cpu_cores"])
                previous_cpu, previous_time = current_cpu, current_time
            self.stop.wait(0.2)

    def __enter__(self):
        self.cpu_start = read_cpu_stats()
        self.memory_events_start = _read_stat("/sys/fs/cgroup/memory.events")
        self.started = time.perf_counter()
        self.thread.start()
        return self

    def __exit__(self, *_):
        self.stop.set()
        self.thread.join()
        self.cpu_end = read_cpu_stats()
        elapsed = time.perf_counter() - self.started
        # A zero clock interval is unmeasurable; cleanup must still preserve
        # the ingest result and publish its final status.
        self.cpu_metrics = cpu_delta(self.cpu_start, self.cpu_end, elapsed) if elapsed > 0 else {}
        if self.max_sampled_cpu_cores is not None:
            self.cpu_metrics["max_sampled_cpu_cores"] = self.max_sampled_cpu_cores
        if self.peak_swap is not None:
            self.cpu_metrics["peak_sampled_swap_bytes"] = self.peak_swap
        memory_events_end = _read_stat("/sys/fs/cgroup/memory.events")
        self.cpu_metrics["memory_events"] = {key: value - self.memory_events_start[key]
                                             for key, value in memory_events_end.items()
                                             if key in self.memory_events_start and value >= self.memory_events_start[key]}
        for path in ("/sys/fs/cgroup/memory.peak", "/sys/fs/cgroup/memory/memory.max_usage_in_bytes"):
            try:
                self.peak = max(self.peak or 0, int(Path(path).read_text()))
                break
            except (OSError, ValueError):
                pass


def write_metrics(path, metrics):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    temporary.write_bytes(orjson.dumps(metrics, option=orjson.OPT_INDENT_2) + b"\n")
    temporary.replace(path)


def person_records(path, store):
    """Stream directly or prefetch a byte/record-bounded person window."""
    if not hasattr(store, "prefetch"):
        yield from iter_records(Path(path))
        return
    chunk, raw_bytes = [], 0
    record_limit = getattr(store, "prefetch_record_limit", 256)
    source_limit = getattr(store, "prefetch_source_bytes", 2 * 1024 * 1024)
    id_limit = getattr(store, "prefetch_max_ids", 2000)

    def emit(rows):
        identifiers = set()
        too_many = False
        for _line, envelope in rows:
            roles = envelope["serialized_data"].get("roles", [])
            if isinstance(roles, list):
                for role in roles:
                    identifier = role.get("organization_id") if isinstance(role, dict) else None
                    if type(identifier) is int and identifier >= 0:
                        identifiers.add(identifier)
                        if len(identifiers) > id_limit:
                            too_many = True
                            break
            if too_many:
                break
        # Large pathological rows use checked individual lookups; regular rows
        # share bounded prefetch. Validation still happens in the ordinary join.
        store.prefetch([] if too_many else identifiers)
        try:
            yield from rows
        finally:
            store.clear_prefetch()

    for line, envelope, source_bytes in iter_records(Path(path), include_size=True):
        if chunk and (len(chunk) >= record_limit or raw_bytes + source_bytes > source_limit):
            yield from emit(chunk)
            chunk, raw_bytes = [], 0
        chunk.append((line, envelope))
        raw_bytes += source_bytes
    if chunk:
        yield from emit(chunk)


def ingest_file(path, store_path, url, batch_bytes, batch_docs, cache_bytes, store_backend="lmdb"):
    client = BulkClient(url)
    overlap = os.environ.get("BULK_OVERLAP", "1") == "1"
    senders = positive_int("BULK_SENDERS", "1", 2)
    sender = ThreadPoolExecutor(max_workers=senders) if overlap else None
    sender_state = threading.local()
    sender_clients = []
    pending = deque()
    stats = {"persons_indexed": 0, "roles": 0, "resolved_org_refs": 0,
             "unresolved_org_refs": 0, "persons_with_unresolved_orgs": 0,
             "retried_documents": 0, "bulk_requests": 0, "bulk_bytes": 0,
             "bulk_call_seconds": 0.0}
    started = time.perf_counter()
    cpu_started = time.process_time()
    batch, size = [], 0

    def send_timed(documents):
        sending_client = client
        if sender is not None:
            # requests sessions are private to one sender thread.
            if not hasattr(sender_state, "client"):
                sender_state.client = BulkClient(url)
                sender_clients.append(sender_state.client)
            sending_client = sender_state.client
        bulk_started = time.perf_counter()
        result = sending_client.send(documents)
        result["bulk_call_seconds"] = time.perf_counter() - bulk_started
        return result

    def record_result(result):
        stats["persons_indexed"] += result.pop("indexed")
        for key, value in result.items():
            stats[key] += value

    def finish_pending():
        while pending:
            record_result(pending.popleft().result())

    def flush():
        nonlocal batch, size
        if not batch:
            return
        # Retain at most one batch per sender plus one building batch. Wait for
        # the oldest result before another submission: the executor queue cannot
        # grow with the input. CPU and memory ceilings still apply to all threads.
        if pending and len(pending) >= senders:
            record_result(pending.popleft().result())
        if _cancel is not None and _cancel.is_set():
            raise RuntimeError("ingest cancelled after another worker failed")
        if sender is None:
            record_result(send_timed(batch))
        else:
            pending.append(sender.submit(send_timed, batch))
        batch, size = [], 0

    try:
        if store_backend == "redis":
            from redis_store import RedisOrganizationStore
            organization_store = RedisOrganizationStore(os.environ.get("REDIS_URL", "redis://redis:6379/0"),
                                                        store_path, cache_bytes=cache_bytes)
        elif store_backend == "lmdb":
            organization_store = open_organization_store(Path(store_path), cache_bytes)
        else:
            raise ValueError("unknown organization backend")
        with organization_store as store:
            next_progress = 25_000
            for line, envelope in person_records(path, store):
                if _cancel is not None and _cancel.is_set():
                    raise RuntimeError("ingest cancelled after another worker failed")
                try:
                    body, joined = enrich_person_json(envelope, store)
                except ValueError as exc:
                    raise ValueError(f"{Path(path).name}:{line}: {exc}") from exc
                document_size = len(body) + 96
                if document_size > 32 * 1024 * 1024:
                    raise ValueError(f"{Path(path).name}:{line}: enriched document exceeds 32 MiB safety cap")
                if batch and (size + document_size > batch_bytes or len(batch) >= batch_docs):
                    flush()
                batch.append((str(envelope["id"]), body))
                size += document_size
                for key, value in joined.items():
                    stats[key] += value
                if stats["persons_indexed"] >= next_progress:
                    log("file_progress", file=Path(path).name, indexed=stats["persons_indexed"],
                        seconds=round(time.perf_counter() - started, 2))
                    next_progress += 25_000
            flush()
            if hasattr(store, "prefetch_metrics"):
                stats.update(store.prefetch_metrics)
            finish_pending()
        stats["worker_wall_seconds"] = time.perf_counter() - started
        stats["worker_cpu_seconds"] = time.process_time() - cpu_started
        log("file_complete", file=Path(path).name, seconds=round(time.perf_counter() - started, 2), **stats)
        return stats
    finally:
        if sender is not None:
            sender.shutdown(wait=True, cancel_futures=True)
        for sending_client in sender_clients:
            sending_client.close()
        client.close()


def run(metrics):
    data_dir = Path(os.environ.get("DATA_DIR", "/data"))
    persons = sorted((data_dir / "person").glob("*.json.gz"))
    organizations = sorted((data_dir / "organization").glob("*.json.gz"))
    if not persons or not organizations:
        raise ValueError(f"expected gzipped NDJSON under {data_dir}/person and {data_dir}/organization")
    workers = positive_int("WORKERS", "8", 8)
    batch_bytes = positive_int("BULK_BYTES", str(8 * 1024 * 1024), 16 * 1024 * 1024)
    batch_docs = positive_int("BULK_DOCS", "2000", 5000)
    cache_bytes = positive_int("ORG_CACHE_BYTES", str(4 * 1024 * 1024), 128 * 1024 * 1024)
    bulk_senders = positive_int("BULK_SENDERS", "1", 2)
    definition = index_definition(positive_int("INDEX_SHARDS", "4", 8))
    url = os.environ.get("ES_URL", "http://elasticsearch:9200")
    stage_root = Path(os.environ.get("STAGE_DIR", "/stage"))
    stage_root.mkdir(parents=True, exist_ok=True)
    metrics.update(workers=workers, bulk_target_bytes=batch_bytes, bulk_max_docs=batch_docs,
                   org_cache_bytes_per_process=cache_bytes,
                   index_shards=definition["settings"]["number_of_shards"],
                   person_files=len(persons), organization_files=len(organizations),
                   bulk_overlap=os.environ.get("BULK_OVERLAP", "1") == "1",
                   bulk_senders=bulk_senders)
    metrics.update({name.lower(): value for name, value in prefetch_settings().items()})
    client = BulkClient(url)
    stage = None
    try:
        info = require_success(es_request(client, "GET", "/"), "connect to Elasticsearch")
        metrics["elasticsearch_version"] = info["version"]["number"]
        existing = es_request(client, "HEAD", "/persons")
        if existing.status_code == 200:
            if os.environ.get("RESET_INDEX", "0") != "1":
                raise ValueError("persons already exists; set RESET_INDEX=1 to explicitly rebuild this index")
            require_success(es_request(client, "DELETE", "/persons"), "delete previous persons index")
        elif existing.status_code != 404:
            raise RuntimeError(f"check persons index: HTTP {existing.status_code}")
        require_success(es_request(client, "PUT", "/persons", definition), "create persons index")
        stage = Path(tempfile.mkdtemp(prefix="ingest-", dir=stage_root))
        log("organization_stage_started", files=len(organizations))
        stage_cpu_start = read_cpu_stats()
        metrics.update(build_store(organizations, stage / "organizations"))
        stage_elapsed = metrics["organization_stage_seconds"]
        metrics["organization_stage_cpu"] = (
            cpu_delta(stage_cpu_start, read_cpu_stats(), stage_elapsed) if stage_elapsed > 0 else {})
        log("organization_stage_complete", **{k: v for k, v in metrics.items() if k.startswith("organization")})
        ingest_started = time.perf_counter()
        ingest_cpu_start = read_cpu_stats()
        # Spawn avoids inheriting threads, HTTP sessions, and mmap handles.
        # Each process owns bounded building/submitted batches. With the default
        # one sender per process, at most eight bulk requests are active. Eight
        # file workers overlap I/O while sharing the unchanged four-CPU quota.
        context = multiprocessing.get_context("spawn")
        cancel = context.Event()
        with ProcessPoolExecutor(max_workers=workers, mp_context=context,
                                 initializer=initialize_worker, initargs=(cancel,)) as pool:
            futures = [pool.submit(ingest_file, str(path), str(stage / "organizations"), url,
                                   batch_bytes, batch_docs, cache_bytes) for path in persons]
            try:
                for future in as_completed(futures):
                    for key, value in future.result().items():
                        metrics[key] = metrics.get(key, 0) + value
            except BaseException:
                cancel.set()
                for future in futures:
                    future.cancel()
                raise
        metrics["person_ingest_seconds"] = time.perf_counter() - ingest_started
        ingest_elapsed = metrics["person_ingest_seconds"]
        metrics["person_ingest_cpu"] = (
            cpu_delta(ingest_cpu_start, read_cpu_stats(), ingest_elapsed) if ingest_elapsed > 0 else {})
        require_success(es_request(client, "PUT", "/persons/_settings", {
            "index": {"refresh_interval": "1s"}
        }), "restore refresh interval")
        require_success(es_request(client, "POST", "/persons/_flush?wait_if_ongoing=true"), "flush persons")
        require_success(es_request(client, "POST", "/persons/_refresh"), "refresh persons")
        count = require_success(es_request(client, "GET", "/persons/_count"), "count indexed persons")["count"]
        metrics["elasticsearch_person_count"] = count
        if type(count) is not int or count < 0:
            raise ValueError("Elasticsearch returned an invalid person count")
        if count != metrics.get("persons_indexed", 0):
            raise ValueError(f"count mismatch: ES={count}, acknowledged={metrics.get('persons_indexed', 0)}; check duplicate person IDs")
        metrics["unresolved_org_ref_percent"] = 100 * metrics.get("unresolved_org_refs", 0) / max(1, metrics.get("roles", 0))
    finally:
        client.close()
        if stage is not None:
            try:
                shutil.rmtree(stage)
            except OSError as exc:
                log("stage_cleanup_failed", error_type=type(exc).__name__)


def main():
    mode = os.environ.get("PIPELINE_MODE", "local")
    if mode != "local":
        from distributed import coordinator_main, worker_main
        if mode == "coordinator":
            return coordinator_main()
        if mode == "worker":
            return worker_main()
        raise ValueError("PIPELINE_MODE must be local, coordinator, or worker")
    started = time.perf_counter()
    metrics_path = Path(os.environ.get("METRICS_PATH", "/metrics/latest.json"))
    metrics = {"status": "running", "run_id": uuid.uuid4().hex,
               "started_at": datetime.now(timezone.utc).isoformat(),
               "peak_elasticsearch_memory_bytes": None}
    write_metrics(metrics_path, metrics)
    exit_code = 0
    with MemoryMonitor() as memory:
        try:
            run(metrics)
            metrics["status"] = "completed"
        except Exception as exc:
            metrics["status"] = "failed"
            metrics["error"] = str(exc)
            log("pipeline_failed", error=str(exc), error_type=type(exc).__name__)
            exit_code = 1
    metrics["wall_seconds"] = time.perf_counter() - started
    metrics["persons_per_second"] = (metrics.get("persons_indexed", 0) / metrics["wall_seconds"]
                                     if metrics["wall_seconds"] > 0 else 0)
    metrics["peak_pipeline_memory_bytes"] = memory.peak
    metrics.update(memory.cpu_metrics)
    metrics["finished_at"] = datetime.now(timezone.utc).isoformat()
    write_metrics(metrics_path, metrics)
    log("pipeline_complete" if not exit_code else "pipeline_failed_metrics", **metrics)
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
