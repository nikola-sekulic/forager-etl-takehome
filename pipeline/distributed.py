"""One coordinator, shared Redis staging, and horizontally scaled file workers."""

import multiprocessing
import os
import socket
import sys
import threading
import time
import uuid
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

import orjson

from bulk import BulkClient, es_request, require_success
from coordination import RedisRunQueue
from main import MemoryMonitor, ingest_file, initialize_worker, log, positive_int, write_metrics
from mapping import index_definition
from redis_store import build_redis_store


HEALTH_MARKER = Path("/tmp/etl-coordinator.json")


class Heartbeat:
    """Loss of a renewable lease cancels further work, including new bulks."""

    def __init__(self, renew, cancel=None, interval=5):
        self.renew, self.cancel, self.interval = renew, cancel, interval
        self.stop = threading.Event()
        self.error = None
        self.thread = threading.Thread(target=self._run, daemon=True)

    def _run(self):
        while not self.stop.wait(self.interval):
            try:
                if not self.renew():
                    raise RuntimeError("coordination lease was lost")
            except Exception as exc:
                self.error = exc
                if self.cancel is not None:
                    self.cancel.set()
                return

    def check(self):
        if self.error is not None:
            raise RuntimeError(f"coordination heartbeat failed ({type(self.error).__name__})") from self.error

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *_):
        self.stop.set()
        self.thread.join()


def queue_client():
    return RedisRunQueue(os.environ.get("REDIS_URL", "redis://redis:6379/0"))


def health():
    """The local marker prevents workers attaching a previous Compose run."""
    queue = queue_client()
    try:
        marker = orjson.loads(HEALTH_MARKER.read_bytes())
        current = queue.current()
        return int(not (current and current["run_id"] == marker["run_id"]
                       and current["coordinator_alive"]
                       and current["state"] in ("PREPARING", "READY", "RUNNING", "FINALIZING")))
    except Exception:
        return 1
    finally:
        queue.close()


def clean_organizations(queue, namespace):
    # Only this immutable run's staging data; queue history and other DBs stay.
    for pattern in (namespace + ":org:*", namespace + ":ready", namespace + ":org-stage-owner"):
        pending = []
        for key in queue.client.scan_iter(match=pattern, count=1000):
            pending.append(key)
            if len(pending) >= 500:
                queue.client.unlink(*pending)
                pending = []
        if pending:
            queue.client.unlink(*pending)


def initialize_index(client):
    info = require_success(es_request(client, "GET", "/"), "connect to Elasticsearch")
    existing = es_request(client, "HEAD", "/persons")
    if existing.status_code == 200:
        if os.environ.get("RESET_INDEX", "0") != "1":
            raise ValueError("persons already exists; set RESET_INDEX=1 to explicitly rebuild this index")
        require_success(es_request(client, "DELETE", "/persons"), "delete previous persons index")
    elif existing.status_code != 404:
        raise RuntimeError(f"check persons index: HTTP {existing.status_code}")
    require_success(es_request(client, "PUT", "/persons", index_definition(positive_int("INDEX_SHARDS", "4", 8))), "create persons index")
    return info["version"]["number"]


def finalize_index(client, metrics):
    require_success(es_request(client, "PUT", "/persons/_settings", {
        "index": {"refresh_interval": "1s"}
    }), "restore refresh interval")
    require_success(es_request(client, "POST", "/persons/_flush?wait_if_ongoing=true"), "flush persons")
    require_success(es_request(client, "POST", "/persons/_refresh"), "refresh persons")
    count = require_success(es_request(client, "GET", "/persons/_count"), "count indexed persons")["count"]
    metrics["elasticsearch_person_count"] = count
    if type(count) is not int or count != metrics.get("persons_indexed", 0):
        raise ValueError(f"count mismatch: ES={count}, acknowledged={metrics.get('persons_indexed', 0)}")
    metrics["unresolved_org_ref_percent"] = 100 * metrics.get("unresolved_org_refs", 0) / max(1, metrics.get("roles", 0))


def coordinator_main():
    started = time.perf_counter()
    run_id, owner = uuid.uuid4().hex, uuid.uuid4().hex
    metrics_path = Path(os.environ.get("METRICS_PATH", "/metrics/latest.json"))
    metrics = {"status": "running", "run_id": run_id, "architecture": "redis-distributed",
               "started_at": datetime.now(timezone.utc).isoformat(),
               "peak_elasticsearch_memory_bytes": None}
    queue = queue_client()
    client = BulkClient(os.environ.get("ES_URL", "http://elasticsearch:9200"))
    namespace = f"etl:run:{run_id}:organizations"
    acquired, published, exit_code = False, False, 0
    HEALTH_MARKER.unlink(missing_ok=True)
    with MemoryMonitor() as memory:
        try:
            if not queue.acquire_coordinator(owner):
                raise RuntimeError("another coordinator already owns this pipeline")
            acquired = True
            # An old HTTP request cannot be fenced by Redis. Only a completed
            # generation is safe to rebuild while keeping the fixed index name.
            previous = queue.current()
            if previous:
                raise RuntimeError("a previous run exists; stop all project services with docker compose down before rebuilding")
            write_metrics(metrics_path, metrics)
            data = Path(os.environ.get("DATA_DIR", "/data"))
            persons = sorted((data / "person").glob("*.json.gz"))
            organizations = sorted((data / "organization").glob("*.json.gz"))
            if not persons or not organizations:
                raise ValueError("expected raw gzip feeds under /data/person and /data/organization")
            config = {"organization_namespace": namespace, "person_files": [str(p) for p in persons]}
            with Heartbeat(lambda: queue.renew_coordinator(owner)) as heartbeat:
                queue.start(run_id, persons, config)
                published = True
                HEALTH_MARKER.write_bytes(orjson.dumps({"run_id": run_id}))
                metrics.update(person_files=len(persons), organization_files=len(organizations))
                metrics["elasticsearch_version"] = initialize_index(client)
                metrics["index_shards"] = positive_int("INDEX_SHARDS", "4", 8)
                log("organization_stage_started", run_id=run_id, backend="redis", files=len(organizations))
                metrics.update(build_redis_store(organizations, os.environ.get("REDIS_URL", "redis://redis:6379/0"),
                                                namespace, positive_int("STAGE_WORKERS", "8", 32)))
                heartbeat.check()
                redis_memory = queue.client.info("memory")
                metrics["redis_memory_after_stage_bytes"] = redis_memory["used_memory"]
                metrics["redis_rss_after_stage_bytes"] = redis_memory["used_memory_rss"]
                log("organization_stage_complete", organizations=metrics["organizations"],
                    seconds=metrics["organization_stage_seconds"])
                if not queue.ready(run_id):
                    raise RuntimeError("coordinator lease lost before publishing jobs")
                ingest_started = time.perf_counter()
                deadline = time.monotonic() + positive_int("RUN_TIMEOUT_SECONDS", "1800")
                next_progress = time.monotonic() + 10
                while True:
                    heartbeat.check()
                    status = queue.status(run_id)
                    if status["state"] == "FAILED":
                        raise RuntimeError(f"file ingestion failed: {status['failures']}")
                    if status["completed"] == status["total_jobs"]:
                        break
                    if time.monotonic() > deadline:
                        raise TimeoutError("person ingestion timed out waiting for workers")
                    if time.monotonic() >= next_progress:
                        log("distributed_progress", completed_files=status["completed"],
                            total_files=status["total_jobs"], active_files=status["leased"])
                        next_progress += 10
                    time.sleep(0.2)
                metrics["person_ingest_seconds"] = time.perf_counter() - ingest_started
                for result in status["stats"]:
                    for key, value in result.items():
                        metrics[key] = metrics.get(key, 0) + value
                if not queue.set_state(run_id, "FINALIZING"):
                    raise RuntimeError("coordinator lease lost before finalization")
                finalize_index(client, metrics)
                # Workers stop claiming once all files finish, then publish their
                # per-container cgroup peak. Do not overwrite a shared metrics file.
                deadline = time.monotonic() + 10
                while True:
                    worker_metrics = queue.worker_metrics(run_id)
                    if worker_metrics and all(m.get("status") != "running" for m in worker_metrics):
                        break
                    if time.monotonic() >= deadline:
                        break
                    time.sleep(0.1)
                metrics["pipeline_containers"] = len(worker_metrics)
                metrics["pipeline_worker_metrics"] = worker_metrics
                metrics["worker_metrics_complete"] = bool(worker_metrics) and all(m.get("status") != "running" for m in worker_metrics)
                peaks = [m["peak_pipeline_memory_bytes"] for m in worker_metrics if m.get("peak_pipeline_memory_bytes")]
                if not peaks:
                    raise RuntimeError("pipeline worker memory measurements are missing")
                metrics["peak_pipeline_memory_bytes"] = max(peaks)
                metrics["sum_pipeline_container_peaks_bytes"] = sum(peaks)
                metrics["pipeline_cpu_seconds"] = sum(m.get("container_cpu_seconds", 0) for m in worker_metrics)
                clean_organizations(queue, namespace)
                if not queue.set_state(run_id, "COMPLETED"):
                    raise RuntimeError("coordinator lease lost after finalization")
                metrics["status"] = "completed"
        except Exception as exc:
            metrics.update(status="failed", error=str(exc))
            log("coordinator_failed", error=str(exc), error_type=type(exc).__name__)
            exit_code = 1
            if published:
                try:
                    queue.set_state(run_id, "FAILED")
                except Exception:
                    pass
        finally:
            if acquired:
                try:
                    queue.release_coordinator(owner)
                except Exception:
                    pass
            queue.close()
            client.close()
    metrics["peak_coordinator_memory_bytes"] = memory.peak
    metrics["wall_seconds"] = time.perf_counter() - started
    metrics["persons_per_second"] = (metrics.get("persons_indexed", 0) / metrics["wall_seconds"]
                                     if metrics["wall_seconds"] > 0 else 0)
    metrics["finished_at"] = datetime.now(timezone.utc).isoformat()
    # A second coordinator must not replace the active coordinator's report.
    if acquired:
        write_metrics(metrics_path, metrics)
    log("coordinator_complete" if not exit_code else "coordinator_failed_metrics", **metrics)
    return exit_code


def _current_generation(queue, run_id):
    current = queue.current()
    return bool(current and current["run_id"] == run_id and current["coordinator_alive"])


def work_loop(run_id, config, owner, settings):
    from main import _cancel as cancel
    queue = queue_client()
    jobs = 0
    try:
        while not cancel.is_set():
            if not _current_generation(queue, run_id):
                raise RuntimeError("coordinator is unavailable or run generation changed")
            job = queue.claim(run_id, owner)
            if job is None:
                state = queue.status(run_id)
                if state["state"] == "FAILED":
                    raise RuntimeError("another worker failed the run")
                if state["completed"] == state["total_jobs"]:
                    return {"jobs_completed": jobs}
                time.sleep(0.2)
                continue
            log("job_claimed", job_id=job["job_id"], owner=owner, attempt=job["attempt"])
            try:
                with Heartbeat(lambda: _current_generation(queue, run_id) and queue.renew(
                        run_id, job["job_id"], job["token"]), cancel) as heartbeat:
                    stats = ingest_file(job["path"], config["organization_namespace"], settings["url"],
                                        settings["batch_bytes"], settings["batch_docs"], settings["cache_bytes"], "redis")
                    heartbeat.check()
                # Join renewal before the terminal transition, otherwise a
                # concurrent renew of an already completed job cancels peers.
                heartbeat.check()
                stats["job_attempts"] = job["attempt"]
                if not queue.complete(run_id, job["job_id"], job["token"], stats):
                    raise RuntimeError("file lease expired before completion acknowledgement")
                jobs += 1
            except Exception as exc:
                cancel.set()
                queue.fail(run_id, job["job_id"], job["token"], str(exc))
                raise
        raise RuntimeError("worker cancelled after another process failed")
    finally:
        queue.close()


def read_cpu():
    try:
        stats = dict(line.split() for line in Path("/sys/fs/cgroup/cpu.stat").read_text().splitlines())
        return int(stats["usage_usec"]) / 1_000_000
    except (OSError, ValueError, KeyError):
        try:
            return int(Path("/sys/fs/cgroup/cpuacct/cpuacct.usage").read_text()) / 1_000_000_000
        except (OSError, ValueError):
            return None


def worker_main():
    started, cpu_started = time.perf_counter(), read_cpu()
    worker_id = socket.gethostname() + "-" + uuid.uuid4().hex[:8]
    queue = queue_client()
    metrics, run_id, exit_code = {"worker_id": worker_id, "status": "running"}, None, 0
    workers = positive_int("WORKERS", "4", 8)
    context = multiprocessing.get_context("spawn")
    cancel = context.Event()
    settings = {"url": os.environ.get("ES_URL", "http://elasticsearch:9200"),
                "batch_bytes": positive_int("BULK_BYTES", "8388608", 16 * 1024 * 1024),
                "batch_docs": positive_int("BULK_DOCS", "2000", 5000),
                "cache_bytes": positive_int("ORG_CACHE_BYTES", str(128 * 1024 * 1024), 256 * 1024 * 1024)}
    metrics.update(workers=workers, org_cache_bytes_per_process=settings["cache_bytes"],
                   bulk_target_bytes=settings["batch_bytes"], bulk_max_docs=settings["batch_docs"],
                   bulk_senders=positive_int("BULK_SENDERS", "1", 2),
                   bulk_overlap=os.environ.get("BULK_OVERLAP", "1") == "1")
    with MemoryMonitor() as memory:
        try:
            deadline = time.monotonic() + 180
            while True:
                current = queue.current()
                if current and current["coordinator_alive"] and current["state"] in ("PREPARING", "READY", "RUNNING"):
                    break
                if time.monotonic() >= deadline:
                    raise TimeoutError("no live coordinator generation became available")
                time.sleep(0.2)
            run_id = current["run_id"]
            metrics["run_id"] = run_id
            queue.register_worker(run_id, worker_id, metrics)
            with ProcessPoolExecutor(max_workers=workers, mp_context=context,
                                     initializer=initialize_worker, initargs=(cancel,)) as pool:
                futures = [pool.submit(work_loop, run_id, current["config"], f"{worker_id}:{i}", settings)
                           for i in range(workers)]
                while not all(future.done() for future in futures):
                    for future in futures:
                        if future.done() and future.exception() is not None:
                            cancel.set()
                            future.result()
                    metrics["peak_pipeline_memory_bytes"] = memory.peak
                    queue.register_worker(run_id, worker_id, metrics)
                    time.sleep(0.2)
                metrics["jobs_completed"] = sum(f.result()["jobs_completed"] for f in futures)
            metrics["status"] = "completed"
        except Exception as exc:
            cancel.set()
            metrics.update(status="failed", error=str(exc))
            log("worker_failed", worker_id=worker_id, error=str(exc), error_type=type(exc).__name__)
            exit_code = 1
    metrics["wall_seconds"] = time.perf_counter() - started
    metrics["peak_pipeline_memory_bytes"] = memory.peak
    cpu_finished = read_cpu()
    if cpu_started is not None and cpu_finished is not None:
        metrics["container_cpu_seconds"] = cpu_finished - cpu_started
        if metrics["wall_seconds"] > 0:
            metrics["average_cpu_cores"] = metrics["container_cpu_seconds"] / metrics["wall_seconds"]
    if run_id:
        try:
            queue.register_worker(run_id, worker_id, metrics)
            write_metrics(Path("/metrics/workers") / run_id / (worker_id + ".json"), metrics)
        except Exception as exc:
            log("worker_metrics_failed", error_type=type(exc).__name__)
            exit_code = 1
    queue.close()
    log("worker_complete" if not exit_code else "worker_failed_metrics", **metrics)
    return exit_code


if __name__ == "__main__":
    sys.exit(health() if sys.argv[1:] == ["health"] else 1)
