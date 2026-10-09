"""Shared immutable organization staging and bounded Redis batch lookups.

Each run owns a fresh namespace. The ready marker is published only after all
source rows have validated and been acknowledged. Readers include that marker
in every network lookup so a Redis restart cannot turn lost keys into dangling
organization references. Call ``prefetch`` at each person-chunk boundary.
"""

from __future__ import annotations

import multiprocessing
import sys
import time
import uuid
from collections import OrderedDict
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Iterable

import orjson
import redis
from redis.backoff import NoBackoff
from redis.retry import Retry

from store import MAX_RECORD_BYTES, _valid_id, iter_records


WRITE_BLOCK_ROWS = 512
WRITE_BLOCK_BYTES = 4 * 1024 * 1024
PREFETCH_IDS = 2000
PREFETCH_RAW_BYTES = 64 * 1024 * 1024
NEGATIVE_CACHE_ENTRIES = 4096
_stage_cancel = None


_PUBLISH_READY = """
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
if redis.call('SET', KEYS[2], ARGV[2], 'NX') then return 1 end
return -1
"""


def _redis_client(url: str) -> redis.Redis:
    # An ambiguous SETNX retry can falsely identify an already accepted row as
    # a duplicate. Fail and use a fresh run namespace instead of retrying it.
    return redis.Redis.from_url(
        url, decode_responses=False, protocol=2,
        socket_connect_timeout=10, socket_timeout=120,
        max_connections=2, health_check_interval=30,
        retry=Retry(NoBackoff(), 0),
    )


def _prefix(namespace: str) -> bytes:
    if not isinstance(namespace, str) or not namespace or len(namespace.encode()) > 256:
        raise ValueError("organization namespace must contain 1 to 256 UTF-8 bytes")
    return namespace.encode("utf-8")


def _initialize_stage(cancel: Any) -> None:
    global _stage_cancel
    _stage_cancel = cancel


def _check_stage_cancelled() -> None:
    if _stage_cancel is not None and _stage_cancel.is_set():
        raise RuntimeError("organization staging cancelled after another loader failed")


def _load_organization_file(path: Path, url: str, prefix: bytes) -> dict[str, int]:
    client = _redis_client(url)
    block: list[tuple[int, bytes, bytes]] = []
    block_bytes = 0
    stats = {"organizations": 0, "organization_raw_bytes": 0,
             "organization_key_bytes": 0, "organization_max_raw_bytes": 0,
             "organization_write_requests": 0}

    def flush() -> None:
        nonlocal block, block_bytes
        if not block:
            return
        _check_stage_cancelled()
        try:
            with client.pipeline(transaction=False) as pipeline:
                for _, key, raw in block:
                    pipeline.setnx(key, raw)
                responses = pipeline.execute()
        except redis.RedisError as error:
            raise RuntimeError(
                f"{path}:{block[0][0]}: organization Redis write failed ({type(error).__name__})"
            ) from None
        if not isinstance(responses, list) or len(responses) != len(block):
            raise RuntimeError(f"{path}:{block[0][0]}: malformed organization write response")
        for (line, _, _), response in zip(block, responses):
            if response is False or (type(response) is int and response == 0):
                raise ValueError(f"{path}:{line}: duplicate organization identifier")
            if response is not True and not (type(response) is int and response == 1):
                raise RuntimeError(f"{path}:{line}: malformed organization write response")
        stats["organization_write_requests"] += 1
        block = []
        block_bytes = 0

    try:
        for line, envelope in iter_records(path):
            _check_stage_cancelled()
            raw = orjson.dumps(envelope["serialized_data"])
            if len(raw) > MAX_RECORD_BYTES:
                raise ValueError(f"{path}:{line}: compact organization body exceeds 16 MiB")
            key = prefix + b":org:" + str(envelope["id"]).encode("ascii")
            if block and (len(block) >= WRITE_BLOCK_ROWS
                          or block_bytes + len(raw) > WRITE_BLOCK_BYTES):
                flush()
            block.append((line, key, raw))
            block_bytes += len(raw)
            stats["organizations"] += 1
            stats["organization_raw_bytes"] += len(raw)
            stats["organization_key_bytes"] += len(key)
            stats["organization_max_raw_bytes"] = max(stats["organization_max_raw_bytes"], len(raw))
        flush()
        return stats
    finally:
        client.close()


def build_redis_store(
    paths: list[Path], redis_url: str, namespace: str, workers: int = 4
) -> dict[str, Any]:
    """Load complete source bodies into a fresh Redis namespace, then mark ready.

    Each loader streams one file with at most one 512-row/4-MiB SETNX pipeline.
    An individual source row may exceed the usual write target, but cannot
    exceed the source reader's 16-MiB cap. No failed or ambiguous load is marked
    ready, and no namespace is flushed or reused.
    """
    if type(workers) is not int or not 1 <= workers <= 32:
        raise ValueError("organization loader workers must be between 1 and 32")
    started = time.monotonic()
    prefix = _prefix(namespace)
    paths = [Path(path) for path in paths]
    actual_workers = min(workers, len(paths)) if paths else 1
    stats: dict[str, Any] = {"organizations": 0, "organization_raw_bytes": 0,
                             "organization_key_bytes": 0, "organization_max_raw_bytes": 0,
                             "organization_write_requests": 0}
    client = _redis_client(redis_url)
    try:
        # Keep a failed stage reserved: only a new namespace may retry staging.
        generation = uuid.uuid4().hex
        if not client.setnx(prefix + b":org-stage-owner", generation):
            raise ValueError("organization staging namespace was already used")
        if client.exists(prefix + b":ready"):
            raise ValueError("organization staging namespace is already ready")
        if actual_workers == 1:
            for path in paths:
                result = _load_organization_file(path, redis_url, prefix)
                for key, value in result.items():
                    stats[key] = max(stats[key], value) if key == "organization_max_raw_bytes" else stats[key] + value
        else:
            context = multiprocessing.get_context("spawn")
            cancel = context.Event()
            with ProcessPoolExecutor(max_workers=actual_workers, mp_context=context,
                                     initializer=_initialize_stage, initargs=(cancel,)) as pool:
                futures = [pool.submit(_load_organization_file, path, redis_url, prefix) for path in paths]
                try:
                    for future in as_completed(futures):
                        for key, value in future.result().items():
                            stats[key] = max(stats[key], value) if key == "organization_max_raw_bytes" else stats[key] + value
                except BaseException:
                    cancel.set()
                    for future in futures:
                        future.cancel()
                    raise
        marker = orjson.dumps({"generation": generation, "organizations": stats["organizations"],
                               "max_raw_org_bytes": stats["organization_max_raw_bytes"]})
        # A nonpersistent Redis restart between acknowledged batches may let
        # later requests reconnect successfully while earlier bodies are lost.
        # Validate the reservation and publish atomically, so that a lost or
        # replaced stage can never become readable using stale loader counts.
        published = client.eval(_PUBLISH_READY, 2, prefix + b":org-stage-owner",
                                prefix + b":ready", generation, marker)
        if published == 0:
            raise RuntimeError("organization stage is unavailable or changed before publication")
        if published != 1:
            raise RuntimeError("organization ready marker already exists")
        stats["organization_stage_seconds"] = time.monotonic() - started
        stats["organization_stage_workers"] = actual_workers
        stats["organization_store_bytes"] = stats["organization_raw_bytes"] + stats["organization_key_bytes"]
        stats["organization_store_bytes_kind"] = "Redis key/value payload; excludes allocator metadata"
        return stats
    except redis.RedisError as error:
        raise RuntimeError(f"organization Redis staging failed ({type(error).__name__})") from None
    finally:
        client.close()


class RedisOrganizationStore:
    """Process-local bytes LRU plus a bounded overlay for the current chunk.

    ``prefetch`` must run at every person-chunk boundary, including chunks whose
    IDs are already cached. It verifies the ready generation and pins returned
    values until ``clear_prefetch``. Redis errors are never treated as misses.
    """

    def __init__(self, url: str, namespace: str, cache_bytes: int = 128 * 1024 * 1024):
        if type(cache_bytes) is not int or cache_bytes < 0:
            raise ValueError("cache_bytes must be a nonnegative integer")
        self._prefix = _prefix(namespace)
        self._ready_key = self._prefix + b":ready"
        self._client = _redis_client(url)
        self._cache_budget = cache_bytes
        self._cache: OrderedDict[int, tuple[bytes | None, int]] = OrderedDict()
        self._cached_bytes = 0
        self._negative_entries = 0
        self._prefetched: dict[int, bytes | None] = {}
        self._closed = False
        try:
            marker = self._client.get(self._ready_key)
            if not isinstance(marker, bytes):
                raise RuntimeError("organization stage is not ready")
            metadata = orjson.loads(marker)
            if (not isinstance(metadata, dict)
                    or not isinstance(metadata.get("generation"), str)
                    or not metadata["generation"]
                    or type(metadata.get("organizations")) is not int
                    or metadata["organizations"] < 0
                    or type(metadata.get("max_raw_org_bytes")) is not int
                    or not 0 <= metadata["max_raw_org_bytes"] <= MAX_RECORD_BYTES
                    or (metadata["organizations"] > 0 and metadata["max_raw_org_bytes"] == 0)):
                raise RuntimeError("invalid organization ready marker")
            self._generation = marker
            self._mget_ids = min(PREFETCH_IDS, max(1, PREFETCH_RAW_BYTES // max(1, metadata["max_raw_org_bytes"])))
        except redis.RedisError as error:
            self.close()
            raise RuntimeError(f"organization Redis lookup failed ({type(error).__name__})") from None
        except (ValueError, TypeError):
            self.close()
            raise RuntimeError("invalid organization ready marker") from None
        except Exception:
            self.close()
            raise

    def _check_open(self) -> None:
        if self._closed:
            raise RuntimeError("organization store is closed")

    def _lookup(self, ids: list[int]) -> list[bytes | None]:
        keys = [self._ready_key] + [self._prefix + b":org:" + str(ident).encode("ascii") for ident in ids]
        try:
            values = self._client.mget(keys)
        except redis.RedisError as error:
            raise RuntimeError(f"organization Redis lookup failed ({type(error).__name__})") from None
        if not isinstance(values, list) or len(values) != len(keys):
            raise RuntimeError("malformed organization lookup response")
        if values[0] != self._generation:
            raise RuntimeError("organization stage is unavailable or changed")
        if any(value is not None and (not isinstance(value, bytes) or not value) for value in values[1:]):
            raise RuntimeError("malformed organization lookup response")
        return values[1:]

    def _remember(self, identifier: int, raw: bytes | None) -> None:
        previous = self._cache.pop(identifier, None)
        if previous is not None:
            self._cached_bytes -= previous[1]
            self._negative_entries -= previous[0] is None
        cost = sys.getsizeof(raw) + sys.getsizeof(identifier) + 192
        if cost > self._cache_budget:
            return
        while self._cache and (self._cached_bytes + cost > self._cache_budget
                              or (raw is None and self._negative_entries >= NEGATIVE_CACHE_ENTRIES)):
            _, (evicted, evicted_cost) = self._cache.popitem(last=False)
            self._cached_bytes -= evicted_cost
            self._negative_entries -= evicted is None
        self._cache[identifier] = (raw, cost)
        self._cached_bytes += cost
        self._negative_entries += raw is None

    def prefetch(self, ids: Iterable[int | None]) -> dict[int, bytes | None]:
        """Validate this chunk's IDs, verify generation, and pin complete results."""
        self._check_open()
        self.clear_prefetch()
        unique: dict[int, None] = {}
        for identifier in ids:
            if identifier is None:
                continue
            if not _valid_id(identifier):
                raise ValueError("organization identifier must be a nonnegative integer or null")
            unique[identifier] = None
            if len(unique) > PREFETCH_IDS:
                raise ValueError("person chunk exceeds 2000 distinct organization references")
        result: dict[int, bytes | None] = {}
        missing = []
        retained_bytes = 0
        for identifier in unique:
            cached = self._cache.get(identifier)
            if cached is None:
                missing.append(identifier)
            else:
                self._cache.move_to_end(identifier)
                result[identifier] = cached[0]
                retained_bytes += len(cached[0]) if cached[0] is not None else 0
        if retained_bytes > PREFETCH_RAW_BYTES:
            raise ValueError("person chunk organization details exceed 64 MiB")
        # Even an empty/all-cache-hit chunk performs one marker-only MGET.
        for offset in range(0, max(1, len(missing)), self._mget_ids):
            batch = missing[offset:offset + self._mget_ids]
            values = self._lookup(batch)
            for identifier, raw in zip(batch, values):
                retained_bytes += len(raw) if raw is not None else 0
                if retained_bytes > PREFETCH_RAW_BYTES:
                    raise ValueError("person chunk organization details exceed 64 MiB")
                result[identifier] = raw
                self._remember(identifier, raw)
        # Keep first occurrence order independent of cache/network partitioning.
        self._prefetched = {identifier: result[identifier] for identifier in unique}
        return self._prefetched

    def clear_prefetch(self) -> None:
        self._prefetched = {}

    def get(self, org_id: int) -> bytes | None:
        self._check_open()
        if not _valid_id(org_id):
            raise ValueError("organization identifier must be a nonnegative integer")
        if org_id in self._prefetched:
            return self._prefetched[org_id]
        cached = self._cache.get(org_id)
        if cached is not None:
            self._cache.move_to_end(org_id)
            return cached[0]
        raw = self._lookup([org_id])[0]
        self._remember(org_id, raw)
        return raw

    def close(self) -> None:
        if not self._closed:
            self.clear_prefetch()
            self._cache.clear()
            self._cached_bytes = 0
            self._negative_entries = 0
            self._client.close()
            self._closed = True

    def __enter__(self) -> RedisOrganizationStore:
        self._check_open()
        return self

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        self.close()
