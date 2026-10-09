"""Stream source feeds and join full organizations through a compressed LMDB store.

The store is immutable after staging. Workers share its filesystem pages while
keeping only a small LRU of JSON bytes in their own process. The production join
uses ``orjson.Fragment`` to embed those bytes without decoding organizations.
"""

from __future__ import annotations

import multiprocessing
import os
import queue
import sys
import time
from collections import OrderedDict
from pathlib import Path
from typing import Any, Iterator

import lmdb
import orjson
from isal import igzip, isal_zlib

if __package__:
    from .codec import open_codec, prepare_codec
else:  # main.py also runs directly inside the container.
    from codec import open_codec, prepare_codec


MAX_RECORD_BYTES = 16 * 1024 * 1024
STORE_MAP_BYTES = 16 * 1024 * 1024 * 1024
WRITE_BATCH_RECORDS = 1000
WRITE_BATCH_BYTES = 64 * 1024 * 1024
STAGE_BLOCK_RECORDS = 256
STAGE_BLOCK_BYTES = 1024 * 1024
STAGE_QUEUE_BLOCKS = 6


def _bounded_env_int(name: str, default: int, maximum: int) -> int:
    try:
        value = int(os.environ.get(name, str(default)))
    except ValueError:
        raise ValueError(f"{name} must be between 1 and {maximum}") from None
    if not 1 <= value <= maximum:
        raise ValueError(f"{name} must be between 1 and {maximum}")
    return value


def _env_flag(name: str) -> bool:
    value = os.environ.get(name, "0")
    if value not in ("0", "1"):
        raise ValueError(f"{name} must be 0 or 1")
    return value == "1"


def _valid_id(value: Any) -> bool:
    # bool is an int subclass, but cannot be an organization/person identifier.
    return type(value) is int and value >= 0


def _validate_envelope(envelope: Any) -> None:
    if not isinstance(envelope, dict) or not _valid_id(envelope.get("id")):
        raise ValueError("invalid envelope identifier")
    body = envelope.get("serialized_data")
    if not isinstance(body, dict):
        raise ValueError("serialized_data must be an object")
    if "forager_id" in body and (
        not _valid_id(body["forager_id"]) or body["forager_id"] != envelope["id"]
    ):
        raise ValueError("forager_id does not match the envelope identifier")


def iter_records(path: Path, include_size: bool = False) -> Iterator:
    """Yield validated NDJSON envelopes with one-based source line numbers.

    Empty lines are errors, as are truncated gzip streams. Error messages give
    the source location and reason without echoing any source record contents.
    A per-line cap bounds memory even for accidentally non-NDJSON input.
    """
    path = Path(path)
    line_number = 0
    try:
        with igzip.open(path, "rb") as stream:
            while True:
                line = stream.readline(MAX_RECORD_BYTES + 1)
                if not line:
                    break
                line_number += 1
                if len(line) > MAX_RECORD_BYTES:
                    raise ValueError(f"{path}:{line_number}: record exceeds 16 MiB")
                try:
                    envelope = orjson.loads(line)
                except orjson.JSONDecodeError:
                    raise ValueError(f"{path}:{line_number}: malformed JSON record") from None
                try:
                    _validate_envelope(envelope)
                except ValueError as error:
                    raise ValueError(f"{path}:{line_number}: {error}") from None
                if include_size:
                    yield line_number, envelope, len(line)
                else:
                    yield line_number, envelope
    except (OSError, EOFError, isal_zlib.error):
        raise ValueError(f"{path}:{line_number + 1}: cannot read gzip feed") from None


def _key(identifier: int) -> bytes:
    return str(identifier).encode("ascii")


def _compressed_blocks(
    paths: list[Path], codec_path: Path | None = None
) -> Iterator[tuple[str, list[tuple[int, bytes, bytes, int]]]]:
    """Bound producer working sets by records and compressed byte size."""
    codec = open_codec(codec_path) if codec_path is not None else None
    for path in paths:
        block: list[tuple[int, bytes, bytes, int]] = []
        block_bytes = 0
        for line_number, envelope in iter_records(path):
            raw = orjson.dumps(envelope["serialized_data"])
            compressed = codec.compress(raw) if codec is not None else isal_zlib.compress(raw, level=1)
            if block and (len(block) >= STAGE_BLOCK_RECORDS
                          or block_bytes + len(compressed) > STAGE_BLOCK_BYTES):
                yield str(path), block
                block = []
                block_bytes = 0
            block.append((line_number, _key(envelope["id"]), compressed, len(raw)))
            block_bytes += len(compressed)
        if block:
            yield str(path), block


def _put_stage_message(output: Any, message: Any, stopped: Any) -> bool:
    while not stopped.is_set():
        try:
            output.put(message, timeout=0.25)
            return True
        except queue.Full:
            pass
    return False


def _stage_producer(
    paths: list[Path], output: Any, stopped: Any, codec_path: Path | None = None
) -> None:
    """Only JSON/compression happens here; LMDB has exactly one writer."""
    try:
        for path, block in _compressed_blocks(paths, codec_path):
            if not _put_stage_message(output, ("rows", path, block), stopped):
                return
        _put_stage_message(output, ("done", None, None), stopped)
    except Exception as error:
        # Feed errors carry only source location and generic validation reasons.
        _put_stage_message(output, ("error", type(error).__name__, str(error)), stopped)
    finally:
        if stopped.is_set():
            # The writer may have failed while the feeder thread was blocked.
            output.cancel_join_thread()
        output.close()


def _parallel_blocks(
    paths: list[Path], workers: int, codec_path: Path | None = None
) -> Iterator[tuple[str, list[tuple[int, bytes, bytes, int]]]]:
    context = multiprocessing.get_context("spawn")
    output = context.Queue(maxsize=STAGE_QUEUE_BLOCKS)
    stopped = context.Event()
    processes = [context.Process(target=_stage_producer,
                                 args=(paths[offset::workers], output, stopped, codec_path))
                 for offset in range(workers)]
    started = []
    finished = 0
    try:
        for process in processes:
            process.start()
            started.append(process)
        while finished < workers:
            try:
                kind, source, block = output.get(timeout=0.25)
            except queue.Empty:
                if any(process.exitcode not in (None, 0) for process in started):
                    raise RuntimeError("organization staging producer exited unexpectedly")
                if all(process.exitcode == 0 for process in started):
                    raise RuntimeError("organization staging ended without completion messages")
                continue
            if kind == "error":
                raise ValueError(block)
            if kind == "done":
                finished += 1
            elif kind == "rows":
                yield source, block
            else:
                raise RuntimeError("invalid organization staging message")
    finally:
        stopped.set()
        # On error, producers may be blocked on a full queue or feeder flush.
        # Signal cancellation, then terminate any process that cannot finish.
        for process in started:
            process.join(timeout=1)
        for process in started:
            if process.is_alive():
                process.terminate()
        for process in started:
            process.join()
        output.cancel_join_thread()
        output.close()


def _put_stage_segment(
    environment: Any,
    transaction: Any,
    path: str,
    segment: list[tuple[int, bytes, bytes, int]],
    pending_keys: set[bytes] | None,
) -> None:
    if pending_keys is None:
        for line_number, key, compressed, _ in segment:
            if not transaction.put(key, compressed, overwrite=False):
                raise ValueError(f"{path}:{line_number}: duplicate organization identifier")
        return
    with transaction.cursor() as cursor:
        consumed, added = cursor.putmulti(
            ((key, compressed) for _, key, compressed, _ in segment),
            overwrite=False,
        )
    if consumed != len(segment):
        raise RuntimeError(f"{path}:{segment[0][0]}: incomplete organization bulk insertion")
    if added != consumed:
        # putmulti skips duplicate keys, so identify the first duplicate against
        # earlier rows in this transaction and a snapshot of committed rows.
        # Only the error path opens another reader. The caller aborts the whole
        # active write transaction, including any rows putmulti did insert.
        seen: set[bytes] = set()
        with environment.begin() as committed:
            with committed.cursor() as cursor:
                for line_number, key, _, _ in segment:
                    if key in pending_keys or key in seen or cursor.set_key(key):
                        raise ValueError(
                            f"{path}:{line_number}: duplicate organization identifier"
                        )
                    seen.add(key)
        raise RuntimeError(f"{path}:{segment[0][0]}: inconsistent organization insertion counts")
    # Bounded by the transaction record/byte caps; values are never retained.
    pending_keys.update(key for _, key, _, _ in segment)


def build_store(paths: list[Path], destination: Path) -> dict[str, Any]:
    """Build a new lookup database; never reuse or append to an existing stage.

    The caller owns a fresh temporary staging parent and cleans it after the
    run. Four bounded producers parse/compress independent files while the
    parent writes LMDB. Writes commit in bounded transactions with deferred
    syncing; the final explicit sync finishes before readers open this store.
    STAGE_WORKERS=1 selects the serial path for controlled comparisons.
    """
    started = time.perf_counter()
    requested_workers = int(os.environ.get("STAGE_WORKERS", "4"))
    if not 1 <= requested_workers <= 4:
        raise ValueError("STAGE_WORKERS must be between 1 and 4")
    write_records = _bounded_env_int("ORG_WRITE_RECORDS", WRITE_BATCH_RECORDS, 100_000)
    write_bytes = _bounded_env_int("ORG_WRITE_BYTES", WRITE_BATCH_BYTES, 256 * 1024 * 1024)
    putmulti = _env_flag("ORG_PUTMULTI")
    read_buffers = _env_flag("ORG_READ_BUFFERS")
    workers = min(requested_workers, len(paths)) if paths else 1
    destination = Path(destination)
    destination.mkdir(parents=True, exist_ok=False)
    codec_metrics = prepare_codec(paths, destination)
    environment = lmdb.open(
        str(destination),
        map_size=STORE_MAP_BYTES,
        subdir=True,
        sync=False,
        metasync=False,
        readahead=False,
        max_readers=128,
    )
    organizations = 0
    raw_bytes = 0
    transaction = environment.begin(write=True)
    pending = 0
    pending_bytes = 0
    pending_keys: set[bytes] | None = set() if putmulti else None
    blocks = (_parallel_blocks(paths, workers, destination) if workers > 1
              else _compressed_blocks(paths, destination))
    try:
        for path, block in blocks:
            offset = 0
            while offset < len(block):
                segment: list[tuple[int, bytes, bytes, int]] = []
                segment_bytes = 0
                while offset < len(block):
                    row = block[offset]
                    compressed_bytes = len(row[2])
                    if compressed_bytes > write_bytes:
                        raise ValueError(
                            f"{path}:{row[0]}: compressed organization exceeds transaction byte limit"
                        )
                    if (pending + len(segment) == write_records
                            or pending_bytes + segment_bytes + compressed_bytes > write_bytes):
                        break
                    segment.append(row)
                    segment_bytes += compressed_bytes
                    offset += 1
                if segment:
                    _put_stage_segment(environment, transaction, path, segment, pending_keys)
                    organizations += len(segment)
                    raw_bytes += sum(row[3] for row in segment)
                    pending += len(segment)
                    pending_bytes += segment_bytes
                if (offset < len(block) or pending == write_records
                        or pending_bytes == write_bytes):
                    transaction.commit()
                    transaction = environment.begin(write=True)
                    pending = 0
                    pending_bytes = 0
                    if pending_keys is not None:
                        pending_keys.clear()
        transaction.commit()
        transaction = None
        environment.sync(True)
    finally:
        blocks.close()
        if transaction is not None:
            transaction.abort()
        environment.close()
    store_bytes = sum(file.stat().st_size for file in destination.iterdir() if file.is_file())
    return {
        "organizations": organizations,
        "organization_raw_bytes": raw_bytes,
        "organization_store_bytes": store_bytes,
        "organization_stage_seconds": time.perf_counter() - started,
        "organization_stage_workers": workers,
        "organization_write_records": write_records,
        "organization_write_bytes": write_bytes,
        "organization_putmulti": putmulti,
        "organization_read_buffers": read_buffers,
        **codec_metrics,
    }


class OrganizationStore:
    """Read-only mmap lookup with a bounded cache of uncompressed JSON bytes."""

    def __init__(self, path: Path, cache_bytes: int = 16 * 1024 * 1024):
        if type(cache_bytes) is not int or cache_bytes < 0:
            raise ValueError("cache_bytes must be a nonnegative integer")
        read_buffers = _env_flag("ORG_READ_BUFFERS")
        self._codec = open_codec(Path(path))
        self._environment = lmdb.open(
            str(path),
            readonly=True,
            lock=False,
            readahead=False,
            max_readers=128,
            create=False,
        )
        self._transaction = self._environment.begin(buffers=read_buffers)
        self._cache: OrderedDict[int, tuple[bytes, int]] = OrderedDict()
        self._cache_budget = cache_bytes
        self._cached_bytes = 0
        self._closed = False

    def get(self, org_id: int) -> bytes | None:
        if self._closed:
            raise RuntimeError("organization store is closed")
        if not _valid_id(org_id):
            raise ValueError("organization identifier must be a nonnegative integer")
        cached = self._cache.get(org_id)
        if cached is not None:
            self._cache.move_to_end(org_id)
            return cached[0]
        compressed = self._transaction.get(_key(org_id))
        if compressed is None:
            return None
        raw = self._codec.decompress(compressed)
        # Decompression creates owned bytes even when LMDB returns a borrowed
        # memoryview. Only these owned bytes may escape or enter the cache.
        # Include byte/key/object overhead so many tiny values cannot grow an
        # otherwise byte-limited cache into an unbounded Python object graph.
        cost = sys.getsizeof(raw) + sys.getsizeof(org_id) + 192
        if cost <= self._cache_budget:
            while self._cached_bytes + cost > self._cache_budget:
                _, (_, evicted_cost) = self._cache.popitem(last=False)
                self._cached_bytes -= evicted_cost
            self._cache[org_id] = (raw, cost)
            self._cached_bytes += cost
        return raw

    def close(self) -> None:
        if not self._closed:
            self._cache.clear()
            self._cached_bytes = 0
            self._transaction.abort()
            self._environment.close()
            self._closed = True

    def __enter__(self) -> OrganizationStore:
        return self

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        self.close()


def _join_person(
    envelope: dict[str, Any], store: OrganizationStore, *, fragments: bool
) -> tuple[dict[str, Any], dict[str, int]]:
    _validate_envelope(envelope)
    body = envelope["serialized_data"].copy()
    source_roles = body.get("roles", [])
    if not isinstance(source_roles, list):
        raise ValueError("person roles must be an array")
    roles: list[dict[str, Any]] = []
    organizations: list[Any] = []
    seen: dict[int, bool] = {}
    unresolved_ids: list[int] = []
    resolved_refs = 0
    unresolved_refs = 0
    for source_role in source_roles:
        if not isinstance(source_role, dict):
            raise ValueError("person role must be an object")
        role = source_role.copy()
        org_id = role.get("organization_id")
        if org_id is None:
            role["organization_unresolved"] = False
        else:
            if not _valid_id(org_id):
                raise ValueError("role organization identifier must be a nonnegative integer or null")
            if org_id not in seen:
                raw = store.get(org_id)
                seen[org_id] = raw is not None
                if raw is None:
                    unresolved_ids.append(org_id)
                else:
                    organizations.append(orjson.Fragment(raw) if fragments else orjson.loads(raw))
            resolved = seen[org_id]
            role["organization_unresolved"] = not resolved
            if resolved:
                resolved_refs += 1
            else:
                unresolved_refs += 1
        roles.append(role)
    body["roles"] = roles
    body["organizations"] = organizations
    body["unresolved_organization_ids"] = unresolved_ids
    body["has_unresolved_organizations"] = bool(unresolved_ids)
    return body, {
        "roles": len(roles),
        "resolved_org_refs": resolved_refs,
        "unresolved_org_refs": unresolved_refs,
        "persons_with_unresolved_orgs": int(bool(unresolved_ids)),
    }


def enrich_person(
    envelope: dict[str, Any], store: OrganizationStore
) -> tuple[dict[str, Any], dict[str, int]]:
    """Return a conventional Python document, preserving all source fields."""
    return _join_person(envelope, store, fragments=False)


def enrich_person_json(
    envelope: dict[str, Any], store: OrganizationStore
) -> tuple[bytes, dict[str, int]]:
    """Serialize the join without parsing or re-encoding organization JSON."""
    body, stats = _join_person(envelope, store, fragments=True)
    return orjson.dumps(body), stats
