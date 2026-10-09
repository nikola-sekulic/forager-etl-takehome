"""Optional byte-bounded, sorted organization lookups for a person window."""

import os
from pathlib import Path
import sys

if __package__:
    from .store import OrganizationStore, _key, _valid_id
else:
    from store import OrganizationStore, _key, _valid_id


def prefetch_settings():
    settings = {}
    for name, default, minimum, maximum in (
        ("ORG_PREFETCH_RECORDS", 0, 0, 4096),
        ("ORG_PREFETCH_SOURCE_BYTES", 2 * 1024**2, 1, 8 * 1024**2),
        ("ORG_PREFETCH_BYTES", 8 * 1024**2, 1, 32 * 1024**2),
        ("ORG_PREFETCH_SORT", 1, 0, 1),
    ):
        try:
            value = int(os.environ.get(name, str(default)))
        except ValueError:
            raise ValueError(f"{name} must be between {minimum} and {maximum}") from None
        if not minimum <= value <= maximum:
            raise ValueError(f"{name} must be between {minimum} and {maximum}")
        settings[name] = value
    return settings


class PrefetchOrganizationStore(OrganizationStore):
    """Pin owned JSON bytes only for the current bounded person window.

    Sorting matches LMDB's encoded keys. Each distinct ID uses the existing
    cache/transaction once; missing IDs are pinned as None. Exhausting the
    overlay budget falls back to ordinary lookups, never to a missing record.
    """

    def __init__(self, path: Path, cache_bytes: int, settings: dict):
        self.prefetch_record_limit = settings["ORG_PREFETCH_RECORDS"]
        self.prefetch_source_bytes = settings["ORG_PREFETCH_SOURCE_BYTES"]
        self.prefetch_max_ids = 4096
        self._prefetch_limit = settings["ORG_PREFETCH_BYTES"]
        self._prefetch_sort = bool(settings["ORG_PREFETCH_SORT"])
        self._prefetched = {}
        self.prefetch_metrics = {
            "org_prefetch_windows": 0, "org_prefetch_distinct_ids": 0,
            "org_prefetch_overlay_hits": 0, "org_prefetch_lmdb_lookups": 0,
            "org_prefetch_decompressions": 0, "org_prefetch_lru_hits": 0,
            "org_prefetch_budget_fallback_windows": 0,
            "org_prefetch_file_peak_bytes_sum": 0,
        }
        super().__init__(path, cache_bytes)

    def _fetch(self, identifier):
        cached = identifier in self._cache
        self.prefetch_metrics["org_prefetch_lru_hits" if cached
                              else "org_prefetch_lmdb_lookups"] += 1
        raw = super().get(identifier)
        if not cached and raw is not None:
            self.prefetch_metrics["org_prefetch_decompressions"] += 1
        return raw

    def _prefetch_fetch(self, identifier):
        # Future rows must not change the LRU's recency or evict existing hot
        # entries. Populate the LRU only when the join consumes an overlay value.
        cached = self._cache.get(identifier)
        if cached is not None:
            self.prefetch_metrics["org_prefetch_lru_hits"] += 1
            return cached[0]
        self.prefetch_metrics["org_prefetch_lmdb_lookups"] += 1
        compressed = self._transaction.get(_key(identifier))
        if compressed is None:
            return None
        self.prefetch_metrics["org_prefetch_decompressions"] += 1
        return self._codec.decompress(compressed)

    def _remember(self, identifier, raw):
        if raw is None:
            return
        if identifier in self._cache:
            self._cache.move_to_end(identifier)
            return
        cost = sys.getsizeof(raw) + sys.getsizeof(identifier) + 192
        if cost <= self._cache_budget:
            while self._cached_bytes + cost > self._cache_budget:
                _, (_, evicted_cost) = self._cache.popitem(last=False)
                self._cached_bytes -= evicted_cost
            self._cache[identifier] = (raw, cost)
            self._cached_bytes += cost

    def prefetch(self, identifiers):
        self.clear_prefetch()
        if self._closed:
            raise RuntimeError("organization store is closed")
        unique = set()
        for identifier in identifiers:
            if not _valid_id(identifier):
                raise ValueError("organization identifier must be a nonnegative integer")
            unique.add(identifier)
            if len(unique) > self.prefetch_max_ids:
                raise ValueError("person window exceeds 4096 distinct organization references")
        ordered = sorted(unique, key=_key) if self._prefetch_sort else unique
        self.prefetch_metrics["org_prefetch_windows"] += 1
        self.prefetch_metrics["org_prefetch_distinct_ids"] += len(unique)
        retained = 0
        try:
            for identifier in ordered:
                raw = self._prefetch_fetch(identifier)
                cost = sys.getsizeof(raw) + sys.getsizeof(identifier) + 192
                if retained + cost > self._prefetch_limit:
                    self.prefetch_metrics["org_prefetch_budget_fallback_windows"] += 1
                    break
                self._prefetched[identifier] = raw
                retained += cost
            self.prefetch_metrics["org_prefetch_file_peak_bytes_sum"] = max(
                self.prefetch_metrics["org_prefetch_file_peak_bytes_sum"], retained)
        except BaseException:
            self.clear_prefetch()
            raise

    def clear_prefetch(self):
        self._prefetched.clear()

    def get(self, org_id):
        if self._closed:
            raise RuntimeError("organization store is closed")
        if not _valid_id(org_id):
            raise ValueError("organization identifier must be a nonnegative integer")
        if org_id in self._prefetched:
            self.prefetch_metrics["org_prefetch_overlay_hits"] += 1
            raw = self._prefetched[org_id]
            self._remember(org_id, raw)
            return raw
        return self._fetch(org_id)

    def close(self):
        self.clear_prefetch()
        super().close()


def open_organization_store(path, cache_bytes):
    settings = prefetch_settings()
    if not settings["ORG_PREFETCH_RECORDS"]:
        return OrganizationStore(Path(path), cache_bytes=cache_bytes)
    return PrefetchOrganizationStore(Path(path), cache_bytes, settings)
