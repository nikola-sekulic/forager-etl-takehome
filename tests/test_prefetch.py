"""Sorted prefetch must preserve joins even when its bounded overlay fills."""

import gzip
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import orjson

from pipeline.prefetch import PrefetchOrganizationStore, open_organization_store, prefetch_settings
from pipeline.store import OrganizationStore, build_store, enrich_person_json
from main import person_records


class PrefetchTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        settings = patch.dict(os.environ, {
            "STAGE_WORKERS": "1", "ORG_CODEC": "isal", "ORG_ZSTD_DICT_BYTES": "0",
            "ORG_READ_BUFFERS": "1", "ORG_PREFETCH_RECORDS": "0",
            "ORG_PREFETCH_SOURCE_BYTES": "2097152", "ORG_PREFETCH_BYTES": "8388608",
            "ORG_PREFETCH_SORT": "1",
        })
        settings.start()
        self.addCleanup(settings.stop)
        source = self.feed("orgs", [{"id": i, "serialized_data": {
            "forager_id": i, "name": f"fixture-{i}", "detail": ["complete", str(i)]}}
            for i in (2, 10, 1, 30)])
        self.stage = self.root / "organizations"
        build_store([source], self.stage)

    def feed(self, name, records):
        path = self.root / (name + ".json.gz")
        with gzip.open(path, "wt", encoding="utf-8") as output:
            for row in records:
                output.write(json.dumps(row) + "\n")
        return path

    def store(self, budget=8388608, records=2, cache=0, source=2097152, sort=1):
        settings = {"ORG_PREFETCH_RECORDS": records, "ORG_PREFETCH_SOURCE_BYTES": source,
                    "ORG_PREFETCH_BYTES": budget, "ORG_PREFETCH_SORT": sort}
        return PrefetchOrganizationStore(self.stage, cache, settings)

    @staticmethod
    def people():
        return [{"id": i, "serialized_data": {"forager_id": i,
            "roles": [{"organization_id": 2}, {"organization_id": 10},
                      {"organization_id": 2}, {"organization_id": 999},
                      {"organization_id": None}], "extra": {"preserved": True}}}
                for i in range(11)]

    def test_encoded_key_order_and_one_fetch_per_distinct_id(self):
        calls = []
        original = PrefetchOrganizationStore._prefetch_fetch
        def recorded(store, identifier):
            calls.append(identifier)
            return original(store, identifier)
        with self.store() as store, patch.object(PrefetchOrganizationStore, "_prefetch_fetch", recorded):
            store.prefetch([2, 10, 1, 2, 999, 999])
            self.assertEqual(calls, [1, 10, 2, 999])
            for _ in range(3):
                self.assertEqual(orjson.loads(store.get(2))["forager_id"], 2)
                self.assertIsNone(store.get(999))
            self.assertEqual(calls, [1, 10, 2, 999])
            self.assertEqual(store.prefetch_metrics["org_prefetch_decompressions"], 3)

    def test_full_join_parity_at_budget_exhaustion_and_without_sort(self):
        people = self.people()
        with OrganizationStore(self.stage, cache_bytes=0) as store:
            expected = [enrich_person_json(person, store) for person in people]
        path = self.feed("persons", people)
        for budget in (1, 350, 8388608):
            for sort in (0, 1):
                with self.subTest(budget=budget, sort=sort), self.store(budget=budget, sort=sort) as store:
                    records = person_records(path, store)
                    actual = [enrich_person_json(person, store) for _, person in records]
                    self.assertEqual(actual, expected)
                    self.assertFalse(store._prefetched)
                    self.assertLessEqual(store.prefetch_metrics["org_prefetch_file_peak_bytes_sum"], budget)
                    if budget == 1:
                        self.assertGreater(store.prefetch_metrics["org_prefetch_budget_fallback_windows"], 0)

    def test_record_and_source_limits_preserve_original_order(self):
        path = self.feed("persons", self.people())
        for source in (2097152, 1):
            with self.store(records=3, source=source) as store:
                self.assertEqual([person["id"] for _, person in person_records(path, store)], list(range(11)))
                self.assertEqual(store.prefetch_metrics["org_prefetch_windows"], 4 if source > 1 else 11)

    def test_closing_generator_releases_overlay_and_owned_bytes_survive(self):
        with self.store() as store:
            records = person_records(self.feed("persons", self.people()), store)
            next(records)
            raw = store.get(2)
            self.assertIs(type(raw), bytes)
            self.assertTrue(store._prefetched)
            records.close()
            self.assertFalse(store._prefetched)
        self.assertEqual(orjson.loads(raw)["forager_id"], 2)
        with self.assertRaises(RuntimeError):
            store.get(2)
        with self.assertRaises(RuntimeError):
            store.prefetch([2])

    def test_invalid_ids_and_oversized_id_window_fail_explicitly(self):
        with self.store() as store:
            for value in (-1, True, "2", None):
                with self.subTest(value=value), self.assertRaises(ValueError):
                    store.prefetch([value])
            with self.assertRaisesRegex(ValueError, "4096"):
                store.prefetch(range(4097))
            self.assertFalse(store._prefetched)

    def test_excessive_reference_list_falls_back_without_dropping_roles(self):
        person = {"id": 55, "serialized_data": {"roles": [
            {"organization_id": i} for i in range(4100)]}}
        path = self.feed("many-roles", [person])
        with OrganizationStore(self.stage, cache_bytes=0) as store:
            expected = enrich_person_json(person, store)
        with self.store() as store:
            actual = [enrich_person_json(row, store) for _, row in person_records(path, store)]
            self.assertEqual(actual, [expected])
            self.assertEqual(store.prefetch_metrics["org_prefetch_distinct_ids"], 0)

    def test_cache_is_reused_and_windows_release_prior_results(self):
        with self.store(cache=4096) as store:
            store.prefetch([2, 10])
            store.get(2)
            store.get(10)
            store.clear_prefetch()
            store.prefetch([2, 30])
            self.assertEqual(set(store._prefetched), {2, 30})
            self.assertEqual(store.prefetch_metrics["org_prefetch_decompressions"], 3)
            self.assertEqual(store.prefetch_metrics["org_prefetch_lru_hits"], 1)

    def test_prefetch_does_not_warm_or_reorder_lru_before_consumption(self):
        with self.store(cache=4096) as store:
            store.get(30)
            store.get(1)
            store.prefetch([1, 2, 10])
            self.assertEqual(list(store._cache), [30, 1])
            store.get(10)
            store.get(2)
            store.get(1)
            self.assertEqual(list(store._cache), [30, 10, 2, 1])

    def test_decode_failure_releases_partial_overlay(self):
        with self.store() as store, patch.object(store, "_prefetch_fetch", side_effect=[b"{}", ValueError("fixture")]):
            with self.assertRaises(ValueError):
                store.prefetch([1, 2])
            self.assertFalse(store._prefetched)

    def test_invalid_configuration_and_disabled_factory(self):
        for settings in ({"ORG_PREFETCH_RECORDS": "-1"}, {"ORG_PREFETCH_RECORDS": "4097"},
                         {"ORG_PREFETCH_BYTES": "0"}, {"ORG_PREFETCH_SORT": "2"},
                         {"ORG_PREFETCH_SOURCE_BYTES": "bad"}):
            with self.subTest(settings=settings), patch.dict(os.environ, settings), self.assertRaises(ValueError):
                prefetch_settings()
        with open_organization_store(self.stage, 4096) as store:
            self.assertIs(type(store), OrganizationStore)


if __name__ == "__main__":
    unittest.main()
