"""Real Redis boundary tests, enabled only for an isolated test connection.

Set TEST_REDIS_URL=redis://redis:6379/15 inside the Compose environment.
Each test uses its own UUID namespace; cleanup deletes only that namespace.
No FLUSH command or production feed is used.
"""

import gzip
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch
import uuid


TEST_URL = os.environ.get("TEST_REDIS_URL")
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "pipeline"))


@unittest.skipUnless(TEST_URL, "TEST_REDIS_URL is required for real Redis integration tests")
class RedisStoreIntegrationTests(unittest.TestCase):
    def setUp(self):
        import redis
        import pipeline.redis_store as adapter

        self.adapter = adapter
        self.client = redis.Redis.from_url(TEST_URL, decode_responses=False)
        self.client.ping()
        self.namespace = "test-etl-" + uuid.uuid4().hex
        self.addCleanup(self.client.close)
        self.addCleanup(self.remove_namespace)
        # Keep Windows sandbox fixtures and their verified cleanup inside repo.
        fixture_base = (Path(__file__).resolve().parent / "__pycache__").resolve()
        fixture_base.mkdir(exist_ok=True)
        self.temporary = tempfile.TemporaryDirectory(prefix="redis-test-", dir=fixture_base)
        self.root = Path(self.temporary.name).resolve()
        self.root.relative_to(fixture_base)
        self.addCleanup(self.temporary.cleanup)

    @property
    def ready_key(self):
        return self.namespace + ":ready"

    def remove_namespace(self):
        keys = list(self.client.scan_iter(match=self.namespace + ":*", count=100))
        if keys:
            self.client.delete(*keys)

    def envelope(self, ident, **fields):
        return {"id": ident, "serialized_data": {
            "forager_id": ident, "name": "Fixture company", **fields}}

    def feed(self, name, records):
        path = self.root / (name + ".json.gz")
        with gzip.open(path, "wt", encoding="utf-8") as source:
            for record in records:
                source.write(json.dumps(record) + "\n")
        return path

    def build(self, records=None, workers=1):
        records = records or [self.envelope(8, addresses=[{"city": "Fixture city"}],
                                             technologies=["Fixture technology"],
                                             custom={"retained": True}), self.envelope(3)]
        path = self.feed("organizations", records)
        metrics = self.adapter.build_redis_store([path], TEST_URL, self.namespace, workers)
        self.assertEqual(metrics["organizations"], len(records))
        self.assertIsNotNone(self.client.get(self.ready_key))
        return records

    def lookup(self, cache_bytes=128 * 1024**2):
        return self.adapter.RedisOrganizationStore(TEST_URL, self.namespace, cache_bytes=cache_bytes)

    def test_batched_prefetch_preserves_complete_bodies_and_known_absent_ids(self):
        records = self.build()
        with self.lookup() as lookup:
            values = lookup.prefetch([8, 3, 8, None, 999])
            self.assertEqual(set(values), {8, 3, 999})
            self.assertEqual(json.loads(values[8]), records[0]["serialized_data"])
            self.assertEqual(json.loads(values[3]), records[1]["serialized_data"])
            self.assertIsNone(values[999])
            self.assertEqual(lookup.get(8), values[8])
            self.assertIsNone(lookup.get(999))
            lookup.clear_prefetch()
            self.assertEqual(json.loads(lookup.get(8)), records[0]["serialized_data"])

    def test_prefetched_store_produces_exact_join_and_preserves_missing_roles(self):
        import orjson
        from pipeline.store import enrich_person_json

        records = self.build()
        person = {"id": 10, "serialized_data": {
            "forager_id": 10, "custom_person_field": ["retained"],
            "organizations": [{"forager_id": 777}],
            "roles": [{"organization_id": 8}, {"organization_id": 8},
                      {"organization_id": 999}, {"organization_id": 999},
                      {"role_title": "Role with no organization ID"}],
        }}
        expected = {
            "forager_id": 10, "custom_person_field": ["retained"],
            "organizations": [records[0]["serialized_data"]],
            "roles": [{"organization_id": 8, "organization_unresolved": False},
                      {"organization_id": 8, "organization_unresolved": False},
                      {"organization_id": 999, "organization_unresolved": True},
                      {"organization_id": 999, "organization_unresolved": True},
                      {"role_title": "Role with no organization ID", "organization_unresolved": False}],
            "unresolved_organization_ids": [999], "has_unresolved_organizations": True,
        }
        with self.lookup() as lookup:
            lookup.prefetch([8, 999])
            body, stats = enrich_person_json(person, lookup)
        self.assertEqual(orjson.loads(body), expected)
        self.assertEqual(stats, {"roles": 5, "resolved_org_refs": 2,
                                 "unresolved_org_refs": 2, "persons_with_unresolved_orgs": 1})
        self.assertNotIn("organization_unresolved", person["serialized_data"]["roles"][0])

    def test_unpublished_namespace_cannot_be_read(self):
        with self.assertRaises((ValueError, RuntimeError)):
            self.lookup()

    def test_deleted_ready_marker_fails_next_chunk_even_when_ids_are_cached(self):
        self.build()
        with self.lookup() as lookup:
            lookup.prefetch([8])
            lookup.clear_prefetch()
            self.client.delete(self.ready_key)
            with self.assertRaises((ValueError, RuntimeError)):
                lookup.prefetch([8])

    def test_changed_generation_fails_next_chunk_instead_of_reusing_cache(self):
        self.build()
        with self.lookup() as lookup:
            lookup.prefetch([8])
            lookup.clear_prefetch()
            marker = json.loads(self.client.get(self.ready_key))
            marker["generation"] = uuid.uuid4().hex
            self.client.set(self.ready_key, json.dumps(marker).encode())
            with self.assertRaises((ValueError, RuntimeError)):
                lookup.prefetch([8])

    def test_empty_prefetch_still_checks_readiness(self):
        self.build()
        with self.lookup() as lookup:
            self.assertEqual(lookup.prefetch([]), {})
            self.client.delete(self.ready_key)
            with self.assertRaises((ValueError, RuntimeError)):
                lookup.prefetch([])

    def test_prefetch_rejects_invalid_ids_and_more_than_2000_unique_ids(self):
        self.build()
        with self.lookup() as lookup:
            for invalid in (True, -1, "8", 8.0, []):
                with self.subTest(kind=type(invalid).__name__):
                    with self.assertRaises(ValueError):
                        lookup.prefetch([invalid])
            with self.assertRaises(ValueError):
                lookup.prefetch(range(2001))

    def test_zero_cache_and_cleared_overlay_refetch_complete_records(self):
        self.build()
        with patch("pipeline.redis_store._redis_client", return_value=self.client):
            with self.lookup(cache_bytes=0) as lookup:
                with patch.object(self.client, "mget", wraps=self.client.mget) as observed:
                    first = lookup.prefetch([8])[8]
                    lookup.clear_prefetch()
                    second = lookup.get(8)
                    self.assertEqual(first, second)
                    self.assertEqual(observed.call_count, 2)

    def test_malformed_mget_responses_fail_without_becoming_dangling_refs(self):
        self.build()
        marker = self.client.get(self.ready_key)
        invalid_responses = [None, [], [marker], [marker, b""], [marker, "decoded-string"]]
        with patch("pipeline.redis_store._redis_client", return_value=self.client):
            with self.lookup() as lookup:
                for response in invalid_responses:
                    lookup.clear_prefetch()
                    with self.subTest(shape=type(response).__name__):
                        with patch.object(self.client, "mget", return_value=response):
                            with self.assertRaises((ValueError, RuntimeError)):
                                lookup.prefetch([8])

    def test_network_error_fails_chunk_without_exposing_server_details(self):
        import redis

        self.build()
        with patch("pipeline.redis_store._redis_client", return_value=self.client):
            with self.lookup() as lookup:
                with patch.object(self.client, "mget", side_effect=redis.ConnectionError("fixture-secret")):
                    with self.assertRaises(RuntimeError) as raised:
                        lookup.prefetch([8])
        self.assertNotIn("fixture-secret", str(raised.exception))

    def test_wire_batches_and_retained_overlay_respect_byte_guard(self):
        import orjson

        records = self.build([self.envelope(ident, details="x" * 1500) for ident in (1, 2, 3)])
        limit = 2 * max(len(orjson.dumps(record["serialized_data"])) for record in records)
        with patch("pipeline.redis_store.PREFETCH_RAW_BYTES", limit):
            with patch("pipeline.redis_store._redis_client", return_value=self.client):
                with self.lookup(cache_bytes=0) as lookup:
                    with patch.object(self.client, "mget", wraps=self.client.mget) as observed:
                        values = lookup.prefetch([1, 2])
                        self.assertLessEqual(sum(len(value) for value in values.values()), limit)
                        lookup.clear_prefetch()
                        with self.assertRaises(ValueError):
                            lookup.prefetch([1, 2, 3])
                        self.assertEqual(lookup._prefetched, {})
                        self.assertEqual(observed.call_count, 3)
                        for call in observed.call_args_list:
                            # One generation marker plus at most two raw values.
                            self.assertLessEqual(len(call.args[0]), 3)

    def test_lru_budget_evicts_and_oversized_values_are_not_cached(self):
        records = [self.envelope(ident) for ident in range(1, 7)]
        records.append(self.envelope(100, details="x" * 4000))
        self.build(records)
        with self.lookup(cache_bytes=700) as lookup:
            lookup.prefetch(range(1, 7))
            lookup.clear_prefetch()
            self.assertLessEqual(lookup._cached_bytes, 700)
            self.assertNotIn(1, lookup._cache)
            first = lookup.get(100)
            self.assertEqual(first, lookup.get(100))
            self.assertNotIn(100, lookup._cache)
            self.assertLessEqual(lookup._cached_bytes, 700)

    def test_negative_cache_has_an_independent_entry_limit(self):
        self.build()
        with patch("pipeline.redis_store.NEGATIVE_CACHE_ENTRIES", 3):
            with patch("pipeline.redis_store._redis_client", return_value=self.client):
                with self.lookup() as lookup:
                    with patch.object(self.client, "mget", wraps=self.client.mget) as observed:
                        values = lookup.prefetch(range(100, 106))
                        self.assertEqual(values, dict.fromkeys(range(100, 106)))
                        self.assertLessEqual(lookup._negative_entries, 3)
                        lookup.clear_prefetch()
                        self.assertIsNone(lookup.get(100))
                        self.assertEqual(observed.call_count, 2)

    def test_duplicate_org_ids_fail_staging_without_publishing_ready(self):
        first = self.feed("first", [self.envelope(8)])
        second = self.feed("second", [self.envelope(8, conflicting=True)])
        with self.assertRaises((ValueError, RuntimeError)):
            self.adapter.build_redis_store([first, second], TEST_URL, self.namespace, 2)
        self.assertIsNone(self.client.get(self.ready_key))

    def test_malformed_feed_fails_staging_without_publishing_ready_or_leaking_values(self):
        broken = self.root / "broken.json.gz"
        with gzip.open(broken, "wb") as source:
            source.write(b'{"private":"fixture-secret"\n')
        with self.assertRaises((ValueError, RuntimeError)) as raised:
            self.adapter.build_redis_store([broken], TEST_URL, self.namespace, 1)
        self.assertNotIn("fixture-secret", str(raised.exception))
        self.assertIsNone(self.client.get(self.ready_key))

    def test_two_namespaces_do_not_collide(self):
        self.build()
        other = self.namespace + ":other"
        path = self.feed("other", [self.envelope(8, other_generation=True)])
        self.adapter.build_redis_store([path], TEST_URL, other, 1)
        with self.lookup() as first, self.adapter.RedisOrganizationStore(TEST_URL, other) as second:
            self.assertNotEqual(first.prefetch([8])[8], second.prefetch([8])[8])

    def test_reusing_stage_namespace_fails_without_replacing_existing_data(self):
        self.build()
        marker = self.client.get(self.ready_key)
        path = self.feed("replacement", [self.envelope(8, replacement=True)])
        with self.assertRaises(ValueError):
            self.adapter.build_redis_store([path], TEST_URL, self.namespace, 1)
        self.assertEqual(self.client.get(self.ready_key), marker)

    def test_lost_stage_after_acknowledged_load_cannot_publish_ready(self):
        path = self.feed("organizations", [self.envelope(8)])
        original_loader = self.adapter._load_organization_file

        def load_then_lose_stage(*args, **kwargs):
            stats = original_loader(*args, **kwargs)
            self.assertEqual(stats["organizations"], 1)
            self.assertIsNotNone(self.client.get(self.namespace + ":org:8"))
            # Simulate nonpersistent restart loss using only this test's keys.
            self.remove_namespace()
            return stats

        with patch.object(self.adapter, "_load_organization_file", side_effect=load_then_lose_stage) as loader:
            with self.assertRaisesRegex(RuntimeError, "stage is unavailable or changed"):
                self.adapter.build_redis_store([path], TEST_URL, self.namespace, 1)
        self.assertEqual(loader.call_count, 1)
        self.assertIsNone(self.client.get(self.ready_key))
        self.assertIsNone(self.client.get(self.namespace + ":org:8"))

    def test_replaced_stage_owner_cannot_publish_ready(self):
        path = self.feed("organizations", [self.envelope(8)])
        original_loader = self.adapter._load_organization_file

        def load_then_replace_owner(*args, **kwargs):
            stats = original_loader(*args, **kwargs)
            self.client.set(self.namespace + ":org-stage-owner", "replacement-generation")
            return stats

        with patch.object(self.adapter, "_load_organization_file", side_effect=load_then_replace_owner):
            with self.assertRaisesRegex(RuntimeError, "stage is unavailable or changed"):
                self.adapter.build_redis_store([path], TEST_URL, self.namespace, 1)
        self.assertIsNone(self.client.get(self.ready_key))

    def test_closed_store_rejects_lookup_and_prefetch(self):
        self.build()
        lookup = self.lookup()
        lookup.close()
        with self.assertRaises(RuntimeError):
            lookup.get(8)
        with self.assertRaises(RuntimeError):
            lookup.prefetch([])


if __name__ == "__main__":
    unittest.main()
