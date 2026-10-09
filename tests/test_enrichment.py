"""Verify the person join preserves complete records and unresolved roles."""

import gzip
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from pipeline.mapping import INDEX_DEFINITION
from pipeline.store import OrganizationStore, build_store, enrich_person, enrich_person_json


class EnrichmentTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        root = Path(self.temporary.name)
        self.orgs = {
            8: {"forager_id": 8, "name": "Feed Organization", "linkedin_id": 88,
                "addresses": [{"country": "HU"}], "technologies": ["Python"],
                "funding_rounds": [{"amount": 20}], "extra_feed_field": {"kept": True}},
            3: {"forager_id": 3, "name": "Earlier ID", "keywords": ["Search"]},
        }
        feed = root / "organizations.json.gz"
        with gzip.open(feed, "wt", encoding="utf-8") as handle:
            for org_id, body in self.orgs.items():
                handle.write(json.dumps({"id": org_id, "serialized_data": body}) + "\n")
        destination = root / "organizations.db"
        # Windows reserves the full LMDB map as a file; allocate a fixture-sized
        # map while exercising the same storage, compression, and join code.
        with patch("pipeline.store.STORE_MAP_BYTES", 8 * 1024 ** 2):
            build_store([feed], destination)
        self.store = OrganizationStore(destination)
        self.addCleanup(self.store.close)

    def test_full_join_deduplicates_in_first_role_order(self):
        original = {
            "forager_id": 17, "first_name": "Person", "skills": ["SQL"],
            "arbitrary_person_field": {"preserved": [1, 2]}, "organizations": [],
            "roles": [
                {"organization_id": 8, "organization_name": "Old role name", "role_title": "Engineer", "extra_role_field": 7},
                {"organization_id": 3, "role_title": "Developer"},
                {"organization_id": 8, "role_title": "Manager"},
                {"organization_id": 999, "role_title": "Contractor"},
                {"organization_id": 999, "role_title": "Consultant"},
                {"organization_id": None, "role_title": "Freelancer"},
            ],
        }
        envelope = {"id": 17, "date_updated": "envelope timestamp", "serialized_data": original}
        result, stats = enrich_person(envelope, self.store)
        encoded, encoded_stats = enrich_person_json(envelope, self.store)
        self.assertEqual(json.loads(encoded), result)
        self.assertEqual(encoded_stats, stats)
        self.assertEqual(result["organizations"], [self.orgs[8], self.orgs[3]])
        self.assertEqual(result["arbitrary_person_field"], original["arbitrary_person_field"])
        self.assertEqual(result["skills"], ["SQL"])
        self.assertEqual(result["roles"][0]["organization_name"], "Old role name")
        self.assertEqual(result["roles"][0]["extra_role_field"], 7)
        self.assertEqual(len(result["roles"]), 6)
        self.assertFalse(result["roles"][0]["organization_unresolved"])
        self.assertTrue(result["roles"][3]["organization_unresolved"])
        self.assertTrue(result["roles"][4]["organization_unresolved"])
        self.assertFalse(result["roles"][5].get("organization_unresolved", False))
        self.assertEqual(result["unresolved_organization_ids"], [999])
        self.assertTrue(result["has_unresolved_organizations"])
        self.assertNotIn("id", result)
        self.assertNotIn("serialized_data", result)
        self.assertEqual(stats, {"roles": 6, "resolved_org_refs": 3,
                                 "unresolved_org_refs": 2, "persons_with_unresolved_orgs": 1})
        self.assertEqual(original["organizations"], [])
        self.assertNotIn("organization_unresolved", original["roles"][0])

    def test_person_with_no_roles_has_empty_join(self):
        result, stats = enrich_person({"id": 18, "serialized_data": {
            "forager_id": 18, "roles": [], "organizations": [], "last_name": "Kept"
        }}, self.store)
        self.assertEqual(result["last_name"], "Kept")
        self.assertEqual(result["organizations"], [])
        self.assertEqual(result["unresolved_organization_ids"], [])
        self.assertFalse(result["has_unresolved_organizations"])
        self.assertEqual(stats, {"roles": 0, "resolved_org_refs": 0,
                                 "unresolved_org_refs": 0, "persons_with_unresolved_orgs": 0})

    def test_input_body_id_must_match_envelope(self):
        with self.assertRaises(ValueError):
            enrich_person({"id": 17, "serialized_data": {"forager_id": 18, "roles": []}}, self.store)

    def test_invalid_role_id_is_rejected(self):
        for invalid_id in ["8", True, {}, 8.5]:
            with self.subTest(invalid_id=invalid_id), self.assertRaises(ValueError):
                enrich_person({"id": 17, "serialized_data": {
                    "forager_id": 17, "roles": [{"organization_id": invalid_id}]
                }}, self.store)

    def test_correctness_query_fields_are_objects_with_keyword_fields(self):
        properties = INDEX_DEFINITION["mappings"]["properties"]
        for name, query_field in [("roles", "role_title"), ("organizations", "name")]:
            with self.subTest(name=name):
                self.assertEqual(properties[name].get("type", "object"), "object")
                self.assertEqual(properties[name]["properties"][query_field]["fields"]["keyword"]["type"], "keyword")


if __name__ == "__main__":
    unittest.main()
