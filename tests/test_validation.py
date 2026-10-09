"""Independent source validation must cover whole files and join edge cases."""

import gzip
import json
from pathlib import Path
import tempfile
import unittest

from bench.validate import canonical, expected_body, organization_ids, sample_persons


class SourceValidationTests(unittest.TestCase):
    def setUp(self):
        fixture_base = (Path(__file__).resolve().parent / "__pycache__").resolve()
        fixture_base.mkdir(exist_ok=True)
        self.temporary = tempfile.TemporaryDirectory(prefix="validation-", dir=fixture_base)
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()
        self.root.relative_to(fixture_base)
        (self.root / "person").mkdir()
        (self.root / "organization").mkdir()

    def write_feed(self, feed, records):
        with gzip.open(self.root / feed / "data.json.gz", "wt", encoding="utf-8") as stream:
            for record in records:
                stream.write(json.dumps(record) + "\n")

    def person(self, ident, roles=None):
        return {"id": ident, "serialized_data": {
            "forager_id": ident, "roles": roles or [{"organization_id": 7}],
            "extra": {"keep": [True, 1, "fixture"]},
        }}

    def test_samples_are_reproducible_spread_and_include_rare_cases(self):
        rows = [self.person(ident) for ident in range(1, 201)]
        rows[150] = self.person(151, [{"organization_id": 999}])
        rows[190] = self.person(191, [{"role_title": "Missing identifier"}])
        self.write_feed("person", rows)
        samples = sample_persons(self.root, {7}, samples_per_file=16)
        self.assertEqual(samples, sample_persons(self.root, {7}, samples_per_file=16))
        identifiers = [sample["id"] for sample in samples]
        self.assertEqual(len(identifiers), len(set(identifiers)))
        self.assertLessEqual(len(identifiers), 18)
        self.assertTrue(any(16 < ident < 151 for ident in identifiers))
        self.assertIn(151, identifiers)
        self.assertIn(191, identifiers)

    def test_organization_identifier_scan_rejects_duplicates(self):
        organization = {"id": 7, "serialized_data": {"name": "Fixture"}}
        self.write_feed("organization", [organization])
        self.assertEqual(organization_ids(self.root), {7})
        self.write_feed("organization", [organization, organization])
        with self.assertRaisesRegex(ValueError, "duplicate"):
            organization_ids(self.root)

    def test_reference_join_preserves_full_details_and_distinguishes_missing_ids(self):
        source = self.person(1, [{"organization_id": 7}, {"organization_id": 999},
                                 {"organization_id": 7}, {"role_title": "No ID"}])
        organization = {"name": "Fixture", "funding_rounds": [{"amount": 123}]}
        body = expected_body(source, {7: organization})
        self.assertEqual(body["extra"], source["serialized_data"]["extra"])
        self.assertEqual(body["organizations"], [organization])
        self.assertEqual(body["unresolved_organization_ids"], [999])
        self.assertEqual([role["organization_unresolved"] for role in body["roles"]],
                         [False, True, False, False])
        self.assertNotIn("organization_unresolved", source["serialized_data"]["roles"][0])
        self.assertNotEqual(canonical({"value": True}), canonical({"value": 1}))


if __name__ == "__main__":
    unittest.main()
