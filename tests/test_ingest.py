"""Verify bounded background bulk sends using complete synthetic source feeds."""

import contextlib
import gzip
import io
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

# main.py supports direct execution from /app/pipeline in Docker. Mirror that
# import path when these integration tests run from a local checkout.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "pipeline"))

from pipeline.main import build_store, ingest_file
from tests.test_bulk import FakeBulkServer, bulk_result


class OverlappedIngestTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        environment = patch.dict(os.environ, {"BULK_OVERLAP": "1", "BULK_SENDERS": "1", "STAGE_WORKERS": "1"})
        environment.start()
        self.addCleanup(environment.stop)
        self.organization = {"forager_id": 8, "name": "Fixture organization",
                             "addresses": [{"country": "HU"}], "technologies": ["fixture"]}
        organizations = self.write_feed("organizations", [{"id": 8, "serialized_data": self.organization}])
        self.store_path = self.root / "organizations-store"
        with patch("store.STORE_MAP_BYTES", 8 * 1024 ** 2):
            build_store([organizations], self.store_path)

    def write_feed(self, name, records):
        path = self.root / f"{name}.json.gz"
        with gzip.open(path, "wt", encoding="utf-8") as handle:
            for record in records:
                handle.write(json.dumps(record) + "\n")
        return path

    def person(self, identifier):
        return {"id": identifier, "serialized_data": {
            "forager_id": identifier, "first_name": "Fixture", "organizations": [],
            "extra_person_field": {"preserved": [1, 2]},
            "roles": [{"role_title": "Fixture role", "organization_id": 8,
                       "extra_role_field": "preserved"}],
        }}

    def ingest(self, path, server):
        with contextlib.redirect_stdout(io.StringIO()):
            return ingest_file(str(path), str(self.store_path), server.url,
                               batch_bytes=1024 ** 2, batch_docs=1, cache_bytes=1024)

    def test_every_batch_and_final_pending_send_are_acknowledged(self):
        persons = [self.person(identifier) for identifier in range(1, 5)]
        path = self.write_feed("persons", persons)
        with FakeBulkServer([bulk_result(201)] * len(persons), delay=0.02) as server:
            metrics = self.ingest(path, server)
            self.assertEqual(metrics["persons_indexed"], len(persons))
            self.assertEqual(metrics["roles"], len(persons))
            self.assertEqual(metrics["resolved_org_refs"], len(persons))
            self.assertEqual(metrics["bulk_requests"], len(persons))
            self.assertEqual(server.max_active, 1)
            self.assertEqual(len(server.requests), len(persons))
            for index, (_url, body) in enumerate(server.requests):
                metadata, source = map(json.loads, body.splitlines())
                self.assertEqual(metadata, {"index": {"_index": "persons", "_id": str(index + 1)}})
                expected = dict(persons[index]["serialized_data"])
                expected.update(
                    roles=[{**expected["roles"][0], "organization_unresolved": False}],
                    organizations=[self.organization], unresolved_organization_ids=[],
                    has_unresolved_organizations=False,
                )
                self.assertEqual(source, expected)

    def test_failed_final_pending_send_propagates(self):
        path = self.write_feed("persons", [self.person(1)])
        with FakeBulkServer([bulk_result(400)], delay=0.02) as server:
            with self.assertRaises(RuntimeError):
                self.ingest(path, server)
            self.assertEqual(len(server.requests), 1)

    def test_background_failure_stops_before_submitting_another_batch(self):
        path = self.write_feed("persons", [self.person(identifier) for identifier in range(1, 5)])
        with FakeBulkServer([bulk_result(400)], delay=0.02) as server:
            with self.assertRaises(RuntimeError):
                self.ingest(path, server)
            self.assertEqual(len(server.requests), 1)
            self.assertEqual(server.ids(0), ["1"])

    def test_two_senders_preserve_every_document_with_reordered_responses(self):
        persons = [self.person(identifier) for identifier in range(1, 6)]
        path = self.write_feed("persons", persons)

        def response_delay(body):
            identifier = json.loads(body.splitlines()[0])["index"]["_id"]
            return 0.15 if identifier == "1" else 0.01

        with patch.dict(os.environ, {"BULK_SENDERS": "2"}), FakeBulkServer(
            [bulk_result(201)] * len(persons), delay=response_delay
        ) as server:
            metrics = self.ingest(path, server)
            self.assertEqual(metrics["persons_indexed"], len(persons))
            self.assertEqual(metrics["bulk_requests"], len(persons))
            self.assertEqual(server.max_active, 2)
            self.assertEqual(len(server.requests), len(persons))
            completed_ids = [json.loads(body.splitlines()[0])["index"]["_id"]
                             for body in server.completed_requests]
            self.assertLess(completed_ids.index("2"), completed_ids.index("1"))
            indexed = {}
            for _url, body in server.requests:
                metadata, source = map(json.loads, body.splitlines())
                identifier = metadata["index"]["_id"]
                self.assertNotIn(identifier, indexed)
                indexed[identifier] = source
            self.assertEqual(set(indexed), {str(identifier) for identifier in range(1, 6)})
            for person in persons:
                expected = dict(person["serialized_data"])
                expected.update(
                    roles=[{**expected["roles"][0], "organization_unresolved": False}],
                    organizations=[self.organization], unresolved_organization_ids=[],
                    has_unresolved_organizations=False,
                )
                self.assertEqual(indexed[str(person["id"])], expected)


if __name__ == "__main__":
    unittest.main()
