"""Verify final run failures using real gzip, LMDB, workers, and local HTTP."""

import contextlib
import gzip
import io
import json
import os
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest.mock import patch

import main


class FixtureElasticsearch:
    """A deterministic HTTP boundary that overwrites stable document IDs."""

    def __init__(self):
        self.documents = {}
        self.created = False
        self.finalized = []
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def reply(self, status, body=None):
                encoded = json.dumps(body or {}).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(encoded)))
                self.end_headers()
                if self.command != "HEAD":
                    self.wfile.write(encoded)

            def do_GET(self):
                if self.path == "/":
                    self.reply(200, {"version": {"number": "8.13.4"}})
                elif self.path == "/persons/_count":
                    self.reply(200, {"count": len(owner.documents), "_shards": {
                        "total": 1, "successful": 1, "failed": 0,
                    }})
                else:
                    self.reply(404)

            def do_HEAD(self):
                self.reply(200 if owner.created else 404)

            def do_PUT(self):
                self.rfile.read(int(self.headers["Content-Length"]))
                if self.path == "/persons":
                    owner.created = True
                self.reply(200, {"acknowledged": True})

            def do_POST(self):
                if self.path.split("?", 1)[0] == "/_bulk":
                    body = self.rfile.read(int(self.headers["Content-Length"]))
                    lines = body.splitlines()
                    items = []
                    for index in range(0, len(lines), 2):
                        identifier = json.loads(lines[index])["index"]["_id"]
                        existed = identifier in owner.documents
                        owner.documents[identifier] = json.loads(lines[index + 1])
                        items.append({"index": {"status": 200 if existed else 201}})
                    self.reply(200, {"errors": False, "items": items})
                else:
                    owner.finalized.append(self.path.split("?", 1)[0])
                    self.reply(200, {"_shards": {
                        "total": 1, "successful": 1, "failed": 0,
                    }})

            def log_message(self, *_args):
                pass

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.url = f"http://127.0.0.1:{self.server.server_port}"

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *_args):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()


class RunFailureTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.data = self.root / "data"
        self.stage = self.root / "stage"
        self.metrics_path = self.root / "metrics" / "latest.json"
        self.organization_path = self.write_feed("organization", [
            {"id": 8, "serialized_data": {"forager_id": 8, "name": "Fixture"}},
        ])
        self.person_path = self.write_feed("person", [
            {"id": 1, "serialized_data": {"forager_id": 1, "roles": [
                {"organization_id": 8, "role_title": "Fixture role"},
            ]}},
        ])
        environment = patch.dict(os.environ, {
            "PIPELINE_MODE": "local", "DATA_DIR": str(self.data),
            "STAGE_DIR": str(self.stage), "METRICS_PATH": str(self.metrics_path),
            "RESET_INDEX": "0", "WORKERS": "1", "STAGE_WORKERS": "1",
            "BULK_BYTES": "1048576", "BULK_DOCS": "1", "BULK_SENDERS": "1",
            "BULK_OVERLAP": "1", "ORG_CACHE_BYTES": "1024", "INDEX_SHARDS": "1",
        })
        environment.start()
        self.addCleanup(environment.stop)
        allocator = patch("store.STORE_MAP_BYTES", 8 * 1024 ** 2)
        allocator.start()
        self.addCleanup(allocator.stop)

    def write_feed(self, kind, rows):
        folder = self.data / kind
        folder.mkdir(parents=True, exist_ok=True)
        path = folder / "fixture.json.gz"
        with gzip.open(path, "wt", encoding="utf-8") as stream:
            for row in rows:
                stream.write(json.dumps(row) + "\n")
        return path

    def test_duplicate_person_ids_fail_after_acknowledged_writes_and_finalization(self):
        self.write_feed("person", [
            {"id": 1, "serialized_data": {"forager_id": 1, "roles": [], "marker": "first"}},
            {"id": 1, "serialized_data": {"forager_id": 1, "roles": [], "marker": "second"}},
        ])
        metrics = {}
        with FixtureElasticsearch() as server, patch.dict(os.environ, {"ES_URL": server.url}), \
                contextlib.redirect_stdout(io.StringIO()):
            with self.assertRaisesRegex(ValueError, "count mismatch: ES=1, acknowledged=2"):
                main.run(metrics)
            self.assertEqual(metrics["persons_indexed"], 2)
            self.assertEqual(metrics["elasticsearch_person_count"], 1)
            self.assertEqual(server.documents["1"]["marker"], "second")
            self.assertEqual(server.finalized, ["/persons/_flush", "/persons/_refresh"])
        self.assertEqual(list(self.stage.iterdir()), [])

    def test_malformed_feed_writes_failed_metrics_and_returns_nonzero(self):
        self.write_feed("organization", [
            {"id": 8, "serialized_data": [], "private_fixture": "must-not-be-logged"},
        ])
        output = io.StringIO()
        with FixtureElasticsearch() as server, patch.dict(os.environ, {"ES_URL": server.url}), \
                contextlib.redirect_stdout(output):
            self.assertEqual(main.main(), 1)
            self.assertEqual(server.documents, {})
            self.assertEqual(server.finalized, [])
        metrics = json.loads(self.metrics_path.read_text(encoding="utf-8"))
        self.assertEqual(metrics["status"], "failed")
        self.assertIn("serialized_data must be an object", metrics["error"])
        self.assertGreater(metrics["wall_seconds"], 0)
        self.assertEqual(metrics["persons_per_second"], 0)
        self.assertIn("finished_at", metrics)
        self.assertNotIn("must-not-be-logged", output.getvalue())
        self.assertNotIn("must-not-be-logged", json.dumps(metrics))
        self.assertEqual(list(self.stage.iterdir()), [])


if __name__ == "__main__":
    unittest.main()
