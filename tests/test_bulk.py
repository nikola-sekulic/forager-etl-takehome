"""Exercise the bulk protocol against a real local HTTP server."""

import json
import socket
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from pipeline.bulk import BulkClient, es_request, require_success


class FakeBulkServer:
    def __init__(self, responses, delay=0):
        self.responses = list(responses)
        self.requests = []
        self.completed_requests = []
        self.delay = delay
        self.active = 0
        self.max_active = 0
        self.lock = threading.Lock()
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                with owner.lock:
                    owner.active += 1
                    owner.max_active = max(owner.max_active, owner.active)
                try:
                    body = self.rfile.read(int(self.headers["Content-Length"]))
                    owner.requests.append((self.path, body))
                    if owner.delay:
                        time.sleep(owner.delay(body) if callable(owner.delay) else owner.delay)
                    if not owner.responses:
                        self.send_error(500, "unexpected request")
                        return
                    response = owner.responses.pop(0)
                    if response is None:
                        # The service could have applied the write before this loss.
                        self.connection.shutdown(socket.SHUT_RDWR)
                        self.connection.close()
                        return
                    http_status, payload = response
                    encoded = payload if isinstance(payload, bytes) else json.dumps(payload).encode()
                    self.send_response(http_status)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(encoded)))
                    self.end_headers()
                    self.wfile.write(encoded)
                    owner.completed_requests.append(body)
                finally:
                    with owner.lock:
                        owner.active -= 1

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

    def ids(self, request_number):
        return [json.loads(line)["index"]["_id"]
                for line in self.requests[request_number][1].splitlines()[::2]]


def bulk_result(*statuses):
    items = []
    for status in statuses:
        result = {"status": status}
        if status >= 300:
            result["error"] = {"type": "test_error", "reason": "synthetic service error"}
        items.append({"index": result})
    return 200, {"errors": any(status >= 300 for status in statuses), "items": items}


class BulkClientTests(unittest.TestCase):
    documents = [("person-1", b'{"name":"A"}'),
                 ("person-2", b'{"name":"B"}'),
                 ("person-3", b'{"name":"C"}')]

    def client(self, server, max_retries=2):
        client = BulkClient(server.url, timeout=2, max_retries=max_retries, backoff=0)
        self.addCleanup(client.close)
        return client

    def test_mixed_item_failures_retry_only_failed_documents(self):
        with FakeBulkServer([bulk_result(201, 429, 503), bulk_result(201, 201)]) as server:
            metrics = self.client(server).send(self.documents)
            self.assertEqual(server.ids(0), ["person-1", "person-2", "person-3"])
            self.assertEqual(server.ids(1), ["person-2", "person-3"])
            self.assertEqual(metrics["indexed"], 3)
            self.assertEqual(metrics["retried_documents"], 2)
            self.assertEqual(metrics["bulk_requests"], 2)
            self.assertEqual(metrics["bulk_bytes"], sum(len(body) for _path, body in server.requests))
            for path, body in server.requests:
                self.assertTrue(path.split("?", 1)[0].endswith("/_bulk"))
                self.assertTrue(body.endswith(b"\n"))
                for line in body.splitlines()[::2]:
                    self.assertEqual(json.loads(line)["index"]["_index"], "persons")

    def test_lost_response_resends_identical_ids_and_sources(self):
        with FakeBulkServer([None, bulk_result(201, 201, 201)]) as server:
            metrics = self.client(server).send(self.documents)
            self.assertEqual(server.requests[0][1], server.requests[1][1])
            self.assertEqual(metrics["indexed"], 3)
            self.assertEqual(metrics["retried_documents"], 3)
            self.assertEqual(metrics["bulk_requests"], 2)

    def test_retryable_http_status_resends_whole_batch(self):
        with FakeBulkServer([(503, {"error": "unavailable"}), bulk_result(201, 201, 201)]) as server:
            metrics = self.client(server).send(self.documents)
            self.assertEqual(server.requests[0][1], server.requests[1][1])
            self.assertEqual(metrics["indexed"], 3)
            self.assertEqual(metrics["retried_documents"], 3)

    def test_permanent_item_error_fails_immediately(self):
        with FakeBulkServer([bulk_result(201, 400, 201)]) as server:
            with self.assertRaises(RuntimeError):
                self.client(server).send(self.documents)
            self.assertEqual(len(server.requests), 1)

    def test_permanent_http_error_fails_immediately(self):
        with FakeBulkServer([(400, {"error": "bad_request"})]) as server:
            with self.assertRaises(RuntimeError):
                self.client(server).send(self.documents)
            self.assertEqual(len(server.requests), 1)

    def test_retry_budget_is_finite(self):
        with FakeBulkServer([bulk_result(429), bulk_result(429), bulk_result(429)]) as server:
            with self.assertRaises(RuntimeError):
                self.client(server).send(self.documents[:1])
            self.assertEqual(len(server.requests), 3)

    def test_missing_item_is_never_accepted_as_success(self):
        with FakeBulkServer([bulk_result(201, 201)]) as server:
            with self.assertRaises(RuntimeError):
                self.client(server, max_retries=0).send(self.documents)

    def test_non_json_response_is_never_accepted_as_success(self):
        with FakeBulkServer([(200, b"this is not JSON")]) as server:
            with self.assertRaises(RuntimeError):
                self.client(server, max_retries=0).send(self.documents)

    def test_malformed_item_is_never_accepted_as_success(self):
        with FakeBulkServer([(200, {"errors": False, "items": [{"index": {}}]})]) as server:
            with self.assertRaises(RuntimeError):
                self.client(server, max_retries=0).send(self.documents[:1])

    def test_errors_flag_must_be_boolean_and_match_item_statuses(self):
        responses = [
            {"items": [{"index": {"status": 201}}]},
            {"errors": "false", "items": [{"index": {"status": 201}}]},
            {"errors": True, "items": [{"index": {"status": 201}}]},
            {"errors": False, "items": [{"index": {"status": 503}}]},
        ]
        for result in responses:
            with self.subTest(result=result), FakeBulkServer([(200, result)]) as server:
                with self.assertRaises(RuntimeError):
                    self.client(server).send(self.documents[:1])
                self.assertEqual(len(server.requests), 1)

    def test_success_status_with_error_payload_is_malformed(self):
        result = {"errors": False, "items": [{"index": {
            "status": 201, "error": {"type": "test_error"},
        }}]}
        with FakeBulkServer([(200, result)]) as server:
            with self.assertRaises(RuntimeError):
                self.client(server, max_retries=0).send(self.documents[:1])

    def test_boolean_status_is_not_a_valid_http_status(self):
        result = {"errors": False, "items": [{"index": {"status": True}}]}
        with FakeBulkServer([(200, result)]) as server:
            with self.assertRaises(RuntimeError):
                self.client(server, max_retries=0).send(self.documents[:1])

    def test_http_success_with_finalization_shard_failure_is_rejected(self):
        result = {"_shards": {"total": 1, "successful": 0, "failed": 1,
                              "failures": [{"reason": "synthetic"}]}}
        for endpoint in ["/_flush", "/_refresh"]:
            with self.subTest(endpoint=endpoint), FakeBulkServer([(200, result)]) as server:
                response = es_request(self.client(server), "POST", "/persons" + endpoint)
                with self.assertRaises(RuntimeError):
                    require_success(response, "finalize persons")


if __name__ == "__main__":
    unittest.main()
