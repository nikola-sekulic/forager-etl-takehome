"""Bounds and lifecycle checks for the optional distributed ingestion path."""

import threading
import unittest
from unittest.mock import patch

from distributed import Heartbeat, work_loop
from main import person_records


class PrefetchStore:
    def __init__(self):
        self.chunks = []
        self.active = False

    def prefetch(self, ids):
        self.chunks.append(set(ids))
        self.active = True

    def clear_prefetch(self):
        self.active = False


class DistributedLifecycleTests(unittest.TestCase):
    def test_lost_heartbeat_cancels_worker_and_raises(self):
        cancel = threading.Event()
        with Heartbeat(lambda: False, cancel, interval=0.005) as heartbeat:
            self.assertTrue(cancel.wait(1))
        with self.assertRaisesRegex(RuntimeError, "heartbeat failed"):
            heartbeat.check()

    def test_prefetch_chunks_respect_record_limit_and_keep_every_source(self):
        store = PrefetchStore()
        rows = [(i, {"id": i, "serialized_data": {"roles": [{"organization_id": i}]}}, 20)
                for i in range(600)]
        with patch("main.iter_records", return_value=iter(rows)):
            output = list(person_records("unused", store))
        self.assertEqual([row[0] for row in output], list(range(600)))
        self.assertEqual([len(chunk) for chunk in store.chunks], [256, 256, 88])
        self.assertFalse(store.active)

    def test_prefetch_chunks_respect_source_byte_limit(self):
        store = PrefetchStore()
        rows = [(i, {"serialized_data": {"roles": [{"organization_id": i}]}}, 800_000)
                for i in range(5)]
        with patch("main.iter_records", return_value=iter(rows)):
            self.assertEqual(len(list(person_records("unused", store))), 5)
        self.assertEqual([len(chunk) for chunk in store.chunks], [2, 2, 1])

    def test_closing_generator_releases_overlay(self):
        store = PrefetchStore()
        with patch("main.iter_records", return_value=iter([(1, {"serialized_data": {}}, 20)])):
            records = person_records("unused", store)
            next(records)
            self.assertTrue(store.active)
            records.close()
        self.assertFalse(store.active)

    def test_large_reference_list_uses_generation_check_and_normal_join_fallback(self):
        store = PrefetchStore()
        envelope = {"serialized_data": {"roles": [{"organization_id": i} for i in range(2001)]}}
        with patch("main.iter_records", return_value=iter([(1, envelope, 50_000)])):
            self.assertEqual(list(person_records("unused", store)), [(1, envelope)])
        self.assertEqual(store.chunks, [set()])

    def test_job_heartbeat_stops_before_terminal_acknowledgement(self):
        cancel = threading.Event()
        events = []

        class Queue:
            def __init__(self):
                self.claims = 0

            def claim(self, *_):
                self.claims += 1
                return {"job_id": "0", "path": "file.gz", "token": "token", "attempt": 1} if self.claims == 1 else None

            def complete(self, *_):
                events.append("complete")
                return True

            def status(self, *_):
                return {"state": "RUNNING", "completed": 1, "total_jobs": 1}

            def close(self):
                pass

        class JobHeartbeat:
            def __init__(self, *_):
                pass

            def __enter__(self):
                events.append("heartbeat_start")
                return self

            def check(self):
                pass

            def __exit__(self, *_):
                events.append("heartbeat_stop")

        settings = {"url": "unused", "batch_bytes": 1024, "batch_docs": 10, "cache_bytes": 100}
        with patch("main._cancel", cancel), patch("distributed.queue_client", return_value=Queue()), \
             patch("distributed._current_generation", return_value=True), \
             patch("distributed.Heartbeat", JobHeartbeat), patch("distributed.ingest_file", return_value={}):
            self.assertEqual(work_loop("run", {"organization_namespace": "ns"}, "owner", settings), {"jobs_completed": 1})
        self.assertLess(events.index("heartbeat_stop"), events.index("complete"))


if __name__ == "__main__":
    unittest.main()
