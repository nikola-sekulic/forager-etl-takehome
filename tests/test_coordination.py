"""Real Redis coordination checks using isolated disposable namespaces.

Set TEST_REDIS_URL=redis://redis:6379/15 in Docker. No test flushes a database or
touches another namespace. Lease timings use Redis TIME, including expiry before
any replacement worker has reclaimed a job.
"""

import os
import time
import unittest
import uuid
from concurrent.futures import ThreadPoolExecutor

from pipeline.coordination import RedisRunQueue


REDIS_URL = os.environ.get("TEST_REDIS_URL") or os.environ.get("REDIS_TEST_URL")


@unittest.skipUnless(REDIS_URL, "set TEST_REDIS_URL to exercise real Redis coordination")
class RedisCoordinationTests(unittest.TestCase):
    def setUp(self):
        self.namespace = "coord-test-" + uuid.uuid4().hex
        self.queue = RedisRunQueue(REDIS_URL, self.namespace)
        self.addCleanup(self.cleanup)
        self.owner = "coordinator-" + uuid.uuid4().hex
        self.assertTrue(self.queue.acquire_coordinator(self.owner))
        self.run_id = uuid.uuid4().hex

    def cleanup(self):
        keys = list(self.queue.client.scan_iter(match=self.namespace + ":*"))
        if keys:
            self.queue.client.delete(*keys)
        self.queue.close()

    def start(self, count=1):
        self.queue.start(self.run_id, [f"/data/person/fixture-{index}.json.gz" for index in range(count)],
                         {"coordinator_owner": self.owner, "fixture": True})

    def ready(self, count=1):
        self.start(count)
        self.assertTrue(self.queue.ready(self.run_id))

    def test_preparing_generation_is_published_before_jobs_are_claimable(self):
        self.start(2)
        current = self.queue.current()
        self.assertEqual(current["run_id"], self.run_id)
        self.assertEqual(current["state"], "PREPARING")
        self.assertEqual(current["config"]["coordinator_owner"], self.owner)
        self.assertTrue(current["coordinator_alive"])
        self.assertIsNone(self.queue.claim(self.run_id, "worker"))
        self.assertTrue(self.queue.ready(self.run_id))
        self.assertIsNotNone(self.queue.claim(self.run_id, "worker"))

    def test_concurrent_workers_claim_each_job_once(self):
        self.ready(8)
        with ThreadPoolExecutor(max_workers=12) as pool:
            claimed = list(pool.map(lambda index: self.queue.claim(self.run_id, f"worker-{index}"), range(12)))
        jobs = [job for job in claimed if job is not None]
        self.assertEqual(len(jobs), 8)
        self.assertEqual({job["job_id"] for job in jobs}, {str(index) for index in range(8)})
        self.assertTrue(all(job["attempt"] == 1 for job in jobs))
        status = self.queue.status(self.run_id)
        self.assertEqual((status["pending"], status["leased"], status["completed"]), (0, 8, 0))

    def test_repeated_ready_acknowledgement_is_idempotent_while_lock_is_owned(self):
        self.ready()
        self.assertTrue(self.queue.ready(self.run_id))
        self.assertIsNotNone(self.queue.claim(self.run_id, "worker"))
        self.assertTrue(self.queue.ready(self.run_id))
        self.assertEqual(self.queue.status(self.run_id)["state"], "RUNNING")

    def test_terminal_stats_are_counted_once_and_wrong_token_is_rejected(self):
        self.ready()
        job = self.queue.claim(self.run_id, "worker")
        self.assertFalse(self.queue.complete(self.run_id, job["job_id"], "wrong-token", {"persons_indexed": 999}))
        self.assertTrue(self.queue.complete(self.run_id, job["job_id"], job["token"], {"persons_indexed": 7}))
        self.assertTrue(self.queue.complete(self.run_id, job["job_id"], job["token"], {"persons_indexed": 999}))
        status = self.queue.status(self.run_id)
        self.assertEqual(status["completed"], 1)
        self.assertEqual(status["stats"], [{"persons_indexed": 7}])
        self.assertEqual(status["leased"], 0)

    def test_expired_lease_cannot_renew_complete_or_fail_before_reclamation(self):
        self.ready()
        job = self.queue.claim(self.run_id, "dead-worker", lease_seconds=0.05)
        time.sleep(0.08)
        self.assertFalse(self.queue.renew(self.run_id, job["job_id"], job["token"], lease_seconds=1))
        self.assertFalse(self.queue.complete(self.run_id, job["job_id"], job["token"], {"persons_indexed": 7}))
        self.assertFalse(self.queue.fail(self.run_id, job["job_id"], job["token"], "stale failure"))
        self.assertEqual(self.queue.status(self.run_id)["completed"], 0)

    def test_dead_worker_job_is_reclaimed_with_new_token_and_attempt(self):
        self.ready()
        original = self.queue.claim(self.run_id, "dead-worker", lease_seconds=0.05)
        time.sleep(0.08)
        replacement = self.queue.claim(self.run_id, "replacement", lease_seconds=1)
        self.assertEqual(replacement["job_id"], original["job_id"])
        self.assertEqual(replacement["path"], original["path"])
        self.assertEqual(replacement["attempt"], 2)
        self.assertNotEqual(replacement["token"], original["token"])
        self.assertFalse(self.queue.complete(self.run_id, original["job_id"], original["token"], {}))
        self.assertTrue(self.queue.complete(self.run_id, replacement["job_id"], replacement["token"],
                                            {"persons_indexed": 7}))

    def test_valid_lease_renews_and_remains_owned(self):
        self.ready()
        job = self.queue.claim(self.run_id, "worker", lease_seconds=0.1)
        self.assertTrue(self.queue.renew(self.run_id, job["job_id"], job["token"], lease_seconds=1))
        time.sleep(0.12)
        self.assertIsNone(self.queue.claim(self.run_id, "other-worker"))
        self.assertTrue(self.queue.complete(self.run_id, job["job_id"], job["token"], {}))

    def test_permanent_job_failure_makes_whole_run_terminal(self):
        self.ready(2)
        failed = self.queue.claim(self.run_id, "failed-worker")
        other = self.queue.claim(self.run_id, "other-worker")
        self.assertTrue(self.queue.fail(self.run_id, failed["job_id"], failed["token"], "synthetic failure"))
        self.assertTrue(self.queue.fail(self.run_id, failed["job_id"], failed["token"], "replacement reason"))
        status = self.queue.status(self.run_id)
        self.assertEqual(status["state"], "FAILED")
        self.assertEqual(status["failures"], {failed["job_id"]: "synthetic failure"})
        self.assertIsNone(self.queue.claim(self.run_id, "new-worker"))
        self.assertFalse(self.queue.renew(self.run_id, other["job_id"], other["token"]))
        self.assertFalse(self.queue.complete(self.run_id, other["job_id"], other["token"], {}))

    def test_finalization_requires_all_job_acknowledgements(self):
        self.ready()
        self.assertFalse(self.queue.set_state(self.run_id, "FINALIZING"))
        self.assertFalse(self.queue.set_state(self.run_id, "COMPLETED"))
        job = self.queue.claim(self.run_id, "worker")
        self.assertTrue(self.queue.complete(self.run_id, job["job_id"], job["token"], {}))
        self.assertTrue(self.queue.set_state(self.run_id, "FINALIZING"))
        self.assertTrue(self.queue.set_state(self.run_id, "COMPLETED"))
        self.assertIsNone(self.queue.claim(self.run_id, "late-worker"))

    def test_coordinator_lock_is_exclusive_and_stale_owner_cannot_release_replacement(self):
        other = RedisRunQueue(REDIS_URL, self.namespace)
        self.addCleanup(other.close)
        self.assertFalse(other.acquire_coordinator("replacement"))
        self.assertFalse(other.renew_coordinator("wrong-owner"))
        self.assertFalse(other.release_coordinator("wrong-owner"))
        self.assertTrue(self.queue.release_coordinator(self.owner))
        self.assertTrue(other.acquire_coordinator("replacement"))
        self.assertFalse(self.queue.renew_coordinator(self.owner))
        self.assertFalse(self.queue.release_coordinator(self.owner))
        self.assertTrue(other.renew_coordinator("replacement"))

    def test_stale_coordinator_cannot_publish_ready_or_finalize(self):
        self.start()
        other = RedisRunQueue(REDIS_URL, self.namespace)
        self.addCleanup(other.close)
        self.assertTrue(self.queue.release_coordinator(self.owner))
        self.assertTrue(other.acquire_coordinator("replacement"))
        self.assertFalse(self.queue.renew_coordinator(self.owner))
        with self.assertRaises(RuntimeError):
            self.queue.ready(self.run_id)
        with self.assertRaises(RuntimeError):
            self.queue.set_state(self.run_id, "FAILED")

    def test_start_refuses_an_interrupted_active_generation(self):
        self.start()
        with self.assertRaises(RuntimeError):
            self.queue.start(uuid.uuid4().hex, ["/data/person/replacement.json.gz"], {})
        self.assertEqual(self.queue.current()["run_id"], self.run_id)
        self.assertEqual(self.queue.current()["state"], "PREPARING")

    def test_worker_measurements_are_registered_separately_from_terminal_stats(self):
        self.ready()
        self.queue.register_worker(self.run_id, "worker-1", {"peak_pipeline_memory_bytes": 20})
        self.queue.register_worker(self.run_id, "worker-2", {"peak_pipeline_memory_bytes": 30})
        self.assertEqual(self.queue.worker_metrics(self.run_id), [
            {"peak_pipeline_memory_bytes": 20}, {"peak_pipeline_memory_bytes": 30},
        ])
        self.assertEqual(self.queue.status(self.run_id)["stats"], [])


if __name__ == "__main__":
    unittest.main()
