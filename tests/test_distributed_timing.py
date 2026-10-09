"""Fast distributed failures must publish terminal metrics at clock resolution."""

import os
import tempfile
import unittest
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import MagicMock, patch

from distributed import coordinator_main, worker_main


class DistributedTimingTests(unittest.TestCase):
    def monitor(self):
        monitor = MagicMock()
        monitor.__enter__.return_value.peak = 1234
        monitor.__exit__.return_value = False
        return monitor

    def test_coordinator_input_failure_publishes_failed_metrics_when_clock_is_frozen(self):
        for elapsed in (0, 0.25):
            with self.subTest(elapsed=elapsed), tempfile.TemporaryDirectory() as directory, ExitStack() as stack:
                queue = MagicMock()
                queue.acquire_coordinator.return_value = True
                queue.current.return_value = None
                client = MagicMock()
                metrics_path = Path(directory) / "latest.json"
                stack.enter_context(patch.dict(os.environ, {
                    "DATA_DIR": directory, "METRICS_PATH": str(metrics_path),
                }))
                stack.enter_context(patch("distributed.queue_client", return_value=queue))
                stack.enter_context(patch("distributed.BulkClient", return_value=client))
                stack.enter_context(patch("distributed.HEALTH_MARKER", Path(directory) / "health.json"))
                stack.enter_context(patch("distributed.MemoryMonitor", return_value=self.monitor()))
                stack.enter_context(patch("distributed.time.perf_counter", side_effect=[10, 10 + elapsed]))
                stack.enter_context(patch("distributed.time.monotonic", return_value=10))
                write_metrics = stack.enter_context(patch("distributed.write_metrics"))
                stack.enter_context(patch("distributed.log"))

                self.assertEqual(coordinator_main(), 1)

                path, metrics = write_metrics.call_args.args
                self.assertEqual(path, metrics_path)
                self.assertEqual(metrics["status"], "failed")
                self.assertIn("expected raw gzip feeds", metrics["error"])
                self.assertEqual(metrics["wall_seconds"], elapsed)
                self.assertEqual(metrics["persons_per_second"], 0)
                self.assertEqual(metrics["peak_coordinator_memory_bytes"], 1234)
                self.assertIn("finished_at", metrics)
                queue.release_coordinator.assert_called_once()
                queue.close.assert_called_once()
                client.close.assert_called_once()

    def test_worker_startup_failure_publishes_failed_metrics_when_clock_is_frozen(self):
        for elapsed in (0, 0.25):
            with self.subTest(elapsed=elapsed), ExitStack() as stack:
                queue = MagicMock()
                queue.current.return_value = {
                    "run_id": "test-run", "coordinator_alive": True,
                    "state": "READY", "config": {},
                }
                stack.enter_context(patch("distributed.queue_client", return_value=queue))
                stack.enter_context(patch("distributed.MemoryMonitor", return_value=self.monitor()))
                stack.enter_context(patch("distributed.time.perf_counter", side_effect=[10, 10 + elapsed]))
                stack.enter_context(patch("distributed.time.monotonic", return_value=10))
                stack.enter_context(patch("distributed.read_cpu", side_effect=[2, 2.5]))
                stack.enter_context(patch("distributed.multiprocessing.get_context"))
                stack.enter_context(patch("distributed.ProcessPoolExecutor", side_effect=RuntimeError("test startup failure")))
                write_metrics = stack.enter_context(patch("distributed.write_metrics"))
                stack.enter_context(patch("distributed.log"))

                self.assertEqual(worker_main(), 1)

                _, metrics = write_metrics.call_args.args
                self.assertEqual(metrics["status"], "failed")
                self.assertEqual(metrics["error"], "test startup failure")
                self.assertEqual(metrics["wall_seconds"], elapsed)
                self.assertEqual(metrics["peak_pipeline_memory_bytes"], 1234)
                self.assertEqual(metrics["container_cpu_seconds"], 0.5)
                if elapsed:
                    self.assertEqual(metrics["average_cpu_cores"], 2)
                else:
                    self.assertNotIn("average_cpu_cores", metrics)
                self.assertEqual(queue.register_worker.call_args.args[2], metrics)
                queue.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
