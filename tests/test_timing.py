"""Zero measurement intervals must not obscure ingestion results."""

import contextlib
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import main


class FinalTimingTests(unittest.TestCase):
    def test_zero_interval_monitor_preserves_original_failure(self):
        with patch("main.time.perf_counter", return_value=100.0), \
                patch("main.read_cpu_stats", return_value={"source": "cgroup_v2", "usage_ns": 1}):
            with self.assertRaisesRegex(ValueError, "fixture input failure"):
                with main.MemoryMonitor() as monitor:
                    raise ValueError("fixture input failure")
        self.assertNotIn("average_cpu_cores", monitor.cpu_metrics)
        self.assertNotIn("container_cpu_seconds", monitor.cpu_metrics)

    def test_zero_interval_failed_run_publishes_final_metrics(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "latest.json"
            with patch.dict(os.environ, {"PIPELINE_MODE": "local", "METRICS_PATH": str(path)}), \
                    patch("main.time.perf_counter", return_value=100.0), \
                    patch("main.run", side_effect=ValueError("fixture input failure")), \
                    contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(main.main(), 1)
            metrics = json.loads(path.read_text())
        self.assertEqual(metrics["status"], "failed")
        self.assertEqual(metrics["error"], "fixture input failure")
        self.assertEqual(metrics["wall_seconds"], 0)
        self.assertEqual(metrics["persons_per_second"], 0)
        self.assertIn("finished_at", metrics)
        self.assertNotIn("average_cpu_cores", metrics)

    def test_zero_interval_success_does_not_invent_throughput(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "latest.json"
            with patch.dict(os.environ, {"PIPELINE_MODE": "local", "METRICS_PATH": str(path)}), \
                    patch("main.time.perf_counter", return_value=100.0), \
                    patch("main.run", side_effect=lambda metrics: metrics.update(persons_indexed=3)), \
                    contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(main.main(), 0)
            metrics = json.loads(path.read_text())
        self.assertEqual(metrics["status"], "completed")
        self.assertEqual(metrics["persons_indexed"], 3)
        self.assertEqual(metrics["persons_per_second"], 0)
        self.assertIn("finished_at", metrics)


if __name__ == "__main__":
    unittest.main()
