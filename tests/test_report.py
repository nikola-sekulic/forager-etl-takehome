import contextlib
import importlib.util
import io
import json
import tempfile
import unittest
from pathlib import Path


spec = importlib.util.spec_from_file_location("performance_report", Path(__file__).resolve().parents[1] / "bench" / "report.py")
report = importlib.util.module_from_spec(spec)
spec.loader.exec_module(report)


class PerformanceReportTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.path = Path(self.temporary.name) / "latest.json"
        self.metrics = {"status": "completed", "persons_indexed": 100,
                        "wall_seconds": 10, "persons_per_second": 10,
                        "peak_pipeline_memory_bytes": 1024 ** 2,
                        "peak_elasticsearch_memory_bytes": None,
                        "roles": 200, "unresolved_org_refs": 4}

    def write(self):
        self.path.write_text(json.dumps(self.metrics), encoding="utf-8")

    def test_completed_report_prints_measured_values(self):
        self.write()
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            report.print_report(self.path)
        self.assertIn("Persons indexed: 100", output.getvalue())
        self.assertIn("10.00", output.getvalue())
        self.assertIn("1.00 MiB", output.getvalue())
        self.assertIn("4 (2.00% of roles)", output.getvalue())
        self.assertIn("not recorded", output.getvalue())

    def test_incomplete_run_cannot_report_success(self):
        for status in ["running", "failed", None]:
            with self.subTest(status=status):
                self.metrics["status"] = status
                self.write()
                with self.assertRaises(ValueError):
                    report.print_report(self.path)

    def test_missing_measurement_is_rejected(self):
        del self.metrics["persons_per_second"]
        self.write()
        with self.assertRaises(ValueError):
            report.print_report(self.path)

    def test_incomplete_distributed_resource_metrics_are_rejected(self):
        self.metrics["worker_metrics_complete"] = False
        self.write()
        with self.assertRaisesRegex(ValueError, "resource measurements are incomplete"):
            report.print_report(self.path)

    def test_missing_metrics_file_is_rejected(self):
        with self.assertRaises(FileNotFoundError):
            report.print_report(self.path)

    def test_resource_sampler_peaks_must_belong_to_same_run(self):
        self.metrics["run_id"] = "current-run"
        self.write()
        self.path.with_name("resource-peaks.json").write_text(json.dumps({
            "run_id": "current-run", "peak_pipeline_memory_bytes": 2 * 1024 ** 2,
            "peak_elasticsearch_memory_bytes": 3 * 1024 ** 2,
        }), encoding="utf-8")
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            report.print_report(self.path)
        self.assertIn("Peak pipeline container memory: 2.00 MiB", output.getvalue())
        self.assertIn("Peak Elasticsearch container memory: 3.00 MiB", output.getvalue())

    def test_previous_run_resource_peaks_are_ignored(self):
        self.metrics["run_id"] = "current-run"
        self.write()
        self.path.with_name("resource-peaks.json").write_text(json.dumps({
            "run_id": "previous-run", "peak_pipeline_memory_bytes": 100 * 1024 ** 2,
            "peak_elasticsearch_memory_bytes": 100 * 1024 ** 2,
        }), encoding="utf-8")
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            report.print_report(self.path)
        self.assertIn("Peak pipeline container memory: 1.00 MiB", output.getvalue())
        self.assertIn("Peak Elasticsearch container memory: not recorded", output.getvalue())

    def test_external_es_lifetime_peak_has_explicit_label(self):
        self.write()
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            report.print_report(self.path, es_peak_bytes=4 * 1024 ** 2)
        self.assertIn("Elasticsearch container lifetime peak: 4.00 MiB", output.getvalue())
        self.assertNotIn("not recorded", output.getvalue())

    def test_external_lifetime_peak_does_not_replace_current_run_peak(self):
        self.metrics["peak_elasticsearch_memory_bytes"] = 3 * 1024 ** 2
        self.write()
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            report.print_report(self.path, es_peak_bytes=100 * 1024 ** 2)
        self.assertIn("Peak Elasticsearch container memory: 3.00 MiB", output.getvalue())
        self.assertIn("Elasticsearch container lifetime peak: 100.00 MiB", output.getvalue())


if __name__ == "__main__":
    unittest.main()
