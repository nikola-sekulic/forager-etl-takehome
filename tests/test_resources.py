import unittest
from unittest.mock import patch

from resources import cpu_delta, read_cpu_stats


class CPUAccountingTests(unittest.TestCase):
    def read_with_files(self, files):
        def read_text(path):
            try:
                return files[path.as_posix()]
            except KeyError:
                raise FileNotFoundError(path) from None

        with patch("resources.Path.read_text", read_text):
            return read_cpu_stats()

    def test_v2_normalizes_microseconds_and_preserves_counts(self):
        stats = self.read_with_files({"/sys/fs/cgroup/cpu.stat": (
            "usage_usec 14000001\nuser_usec 11000000\nsystem_usec 3000001\n"
            "nr_periods 120\nnr_throttled 9\nthrottled_usec 123456\n")})
        self.assertEqual(stats, {"source": "cgroup_v2", "usage_ns": 14000001000,
                                "user_ns": 11000000000, "system_ns": 3000001000,
                                "nr_periods": 120, "nr_throttled": 9,
                                "throttled_ns": 123456000})

    def test_v1_split_controllers_preserve_nanoseconds(self):
        stats = self.read_with_files({
            "/sys/fs/cgroup/cpuacct/cpuacct.usage": "5000000001\n",
            "/sys/fs/cgroup/cpuacct/cpuacct.usage_user": "4000000000\n",
            "/sys/fs/cgroup/cpuacct/cpuacct.usage_sys": "1000000001\n",
            "/sys/fs/cgroup/cpu/cpu.stat": (
                "nr_periods 31\nnr_throttled 7\nthrottled_time 2222222222\n"),
        })
        self.assertEqual(stats, {"source": "cgroup_v1", "usage_ns": 5000000001,
                                "user_ns": 4000000000, "system_ns": 1000000001,
                                "nr_periods": 31, "nr_throttled": 7,
                                "throttled_ns": 2222222222})

    def test_v1_combined_controller_and_root_fallback(self):
        for directory in ("/sys/fs/cgroup/cpu,cpuacct", "/sys/fs/cgroup/cpuacct,cpu",
                          "/sys/fs/cgroup/cpu", "/sys/fs/cgroup"):
            with self.subTest(directory=directory):
                stats = self.read_with_files({f"{directory}/cpuacct.usage": "15",
                                             f"{directory}/cpu.stat": "nr_periods 4"})
                self.assertEqual(stats, {"source": "cgroup_v1", "usage_ns": 15,
                                        "nr_periods": 4})

    def test_no_cgroup_accounting_is_unavailable_not_zero(self):
        self.assertIsNone(self.read_with_files({}))

    def test_malformed_negative_and_unknown_v2_fields(self):
        stats = self.read_with_files({"/sys/fs/cgroup/cpu.stat": (
            "usage_usec 0\nuser_usec -1\nsystem_usec nan\n"
            "nr_periods wrong\nthrottled_usec 5 extra\nfuture_field 8\n")})
        self.assertEqual(stats, {"source": "cgroup_v2", "usage_ns": 0})

    def test_invalid_v2_usage_can_fall_back_to_v1(self):
        stats = self.read_with_files({"/sys/fs/cgroup/cpu.stat": "usage_usec wrong",
                                     "/sys/fs/cgroup/cpuacct/cpuacct.usage": "50"})
        self.assertEqual(stats, {"source": "cgroup_v1", "usage_ns": 50})

    def test_delta_measures_whole_container_cpu_against_quota(self):
        start = {"source": "cgroup_v2", "usage_ns": 100000000000,
                 "user_ns": 80000000000, "system_ns": 20000000000,
                 "nr_periods": 50, "nr_throttled": 20, "throttled_ns": 2000000000}
        end = {"source": "cgroup_v2", "usage_ns": 130000000000,
               "user_ns": 104000000000, "system_ns": 26000000000,
               "nr_periods": 150, "nr_throttled": 45, "throttled_ns": 15000000000}
        self.assertEqual(cpu_delta(start, end, 10), {
            "container_cpu_seconds": 30.0, "average_cpu_cores": 3.0,
            "cpu_quota_utilization_percent": 75.0,
            "container_cpu_user_seconds": 24.0, "container_cpu_system_seconds": 6.0,
            "cpu_cgroup_periods": 100, "cpu_cgroup_throttled_periods": 25,
            "cpu_cgroup_throttled_seconds": 13.0,
        })

    def test_missing_optional_counters_do_not_invent_zero_metrics(self):
        result = cpu_delta({"usage_ns": 0}, {"usage_ns": 2000000000}, 1)
        self.assertEqual(result, {"container_cpu_seconds": 2.0,
                                  "average_cpu_cores": 2.0,
                                  "cpu_quota_utilization_percent": 50.0})

    def test_missing_reset_or_changed_accounting_returns_no_measurement(self):
        cases = [(None, {"usage_ns": 1}), ({"usage_ns": 0}, None),
                 ({"usage_ns": 2}, {"usage_ns": 1}),
                 ({"source": "cgroup_v1", "usage_ns": 0},
                  {"source": "cgroup_v2", "usage_ns": 100}),
                 ({"usage_ns": -1}, {"usage_ns": 100}),
                 ({"usage_ns": True}, {"usage_ns": 100}),
                 ({}, {"usage_ns": 1})]
        for start, end in cases:
            with self.subTest(start=start, end=end):
                self.assertEqual(cpu_delta(start, end, 1), {})

    def test_optional_reset_does_not_invalidate_usage(self):
        result = cpu_delta({"usage_ns": 0, "nr_throttled": 9},
                           {"usage_ns": 1000000000, "nr_throttled": 1}, 1)
        self.assertEqual(result["container_cpu_seconds"], 1)
        self.assertNotIn("cpu_cgroup_throttled_periods", result)

    def test_burst_measurement_above_quota_is_not_clamped(self):
        result = cpu_delta({"usage_ns": 0}, {"usage_ns": 5000000000}, 1, 4)
        self.assertEqual(result["cpu_quota_utilization_percent"], 125)

    def test_invalid_elapsed_or_quota_is_rejected(self):
        for value in (0, -1, float("nan"), float("inf"), True, "4"):
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    cpu_delta(None, None, value)
                with self.assertRaises(ValueError):
                    cpu_delta(None, None, 1, value)


if __name__ == "__main__":
    unittest.main()
