"""Read container CPU accounting without estimating from individual workers.

Counters cover every process and thread in the container, including organization
staging. CPU use is the difference between two snapshots; cgroup lifetime totals
must not be presented as the current ingestion's consumption.
"""

import math
from collections.abc import Mapping
from pathlib import Path


def _read_counter(path):
    try:
        value = int(Path(path).read_text().strip())
    except (OSError, ValueError):
        return None
    return value if value >= 0 else None


def _read_stat(path):
    try:
        lines = Path(path).read_text().splitlines()
    except OSError:
        return {}
    counters = {}
    for line in lines:
        fields = line.split()
        if len(fields) != 2:
            continue
        try:
            value = int(fields[1])
        except ValueError:
            continue
        if value >= 0:
            counters[fields[0]] = value
    return counters


def read_cpu_stats():
    """Return normalized nanosecond counters, or None outside readable cgroups.

    v2 reports microseconds; v1 reports nanoseconds. Normalize before calculating
    deltas so the public calculation does not depend on the cgroup version.
    User/system counters and throttling counters are optional on both versions.
    """
    values = _read_stat("/sys/fs/cgroup/cpu.stat")
    if "usage_usec" in values:
        result = {"source": "cgroup_v2", "usage_ns": values["usage_usec"] * 1000}
        for name in ("user", "system", "throttled"):
            if f"{name}_usec" in values:
                result[f"{name}_ns"] = values[f"{name}_usec"] * 1000
        for name in ("nr_periods", "nr_throttled"):
            if name in values:
                result[name] = values[name]
        return result

    for directory in (
        "/sys/fs/cgroup/cpuacct",
        "/sys/fs/cgroup/cpu,cpuacct",
        "/sys/fs/cgroup/cpuacct,cpu",
        "/sys/fs/cgroup/cpu",
        "/sys/fs/cgroup",
    ):
        usage = _read_counter(f"{directory}/cpuacct.usage")
        if usage is None:
            continue
        result = {"source": "cgroup_v1", "usage_ns": usage}
        for filename, name in (("cpuacct.usage_user", "user_ns"),
                               ("cpuacct.usage_sys", "system_ns")):
            counter = _read_counter(f"{directory}/{filename}")
            if counter is not None:
                result[name] = counter
        for stat_path in (
            f"{directory}/cpu.stat",
            "/sys/fs/cgroup/cpu/cpu.stat",
            "/sys/fs/cgroup/cpu,cpuacct/cpu.stat",
            "/sys/fs/cgroup/cpuacct,cpu/cpu.stat",
            "/sys/fs/cgroup/cpu.stat",
        ):
            stats = _read_stat(stat_path)
            if not stats:
                continue
            for name in ("nr_periods", "nr_throttled"):
                if name in stats:
                    result[name] = stats[name]
            if "throttled_time" in stats:
                result["throttled_ns"] = stats["throttled_time"]
            if any(name in stats for name in ("nr_periods", "nr_throttled", "throttled_time")):
                break
        return result
    return None


def _counter_delta(start, end, name):
    before, after = start.get(name), end.get(name)
    if type(before) is not int or type(after) is not int or before < 0 or after < before:
        return None
    return after - before


def cpu_delta(start, end, wall_seconds, quota_cpus=4):
    """Calculate CPU consumed during the measured interval.

    Average cores equal CPU seconds divided by elapsed seconds. Four average
    cores are 100% of a four-CPU quota. Quota utilization is not clamped: short
    intervals can observe scheduling bursts above the nominal quota.

    Throttled time is the kernel's accumulated cgroup throttle counter. It can
    exceed wall time because accounting from different CPUs may overlap; it is
    not a wall-clock idle percentage. Missing/reset counters are omitted.
    """
    if (isinstance(wall_seconds, bool)
            or not isinstance(wall_seconds, (int, float))
            or not math.isfinite(wall_seconds) or wall_seconds <= 0):
        raise ValueError("wall_seconds must be positive and finite")
    if (isinstance(quota_cpus, bool)
            or not isinstance(quota_cpus, (int, float))
            or not math.isfinite(quota_cpus) or quota_cpus <= 0):
        raise ValueError("quota_cpus must be positive and finite")
    if not isinstance(start, Mapping) or not isinstance(end, Mapping):
        return {}
    if start.get("source") != end.get("source"):
        return {}
    usage = _counter_delta(start, end, "usage_ns")
    if usage is None:
        return {}
    cpu_seconds = usage / 1_000_000_000
    average_cores = cpu_seconds / wall_seconds
    result = {
        "container_cpu_seconds": cpu_seconds,
        "average_cpu_cores": average_cores,
        "cpu_quota_utilization_percent": 100 * average_cores / quota_cpus,
    }
    optional_seconds = {
        "user_ns": "container_cpu_user_seconds",
        "system_ns": "container_cpu_system_seconds",
        "throttled_ns": "cpu_cgroup_throttled_seconds",
    }
    for counter, metric in optional_seconds.items():
        difference = _counter_delta(start, end, counter)
        if difference is not None:
            result[metric] = difference / 1_000_000_000
    for counter, metric in (("nr_periods", "cpu_cgroup_periods"),
                            ("nr_throttled", "cpu_cgroup_throttled_periods")):
        difference = _counter_delta(start, end, counter)
        if difference is not None:
            result[metric] = difference
    return result
