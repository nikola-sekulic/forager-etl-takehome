"""Redis run state and expiring, token-fenced file jobs for scaled workers.

Only the dedicated coordinator initializes/finalizes a run. Workers replay an
expired file job using stable Elasticsearch IDs; Redis accepts its terminal
statistics once. Redis TIME keeps lease decisions independent of client clocks.
"""

from __future__ import annotations

import json
import math
import re
import uuid
from pathlib import Path
from typing import Any

import redis


_CLAIM = """
local state = redis.call('HGET', KEYS[1], 'state')
if state ~= 'READY' and state ~= 'RUNNING' then return nil end
local clock = redis.call('TIME')
local now = tonumber(clock[1]) + tonumber(clock[2]) / 1000000
for _, id in ipairs(redis.call('ZRANGEBYSCORE', KEYS[3], '-inf', now)) do
    local job = ARGV[1] .. id
    if redis.call('HGET', job, 'state') == 'leased' then
        redis.call('HSET', job, 'state', 'pending', 'token', '', 'owner', '')
        redis.call('RPUSH', KEYS[2], id)
    end
    redis.call('ZREM', KEYS[3], id)
end
local id = redis.call('LPOP', KEYS[2])
while id do
    local job = ARGV[1] .. id
    if redis.call('HGET', job, 'state') == 'pending' then
        local attempt = redis.call('HINCRBY', job, 'attempt', 1)
        redis.call('HSET', job, 'state', 'leased', 'token', ARGV[3], 'owner', ARGV[2])
        redis.call('ZADD', KEYS[3], now + tonumber(ARGV[4]), id)
        redis.call('HSET', KEYS[1], 'state', 'RUNNING')
        return cjson.encode({job_id=id, path=redis.call('HGET', job, 'path'),
                             token=ARGV[3], attempt=attempt})
    end
    id = redis.call('LPOP', KEYS[2])
end
return nil
"""

_RENEW = """
local state = redis.call('HGET', KEYS[1], 'state')
if state ~= 'READY' and state ~= 'RUNNING' then return 0 end
local clock = redis.call('TIME')
local now = tonumber(clock[1]) + tonumber(clock[2]) / 1000000
local expiry = redis.call('ZSCORE', KEYS[2], ARGV[1])
if not expiry or tonumber(expiry) <= now then return 0 end
if redis.call('HGET', KEYS[3], 'state') ~= 'leased' or
   redis.call('HGET', KEYS[3], 'token') ~= ARGV[2] then return 0 end
redis.call('ZADD', KEYS[2], now + tonumber(ARGV[3]), ARGV[1])
return 1
"""

_COMPLETE = """
if redis.call('HGET', KEYS[3], 'state') == 'done' then
    if redis.call('HGET', KEYS[3], 'token') == ARGV[2] then return 1 end
    return 0
end
local state = redis.call('HGET', KEYS[1], 'state')
if state ~= 'READY' and state ~= 'RUNNING' then return 0 end
local clock = redis.call('TIME')
local now = tonumber(clock[1]) + tonumber(clock[2]) / 1000000
local expiry = redis.call('ZSCORE', KEYS[2], ARGV[1])
if not expiry or tonumber(expiry) <= now then return 0 end
if redis.call('HGET', KEYS[3], 'state') ~= 'leased' or
   redis.call('HGET', KEYS[3], 'token') ~= ARGV[2] then return 0 end
redis.call('HSET', KEYS[3], 'state', 'done')
redis.call('ZREM', KEYS[2], ARGV[1])
redis.call('HSET', KEYS[4], ARGV[1], ARGV[3])
redis.call('HINCRBY', KEYS[1], 'completed', 1)
return 1
"""

_FAIL = """
if redis.call('HGET', KEYS[3], 'state') == 'failed' then
    if redis.call('HGET', KEYS[3], 'token') == ARGV[2] then return 1 end
    return 0
end
local state = redis.call('HGET', KEYS[1], 'state')
if state ~= 'READY' and state ~= 'RUNNING' then return 0 end
local clock = redis.call('TIME')
local now = tonumber(clock[1]) + tonumber(clock[2]) / 1000000
local expiry = redis.call('ZSCORE', KEYS[2], ARGV[1])
if not expiry or tonumber(expiry) <= now then return 0 end
if redis.call('HGET', KEYS[3], 'state') ~= 'leased' or
   redis.call('HGET', KEYS[3], 'token') ~= ARGV[2] then return 0 end
redis.call('HSET', KEYS[3], 'state', 'failed')
redis.call('ZREM', KEYS[2], ARGV[1])
redis.call('HSET', KEYS[4], ARGV[1], ARGV[3])
redis.call('HSET', KEYS[1], 'state', 'FAILED')
return 1
"""

_READY = """
if redis.call('GET', KEYS[2]) ~= ARGV[1] then return 0 end
local state = redis.call('HGET', KEYS[1], 'state')
if state == 'READY' or state == 'RUNNING' then return 1 end
if state ~= 'PREPARING' then return 0 end
redis.call('HSET', KEYS[1], 'state', 'READY')
return 1
"""

_SET_STATE = """
if redis.call('GET', KEYS[2]) ~= ARGV[1] then return 0 end
local state = redis.call('HGET', KEYS[1], 'state')
if not state then return 0 end
if state == ARGV[2] then return 1 end
if state == 'FAILED' or state == 'COMPLETED' then return 0 end
if ARGV[2] == 'FINALIZING' then
    if tonumber(redis.call('HGET', KEYS[1], 'completed')) ~=
       tonumber(redis.call('HGET', KEYS[1], 'total_jobs')) then return 0 end
elseif ARGV[2] == 'COMPLETED' then
    if state ~= 'FINALIZING' then return 0 end
elseif ARGV[2] ~= 'FAILED' then return 0 end
redis.call('HSET', KEYS[1], 'state', ARGV[2])
return 1
"""

_RENEW_COORDINATOR = """
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
redis.call('PEXPIRE', KEYS[1], ARGV[2])
return 1
"""

_RELEASE_COORDINATOR = """
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
return redis.call('DEL', KEYS[1])
"""


def _json(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), ensure_ascii=True, allow_nan=False)


def _seconds(value: float) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError("lease duration must be a positive number")
    if not math.isfinite(value) or value <= 0:
        raise ValueError("lease duration must be a positive number")
    return value


class RedisRunQueue:
    """Run coordination; lease holders never share stateful HTTP clients."""

    def __init__(self, redis_url: str, namespace: str = "etl"):
        if not isinstance(namespace, str) or not namespace or any(ord(c) < 32 for c in namespace):
            raise ValueError("invalid Redis namespace")
        self.namespace = namespace.rstrip(":")
        if not self.namespace:
            raise ValueError("invalid Redis namespace")
        self.client = redis.Redis.from_url(redis_url, decode_responses=True,
                                          socket_connect_timeout=10, socket_timeout=10)
        self.coordinator_key = f"{self.namespace}:coordinator"
        self.current_key = f"{self.namespace}:current"
        self._coordinator_owner: str | None = None
        self._scripts = {
            name: self.client.register_script(script)
            for name, script in (("claim", _CLAIM), ("renew", _RENEW),
                                 ("complete", _COMPLETE), ("fail", _FAIL),
                                 ("ready", _READY), ("state", _SET_STATE),
                                 ("renew_coordinator", _RENEW_COORDINATOR),
                                 ("release_coordinator", _RELEASE_COORDINATOR))
        }

    def _base(self, run_id: str) -> str:
        if not isinstance(run_id, str) or not re.fullmatch(r"[A-Za-z0-9._-]{1,128}", run_id):
            raise ValueError("invalid run identifier")
        return f"{self.namespace}:run:{run_id}"

    def _owner(self) -> str:
        if not self._coordinator_owner:
            raise RuntimeError("this client does not hold the coordinator lease")
        return self._coordinator_owner

    def acquire_coordinator(self, owner: str, ttl: float = 120) -> bool:
        if not isinstance(owner, str) or not owner:
            raise ValueError("coordinator owner must be a nonempty string")
        acquired = bool(self.client.set(self.coordinator_key, owner, nx=True,
                                        px=max(1, math.ceil(_seconds(ttl) * 1000))))
        if acquired:
            self._coordinator_owner = owner
        return acquired

    def renew_coordinator(self, owner: str, ttl: float = 120) -> bool:
        renewed = bool(self._scripts["renew_coordinator"](
            keys=[self.coordinator_key], args=[owner, max(1, math.ceil(_seconds(ttl) * 1000))]))
        if renewed:
            self._coordinator_owner = owner
        return renewed

    def release_coordinator(self, owner: str) -> bool:
        released = bool(self._scripts["release_coordinator"](keys=[self.coordinator_key], args=[owner]))
        if self._coordinator_owner == owner:
            self._coordinator_owner = None
        return released

    def start(self, run_id: str, jobs: list[str | Path | dict], config: dict) -> dict:
        """Publish a fresh generation atomically, only while owning the lock.

        String/Path jobs get stable positional IDs. Dict jobs provide ``path``
        and an optional ``job_id``. Refuse an interrupted active generation:
        queue fencing cannot cancel Elasticsearch writes already in flight.
        Operators must stop old workers before an explicit failed-run rebuild.
        """
        base = self._base(run_id)
        owner = self._owner()
        if not isinstance(config, dict):
            raise ValueError("run config must be an object")
        config_json = _json(config)
        normalized = []
        for index, job in enumerate(jobs):
            job_id = str(job.get("job_id", index)) if isinstance(job, dict) else str(index)
            path = str(job["path"]) if isinstance(job, dict) else str(job)
            if not job_id or not path:
                raise ValueError("job identifiers and paths must be nonempty")
            normalized.append((job_id, path))
        if not normalized or len({job_id for job_id, _path in normalized}) != len(normalized):
            raise ValueError("a run needs unique nonempty jobs")
        with self.client.pipeline() as pipe:
            pipe.watch(base, self.current_key, self.coordinator_key)
            if pipe.get(self.coordinator_key) != owner:
                raise RuntimeError("coordinator lease lost before run initialization")
            if pipe.exists(base):
                raise ValueError("run identifier already exists")
            previous = pipe.get(self.current_key)
            previous_base = self._base(previous) if previous else None
            if previous_base:
                pipe.watch(previous_base)
                previous_state = pipe.hget(previous_base, "state")
            else:
                previous_state = None
            if previous_state not in (None, "FAILED", "COMPLETED"):
                raise RuntimeError("previous run is still active; stop old workers and recover explicitly before restarting")
            seconds, microseconds = self.client.time()
            pipe.multi()
            pipe.hset(base, mapping={"run_id": run_id, "state": "PREPARING", "config": config_json,
                                     "total_jobs": len(normalized), "completed": 0,
                                     "coordinator_owner": owner,
                                     "created_at": seconds + microseconds / 1_000_000})
            for job_id, path in normalized:
                pipe.hset(base + ":job:" + job_id,
                          mapping={"path": path, "state": "pending", "attempt": 0})
            pipe.rpush(base + ":pending", *(job_id for job_id, _path in normalized))
            pipe.set(self.current_key, run_id)
            pipe.execute()
        return self.current()

    def current(self) -> dict | None:
        run_id = self.client.get(self.current_key)
        if not run_id:
            return None
        result = self.client.hgetall(self._base(run_id))
        if not result:
            raise RuntimeError("current run metadata is missing")
        result["config"] = json.loads(result["config"])
        result["total_jobs"] = int(result["total_jobs"])
        result["completed"] = int(result["completed"])
        result["created_at"] = float(result["created_at"])
        result["coordinator_alive"] = self.client.get(self.coordinator_key) == result["coordinator_owner"]
        return result

    def ready(self, run_id: str) -> bool:
        return bool(self._scripts["ready"](keys=[self._base(run_id), self.coordinator_key],
                                            args=[self._owner()]))

    def claim(self, run_id: str, owner: str, lease_seconds: float = 60) -> dict | None:
        base = self._base(run_id)
        if not isinstance(owner, str) or not owner:
            raise ValueError("worker owner must be a nonempty string")
        result = self._scripts["claim"](
            keys=[base, base + ":pending", base + ":leased"],
            args=[base + ":job:", owner, uuid.uuid4().hex, _seconds(lease_seconds)])
        return json.loads(result) if result else None

    def renew(self, run_id: str, job_id: str, token: str, lease_seconds: float = 60) -> bool:
        base = self._base(run_id)
        return bool(self._scripts["renew"](
            keys=[base, base + ":leased", base + ":job:" + str(job_id)],
            args=[job_id, token, _seconds(lease_seconds)]))

    def complete(self, run_id: str, job_id: str, token: str, stats: dict) -> bool:
        if not isinstance(stats, dict):
            raise ValueError("job statistics must be an object")
        base = self._base(run_id)
        return bool(self._scripts["complete"](
            keys=[base, base + ":leased", base + ":job:" + str(job_id), base + ":stats"],
            args=[job_id, token, _json(stats)]))

    def fail(self, run_id: str, job_id: str, token: str, error: str) -> bool:
        base = self._base(run_id)
        return bool(self._scripts["fail"](
            keys=[base, base + ":leased", base + ":job:" + str(job_id), base + ":failures"],
            args=[job_id, token, str(error)[:2000]]))

    def status(self, run_id: str) -> dict:
        base = self._base(run_id)
        with self.client.pipeline(transaction=True) as pipe:
            pipe.hgetall(base)
            pipe.llen(base + ":pending")
            pipe.zcard(base + ":leased")
            pipe.hgetall(base + ":stats")
            pipe.hgetall(base + ":failures")
            state, pending, leased, stats, failures = pipe.execute()
        if not state:
            raise ValueError("unknown run identifier")
        return {"run_id": run_id, "state": state["state"],
                "total_jobs": int(state["total_jobs"]), "pending": pending, "leased": leased,
                "completed": int(state["completed"]),
                "stats": [json.loads(stats[job_id]) for job_id in sorted(stats)],
                "failures": failures}

    def set_state(self, run_id: str, state: str) -> bool:
        if state not in ("FINALIZING", "COMPLETED", "FAILED"):
            raise ValueError("coordinator can set FINALIZING, COMPLETED, or FAILED")
        return bool(self._scripts["state"](keys=[self._base(run_id), self.coordinator_key],
                                            args=[self._owner(), state]))

    def register_worker(self, run_id: str, worker_id: str, metrics: dict) -> None:
        if not isinstance(metrics, dict) or not isinstance(worker_id, str) or not worker_id:
            raise ValueError("invalid worker measurements")
        self.client.hset(self._base(run_id) + ":workers", worker_id, _json(metrics))

    def worker_metrics(self, run_id: str) -> list[dict]:
        metrics = self.client.hgetall(self._base(run_id) + ":workers")
        return [json.loads(metrics[worker_id]) for worker_id in sorted(metrics)]

    def close(self) -> None:
        self.client.close()
