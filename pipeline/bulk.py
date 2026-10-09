"""Bounded bulk requests with item-level retries and idempotent transport retries."""

import random
import time

import orjson
import requests


RETRYABLE = {429, 502, 503, 504}


class BulkError(RuntimeError):
    pass


class BulkClient:
    def __init__(self, url, timeout=120, max_retries=6, backoff=0.5):
        self.url = url.rstrip("/")
        self.timeout = timeout
        self.max_retries = max_retries
        self.backoff = backoff
        self.session = requests.Session()
        self.session.trust_env = False

    def close(self):
        self.session.close()

    def send(self, documents):
        if not documents:
            return {"indexed": 0, "retried_documents": 0, "bulk_requests": 0, "bulk_bytes": 0}
        # Retaining at most one batch allows selective retries without rereading input.
        pending = documents
        stats = {"indexed": 0, "retried_documents": 0, "bulk_requests": 0, "bulk_bytes": 0}
        for attempt in range(self.max_retries + 1):
            if attempt:
                stats["retried_documents"] += len(pending)
                time.sleep(min(30, self.backoff * 2 ** (attempt - 1)) * random.uniform(0.75, 1.25))
            # Join existing source buffers once instead of copying each source
            # repeatedly while concatenating its metadata and newlines.
            pieces = []
            for doc_id, body in pending:
                pieces.extend((orjson.dumps({"index": {"_index": "persons", "_id": doc_id}}),
                               b"\n", body, b"\n"))
            payload = b"".join(pieces)
            stats["bulk_requests"] += 1
            stats["bulk_bytes"] += len(payload)
            try:
                response = self.session.post(
                    self.url + "/_bulk",
                    params={"filter_path": "errors,items.*.status,items.*.error"},
                    data=payload, headers={"Content-Type": "application/x-ndjson"},
                    timeout=self.timeout,
                )
            except requests.RequestException as exc:
                # Stable envelope IDs make an ambiguous accepted request safe to resend.
                if attempt == self.max_retries:
                    raise BulkError(f"bulk transport failed after {attempt + 1} attempts ({type(exc).__name__})") from exc
                continue
            if response.status_code in RETRYABLE:
                if attempt == self.max_retries:
                    raise BulkError(f"bulk HTTP {response.status_code}: retry budget exhausted")
                continue
            if response.status_code != 200:
                raise BulkError(f"bulk HTTP {response.status_code}")
            try:
                result = orjson.loads(response.content)
                if not isinstance(result, dict):
                    raise ValueError("expected JSON object")
                if type(result.get("errors")) is not bool:
                    raise ValueError("missing or invalid errors flag")
                items = result["items"]
                if not isinstance(items, list) or len(items) != len(pending):
                    raise ValueError("response item count does not match request")
                retry = []
                saw_error = False
                for document, item in zip(pending, items):
                    if not isinstance(item, dict) or len(item) != 1 or "index" not in item:
                        raise ValueError("invalid bulk action result")
                    detail = item["index"]
                    status = detail["status"]
                    if not isinstance(status, int) or isinstance(status, bool):
                        raise ValueError("invalid bulk item status")
                    if 200 <= status < 300:
                        if "error" in detail:
                            raise ValueError("successful item contains an error")
                        stats["indexed"] += 1
                    elif status in RETRYABLE:
                        saw_error = True
                        retry.append(document)
                    else:
                        # ES error reasons can contain source values. Report type and ID only.
                        error = detail.get("error", {})
                        kind = error.get("type", "unknown") if isinstance(error, dict) else "unknown"
                        raise BulkError(f"bulk item id={document[0]} status={status} type={kind}")
                if result["errors"] != saw_error:
                    raise ValueError("errors flag disagrees with item statuses")
            except (ValueError, KeyError, TypeError) as exc:
                raise BulkError(f"malformed bulk response: {exc}") from exc
            if not retry:
                return stats
            pending = retry
        raise BulkError(f"{len(pending)} bulk items exhausted retry budget")


def es_request(client, method, path, body=None):
    """Retry control requests; do not expose payloads containing feed data in errors."""
    for attempt in range(client.max_retries + 1):
        try:
            response = client.session.request(
                method, client.url + path, json=body, timeout=client.timeout,
            )
            if response.status_code in RETRYABLE:
                response.raise_for_status()
            else:
                return response
        except requests.RequestException as exc:
            if attempt == client.max_retries:
                raise RuntimeError(f"ES {method} {path} failed ({type(exc).__name__})") from exc
            time.sleep(min(30, client.backoff * 2 ** attempt))


def require_success(response, operation):
    if not 200 <= response.status_code < 300:
        raise RuntimeError(f"{operation}: HTTP {response.status_code}")
    try:
        result = response.json()
    except ValueError as exc:
        raise RuntimeError(f"{operation}: invalid JSON response") from exc
    if not isinstance(result, dict):
        raise RuntimeError(f"{operation}: expected JSON object")
    if "_shards" in result:
        shards = result["_shards"]
        if (not isinstance(shards, dict)
                or any(type(shards.get(key)) is not int or shards[key] < 0
                       for key in ("failed", "successful", "total"))
                or shards["failed"] != 0
                or shards.get("successful") != shards.get("total")):
            raise RuntimeError(f"{operation}: incomplete shard response")
    if "acknowledged" in result and result["acknowledged"] is not True:
        raise RuntimeError(f"{operation}: request was not acknowledged")
    return result
