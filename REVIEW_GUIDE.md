# Candidate review guide

Read the referenced code and run the documented commands. This is a preparation
aid, not evidence of completed human review. Record only actual review decisions
in `AI_NOTES.md`.

## 1. What is the join, and what survives it?

In `pipeline/store.py::_join_person`, output starts with the person's
`serialized_data`. Every role stays in original order, including repeated roles
and missing references. Join `roles[].organization_id` against the organization
**envelope `id`**, then embed
the organization's complete `serialized_data`. Keep each resolved organization
once, in first-role order. Replace the input's thin `organizations` array; retain
other person fields and original role names. A non-null absent ID is flagged at
role and person level; a missing/null ID is not an unresolved reference.

## 2. Why LMDB rather than an in-memory Python dictionary?

Compact organization JSON is about 1.354 GB; the audit estimates decoded Python
objects at 5.16 GB, beyond the 2 GiB budget. `OrganizationStore` stores Zstd-compressed
JSON values on disk and lets processes share filesystem/mmap pages. A fresh
32 KiB dictionary is trained from at most 4 MiB of sampled organization JSON;
training counts toward the run and tiny feeds fall back to plain Zstd. Codec
metadata validates dictionary integrity before each process opens its own
native context. Each worker has a 4 MiB cache of
decoded JSON **bytes**, not full Python objects. `orjson.Fragment` embeds those
bytes without repeated parsing/encoding. The 16 GiB map ceiling is virtual
address space, not reserved RAM. Staging is rebuilt and synced before readers start.

## 3. Why are roles and organizations objects rather than nested fields?

In `pipeline/mapping.py`, object arrays support the grader's ordinary term
queries; explicit `.keyword` mappings support exact values. Nested arrays would
require different queries and add hidden Lucene documents. The tradeoff is that
conditions on multiple fields can match
different array members. `dynamic: false` retains unmapped fields in `_source`
while avoiding field explosion. Text fields omit positions and norms: ordinary
matching works, but phrase queries are unsupported.

## 4. How can eight workers respect four CPUs, and how is memory bounded?

In `pipeline/main.py` and `docker-compose.yml`, Docker enforces four cores of quota
across **all** pipeline processes/threads; eight processes overlap I/O waits.
Four staging producers feed one writer through a six-block queue.
Normal blocks target 256 records or 1 MiB compressed; transactions cap at 10,000
records or 64 MiB compressed. Their payload cap does not measure dirty-page
memory; Docker still enforces the ceiling. Buffer reads avoid a compressed copy;
only owned decompressed bytes escape the store. Each person worker retains one sending batch, one building batch, and
its small cache. Bulks target 8 MiB or 2,000 documents. Oversized individual rows
can exceed normal batch targets. The 16 MiB input limit bounds line reads; the
32 MiB enriched limit is checked after serialization, not a strict transient
allocation cap. Maximum tuning settings do not guarantee fit.

## 5. What happens when Elasticsearch accepts a request but the response is lost?

In `pipeline/bulk.py`, person envelope IDs become Elasticsearch `_id`s, so replay
overwrites safely. Transport failures resend the pending batch; item-level
transient failures retry only affected items.
Retries are bounded with backoff. Every item is checked, including contradictory
responses. Permanent failures fail the run and cancel peers.

## 6. What does successful completion actually guarantee?

In `pipeline/main.py`, all pending bulks must be acknowledged.
Refresh returns to one second, flush/refresh shard results are checked, and the
index count must equal acknowledged inputs. This catches duplicate person IDs.
Request translog durability stays enabled; this single-node setup has zero
replicas. Failure can leave a partial index or disabled refresh. Default reruns
refuse an existing index; `RESET_INDEX=1` explicitly rebuilds `persons`. This is
an initial loader, not an incremental or zero-downtime production pipeline.

## 7. What performance claim is supported?

Read `EVALUATION.md` and the matching artifact. Final October 9 verification
used fresh ES/staging volumes: one million persons in **65.29 seconds** at
**15,316/s** with **1.384 GiB** peak. All 155 tests and 267 complete-source
comparisons passed; the supplied official test was skipped at the provider's
instruction. The different storage/cache context matters when comparing results.
The review's native timing issue is fixed: high-resolution duration clocks and
zero-interval guards preserve failure metrics without obscuring the original error.

The earlier default repeat took
50.04 seconds at 19,985 persons/s and 1.393 GiB peak. Sorted organization batching
took 50.60 seconds and 14.3% more CPU work, so it stays disabled. Only 0.88% of
lookup consumptions were duplicates within the tested windows. All five profiles
and four full runs are in `bench/prefetch_trials.json`; 150 tests and independent
267-document validation passed. The optional implementation in `pipeline/prefetch.py`
bounds windows and owned bytes; sorted point reads do not guarantee sequential I/O.

The earlier Zstd confirmation
indexed one million persons in 47.62 seconds at about 21,000/s, with 1.394 GiB
peak pipeline memory and 2.62 average CPU cores. The adjacent preserved-original
control took 53.01 seconds and used 1.722 GiB: observed throughput rose 11.3%,
peak memory fell 19.0%, and CPU work fell 14.3%. The candidate first took 49.61
seconds; the first control took 60.61 seconds. Full-run time includes raw staging,
dictionary preparation, ingestion and finalization,
but excludes image build and Elasticsearch startup. ES has separate resources.
These A/B/A/B warm-cache observations support the selected configuration;
they do not establish an absolute optimum or a guaranteed percentage improvement.
`putmulti()` and larger ISA-L transactions were tested but not selected.

The earlier pre-Zstd clean submission check used fresh Elasticsearch and staging volumes
and took **83.87 seconds (11,923/s)** at **1.539 GiB** peak. Raw input cache was
warm from auditing. It remains in `bench/pre_lmdb_default_run.json`.
Be clear about those different contexts; current comparisons are in
`bench/lmdb_trials.json` and `bench/prefetch_trials.json`, the latest default result
in `bench/latest_default_run.json`,
and final checks in `bench/final_review.json`.

## 8. What would horizontal scaling require?

The overlay, `pipeline/distributed.py` and `pipeline/coordination.py` use optional
Redis to stage organizations once; workers claim renewable file leases.
Stable IDs support file replay and ownership tokens
prevent stale completion statistics. Each replica has its own 2 GiB/four-CPU
ceiling. Only eight person gzip files exist, so file-level concurrency stops at
eight. Multi-host execution needs shared raw files and reachable self-hosted
Redis/ES. Redis/coordinator failure requires a clean stack restart; cross-run
tokens cannot fence old Elasticsearch writes. One replica was benchmarked;
multi-replica throughput was not.

## 9. Why is the official grader skipped, and what did AI do?

In `bench/expected.json`, the frozen fixture contains two placeholder zeros.
Independent source auditing and indexed queries agree on 7,081 Project Manager
matches and 647 Dell Technologies matches. On October 9, 2026, the provider
acknowledged the fixture error and told the candidate to skip this test. Preserve
the files and describe its status as skipped. Their reply did not confirm these
counts; independent validation remains separate from the official grader.
As `AI_NOTES.md` records, Codex authored essentially all new code, tests and
documentation. The human supplied data, installed Docker and directed priorities;
claim only subsequent review or changes that you actually perform.
