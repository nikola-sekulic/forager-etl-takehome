# Forager person/organization ETL

A streaming Python pipeline joins the full organization feed onto person roles
and indexes one document per person in a single Elasticsearch index, `persons`.
The pipeline stays limited to **2 GiB and four CPUs**.

Final submission verification on October 9 indexed **1,000,000 people in
65.29 seconds** at **15,316 persons/second**, with **1.384 GiB peak pipeline
memory**, using fresh isolated Elasticsearch/staging volumes and the final
defaults. All **155 tests passed without skips**, and independent validation
passed all aggregate checks and **267 complete raw-source comparisons**.
The supplied official correctness test is skipped at the provider's instruction.
See [bench/final_review.json](bench/final_review.json) and
[bench/latest_default_run.json](bench/latest_default_run.json).
Duration measurements use high-resolution timers; zero elapsed intervals still
allow final failure metrics to be saved. The preceding 64.74-second verification
is preserved in `bench/pre_timing_default_run.json`.

The earlier default repeat with reused ES indexed **1,000,000 people in 50.04 seconds** at
**19,985 persons/second**, with **1.393 GiB peak pipeline memory**. Sorted
organization batching was tested and remains disabled: its repeat took 50.60
seconds and used 14.3% more CPU work. All 150 Redis/Zstd-enabled tests passed,
and the final batching trial passed 267 complete raw-source comparisons.
See [bench/prefetch_trials.json](bench/prefetch_trials.json) for all comparisons.

The earlier codec experiment indexed **1,000,000 people in 47.62 seconds**:
**21,000 persons/second**, including raw organization staging, dictionary
training, final flush, refresh and count verification. Peak pipeline cgroup
memory was **1.394 GiB**; CPU averaged **2.62 of four cores**. The adjacent
original-image control took 53.01 seconds: **11.3% higher observed throughput**
and **19.0% less peak memory**. The candidate also ran in 49.61 seconds;
the first control took 60.61 seconds on fresh ES volumes. These A/B/A/B trials
used fresh staging each time, a reused ES container after the first run, and
warm raw input caches. Timings vary with host/cache conditions; no absolute
optimum is claimed. All aggregate counts and **267 complete source comparisons**
passed, and that Redis/Zstd-enabled suite passed **140 tests with no skips**. See
[EVALUATION.md](EVALUATION.md) and
[bench/pre_prefetch_default_run.json](bench/pre_prefetch_default_run.json).
Those codec profiles and four full runs are in
[bench/lmdb_trials.json](bench/lmdb_trials.json). The earlier 83.87-second
fresh-volume verification remains in `bench/pre_lmdb_default_run.json`.

## Run

Only Docker with Compose is required. Extract the raw bundle under
`data/person/*.json.gz` and `data/organization/*.json.gz`, then run:

```sh
docker compose up --build
```

Elasticsearch stays running after the pipeline exits successfully. For a command
that returns when ingestion ends, with the pipeline's exit status:

```sh
docker compose up -d --wait elasticsearch
docker compose up --build --exit-code-from pipeline pipeline
```

Then restart the stopped Elasticsearch service before querying:

```sh
docker compose up -d --wait elasticsearch
docker compose run --rm --no-deps pipeline python /app/bench/validate.py
docker compose run --rm --no-deps pipeline python -m unittest discover -s /app/tests -v
bash bench/perf.sh
```

The basic test command skips the real Redis integration tests. To run the
entire suite, use the isolated Redis test connection documented below.

PowerShell users can print persisted metrics without Bash:

```powershell
docker compose run --rm --no-deps pipeline python /app/bench/report.py
```

`bench/perf.sh` also reports Elasticsearch's container lifetime cgroup memory
peak when available. That peak includes startup, page cache, and any earlier runs
on the same container; it is labeled separately from run-specific measurements.
The pipeline writes current measurements to `metrics/latest.json` on the host.
The reporter refuses missing, failed, or incomplete runs. The JSON in
`bench/latest_default_run.json` is the latest default confirmation;
`bench/measured_run.json` preserves the earlier 57.56-second confirmation.
Neither is a fallback for subsequent runs. The original result is in
`bench/baseline_run.json`, and all trials are in `bench/performance_trials.json`.

## Supplied correctness test: skipped at provider's instruction

On October 9, 2026, the exercise provider acknowledged the fixture mistake and
instructed the candidate to skip this test. That instruction was relayed by the
candidate. The supplied `bench/correctness.py` and `bench/expected.json` remain
unchanged; the official test is skipped rather than reported as passing.
The provider's reply did not confirm our measured counts. Independent validation
continues to check the raw data and the indexed output.

`bench/expected.json` explicitly says it contains placeholder counts. Its two
zero expectations conflict with the supplied raw data:

| Query | Supplied expectation | Raw audit and indexed result |
| --- | ---: | ---: |
| All persons | 1,000,000 | 1,000,000 |
| `roles.role_title.keyword = Project Manager` | 0 | 7,081 |
| `organizations.name.keyword = Dell Technologies` | 0 | 647 |

The original test and fixture remain unchanged, as the brief requires. Therefore
`python bench/correctness.py` returns exit 1 for the two invalid zero expectations.
`bench/validate.py` runs those same queries against independently audited counts,
checks 1,858 persons with unresolved references, and verifies every field in
bounded samples spread across each entire person file. It also retains examples
with missing and unresolved organization IDs; the latest run matched 267 whole
raw-source joins. Validation scans the raw feeds independently and runs after
ingestion, so its elapsed time is separate from ingestion throughput.
It passes against the measured run. The provider's instruction resolves the
submission blocker; no corrected fixture or additional data is needed to
complete the current submission checks.

The final review evidence is in `bench/final_review.json`.
`REVIEW_GUIDE.md` explains the implementation choices to inspect and defend;
it is a preparation aid, not a claim of completed human review.

The reproducible raw-data audit is independent of ingestion and is optional:

```sh
docker compose run --rm --no-deps pipeline python /app/bench/audit_data.py --data-dir /data --output /metrics/data-audit.json
```

Its aggregate-only report is included as `bench/data_audit.json`. No precomputed
or enriched input is used by the pipeline.

## How it works

1. Train a fresh 32 KiB Zstd dictionary from a bounded organization sample
   (at most 4 MiB); sampling and training count toward ingestion time. Tiny
   valid inputs fall back to plain Zstd. Four producer processes stream and
   validate gzip NDJSON with ISA-L/orjson, then compress full JSON with Zstd
   level 1. Each process opens its own codec context. A single writer stages full `serialized_data`
   bodies in LMDB using the **envelope ID** as the key. A queue holds at most six
   blocks, each normally at most 256 records or 1 MiB of compressed values.
   Transactions contain at most 10,000 records or 64 MiB of compressed values.
   Sync the immutable database
   before readers start. `STAGE_WORKERS=1` selects serial staging for comparison.
2. Eight processes stream the eight person files under the same four-CPU quota.
   Each retains one sending batch, one building batch, and a byte-bounded 4 MiB
   organization cache. The smaller private caches leave more room for the
   shared filesystem cache. LMDB mmap
   pages are shared by the
   workers and accounted for in the pipeline's memory cgroup. Each worker opens
   a read-only environment after spawning, reuses one read transaction per file,
   and reads compressed values through borrowed buffers. Decompression returns
   owned bytes; borrowed views never enter the cache or leave the store. Organization JSON
   is embedded with `orjson.Fragment`, avoiding repeated parsing and encoding.
3. Join every non-null role organization ID. `organizations` contains distinct,
   complete feed records in first-role order; it replaces any thin organization
   array already present in the person input. Preserve all other source fields
   and roles. Each role gets `organization_unresolved`; each person gets
   `unresolved_organization_ids` and `has_unresolved_organizations`. Missing or
   null IDs receive `organization_unresolved: false`; missing IDs are distinct
   from dangling references.
4. Send batches targeted at 8 MiB or 2,000 documents, whichever comes first.
   A background sender per worker overlaps Elasticsearch work with preparation
   of the next batch. At most eight requests are active by default; every final
   result is checked before file completion. Bulk payloads reuse body buffers
   and join them once, avoiding repeated copies. Use the person envelope
   ID as `_id`, retry only transient failed items, and safely resend ambiguous
   transport failures with stable IDs. Check every result; malformed rows and
   permanent bulk failures fail the run. A worker failure cancels its peers.
5. Restore one-second refreshes, flush and refresh all shards, and verify that
   the Elasticsearch count equals the acknowledged input count. This also
   detects duplicate person IDs. Record metrics and remove temporary staging.

Malformed envelopes, mismatched source IDs, duplicate organization IDs,
non-object roles, or invalid role IDs fail with source location information.
No source records appear in progress logs. Blank lines are rejected. Input rows
are capped at 16 MiB and enriched documents at 32 MiB to reject oversized
records. The enriched size is checked after join serialization, so it is an
output validation limit rather than a strict transient allocation cap. An
individual large document may exceed the normal bulk target.
The compressed staging database has a 16 GiB map ceiling; disk usage was 0.769 GiB
on Linux for this bundle. A forcibly terminated process can leave a temporary
stage directory; future runs always create a fresh one and never reuse it.
Codec metadata validates the dictionary's size and SHA256 before readers start.
The temporary dictionary and store are removed after ingestion.

## Optional horizontal workers

The default remains a single pipeline with local LMDB staging. Shared Redis
staging and file leases are available in `docker-compose.scale.yml`:

```sh
docker compose -f docker-compose.yml -f docker-compose.scale.yml up --build --scale pipeline=2
```

For an existing `persons` index, first stop the entire project with
`docker compose -f docker-compose.yml -f docker-compose.scale.yml down`, then
set `RESET_INDEX=1` as shown below and run the overlay command. Stopping the
stack clears ephemeral Redis state and prevents an old in-flight Elasticsearch
write from reaching a rebuilt index. Data volumes persist. The coordinator
refuses any previous Redis generation until this clean restart.

One coordinator initializes the index and stages complete organization JSON
once in Redis using up to eight file loaders. Redis disables persistence and
eviction; staging is reconstructible. Each constrained pipeline replica starts
four processes, claims available gzip-file jobs, and renews 60-second leases.
Expired jobs can be replayed by another worker with the same document IDs;
token-fenced completion counts each file once. A permanent input/bulk failure
fails the entire run. Coordinator or Redis loss requires a clean rebuild.

Organization lookups use bounded MGET prefetches over at most 256 people or
2 MiB of raw input per chunk, a 64 MiB organization overlay, and a 128 MiB cache
per process. A single oversized source row retains the 16 MiB source limit.
Pathological chunks with over 2,000 distinct references use checked individual
lookups instead; the enriched 32 MiB output limit is still checked after
serialization. Redis failures or a lost ready-generation marker fail ingestion
instead of becoming unresolved references. No external services are used.

Every replica retains **2 GiB / four CPUs**. Two replicas allow **4 GiB / eight
CPUs in aggregate**, plus Elasticsearch, Redis and the coordinator. There are
only eight person files, so at most eight file jobs run concurrently; finer
runtime partitioning would be needed to scale beyond that. Multi-host workers
would need access to the same self-hosted Redis/ES endpoints and raw files;
Compose's local bridge alone does not span hosts.

The one-replica Redis trial indexed all 1,000,000 people in **59.03 seconds**
(16,940.80/s), versus the prior local confirmation's **57.56 seconds**. This
single-trial 2.6% difference is within plausible host/cache variation and does
not demonstrate a speed gain. A subsequent fresh default run took 60.24 seconds;
both adjacent trials started fresh ES containers, with existing data volumes
and warm host input cache. The differences are too small to infer a reliable
throughput improvement or regression. Pipeline peak memory was 0.832 GiB, but Redis
allocated 1.457 GiB after staging and the coordinator peaked at 0.331 GiB.
Those separate measurements are not a simultaneous total peak. Eight
organization producers also run outside the pipeline CPU quota. The simpler
local loader remains the default. Two-replica throughput was not benchmarked,
as scaling is optional for this take-home.

The run and its full-source validation are saved in `bench/redis_single_run.json`.
The coordinator writes aggregate metrics to `metrics/latest.json`; replicas
write separate files under `metrics/workers/<run-id>/`. The reporter refuses
incomplete final worker resource measurements. Real Redis tests can be run:

```sh
docker compose -f docker-compose.yml -f docker-compose.scale.yml up -d --wait redis
docker compose run --rm --no-deps -e TEST_REDIS_URL=redis://redis:6379/15 pipeline python -m unittest discover -s /app/tests -v
```

## Mapping and tuning

Four primary shards, zero replicas, and disabled refreshes during ingestion
reduce indexing work. Translog durability remains `request`; acknowledged bulk
writes are synced. Elasticsearch has a 2 GiB heap and a 512 MiB indexing buffer,
with no service resource ceiling.

`roles` and `organizations` are **object** arrays so the exercise's plain `term`
queries count people correctly. Role titles and organization names have explicit
`text` plus `.keyword` mappings. Object arrays permit cross-object matches in
multi-field queries; use a nested mapping and nested queries if that isolation is
needed in a future API. That would change the supplied query contract and add
hidden Lucene documents.

The search surface also includes names, headline, country, city, industry,
skills, IDs, and unresolved flags. `dynamic: false` keeps every other field in
`_source` without indexing it or allowing field/type explosion. In particular,
rich organization details, URLs, and varied date strings remain intact. Stored
numeric metadata uses `index: false` and `doc_values: false`; searchable IDs
omit doc values, while keyword attributes retain them for useful aggregations.
Text fields omit norms and positions. This supports term and ordinary full-text
matching; phrase queries on those fields are unsupported. See
[Elastic's indexing guidance](https://www.elastic.co/docs/deploy-manage/production-guidance/optimize-performance/indexing-speed)
and [object-array semantics](https://www.elastic.co/docs/reference/elasticsearch/mapping-reference/array).

## Reruns and configuration

An existing `persons` index fails with a clear message. Explicitly rebuild it:

```sh
RESET_INDEX=1 docker compose up --build pipeline
```

In PowerShell:

```powershell
$env:RESET_INDEX = '1'
docker compose up --build pipeline
Remove-Item Env:RESET_INDEX
```

This deletes only the `persons` index. Source data is mounted read-only. Stop
services with `docker compose down`; volumes persist. To delete this project's
stored index and staging volumes intentionally, use `docker compose down -v`.

Compose exposes `WORKERS` (1-8; default 8), `STAGE_WORKERS` (1-4; default 4),
`ORG_CACHE_BYTES` (up to 128 MiB per process; default 4 MiB), `INDEX_SHARDS`
(1-8; default 4), `BULK_OVERLAP` (0/1), `BULK_SENDERS` (1-2), `BULK_BYTES`
(up to 16 MiB; default 8 MiB), `BULK_DOCS` (up to 5,000), and `RESET_INDEX`.
The default is one sender. Two senders permit sixteen active HTTP requests at
eight person workers, with at most two submitted batches plus one building
batch per worker. Extra senders, larger private caches and eight shards did
not show a reliable person-phase gain in the recorded trials. A 32 MiB cache
per process reached the memory ceiling in the earlier ISA-L trials; it was not selected. Changing batch
sizes or worker counts warrants a new
measurement. No parameter raises the pipeline container ceilings. The default
is a single-node initial-load solution. The optional overlay adds file-job
replay and multiple workers; neither mode offers incremental updates,
coordinator failover, Elasticsearch replica recovery, or automatic schema
migration. Elasticsearch availability and input integrity failures require a
fresh rebuild.

Organization staging controls are also exposed by Compose:

| Variable | Default | Supported values |
| --- | --- | --- |
| `ORG_CODEC` | `zstd` | `zstd`, `isal` |
| `ORG_ZSTD_DICT_BYTES` | `32768` | 0, 32768, 65536; use 0 with ISA-L |
| `ORG_WRITE_RECORDS` | `10000` | 1-100000 |
| `ORG_WRITE_BYTES` | `67108864` | 1-268435456 compressed bytes |
| `ORG_READ_BUFFERS` | `1` | 0/1 |
| `ORG_PUTMULTI` | `0` | 0/1; experimental writer, slower in the measured samples |

The transaction byte limit bounds compressed payloads, not total dirty-page
memory. Larger settings need a new resource measurement. Input remains in its
original order; global sorting and `append=True` would require additional
preprocessing of the unsorted IDs. The optional Redis path uses its existing
shared JSON staging and does not use these LMDB codec controls.

`bench/profile_store.py` rebuilds the full organization store and compares
128 raw organization bodies plus 160,000 streamed person joins, without ES:

```sh
docker compose run --rm --no-deps pipeline python /app/bench/profile_store.py
```

This is a serial candidate screen, not full-ingestion throughput.
`bench/lmdb_trials.json` records every screen and the four A/B/A/B full runs;
`bench/compression_probe.json` records the separate held-out codec experiment.
`bench/lmdb.compose.yml` supplies the isolated benchmark overlay (port 19200).

Optional local organization batching is disabled (`ORG_PREFETCH_RECORDS=0`).
For experiments, set it to 256; source windows default to 2 MiB and organization
overlays to 8 MiB per worker. `ORG_PREFETCH_SORT=1` sorts encoded keys. Full bodies
and role order are preserved; exhausted budgets fall back to individual reads.
The measured results and reproduction commands are in `EVALUATION.md`.
