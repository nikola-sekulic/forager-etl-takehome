# Pipeline evaluation

## Timing fix and final verification: October 9, 2026

The requested review found a native Windows timing edge case: a fast input
failure could have zero elapsed time under the coarse monotonic clock, causing
CPU accounting to raise during cleanup and leave persisted metrics marked
`running`. Duration measurements now use `perf_counter`. Zero intervals omit
unmeasurable CPU averages and produce a zero reported rate without inventing
positive elapsed time; the original failure and final metrics are preserved.
Coordinator/worker measurement paths have the same guards. Their deadline and
lease clocks remain unchanged.

Five new deterministic tests exercise local, coordinator and worker finalization
with frozen measurement clocks. The native focused suite passed **17 tests in
0.041 s**, and **20/20** actual missing-input failures at a frozen clock retained
terminal failed metrics. The full real-Redis/Zstd Docker suite passed **155 tests
with no skips in 13.877 s**.

A fresh isolated raw ingestion of the fixed image indexed **1,000,000 people in
65.290 s**, at **15,316.21 persons/s**, with **1.384 GiB peak pipeline memory**,
**158.280 CPU-seconds**, and **2.42 average pipeline cores**. It retained all
2,083,023 roles and 6,747,225,963 bulk bytes, with zero retries, sampled swap or
OOM events. Inspection confirmed exit 0, read-only source mounts and unchanged
2 GiB/four-CPU limits. Independent post-ingest validation passed all four
aggregate checks and **267/267 complete raw-source comparisons**. The index was
green with four primaries, one-second refresh, zero replicas and request
translog durability. The performance reporter exited 0. The provider-waived
official test remains skipped, and its files are unchanged.

Full regression tests preceded ingestion; test Redis and the original ES service
were stopped during the timed run. ES/staging volumes and the pipeline container
were fresh, host cache state was uncontrolled, and no heavy work ran concurrently.
Build/ES startup are excluded; raw staging, dictionary preparation, all ingestion
and finalization are included. The 65.29 s observation is close to the earlier
64.74 s fresh-volume run and does not demonstrate a timing-fix performance gain.

Current evidence is in `bench/latest_default_run.json` and
`bench/final_review.json`; earlier results/review are preserved in
`bench/pre_timing_default_run.json` and `bench/pre_timing_review.json`.
All **27 full ingests** remain in `bench/performance_trials.json`.

## Provider clarification: October 9, 2026

The candidate relayed the provider's reply acknowledging their fixture mistake
and instructing: "for now please skip this test." The supplied correctness test
is therefore skipped in final verification. Its files remain unchanged and no
official pass is claimed. Earlier failure reports below are historical evidence.
The provider did not confirm the independently measured counts. Raw-source
validation and the regression suite remain part of submission verification.
This clarification removes the fixture blocker for the current submission.

Before the timing fix, verification used a new isolated ES/staging volume and a new constrained
pipeline container with the final defaults, including disabled local prefetch.
The full real-Redis/Zstd suite passed **150 tests with no skips in 13.866 s**.
Test Redis and the original ES service were stopped during the timed ingest;
no heavy tests or validation ran concurrently. Host cache state was uncontrolled
and no fresh audit deliberately warmed input. Image build and ES startup are
excluded; raw staging, dictionary preparation and all finalization are included.

The run indexed **1,000,000 people in 64.738 s**, at **15,446.81 persons/s**,
with **1.387 GiB peak pipeline memory**, **161.969 CPU-seconds**, and **2.50
average pipeline cores**. Organization staging took **11.498 s** and person
ingestion **52.305 s**. It retained 2,083,023 roles and all 6,747,225,963 bulk
bytes, with zero retries, zero sampled swap and zero OOM events. Docker
inspection confirmed 2 GiB/four CPUs, exit 0 and read-only source mounts.
Independent validation after ingestion passed all four aggregate queries and
**267/267 complete raw-source comparisons**. The index was green with four
active primary shards, zero unassigned shards, one-second refresh, zero replicas
and request translog durability. The performance reporter exited 0.

This fresh-volume run is not a controlled performance comparison with earlier
reused-ES timings. It is saved in `bench/pre_timing_default_run.json` and
`bench/pre_timing_review.json`; the earlier repeat/review are preserved in
`bench/pre_submission_default_run.json` and `bench/pre_submission_review.json`.
At that stage **26 full runs** were retained in `bench/performance_trials.json`. The isolated
verification uses `bench/review.compose.yml` with project `etl-submission-check`.
No official test was executed in this verification, and no official pass is claimed.

## Sorted LMDB prefetch: tested, disabled by default

Deduplicating organization IDs in bounded person windows and sorting their
encoded LMDB keys did not demonstrate a useful ingestion gain. The optional
implementation retains full owned JSON bytes, uses the existing read transaction,
and bounds source windows, distinct IDs and retained organization bytes.
`ORG_PREFETCH_RECORDS=0` keeps the existing cached individual lookups.

Four full raw ingests alternated the preserved selected Zstd baseline and sorted
256-person prefetch. The first control started with fresh isolated ES storage;
subsequent runs reused ES. Each used a fresh constrained pipeline container,
rebuilt organizations and recreated the persons index. Input caches were warm;
host cache and ES warmup were uncontrolled. No heavy work ran concurrently.

| Configuration | Stage s | Person s | Total s | Persons/s | Peak GiB | CPU-seconds |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Default, first/fresh ES | 6.883 | 45.092 | 55.597 | 17,986.50 | 1.383 | 139.928 |
| Sorted 256, first | 6.636 | 39.872 | 50.461 | 19,817.41 | 1.414 | 145.863 |
| Default, repeat | 8.087 | 37.782 | **50.037** | **19,985.15** | **1.393** | **122.322** |
| Sorted 256, repeat | 9.004 | 37.718 | 50.596 | 19,764.28 | 1.416 | 139.823 |

The repeated candidate took 1.12% more total time and 14.31% more CPU work.
Person-phase times were effectively equal. Only 9,637 of 1,092,294 lookup
consumptions were removed by within-window deduplication: **0.88%**. Sorted
point lookups do not guarantee sequential physical I/O. The compact store and
existing cache already make these reads inexpensive on this dataset; window
collection, sorting and overlay bookkeeping add work. The slower first control
includes ES warmup and is insufficient evidence for a batching speedup.

Five serial 160,000-person profiles emitted identical complete-document digests.
Person times were 6.115 s (off), 6.324 s (sorted 256), 6.992 s (sorted 1,024),
6.242 s (unsorted 256), and 6.002 s (off repeat). Prefetch reads to window end,
which can exceed the profile's sample cutoff; the full ingests have no cutoff.
These profiles exclude Elasticsearch and cannot substitute for full timings.

The candidate passed **150 tests, no skips, in 14.405 s**, using real Redis and
Zstd with prefetch enabled. Independent validation of the final candidate index
passed all four aggregate checks and **267 complete raw-source comparisons**.
Every full run indexed one million persons, retained all 2,083,023 roles and
6,747,225,963 bulk bytes, and had zero retries/OOM events. Docker inspection
confirmed 2 GiB/four CPUs and read-only source mounts in all four runs. The
candidate index was green with four primary shards, one-second refresh, zero
replicas and request translog durability. The frozen official grader was not
rerun in this experiment; its unresolved placeholders remain unchanged.

All evidence is in `bench/prefetch_trials.json`; all **25 full runs** are retained
in `bench/performance_trials.json`. `bench/latest_default_run.json` records the
new 50.037-second baseline repeat. The earlier 47.618-second default and review
are preserved in `bench/pre_prefetch_default_run.json` and
`bench/pre_prefetch_review.json`. Local timing variation prevents claims of an
absolute maximum. `bench/prefetch.compose.yml` reproduces the isolated setup.

To reproduce a sorted trial after building/starting that project, run:

```sh
docker compose -p etl-prefetch-test -f docker-compose.yml -f bench/prefetch.compose.yml build pipeline
docker compose -p etl-prefetch-test -f docker-compose.yml -f bench/prefetch.compose.yml up -d --wait elasticsearch
docker compose -p etl-prefetch-test -f docker-compose.yml -f bench/prefetch.compose.yml run --rm --no-deps -e RESET_INDEX=1 -e ORG_PREFETCH_RECORDS=256 pipeline
```

Use records `0` for a control. Optional bounds are `ORG_PREFETCH_SOURCE_BYTES`
(default 2 MiB, maximum 8 MiB), `ORG_PREFETCH_BYTES` (default 8 MiB, maximum
32 MiB), records (maximum 4,096), and `ORG_PREFETCH_SORT` (0/1). At most 4,096
distinct IDs are collected; pathological windows fall back to individual reads.
Byte-budget exhaustion also falls back without falsely marking IDs missing.
Redis's existing remote prefetch retains its own limits.

## Earlier LMDB codec and transaction tuning

The selected configuration in that experiment indexed **1,000,000 people in 47.618 seconds**
at **21,000.28 persons/s**, with **1.394 GiB peak pipeline memory**. It uses
Zstd level 1 with a freshly trained 32 KiB dictionary, write transactions capped
at 10,000 records or 64 MiB of compressed values, and LMDB buffer reads.
Per-record `put(overwrite=False)` remains the default; `putmulti` was tested
but did not improve staging. The pipeline remains limited to **2 GiB/four CPUs**.
Eight person workers, four staging producers, 4 MiB private caches, one sender
per worker, 8 MiB bulks and four primary shards remain as previously selected.

Four full ingests alternated the preserved pre-optimization image (A) and the
candidate (B), in A/B/A/B order. The first A used fresh ES storage; later runs
reused that ES container and storage. Each ingest used a fresh pipeline
container, rebuilt all organizations from raw files into a temporary store,
and recreated the person index. Input caches were warm from profiling, and
host cache/ES warmup were not controlled. No heavy profiling or validation
ran concurrently with ingestion. Image builds and ES startup are excluded;
dictionary sampling/training, all staging, ingestion and finalization are
included in the reported times.

| Configuration | Stage s | Person s | Total s | Persons/s | Pipeline peak GiB |
| --- | ---: | ---: | ---: | ---: | ---: |
| A: original, first ingest into fresh ES storage | 7.754 | 49.353 | 60.606 | 16,500.14 | 1.720 |
| B: Zstd candidate, first ingest | 7.251 | 38.840 | 49.613 | 20,155.98 | 1.395 |
| A: original, repeat | 10.575 | 38.824 | 53.010 | 18,864.42 | 1.722 |
| B: selected default, repeat | 9.495 | 34.474 | **47.618** | **21,000.28** | **1.394** |

Against the adjacent original repeat, the selected repeat observed **11.32%
higher throughput**, **10.17% less total wall time**, and **19.05% lower peak
pipeline memory**. Its staged files occupied 826,138,948 bytes, versus
1,190,666,368 bytes for that original repeat: **30.62% less space**. These four
local measurements support the selection, but do not establish a portable
speedup or an absolute maximum. All four runs indexed the same 1,000,000
persons and 2,083,023 roles, emitted 6,747,225,963 bulk bytes, and retained
3,638 unresolved references. Each had zero document retries, zero sampled
swap, and zero cgroup memory-limit/OOM events.

The selected repeat consumed **124.775 CPU-seconds**, versus 145.605 for the
original repeat, a **14.31% reduction**. It averaged **2.62 pipeline cores**
(65.51% of the four-core quota) overall and **2.99 cores** (74.74%) during
person ingestion. The original repeat averaged 2.75 overall and 3.05 during
person ingestion. The codec probe observed faster decompression, and the
smaller store reduces its file-page footprint. Lower CPU/RAM consumption
together with higher throughput is useful. Remaining waits and phase tails
still prevent sustained full CPU occupancy.

Selection began with a codec-only probe of 3,072 held-out organization bodies
and a separate full-store/160,000-person serial microprofile. Every tested
codec round-tripped complete bytes; all store variants emitted the same
complete-person SHA256. The original store's staging time changed from
16.77 s to 9.38 s on repetition, illustrating why a single warm-cache profile
is insufficient. A 50,000-record ISA-L/putmulti trial staged in 9.68 s while
peaking at 1.805 GiB. Zstd with 10,000-record per-row writes staged in 6.04 and
6.31 s, whereas the two corresponding putmulti trials took 7.33 and 8.43 s.
Those are profiles without Elasticsearch, not full-ingestion throughput.
Buffer reads avoid copying compressed values; the profile does not establish
an isolated speedup for that flag. Full A/B results test the combined selection.

Dictionary preparation retains at most 4 MiB of sample bytes, selects at most
128 records per file from its first 2,048 rows, and bounds selected records at
64 KiB. The selected full run trained from 1,024 samples containing
2,612,524 bytes; sampling/training/setup took **0.381 s**, included in staging.
Training uses only that run's raw input. Immutable metadata records the
dictionary SHA256, and spawned producers/readers verify and reopen it in their
own processes. Tiny valid inputs fall back to plain Zstd with a recorded reason;
malformed input still fails. Metadata and dictionary files remain in temporary
staging and are removed during normal cleanup. Full source JSON is preserved;
there is no pickle or conversion to a reduced schema.

The final-default Docker suite passed **140 tests with no skips** in **14.015 s**,
using real Zstd and Redis. An earlier suite passed the same 140 in 13.876 s.
New checks cover transaction boundaries, duplicate rejection across blocks/
commits, owned values after closing a buffer reader, trained-dictionary joins
through spawned producers, metadata/dictionary corruption, strict Zstd decode
limits, and tiny-input fallback. Final independent validation passed all four
aggregate checks and **267/267 entire-source comparisons**. The source/mapping
and request translog durability remain unchanged.

The final index was green with four active primary shards, no unassigned
shards, one-second refresh and request translog durability. Docker inspection
confirmed the 2,147,483,648-byte/four-CPU limits and exit 0 for every full trial.
The unchanged official grader still returned exit 1 solely for its two supplied
zero placeholders: the 1,000,000-person check passed, while observed title and
organization counts remained 7,081 and 647. Both frozen files were unchanged.

The reproducible probes are `bench/profile_compression.py` and
`bench/profile_store.py`. Their aggregate evidence and all four full runs are
preserved in `bench/compression_probe.json` and `bench/lmdb_trials.json`;
`bench/pre_prefetch_default_run.json` holds that selected repeat. The earlier 83.871 s
fresh-volume verification is retained in `bench/pre_lmdb_default_run.json`
and the historical section below. Current verification is in
`bench/pre_prefetch_review.json`; at that point `bench/performance_trials.json`
contained **21 full runs**, including those four comparisons. The isolated overlay is
`bench/lmdb.compose.yml` with project name `etl-lmdb-tuning`.

## Verification before LMDB codec tuning

Three independent agent reviews checked the local ingestion, optional scaling,
and deliverables. The pipeline image rebuilt successfully without cached build
steps, including installation of pinned direct dependencies. An isolated Compose
project used fresh Elasticsearch and staging volumes; its pipeline/ES override
changed only the host ES port and metrics destination. A separate idle test Redis
service supported integration tests. The raw input cache was warmed by a
fresh independent audit, so this is not a controlled cold-cache experiment.

The then-current local defaults indexed **1,000,000 people in 83.871 seconds**:
**11,923.06 persons/s**, with **1.539 GiB peak pipeline memory** and **2.55
average pipeline CPU cores**. Organization staging took 17.034 s and the person
phase 62.837 s. This is slower than the 47.30/53.10-second reused-ES trials below;
the fresh-volume result is preserved rather than discarded. No portable
throughput threshold or uniform improvement is inferred from these different
execution contexts. Cgroup memory-limit/OOM events were zero; sampled swap peaked
at 44 KiB. Docker inspection confirmed 2,147,483,648 bytes, four CPUs, exit 0,
and no OOM kill. ES ended green with four active primary shards, zero unassigned
shards, one-second refresh and request translog durability.

All **105 tests passed with no skips in Docker** (14.254 s), including real
Redis checks. New regressions exercise duplicate-person count rejection,
malformed-input failure metrics, deterministic source sampling and Redis staging
ownership loss. Ready publication now atomically verifies the original staging
owner. The integrated coordinator already rejected restart loss through its run
metadata; this additionally protects the staging component itself.

The fresh full raw audit reproduced all prior feed count/error aggregates in
70.849 s. Independent validation passed the three exercise counts, the 1,858
unresolved-person count, and **267 complete raw-source comparisons**. Sampling
now retains 32 deterministic reservoir samples per file plus distinct examples
of missing and unresolved organization IDs. It scans every person file, retains
bounded samples and reconstructs joins without production code. This validation
runs after ingestion and is excluded from ingestion timing.

The unchanged official grader still exits 1 solely for the supplied zero
placeholders. The documented performance script exits 0 and prints the new
measurement. That verification's evidence is in `bench/pre_lmdb_review.json` and
`bench/pre_lmdb_default_run.json`; those 17 full runs remain in
`bench/performance_trials.json`. `REVIEW_GUIDE.md` supports actual candidate
review; no human code review is claimed by these agent checks.

`bench/review.compose.yml` preserves the verification overlay. Use it after
`docker-compose.yml` with project name `etl-final-review` to isolate its volumes
and bind ES to port 19200. It requires Compose support for `!override` (verified
with Compose 5.5.1); the default submission command does not use this overlay.

## Earlier CPU and memory tuning

The user requested fuller use of CPU/RAM under the existing limits. Eight full
trials tested process count, organization-cache budget, staging producers,
bulk overlap, and primary shards. The selection used **eight person processes,
four staging producers, 4 MiB private caches, one sender with 8 MiB batches,
and four primary shards**. The pipeline remains capped at **2 GiB/four CPUs**;
Redis is optional. The source fields, mapping and request translog durability
were preserved.

| Configuration | Stage s | Person s | Total s | Person CPU cores | Pipeline peak GiB |
| --- | ---: | ---: | ---: | ---: | ---: |
| Four processes, 16 MiB caches, three stage producers: control | 9.45 | 44.40 | 56.98 | 1.94 | 1.529 |
| Eight processes, 16 MiB caches, three stage producers | 12.23 | 36.87 | 52.72 | 3.08 | 1.869 |
| Eight processes, 32 MiB caches, four stage producers | 9.96 | 37.39 | 50.51 | 2.98 | 2.000* |
| Eight processes, two 4 MiB senders, 16 MiB caches | 9.39 | 37.53 | 50.43 | 2.94 | 1.831 |
| Eight primary shards, eight processes, 16 MiB caches | 10.71 | 37.74 | 52.16 | 2.83 | 1.868 |
| Four shards, eight processes, 16 MiB caches | 10.38 | 37.54 | 51.25 | 2.91 | 1.872 |
| Selected 4 MiB caches: first trial | 8.76 | 35.24 | **47.30** | 3.02 | 1.725 |
| Selected defaults: repeat confirmation | 10.11 | 39.71 | **53.10** | **2.93** | **1.726** |

*The 32 MiB cache trial's cgroup peak was 2,147,487,744 bytes, one 4 KiB page
above the configured 2,147,483,648-byte limit. The hard limit was not raised;
kernel accounting can briefly overshoot. This configuration was rejected for
its lack of memory headroom and no person-phase improvement.

The confirmation indexed **18,833.58 persons/s**, versus the instrumented
four-process control's **17,551.23/s**: **7.31% higher observed throughput**,
with **6.81% lower total wall time**. The same defaults first reached 21,141.27/s
(47.30 seconds). That material repeat variation means the fastest trial is not
the headline, and an absolute maximum or a stable percentage gain is not
claimed. Trials reused the ES container and warmed host cache, with a fresh
pipeline container, freshly staged organizations and recreated index each time.
Only the low-frequency numeric ES sampler ran alongside ingestion.

CPU measurements come from **cgroup counter deltas for every container process
and thread**, rather than only worker timers. The confirmation consumed
144.95 CPU-seconds over 53.10 seconds: **2.73 average cores / 68.25% of quota**.
Its person phase used **2.93 cores / 73.37%**, versus the control's **1.94 /
48.59%**. Staging used 2.80 cores. Short sampled bursts reached the CPU ceiling;
200 ms samples can cross scheduler/accounting boundaries and exceed four, so
they are not interpreted as a higher configured quota. Throttled CPU counters
also sum overlapping per-CPU time and are not wall-clock idle percentages.

Peak pipeline memory was **1,853,595,648 bytes / 1.726 GiB**, about **86.3% of the
2 GiB allowance**, including shared mmap and filesystem pages. The confirmation
recorded zero sampled swap and zero cgroup memory-limit/OOM events. Filling
private caches to the memory ceiling did not improve the person phase; using
less duplicate private cache left room for shared LMDB pages. Remaining idle
CPU includes bulk-response/I/O waits, phase barriers and end-of-file tails;
forcing 100% occupancy is not itself a throughput improvement.

ES averaged **4.79 CPU cores** over an approximately 54.11-second sampled
interval around the confirmation. Its write queue peaked at 12, with zero
write rejections, zero recorded indexing/merge throttling, no old-generation
GC, and 1.511 seconds of young-GC counter time. The two-sender and eight-shard
trials raised queue peaks to 35 and 37 without a person-phase gain. ES CPU is
separate from the pipeline's four-core quota. ES heap remains 2 GiB and its
indexing buffer 512 MiB.

Snapshots and numeric ES summaries for all eight runs are saved in
`bench/cpu_ram_trials.json`; that series' repeat confirmation is in
`bench/cpu_final_default_run.json`. Every run acknowledged 1,000,000 persons and
2,083,023 roles with zero bulk document retries. Independent counts and all
24 full-source sample joins passed on the final index. The frozen placeholder
grader defect described below remains unchanged.

At that stage, all **98 tests passed in Docker** in 12.174 seconds. The new tests validate
cgroup v1/v2 CPU normalization, counter resets/missing readings, throttle
interpretation, and shard-tuning preservation of mappings/durability. The
existing full enrichment, HTTP, spawned staging and real Redis checks passed.

## Earlier local and optional Redis measurements

After the user clarified that horizontal scaling only needs to be easy to
enable later, local LMDB staging remained the default. The Redis coordinator
and leased worker path lives in the optional `docker-compose.scale.yml` overlay.
Both full runs below used one constrained pipeline container and raw feeds:

| Configuration | Stage s | Person s | Total s | Persons/s | Pipeline peak GiB |
| --- | ---: | ---: | ---: | ---: | ---: |
| Optional Redis, one pipeline | 9.5803 | 43.9304 | **59.0291** | **16,940.80** | **0.832** |
| Pre-CPU-tuning LMDB confirmation | 10.8026 | 45.9792 | **60.2408** | **16,600.03** | **1.528** |

Snapshots are in `bench/redis_single_run.json` and `bench/pre_cpu_default_run.json`.
Both indexed 1,000,000 unique persons, preserved 2,083,023 roles, acknowledged
816 bulk requests with zero document retries, and passed independent counts
and 24 complete raw-source sample comparisons. That local confirmation ran at
10:32:29–10:33:29 CEST on 7 October 2026. Its cgroup peak was 1,640,726,528 bytes;
Docker inspection again confirmed 2,147,483,648 bytes and four CPUs.

Redis took 2.6% longer than the earlier 57.5565-second local confirmation, but
2.0% less time than the adjacent 60.2408-second local run. These small
single-trial differences do not establish a throughput improvement or
regression. Both new runs started fresh ES containers using existing data
volumes and a warm host cache. ES startup and builds are excluded; all raw
staging and finalization/cleanup are included. The earlier tuning trials reused
an ES container. These are observations, not a controlled statistical study.

Redis allocated 1,564,619,520 bytes (1.457 GiB) after staging, with
1,597,419,520 bytes RSS (1.488 GiB). The coordinator peaked at 355,655,680 bytes
(0.331 GiB). These services are additional to the pipeline and ES, and these
different-phase readings are not a simultaneous combined memory peak. Eight
organization loaders ran in the unconstrained coordinator instead of three
inside the constrained local pipeline. The Redis pipeline averaged 1.832 CPU
cores over its 52.97-second lifetime, including waiting for staging. Local
person-worker CPU time was 88.12 seconds over a 45.98-second person phase,
about 1.92 cores; this omits parent/system CPU. Waiting for Elasticsearch and
I/O leaves CPU headroom. Filling all remaining RAM is not itself a throughput
goal; the earlier extra-sender trial did not improve total time reliably.

The optional architecture stages full orgs once, batch-prefetches raw JSON,
and uses renewable file leases and token-fenced terminal statistics. Real
Redis tests cover concurrent claims, expiry, replay, stale ownership, and
idempotent acknowledgements. One full Redis pipeline run was tested; no
two-replica throughput claim is made. Each replica stays at 2 GiB/four CPUs;
two would double the aggregate pipeline budget. Eight gzip files bound useful
file-level concurrency. Redis/coordinator failure requires stopping the entire
stack and rebuilding; worker leases support same-generation file replay.

All **84 tests passed in Docker** in 12.163 seconds, including 31 real Redis
checks, heartbeat/chunk lifecycle tests, the existing ingestion suite, and
rejection of incomplete distributed resource reporting. Frozen placeholder
fixtures remain unchanged. The historical confirmation below is preserved.

## Measured full run

On 7 October 2026, 10:00:38–10:01:36 CEST, the pipeline indexed the supplied
raw bundle into a fresh Elasticsearch 8.13.4 `persons` index. The container
exited 0. The benchmark is in `bench/measured_run.json`; current-run reporting
uses `metrics/latest.json` and `bash bench/perf.sh`.

| Measurement | Observed value |
| --- | ---: |
| Persons indexed | 1,000,000 |
| Full-run throughput | **17,374.25 persons/s** |
| Wall-clock total | **57.5565 s** |
| Organization staging | 10.3539 s |
| Person ingestion | 43.1943 s |
| Person-stage throughput, excluding staging/finalization | 23,151.18 persons/s |
| Peak pipeline cgroup memory | 1,642,057,728 bytes / **1.529 GiB** |
| Elasticsearch container lifetime cgroup peak | 6,076,887,040 bytes / **5.660 GiB** |
| Organization staging file size | 1,189,990,528 bytes / 1.108 GiB |
| Bulk requests | 816 |
| Bulk request bytes | 6,747,225,963 |
| Retried document attempts | 0 |

The rate is persons divided by monotonic elapsed time from pipeline entry to
completed finalization. It includes connection/index setup, reading/staging all
organizations, reading/joining all people, bulk acknowledgements, final flush,
refresh, count, and stage cleanup. Docker image download/build and Elasticsearch
startup precede this interval and are excluded. Raw gzip inputs were mounted
read-only; no precomputed joins or staging data were reused.

The Docker Desktop Linux VM reported 12 logical CPUs and 8,297,881,600 bytes
RAM. Docker inspection confirmed pipeline memory=2,147,483,648 bytes and
NanoCpus=4,000,000,000. Elasticsearch used a 2 GiB JVM heap, a 512 MiB indexing
buffer, and no container ceiling. The measured pipeline, staging, and Elasticsearch ran through
Compose; the independent raw-data audit also ran locally for verification.

Pipeline memory is the cgroup v2 `memory.peak`, supplemented by 200 ms
`memory.current` samples. It includes every worker and charged file/mmap page
cache; it is not the cache-subtracted figure shown by `docker stats`.
Elasticsearch's peak was read from its reused container after the tuning runs
and verification. It includes startup, earlier runs, and page cache; it is not
attributed to the selected run. The per-run ES peak is unavailable and remains
null in the selected metrics. The original fresh-container measurement is
preserved in `bench/baseline_run.json`. The pipeline cannot read another
container's cgroup; `bench/perf.sh` labels its external reading as a lifetime
peak. Any run-specific resource sidecar must have a matching run ID.

The selected configuration ran twice: 54.0920 s (18,487.01 persons/s) and the
57.5565 s confirmation above (17,374.25 persons/s). Relative to the original
88.5158 s run, observed throughput improved 53.79–63.64%, while wall time fell
34.98–38.89%. Use the confirmation for the reported headline, rather than only
the fastest trial. The source bytes, indexed counts, search mapping, translog
durability, pipeline CPU ceiling, and memory ceiling stayed unchanged.

`bench/performance_trials.json` preserves every full trial:

| Configuration | Stage s | Person s | Total s | Pipeline peak GiB |
| --- | ---: | ---: | ---: | ---: |
| Original serial stage, synchronous 4 MiB bulks | 24.65 | 60.80 | 88.52 | 1.414 |
| Instrumented serial baseline, concurrent development* | 33.76 | 76.86 | 116.26 | 1.440 |
| Parallel stage, overlapped 4 MiB bulks | 11.25 | 52.56 | 65.44 | 1.479 |
| Parallel stage, overlapped 8 MiB, fewer payload copies | 9.59 | 40.43 | 54.09 | 1.531 |
| Same 8 MiB configuration with two senders per worker | 15.25 | 41.54 | 60.43 | 1.621 |
| Selected one-sender configuration, confirmation | 10.35 | 43.19 | 57.56 | 1.529 |

*The instrumented baseline overlapped development/microprofiling and is not an
isolated control. It is included as evidence but not used for the headline
speedup. Other trials used a warmed host cache and reused ES container, with
no active merges or indexing throttling observed before candidate trials.
They are not a randomized repeated cold-cache hardware-normalized study. Disk
cache, Windows bind mounts, CPU, host load, and ES resources still cause
variation. This earlier series did not vary shard count; the later CPU/RAM
series above compared four and eight shards. Optimality is not claimed.

## Earlier correctness validation

The independent streaming audit in `bench/audit_data.py` found no malformed
rows, duplicate envelope IDs, or source/envelope ID mismatches. Its saved report
is `bench/data_audit.json`; these counts are independent of production staging
and enrichment:

| Check | Raw-data result | Elasticsearch / pipeline result |
| --- | ---: | ---: |
| Unique persons / top-level documents | 1,000,000 | 1,000,000 |
| Unique organizations staged | 529,041 | 529,041 |
| Roles retained | 2,083,023 | 2,083,023 |
| Persons with `Project Manager` title | 7,081 | 7,081 |
| Persons joined to `Dell Technologies` | 647 | 647 |
| Resolved role references | 1,381,699 | 1,381,699 |
| Unresolved role references | 3,638 | 3,638 |
| Persons with unresolved references | 1,858 | 1,858 |
| Full raw-source sample bodies | 24 | 24 exact matches |

There are 3,085 distinct absent organization IDs. Unresolved references are
0.174650% of all roles, or 0.262608% of roles with a non-null organization ID.
Another 697,686 roles have no organization ID; those are preserved and are not
classified as dangling references. Roles remain array members of person
`_source`, so there is no separate role-document count in Elasticsearch.

The supplied `bench/expected.json` contains a note identifying placeholder
expectations and sets both query counts to zero. The unchanged
`bench/correctness.py` therefore returned **exit 1**: total count passed, title
and organization checks failed against those invalid zeros. Both frozen files
are preserved. This is a supplied-fixture defect, not a passing official test
claim; corrected expected values are needed from the exercise provider to make
that particular script green.

`bench/validate.py` returned **exit 0** for the audited expectations. It runs the
same term queries, checks the unresolved-person flag count, and independently
reconstructs three people per input file from the original person and full org
feeds. All 24 entire `_source` bodies matched, including organization fields
absent from role denormalizations. The original input has 17,302 people with
thin prefilled organization arrays; the production join replaces those arrays
with full resolved feed records. Simply copying role organization names would
produce 570 Dell matches, not the correct 647.

At that earlier stage, all **46 unit and local HTTP integration tests passed in
Docker** (11.501 s). They cover full-detail enrichment, role/organization ordering
and deduplication, unresolved/missing references, JSON-fragment equivalence,
cache eviction, invalid/truncated input, duplicate org rejection, selective
item retries, ambiguous lost responses with stable IDs, permanent failures,
retry exhaustion, malformed/contradictory bulk responses, HTTP-200 partial
shard failures, and incomplete/mismatched performance evidence. New tests cover
real spawned parallel staging, producer record/byte caps, prompt producer
failure cancellation, bounded background sends, reversed response completion,
and failed/final pending acknowledgements. Bash syntax
checking passed for `bench/perf.sh`; its reporter ran against completed metrics.

## Decisions and limits

**Compressed LMDB staging.** Organization compact JSON occupies 1.354 GB before
Python object overhead. The sampled decoded-object estimate is 5.16 GB, beyond
the pipeline budget. An immutable on-disk store shares mmap pages across eight
workers; compression and 4 MiB byte caches constrain private memory. Deferred
stage syncing is safe because staging is rebuildable and readers start only
after final sync. Four producers parse/compress independent files while a
single writer drains a six-block queue; each normal block is capped at 256 rows
or 1 MiB of compressed values. A single oversized row is still subject to the
16 MiB input limit. The selected Compose settings cap write transactions at
10,000 records or 64 MiB of compressed values, with per-row duplicate-rejecting
writes. These payload bounds do not replace the container memory ceiling;
LMDB pages and Python overhead are included in the measured cgroup peak.
The runtime-trained Zstd dictionary and its metadata live only in temporary
staging. Buffer reads avoid a compressed-value copy; decompression creates
owned bytes before cache insertion or return. Failures cancel producers and
release queues. Staging took 9.495 s in the current full repeat, versus 10.575 s
for its adjacent original control. LMDB page overhead means the
file is larger than compressed values alone; raw bytes are embedded directly
with `orjson.Fragment` on lookup. No unconstrained staging service or hidden
external compute is used. The 16 GiB map ceiling is virtual address space on
Linux, not allocated RAM. Windows LMDB can report this ceiling as apparent file
size, which is why measured staging happens in Docker/Linux.

**Denormalized full organizations.** Reads need no secondary lookup. Each person
contains full details for each distinct resolved organization, while roles
retain their original denormalized fields. This creates repeated source bytes
and makes updates a reingestion concern; parent-child or an application-side
join would reduce duplication but change the requested output and query path.

**Explicit object mapping.** Object arrays match the plain person-level `term`
query contract. Nested mappings would add a hidden Lucene document per array
member and require different queries. Multi-field conditions can cross object
boundaries; nested isolation is a future schema/API change. Full organization
metadata stays in `_source`, with only IDs and names indexed. Dynamic mapping
is disabled; no feed field is discarded. Many dates use a numeric timezone
offset rather than the example's `Z`, so dates remain stored strings. Explicit
`.keyword` fields support exact counts; norms and positions are omitted where
unneeded. Phrase searches on those text fields are unsupported.

**Bounded request concurrency and request durability.** Eight worker processes
each own one sending batch and one building batch by default. One background
sender overlaps ES work with preparation, and joining retained source byte
buffers once avoids repeated payload copies. Bulks target 8 MiB or 2,000 docs,
which reduced requests from 1,627 to 816 in an earlier bulk-size trial. Waiting before further submissions
bounds futures and provides backpressure; each sender has a private HTTP
session. The default permits eight active requests within the four-CPU quota.
A two-sender trial permitted sixteen active requests and increased the ES write
queue without establishing a repeatable throughput gain, so each worker retains
one sender.
Worker CPU seconds include thread CPU; `worker_wall_seconds` and
`bulk_call_seconds` are summed overlapping durations, not extra elapsed stages.
Failed transient items get exponential jittered
retries, while ambiguous transport failures resend stable document IDs. The
observed healthy run needed zero retries. Replicas are zero for a single node,
refreshes are disabled only during load and restored after success, and
translog durability remains `request`. Final shard-result checks and document
count prevent partial completion from being reported as success. Elastic's
[indexing guidance](https://www.elastic.co/docs/deploy-manage/production-guidance/optimize-performance/indexing-speed)
and [object-array semantics](https://www.elastic.co/docs/reference/elasticsearch/mapping-reference/array)
informed these choices.

**Failure/restart behavior.** Strict validation fails a corrupt input rather
than silently indexing less data. Running workers observe a cancellation flag
after a peer fails; an in-flight HTTP request can finish before cancellation.
Failure leaves a partial `persons` index; `RESET_INDEX=1` explicitly rebuilds
that one index. There is no resume checkpoint, incremental update policy, or
zero-downtime replacement alias. The successful run restores refresh, but a
failed/terminated run can leave it disabled until rebuild. SIGKILL can leave
scratch files; a new run never trusts them. Inputs beyond 16 MiB per row,
32 MiB per enriched document, or the LMDB map/disk capacity fail validation.
The enriched size check runs after serialization and is not a strict transient
allocation cap. These limits were not reached by the supplied data.
