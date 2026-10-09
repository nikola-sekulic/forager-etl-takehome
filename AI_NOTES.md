# AI usage notes

## Where the human stepped in

I directed the requirements, performance investigation, and verification work.
My work focused on the following engineering decisions and checks:

- **Prepared the inputs and execution environment.** I extracted the supplied
  data into `data/`, installed Docker when it was initially unavailable, and
  confirmed that I had no additional information about the expected counts.
- **Set the engineering priorities.** I asked how the results would be scored,
  challenged whether ingestion could be faster within the existing constraints,
  and repeatedly requested retests, latest numbers, CPU usage, and evidence that
  RAM and CPU were being used productively. Those requests drove the measured
  optimization work and resource accounting.
- **Defined the scaling scope.** I required a straightforward path to horizontal
  scaling, highlighted the brief's allowance for local Compose staging services,
  and clarified that deploying multiple replicas was unnecessary for this
  take-home. That direction shaped the optional Redis path while keeping the
  default local LMDB workflow.
- **Investigated the join and memory model.** I asked for an end-to-end walkthrough
  and questioned how a person finds its organization across chunk files and
  whether all organizations could fit in memory. These questions prompted an
  explicit explanation of envelope-ID lookup, disk-backed staging, shared page
  cache, and bounded private caches.
- **Researched and supplied concrete optimization ideas.** I brought detailed
  LMDB guidance covering transaction size, sorted writes, map sizing,
  compression, read-only worker environments, transaction reuse, locality, and
  caching. I followed with recommendations tailored to 2 GiB and four cores:
  process layout, byte-bounded bulks, `orjson`, trained Zstd dictionaries, GC,
  pre-serialized actions, retry safety, checkpoints, and Elasticsearch settings.
  I asked Codex to assess these ideas before changing anything, then authorized
  the worthwhile changes with testing. The later storage/compression comparisons
  were a direct result of this investigation.
- **Required the batch-lookup experiment.** I proposed collecting unique
  organization IDs for a person batch, sorting them, fetching them in one read
  transaction, and joining from a temporary dictionary. I explicitly requested
  a test. The full comparison found no useful speedup, so Codex kept it disabled
  and recorded the negative result. Supplying a plausible hypothesis and
  requiring measurement was part of my contribution; the result was not assumed.
- **Resolved the grading ambiguity and demanded review.** I contacted the exercise
  provider and relayed its instruction to skip the mistaken correctness test.
  I also requested thorough checks while awaiting the response, a final code
  review, and the fix for the timing issue that review uncovered.

Codex authored the implementation, tests, profiling tools, and documentation,
executed the checks, and selected low-level settings from the measurements.
I supplied the direction, technical research, scope decisions, and requests for
evidence described above. This account does not claim candidate-written code
or a completed manual code review.

## Provider clarification: October 9, 2026

The human candidate relayed the exercise provider's response: "I seem to have
missed this. This was a mistake on our part, for now please skip this test."
Codex updated the current submission guidance to skip the supplied correctness
test, preserved both frozen files, and retained independent validation. The
reply is a waiver of that test, not confirmation of the measured counts.
Earlier references to awaiting corrected expectations describe the historical
state. Contacting the provider resolved the submission's grading ambiguity.

Codex then built the final image and ran a fresh isolated Docker verification.
A read-only subagent checked submission requirements and waiver wording while
the parent ran the checks; it made no edits. The full suite passed 150 tests,
no skips, in 13.866 s. Final defaults ingested one million raw person rows in
64.738 s at 1.387 GiB peak pipeline memory. Independent validation matched all
four aggregate counts and 267 entire-source joins. Codex preserved prior
measurements/review, recorded the official test as skipped, and retained all
26 full runs. These remain agent-run checks. The review found the Git remote
still points to the starter repository; no push or publication was performed.

## Review follow-up and timing fix: October 9, 2026

I asked for another code review, then requested the fix it identified and a more
complete account of my contribution. Codex reproduced a native Windows
zero-duration failure that could prevent final failure metrics from being saved,
changed duration timers to `perf_counter`, and added zero-interval guards in the
local and optional distributed paths. Five new regressions cover frozen-clock
finalization; the focused native suite passed 17 tests and 20 repeated actual
missing-input failures retained terminal failed metrics.

The full Docker suite passed **155 tests with no skips in 13.877 s**. A fresh
constrained ingest indexed one million people in **65.290 s**, using **1.384 GiB**
peak pipeline memory, followed by all four passing aggregate checks and **267
complete raw-source matches**. Earlier measurements are preserved; all **27 full
runs** are retained. This was a reliability fix, with no measured speedup claim.
Codex handled implementation, distributed regression checks and documentation
through agents with separate file ownership. My direction, research and
verification requests are documented at the start of this file; code and test
authorship remains attributed to Codex.

## Tools used

OpenAI Codex desktop performed the implementation, data exploration, review,
and test orchestration. This session used its configured GPT-6-family agent;
no exact model build/version or desktop app version was captured. The initial three parallel
Codex subagents inherited the session settings. PowerShell, Docker/Compose,
the bundled Python 3.12 runtime, standard-library unittest, and a real local
HTTP test server supplied the execution and verification tools. Elastic's
primary documentation was consulted for mapping and indexing settings;
LMDB and python-zstandard documentation informed buffer ownership and
runtime dictionary APIs during the later storage tuning.

No Claude, Cursor, Aider, custom plugin, MCP server, or personal skill was
configured for this task. The desktop's existing tool integration exposed the
shell/runtime and subagent tools; it was not a new integration authored here.

## Task artifacts

- `AGENTS.md`: actual project instructions authored for this task. They protect
  the original grader files, require streaming/bounded memory and complete
  joins, separate ownership, and forbid fabricated measurements or fabricated human interventions.
- `tests/`: executable verification rails, including fault-injecting HTTP tests.
- `bench/audit_data.py` and `bench/data_audit.json`: reproducible independent
  source-count/type audit and its aggregate-only observed report.
- `bench/validate.py`: independent reference joins compared with complete ES
  source bodies; it imports no production join code.
- `bench/perf.sh`, `bench/report.py`, and `bench/measured_run.json`: measured
  reporting rather than hardcoded success or invented throughput.
- `bench/baseline_run.json` and `bench/performance_trials.json`: preserved
  original benchmark, every tuning trial, and the final confirmation. These
  include the slower two-sender trial rather than hiding it.
- `bench/profile_transform.py`: reproducible real-data microprofiling. It builds
  a temporary lookup only for sampled references, verifies candidate byte/stat
  equivalence, alternates measurement order, and never supplies preprocessed
  data to production ingestion. Its reports are local warm-cache microprofiles.
- `pipeline/codec.py`, `bench/profile_compression.py`, and
  `bench/profile_store.py`: runtime codec handling and independent bounded
  comparisons of codec/store variants. Fresh dictionaries stay in temporary
  staging; reports contain aggregate measurements and digests.
- `bench/lmdb_trials.json` and `bench/latest_default_run.json`: the later
  codec/transaction experiments, all four A/B full ingests, and the selected
  repeat. `bench/pre_lmdb_default_run.json` preserves the earlier slower
  fresh-volume verification, rather than overwriting that evidence.
  `bench/final_review.json` records current verification; its prior counterpart
  is preserved in `bench/pre_lmdb_review.json`. All 27 full runs remain in
  `bench/performance_trials.json`.
- `pipeline/prefetch.py`, `tests/test_prefetch.py`, `bench/prefetch.compose.yml`
  and `bench/prefetch_trials.json`: the user's requested bounded sorted-lookup
  experiment, ten new regression checks, five profiles and four full runs.
  Prefetch stays disabled. The previous default result/review are preserved in
  `bench/pre_prefetch_default_run.json` and `bench/pre_prefetch_review.json`.

The user asked Codex to test batching after suggesting deduplicated, sorted
organization lookups. Codex implemented and tested the optional version without
subagents for this experiment. The repeated baseline took 50.037 s; sorted
256-person prefetch took 50.596 s with 14.31% more CPU work. Deduplication removed
only 0.88% of lookup consumptions. Codex retained the disabled default and saved
negative results. The real Redis/Zstd suite passed 150 tests without skips in
14.405 s, and independent final-candidate validation matched 267 entire source
documents and all four aggregates. These were agent-run checks, not human review.

These are files in the submission repository. There are no unshipped claims
about skills, hooks, MCP settings, or saved agent configurations. The subagents
were dispatched through Codex's built-in collaboration calls; no custom agent
config was authored. Their initial task scopes were organization storage/enrichment,
raw-data audit/validation, and tests/performance reporting, while the parent
owned bulk HTTP, orchestration, mapping, Compose, and final documentation.

## Agentic loops

1. **Read -> inspect -> constrain.** Read the brief, evaluation templates,
   Compose limits, data README, and untouched correctness tests. Check actual
   file layout and runtime availability before designing the join. Discovery
   showed the fixture zeros were explicitly placeholders; preserve them and
   derive independent counts instead of changing the grader to pass.
2. **Parallel fan-out with file ownership.** Assign storage, data audit, and
   protocol tests to separate agents. Communicate the exact function interfaces,
   counters, unresolved-reference policy, and permitted files. The parent
   implemented the process/bulk path while the audit scanned all raw inputs.
3. **Observe -> revise.** Raw inspection found prefilled thin organization
   arrays and roles without organization IDs, contradicting simplified example
   assumptions. Adopt a complete feed-based replacement join and distinguish
   missing IDs from unresolved IDs. Byte/memory profiling supported compressed
   staging instead of keeping decoded organization objects in memory.
4. **Fault injection -> review -> fix.** Tests exercised selective 429/503
   retries, response loss, permanent failures, and malformed replies through
   real HTTP. Independent review found that HTTP 200 can still contain failed
   shard results and that bulk `errors` flags could contradict item statuses.
   Tighten both validators and add regression tests. Add a shared cancellation
   event so peer files stop after a failure.
5. **Container verification -> independent reference comparison.** Build the
   image, run the test suite under Compose, ingest the full raw dataset with
   unchanged container ceilings, and read cgroup peaks. Verify all three
   exercise queries plus unresolved-person counts, then compare 24 whole
   documents against independently reconstructed raw-source joins. Execute the
   original grader too, reporting its two placeholder failures honestly.
6. **Evidence -> write-up.** Persist measured metrics and source audit, and
   write run commands and trade-offs from the observed run. The performance
   reporter rejects incomplete runs and excludes another run's sampled peaks.
7. **Profile -> bounded experiments -> confirm.** After the user asked about
   further speed, add worker CPU, wall, and bulk-call timers. Parallel agents
   explored staging, sending, and transformation costs. Microprofiles showed
   that joining a pieces list avoided redundant copies with identical NDJSON
   bytes; role mutation and cache removal had inconsistent/small gains, so they
   were rejected. Test parallel producers and bounded background senders before
   running full trials. Increase bulks from 4 to 8 MiB and compare one versus two
   senders. Two senders were slower and used more memory, so select one. Repeat
   the selected configuration, preserve every trial, and revalidate all counts
   and entire source samples. No CPU/memory ceiling was raised and no indexed
   detail or durability guarantee was removed.
8. **User tips -> isolated storage profiles -> full comparison.** The user
   supplied practical LMDB tips, first requested an assessment without changes,
   then explicitly authorized worthwhile optimizations and testing. Parallel
   agents owned transaction/buffer changes, codecs, regression/profiling checks,
   and evaluation/AI notes; the parent owned full-run orchestration, Compose
   defaults and final combined review. Preserve the original image as a control.
   Test larger transactions, `putmulti`, buffer reads and trained Zstd against
   complete organization bytes and complete emitted person digests. The original
   profile's large repeat variation prevented treating an early stage-time
   improvement as a proven gain. Reject the 50,000-record transaction and
   `putmulti` selections on memory/performance evidence. Run A/B/A/B full
   ingests under the unchanged limits before selecting 10,000-record writes,
   Zstd level 1 with a fresh 32 KiB dictionary, and buffer reads. Verify real
   dictionary training, tiny-input fallback, corruption failures and spawned
   readers; revalidate full source bodies after the final ingest.

## Where the agent helped

Codex wrote the compressed LMDB stage, fragment-based enrichment, bounded
multiprocess bulk ingestion, and all new tests and documentation. Parallel
work produced a full raw audit while implementation was underway. The audit
found 647 actual Dell matches versus 570 when only role-name denormalization is
used, which is a concrete guard against a superficially passing join.

The review/test loop caught partial shard success and contradictory bulk
responses before final delivery. The original constrained run measured
11,297.42 persons/s at 1.414 GiB peak pipeline memory. After optimization, the
confirmation measured 17,374.25 persons/s at 1.529 GiB; a prior selected-settings
trial reached 18,487.01 persons/s. These results come from full-run metrics,
not an extrapolated local parsing benchmark.

## Time leverage

The explicitly timed working segment from 09:19:44 to 09:30:52 CEST on
7 October 2026 was 11 minutes 8 seconds, followed by final documentation/review.
Initial exploration occurred before the first explicit clock reading, and
human Docker installation time was not tracked. The data audit took 67.782 s;
the original ingest took 88.516 s, excluding image download/build and ES startup.
These are different measurements and are not added together as candidate time.
The later optimization phase included five additional full runs and a final
46-test Docker suite; the selected configuration's confirmation took 57.556 s.
Full trial times and timestamps are preserved in `bench/performance_trials.json`.

Effectively all newly authored implementation/test/documentation content was
AI-generated; the provided starter files and frozen grader are excluded from
that estimate. No precise counterfactual manual-time estimate was measured.
Parallel auditing and HTTP failure testing are the work that the agent made
practical within this short session.

## Retro

Before the later LMDB tuning, the user requested a final submission check and
three further agents reviewed
local correctness, optional scaling and submission evidence. Codex expanded
independent validation from file prefixes to deterministic reservoirs throughout
all person files, with explicit missing/unresolved-reference examples. New
regressions check duplicate person IDs, failed metrics and lost Redis staging
ownership. Atomic ready publication verifies the original owner; the integrated
coordinator already rejected Redis restart loss. Codex corrected stale historical
documentation and created `REVIEW_GUIDE.md` without claiming completed human review.
`bench/review.compose.yml` ships the isolated verification overlay that was
initially authored in the ignored metrics directory; no custom tooling config
from this review is described without its actual artifact.

A build without cached build steps installed dependencies successfully. The full
Docker suite passed **105 tests with no skips** in 14.254 s. A fresh raw audit
matched all prior count/error aggregates. A fresh ES/staging-volume ingestion
took **83.871 s (11,923.06 persons/s)** at **1.539 GiB** peak pipeline RAM,
followed by passing aggregate counts and **267 complete source comparisons**.
Raw input cache was warmed by the audit. This slower fresh-volume measurement
is recorded alongside the earlier reused-ES timings. At that stage, the unchanged
official grader still failed only its two placeholder expectations. The candidate
requested this thorough verification; Codex performed the reviews, code fixes,
and validation. The provider subsequently waived that test as recorded above.

Obtain verified expected counts from the exercise provider up front, but keep
source-derived counts as an independent oracle. The bounded stage/bulk sweep
showed useful gains, while higher network concurrency did not. With more time,
repeat randomized cold-cache runs to confirm the observed shard/cache-size choices.
The strongest next engineering step is an explicit restart/checkpoint policy
or replacement-index alias, rather than presenting a one-shot initial loader
as a production incremental pipeline. A human review of the final diff remains
valuable and should be documented as actual review, not retroactively invented.
