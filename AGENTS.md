# Working instructions

Implement the Forager take-home against the raw gzipped NDJSON in `data/`.
Keep the pipeline service at 2 GiB memory and four CPUs. Never modify
`bench/correctness.py` or `bench/expected.json`: supplied expected counts are
placeholders; report independent observed counts separately.

Stream input, preserve the full `serialized_data` bodies, join against organization
envelope IDs, retain every role, and explicitly flag unresolved references.
Bound queues, caches, batches, and worker counts. A failed row or bulk item must
make the run fail rather than silently reducing the dataset.

Run focused unit/integration checks and the full Docker ingest when available.
Write only observed measurements and actual AI/human interventions in the
evaluation and AI notes. Do not print personal data during profiling or tests.

Parallel ownership: organization staging/enrichment, independent data audit,
HTTP/ingestion orchestration, and tests/reporting can be developed separately.
Review the interfaces and combined behavior before delivering.
