# StackTrack AI
## Historical Data Warehouse for Tracking the Evolution of the Open-Source AI Ecosystem

**Document status:** Living project specification
**Document type:** Agile project charter + architecture + implementation guide
**Version:** 0.1.0
**Initial scope:** GitHub/GH Archive + Hugging Face
**Planned extension:** PyPI + optional arXiv
**Architecture:** Medallion (Bronze → Silver → Gold)
**Primary objective:** Build a reproducible, incremental, historical data platform that measures how AI technologies evolve across software engineering activity, AI-model ecosystem activity, and package adoption.

---

## 1. Executive Summary

StackTrack AI is a data-engineering project that builds a historical analytical warehouse for studying how open-source AI technologies change over time.

The project combines heterogeneous public data sources representing different parts of the AI ecosystem:

- **GitHub / GH Archive** — software-engineering activity and public repository events.
- **Hugging Face Hub** — models, datasets, Spaces, metadata, popularity/adoption indicators, and repository changes.
- **PyPI** — Python package adoption through package download statistics.
- **arXiv (optional future phase)** — research activity and the research-to-engineering transition.

The project is intentionally designed as a **living Agile system**. The architecture and requirements are versioned and can be extended as new sources, entities, metrics, and analytical questions are validated.

The core analytical idea is:

> **Observe the AI stack as a temporal ecosystem rather than treating repositories, models, and packages as isolated objects.**

The system should allow an analyst to study questions such as:

- How does engineering activity around an AI technology change over time?
- How does model-ecosystem activity change relative to engineering activity?
- Which technologies are growing, stable, declining, or emerging?
- Does increased engineering activity precede increased model adoption?
- Which technologies have large downstream ecosystems?
- How quickly do technologies move between ecosystem stages?
- How does the composition of the AI stack change over time?
- Which technologies have strong package usage but comparatively different repository activity?
- What happens to a technology's ecosystem after major releases or periods of intense development?

The project is **not** intended to claim that the collected sources represent the entire AI industry. It analyzes the public ecosystems visible through the selected sources and explicitly records source limitations and coverage assumptions.

---

## 2. Project Vision

### 2.1 Vision

Create a reproducible data platform that turns large, heterogeneous, continuously changing public AI-ecosystem data into a historical analytical model.

The final system should demonstrate real data-engineering practices rather than simply downloading datasets and producing charts.

### 2.2 What makes this a data-engineering project?

The project intentionally includes:

1. Large-scale ingestion.
2. Multiple heterogeneous source schemas.
3. Full historical loading where available.
4. Incremental ingestion.
5. Different incremental strategies for different sources.
6. Raw immutable Bronze storage.
7. Data cleaning and normalization.
8. Cross-source identity resolution.
9. Slowly changing/historical dimensions where useful.
10. Fact and dimension modeling.
11. Partitioning and file-format decisions.
12. Data-quality checks.
13. Pipeline observability.
14. Reprocessing/idempotency.
15. Late-arriving data handling.
16. Analytical Gold datasets.
17. Reproducible orchestration.
18. Documentation and data lineage.

---

## 3. Project Scope

### 3.1 Core scope

The initial production scope consists of:

#### Source A — GitHub / GH Archive

Purpose:

- Historical public GitHub event activity.
- Repository activity.
- Contributor activity.
- Pull requests.
- Issues.
- Stars/watch activity where available in the captured event stream.
- Forks.
- Releases and other public event types.

Ingestion pattern:

- Historical batch/backfill.
- Hourly incremental ingestion.

Important source limitation:
GH Archive should be treated as the public event stream it captures, not as a guaranteed complete record of all GitHub activity. Coverage/completeness must be monitored and documented.

Rate Limits & Throttling:
While GH Archive serves static files without hard API limits, backfill scripts must enforce sequential downloads with polite delays (1-2s) to avoid HTTP timeouts or IP blocks from excessive concurrent connections.

#### Source B — Hugging Face Hub

Purpose:

- AI model ecosystem.
- Dataset ecosystem where included.
- Spaces where included.
- Repository metadata.
- Downloads/likes and other available metadata.
- Creation and modification timestamps.
- AI-library/task/tag metadata where available.

Ingestion pattern:

- Initial snapshot/full extraction of the defined universe.
- Periodic snapshot extraction.
- Snapshot-diff incremental processing.
- Optional future webhook/event-driven ingestion.

The Hugging Face Hub is Git-based and provides repository versioning, commits, diffs, branches, and API access. The Hub documentation currently reports more than 2M models, 1.5M datasets, and 1.5M Spaces, making it a major ecosystem source.

Rate Limits & Throttling:
The Hugging Face Hub API enforces limits over 5-minute rolling windows.
- The pipeline MUST authenticate via a secure token to maximize limit thresholds.
- All HTTP calls must implement automatic retry logic with exponential backoff to handle `429 Too Many Requests` responses cleanly without crashing the DAG.

### 3.2 Planned extension

#### Source C — PyPI

Purpose:

- Python package adoption.
- Download time series.
- Package-level usage trends.

Ingestion pattern:

- Historical load available through the chosen PyPI statistics interface.
- Daily incremental loads.

Constraint:
The selected PyPI Stats API currently exposes a limited historical window, so the system must not pretend it provides unlimited historical download data.

#### Source D — arXiv

Purpose:

- Research activity.
- AI/ML paper trends.
- Optional research → engineering transition analysis.

Ingestion pattern:

- Historical/bounded backfill.
- Incremental paper ingestion.

Status:

- Stretch goal.
- Must not block the GitHub + Hugging Face implementation.

---

## 4. Non-Goals

The following are explicitly outside the initial scope:

- Measuring all AI activity worldwide.
- Scraping private repositories.
- Collecting private Hugging Face repositories.
- Building an AI model ranking system.
- Building an investment recommendation system.
- Predicting which technology will dominate the industry.
- Claiming causal relationships from observational data.
- Building a generic GitHub analytics dashboard.
- Building a generic Hugging Face search interface.
- Downloading model weights or large model files unless explicitly justified.
- Storing sensitive user information.
- Treating stars/downloads as a universal definition of technology quality.

---

## 4a. Privacy & PII Compliance

This section documents the PII (Personally Identifiable Information) analysis for each data source and defines the official data-handling strategy across all Medallion layers.

### PII Field Inventory

#### GH Archive — Two Layers of PII

**Layer 1 — Event metadata (present in all event types):**

| Field | PII Classification |
|---|---|
| `actor.login` | ⚠️ Quasi-PII — public GitHub username, uniquely maps to a real person |
| `actor.display_login` | ⚠️ Quasi-PII — usually identical to `actor.login` |
| `actor.url` | ⚠️ Quasi-PII — API URL that resolves to a real user profile |
| `actor.avatar_url` | ⚠️ Quasi-PII — profile picture URL linked to a real identity |

**Layer 2 — `PushEvent` commit payload:**

| Field | PII Classification |
|---|---|
| `payload.commits[].author.email` | 🔴 **Hard PII** — real email address embedded in Git commit metadata (GDPR Article 4) |
| `payload.commits[].author.name` | 🔴 **Hard PII** — real person's name as configured in local Git client |

> By ingesting GH Archive data, the project becomes a **data processor** under GDPR for these fields. The fact that this data is publicly accessible on GitHub does not remove compliance obligations. The pipeline must apply data minimization (GDPR Article 5) — if a field is not required for analysis, it must not be propagated.

#### Hugging Face Hub API

| Field | PII Classification |
|---|---|
| `author` | ⚠️ Quasi-PII — public HF username/org namespace, may map to a real name |

The HF metadata API exposes no email addresses, IP addresses, or other hard PII.

#### PyPI Stats API

All fields are aggregate download statistics with no individual identifiers. Classified as **non-personal data**. No compliance action required.

---

### Three-Tier Privacy Handling Strategy

The project applies the following policy across Medallion layers:

#### Bronze — Raw, Access-Controlled
- All source data is stored as-is, including all PII fields.
- Bronze storage is **access-controlled** and treated as a restricted zone.
- It is never exposed as a direct query source or dashboard backend.
- Raw preservation is required for pipeline reprocessability and auditability.

#### Silver — Hashed / Dropped at the Boundary
The Bronze → Silver Spark transformation enforces the following field-level policy:

| Source Field | Silver Treatment |
|---|---|
| `actor.login` (GH) | SHA-256 hashed → `actor_id_hash`. Preserves the ability to count unique contributors. |
| `actor.display_login` (GH) | **Dropped** |
| `actor.url` (GH) | **Dropped** |
| `actor.avatar_url` (GH) | **Dropped** |
| `payload.commits[].author.email` (GH) | **Dropped immediately** — never propagated beyond Bronze |
| `payload.commits[].author.name` (GH) | **Dropped immediately** — never propagated beyond Bronze |
| `author` (HF) | Retained — public platform identifier used for model attribution |
| All PyPI fields | Retained — no PII present |

#### Gold — Aggregated Only
- No individual identifiers (raw or hashed) appear in any Gold fact or dimension table.
- Contributor metrics are expressed only as aggregate counts derived from hashed Silver IDs.
- Dashboard consumers have access only to technology-level and time-period-level aggregations.

### PII Summary

| Source | Hard PII? | Quasi-PII? | Strategy |
|---|---|---|---|
| GH Archive (event metadata) | No | Yes | Hash in Silver, aggregate only in Gold |
| GH Archive (PushEvent commits) | **Yes** | — | **Dropped at Bronze → Silver boundary** |
| Hugging Face Hub API | No | Yes | Retain in Silver, aggregate only in Gold |
| PyPI Stats API | **None** | None | No action required |

---

## 5. Core Research Questions

The project should maintain a living research-question backlog.

### RQ-01 — Engineering evolution

How does engineering activity around AI technologies change over time?

Potential measures:

- events/day
- commits/events where observable
- pull requests
- issues
- releases
- active contributors
- active repositories
- contribution concentration
- repository creation/archival activity

### RQ-02 — Ecosystem adoption

How does the AI model ecosystem around a technology change over time?

Potential measures:

- number of related models
- model creation rate
- model modification rate
- model downloads
- model likes
- organizations/authors
- task/category distribution

### RQ-03 — Engineering vs adoption

Do engineering activity and AI-model ecosystem activity move together or diverge?

The system should compare time series rather than declare one metric to be inherently superior.

### RQ-04 — Technology emergence

How can emerging technologies be detected from changes in activity across sources?

Possible signals:

- acceleration in GitHub activity
- new HF models
- increasing HF downloads
- package growth
- increasing contributor count

### RQ-05 — Technology lifecycle

Can technology lifecycle stages be described from historical data?

Possible descriptive stages:

```text
Emerging
   ↓
Growing
   ↓
Mature
   ↓
Stable / Declining
```

These are analytical classifications, not claims about technical quality.

### RQ-06 — Cross-source lag

How much time separates observable activity in different ecosystems?

Example:

```text
GitHub activity increase
        ↓
HF model growth
        ↓
PyPI usage growth
```

This should be analyzed as temporal association, not causal proof.

---

## 6. Data Source Documentation

### 6.1 GH Archive

Official documentation: https://www.gharchive.org/

Data pattern:

```text
https://data.gharchive.org/YYYY-MM-DD-H.json.gz
```

Conceptual example:

```text
2026-09-27-00.json.gz
2026-09-27-01.json.gz
2026-09-27-02.json.gz
```

Expected characteristics:

- JSON events.
- Hourly files.
- Large historical volume.
- Multiple event types.
- Public GitHub activity.
- Append-oriented ingestion.

Initial event types of interest:

The exact supported set must be discovered from the source rather than hardcoded permanently.

Likely useful categories include:

- PushEvent
- PullRequestEvent
- IssuesEvent
- IssueCommentEvent
- WatchEvent
- ForkEvent
- ReleaseEvent
- CreateEvent
- DeleteEvent
- MemberEvent
- PublicEvent

The Silver layer must preserve an `event_type` field so new event types can be added without redesigning Bronze.

---

## 7. Hugging Face Data Model

The Hub exposes APIs for models, datasets, Spaces, and related resources. It also provides a Python client and CLI.

Useful model-level fields may include:

- model_id
- author
- createdAt
- lastModified
- downloads
- downloadsAllTime
- likes
- library_name
- pipeline_tag
- tags
- baseModels
- datasets
- spaces
- transformersInfo
- private
- gated
- sha

The exact API response schema must be captured and versioned during implementation because source APIs evolve.

Recommended initial HF entities:

Start with:

- HF Model
- HF Organization/User
- HF Model metadata snapshot
- HF Model adoption metrics

Datasets and Spaces should be added only after the model pipeline is stable.

---

## 8. PyPI Data Model

Potential package-level fields:

- package_name
- date
- downloads
- period

Additional package metadata can be collected separately if needed.

The primary role of PyPI is not to create another huge raw source. Its role is to provide a different measurement of ecosystem adoption.

---

## 9. Technology Universe

The project must define what counts as an AI technology.

Do NOT ingest every GitHub repository and call the result the AI ecosystem.

Create a curated technology universe.

Example categories:

```text
AI Frameworks
    PyTorch
    TensorFlow
    JAX
    Keras

Model / LLM Tooling
    Transformers
    PEFT
    Accelerate
    TRL

Inference / Serving
    vLLM
    TGI
    llama.cpp
    Ollama

Agent / Application Frameworks
    LangChain
    LlamaIndex
    AutoGen

Distributed / ML Infrastructure
    Ray
    DeepSpeed
    MLflow

Computer Vision
    OpenMMLab ecosystem
    Ultralytics

Generative AI
    Diffusers
    ComfyUI
```

This list is only an initial example.

The final technology universe must be data-driven and documented.

---

## 10. Cross-Source Identity Resolution

This is a central part of the project.

There is no guaranteed universal identifier shared between GitHub, Hugging Face, and PyPI.

Therefore create a conformed mapping dimension.

### Technology Crosswalk

Example fields:

- technology_id
- technology_name
- category
- github_repo
- github_org
- hf_org
- hf_repo_patterns
- pypi_package
- arxiv_id
- mapping_method
- mapping_confidence
- valid_from
- valid_to
- is_current

Example:

```text
technology_id: TECH-0001
technology_name: PyTorch
github_repo: pytorch/pytorch
hf_org: pytorch
pypi_package: torch
category: framework
```

Mapping methods:

- MANUAL_VERIFIED
- OFFICIAL_METADATA
- README_REFERENCE
- PACKAGE_METADATA
- HEURISTIC
- AUTOMATIC_MATCH

Every mapping should record how it was established.

Do not hide uncertain mappings.

---

## 11. Medallion Architecture

The project uses:

```text
                 ┌──────────────┐
                 │   SOURCES    │
                 └──────┬───────┘
                        ↓
                 ┌──────────────┐
                 │   BRONZE     │
                 │ Raw immutable│
                 └──────┬───────┘
                        ↓
                 ┌──────────────┐
                 │   SILVER     │
                 │ Cleaned +    │
                 │ normalized   │
                 └──────┬───────┘
                        ↓
                 ┌──────────────┐
                 │    GOLD      │
                 │ Analytical   │
                 │ warehouse    │
                 └──────┬───────┘
                        ↓
                 ┌──────────────┐
                 │   BI / API   │
                 └──────────────┘
```

---

## 12. Bronze Layer

### Purpose

Preserve source data as close to the original representation as practical.

Properties:

- Immutable.
- Replayable.
- Partitioned.
- Source-specific.
- No destructive transformations.
- Metadata about ingestion must be stored.

### Bronze metadata

Every ingestion unit should have:

- source
- ingestion_run_id
- ingested_at
- source_timestamp
- source_uri
- source_partition
- schema_version
- raw_record_hash
- pipeline_version

### Example Bronze layout

```text
data/
  bronze/
    github/
      events/
        year=2026/
          month=09/
            day=27/
              hour=00/
                part-*.json.gz

    huggingface/
      models/
        snapshot_date=2026-09-27/
          part-*.json

    pypi/
      downloads/
        date=2026-09-27/
          part-*.json
```

The exact storage layout can change after benchmarking.

---

## 13. Silver Layer

The Silver layer produces trustworthy, normalized entities.

### GitHub Silver

Potential table: `silver_github_events`

Fields:

- event_id
- event_type
- event_time
- repository_id
- repository_name
- repository_owner
- actor_id
- actor_login
- payload
- source_run_id
- ingested_at

Nested event payload should be normalized into specialized tables when justified.

Potential specialized tables:

- silver_github_push
- silver_github_pull_requests
- silver_github_issues
- silver_github_releases
- silver_github_stars
- silver_github_forks

### HF Silver

Potential tables:

- silver_hf_models
- silver_hf_model_snapshots
- silver_hf_model_tags
- silver_hf_model_datasets
- silver_hf_model_metrics

### PyPI Silver

- silver_pypi_downloads
- silver_pypi_packages

---

## 14. Gold Layer

The Gold layer is optimized for analytical questions.

Recommended dimensions:

- dim_date
- dim_technology
- dim_repository
- dim_hf_model
- dim_package
- dim_organization
- dim_category

Recommended facts:

- fact_github_activity
- fact_hf_model_activity
- fact_hf_adoption
- fact_pypi_downloads
- fact_technology_daily_snapshot

---

## 15. Recommended Star Schema

### dim_date

- date_key
- date
- day
- week
- month
- quarter
- year
- day_of_week
- is_weekend

### dim_technology

- technology_key
- technology_id
- technology_name
- category
- subcategory
- description
- github_repo
- hf_namespace
- pypi_package
- mapping_method
- mapping_confidence
- valid_from
- valid_to
- is_current

### fact_github_activity

Grain: One technology × one time interval × one activity measurement.

Example:

- date_key
- technology_key
- event_type
- event_count
- unique_contributors
- unique_repositories

The exact grain must be finalized before implementation.

### fact_hf_adoption

Possible grain: One HF model × one snapshot date.

Fields:

- date_key
- technology_key
- hf_model_key
- downloads
- downloads_all_time
- likes
- last_modified

### fact_pypi_downloads

Possible grain: One package × one day.

Fields:

- date_key
- technology_key
- package_key
- downloads

---

## 16. Time and Historical Modeling

Historical data is a core feature.

The system should distinguish:

**Event time** — When something happened at the source. `event_time`

**Ingestion time** — When our pipeline received it. `ingested_at`

**Effective time** — When a dimension attribute became valid. `valid_from`, `valid_to`

These timestamps must not be confused.

---

## 17. Incremental Architecture

The project intentionally uses different incremental strategies.

### 17.1 GitHub incremental

Pattern:

```text
last_successful_hour
        ↓
download next hourly archive
        ↓
validate
        ↓
write Bronze
        ↓
update checkpoint
```

Checkpoint example:

```text
github_last_processed:
2026-09-27T11:00:00
```

If the job fails:

```text
download
   ↓
write
   ↓
validation
   ↓
checkpoint
```

The checkpoint must only advance after successful persistence and validation.

### 17.2 Hugging Face incremental

Pattern:

```text
Snapshot N
    ↓
Snapshot N+1
    ↓
normalize IDs
    ↓
hash relevant fields
    ↓
compare
    ↓
NEW
UPDATED
UNCHANGED
MISSING
```

Do not physically delete historical records when a model disappears from the current snapshot.

Instead:

```text
status = DELETED_OR_MISSING
```

with appropriate uncertainty wording.

A missing record may represent deletion, privacy change, API filtering, or extraction failure.

### 17.3 PyPI incremental

Pattern:

```text
last successful date
        ↓
request download statistics
        ↓
validate
        ↓
append daily fact
        ↓
checkpoint
```

---

## 18. Idempotency

Every ingestion operation must be safe to run twice.

Example:

```text
Run 1
GH archive 2026-09-27-10
        ↓
stored

Run 2
GH archive 2026-09-27-10
        ↓
recognized as already processed
        ↓
no duplicate
```

Possible mechanisms:

- source URI + source partition key
- event ID
- record hash
- unique constraints
- merge/upsert
- ingestion manifest

---

## 19. Ingestion Manifest

Create a metadata table: `ingestion_manifest`

Suggested fields:

- run_id
- source
- dataset
- partition
- source_uri
- started_at
- completed_at
- status
- records_read
- records_written
- records_rejected
- checksum
- schema_version
- pipeline_version
- error_message

Example:

```text
run_id: RUN-20260927-0012
source: github
partition: 2026-09-27-11
status: SUCCESS
records_read: 482391
records_written: 481992
records_rejected: 399
```

This becomes the basis for observability.

---

## 20. Data Quality

Every source must have validation rules.

**Schema checks**

- Required fields exist.
- Data types are valid.
- Unknown schema changes are detected.

**Completeness**

Examples:

- `event_time IS NOT NULL`
- `repository_id IS NOT NULL`

**Uniqueness**

Examples:

- `event_id` unique
- `snapshot_id` unique

**Referential integrity**

Examples:

- `technology_key` exists
- `date_key` exists

**Range validation**

Examples:

- `downloads >= 0`
- `likes >= 0`

**Freshness**

Examples:

- `latest_github_partition` within expected interval
- `latest_hf_snapshot` within expected schedule
- `latest_pypi_date` within expected schedule

**Volume anomaly detection**

If yesterday:

```text
10,000,000 records
```

and today:

```text
30,000 records
```

the pipeline should flag the anomaly instead of silently treating it as normal.

---

## 21. Source Completeness Monitoring

Because source APIs/archives can change, maintain: `source_quality_metrics`

Possible fields:

- source
- partition
- expected_records
- actual_records
- coverage_status
- missing_partitions
- schema_change_detected
- freshness_minutes

For GH Archive specifically, document that archive coverage can differ from GitHub's own complete internal activity stream.

---

## 21a. Data Volume Analysis & Capacity Planning

This section documents the measured and estimated data volumes for each source, the impact of the technology universe filter on actual processed volumes, and the infrastructure capacity constraints that govern pipeline design decisions.

All figures are derived from official source documentation and published community analyses. Sources are cited inline.

---

### GH Archive — Volume Analysis

**Official source:** https://www.gharchive.org/  
**Community sizing:** https://clickhouse.com/docs/getting-started/example-datasets/github  
**Hourly file sizing:** https://aravpanwar.com (community analysis)

#### Raw Source (Unfiltered — All of GitHub)

| Metric | Value | Source |
|---|---|---|
| Archive coverage start | 2011 | gharchive.org |
| Total events (all years) | **11+ billion** | ClickHouse analysis |
| Total compressed size (all years) | **~17+ TB** | Community analysis |
| Estimated annual size (recent years) | **~7 TB compressed** | Community estimate |
| Hourly file size (2024–2026) | **100 MB – 500 MB** per `.json.gz` | aravpanwar.com |
| Files per day | **24** (one per hour) | gharchive.org |
| 1 day of raw data | **2.4 GB – 12 GB** compressed | Calculated |
| 1 month of raw data | **72 GB – 360 GB** compressed | Calculated |
| 1 year of raw data | **~7 TB** compressed | Community estimate |

> ⚠️ **Source Quality Note (2025+):** Since mid-2025, GH Archive has been dominated by `PushEvent` records. Stars, forks, issues, and pull requests are significantly undercounted or absent in recent hourly archives. This is an active limitation that must be documented in source quality metrics and flagged by the completeness monitoring system. Reference: ClickHouse GitHub analysis.

#### Filtered Volume (Our AI Technology Universe — ~50–100 Repositories)

The pipeline applies the technology universe filter at the Bronze ingestion step. Only events from our curated AI repositories are retained.

Estimated proportion of our repositories vs. all GitHub: **~0.001% – 0.01%** of total events.

| Metric | Raw (All GitHub) | Filtered (Our Universe) |
|---|---|---|
| Hourly Bronze file | 100–500 MB | **~100 KB – 5 MB** |
| Daily Bronze ingest | 2.4–12 GB | **~2.4 MB – 120 MB** |
| Monthly Bronze total | 72–360 GB | **~72 MB – 3.6 GB** |
| Daily Silver (Parquet) | — | **~1 MB – 50 MB** |
| Monthly Silver total | — | **~30 MB – 1.5 GB** |
| Gold daily fact (aggregated) | — | **< 10 MB/day** |

**Project full load scope:** 3 months historical (e.g., January–March 2026)  
**Estimated full load Bronze (filtered):** ~216 MB – 10.8 GB  
**Estimated full load Silver (filtered, Parquet):** ~90 MB – 4.5 GB

#### GH Archive Incremental Pattern

| Property | Value |
|---|---|
| Frequency | Hourly |
| Mechanism | Checkpoint: `last_processed_hour` → next available hour |
| Incremental Bronze per run | ~100 KB – 5 MB (filtered) |
| Incremental Silver delta per day | ~1 MB – 50 MB (filtered) |
| Idempotency mechanism | Partition key + ingestion manifest |

---

### Hugging Face Hub — Volume Analysis

**Official API:** https://huggingface.co/api/models  
**Python client:** https://huggingface.co/docs/huggingface_hub/  
**Growth data:** https://aiworld.eu (milestone analysis)  
**Live Hub stats:** https://huggingface.co/spaces/cfahlgren1/hub-stats

#### Raw Source (Unfiltered — All HF Models)

| Metric | Value | Source |
|---|---|---|
| Total public models (mid-2026) | **~3 million** | huggingface.co |
| Time to reach first million models | > 1,000 days | aiworld.eu |
| Time to reach second million | 335 days (65% faster) | aiworld.eu |
| Estimated new models globally per day | **~2,000 – 5,000** | Calculated from growth rate |
| Metadata per model (JSON, standard fields) | **~1 KB – 10 KB** | HF API documentation |
| Full snapshot of all models | **~3 GB – 30 GB** | Calculated |
| Daily global incremental diff | **~2 MB – 50 MB** | Calculated |

#### Filtered Volume (Our AI Technology Universe)

The pipeline extracts metadata only for models matching our curated technology tags (e.g., `pytorch`, `transformers`, `diffusers`, `langchain`, `vllm`).

| Metric | Raw (All HF Models) | Filtered (Our Universe) |
|---|---|---|
| Full snapshot | 3–30 GB | **~5 MB – 50 MB** |
| Daily incremental diff | 2–50 MB | **< 1 MB** |
| New relevant models per day | 2,000–5,000 globally | **~10–100/day** |
| Silver per snapshot (Parquet) | — | **< 20 MB** |
| Gold daily fact | — | **< 5 MB/day** |

#### Hugging Face Incremental Pattern

| Property | Value |
|---|---|
| Frequency | Daily snapshot |
| Mechanism | Snapshot-diff: compare `lastModified` + field hash (Snapshot N vs N+1) |
| Statuses produced | `NEW`, `UPDATED`, `UNCHANGED`, `MISSING` |
| Bronze per run (filtered) | < 1 MB |
| MISSING record policy | Preserved in Bronze with status flag; may indicate deletion, privacy change, or API filtering — not assumed to be confirmed deletion |

---

### PyPI Stats — Volume Analysis

**Official API:** https://pypistats.org/api  
**BigQuery dataset:** `bigquery-public-data.pypi.file_downloads`  
**Usage policy:** https://pypistats.org/about (bulk extraction guidance)

#### Raw Source

| Metric | Value | Source |
|---|---|---|
| Full PyPI download history (BigQuery) | **~440 TB** | BigQuery public dataset |
| API retention window | **180 days only** | pypistats.org |
| Update frequency | Once daily | pypistats.org |
| Per-package, per-day response | **~1–2 KB** JSON | pypistats.org |
| BigQuery free query tier | **1 TB/month** | Google Cloud free tier |

> ⚠️ **Hard Constraint:** The pypistats.org API provides only 180 days of time-series history. The project will not claim unlimited PyPI history. The ingestion manifest must record the earliest date actually available. For deeper historical analysis, BigQuery is the correct interface.

> ⚠️ **Usage Policy:** pypistats.org explicitly discourages bulk extraction of all packages via the API. The project will only query our curated list of ~50–100 packages, which is entirely within acceptable API usage.

#### Filtered Volume (Our Technology Universe — ~50–100 Packages)

| Metric | Raw (All PyPI) | Filtered (Our Packages) |
|---|---|---|
| Full load (180-day baseline) | 440 TB (BigQuery) | **< 10 MB** |
| Daily incremental per run | N/A | **< 1 MB** |
| Bronze (180-day baseline) | — | **< 10 MB** |
| Silver Parquet (180-day baseline) | — | **< 5 MB** |
| Gold daily fact | — | **< 1 MB/day** |

---

### Infrastructure Capacity & FinOps

This project is designed to run entirely within free-tier and student program limits. All platforms below have been researched and evaluated. Sources are cited inline.

---

#### Platform 1 — Databricks Free Edition *(Primary)*

**Reference:** https://www.databricks.com/learn/free-edition  
**Academic program:** https://www.databricks.com/university

| Constraint | Free Edition Limit | Impact on This Project |
|---|---|---|
| Cost | **Free** — no credit card required | ✅ Zero billing risk |
| Compute | Serverless only, fair usage quota (daily reset) | ✅ Sufficient for filtered volumes |
| SQL Warehouse | 1 × 2X-Small | ✅ Adequate for Gold queries |
| Concurrent job tasks | 5 max | ✅ Daily pipeline fits |
| GPU compute | ❌ Not available | ✅ Not required — metadata only |
| Custom external storage | ❌ Not available | ⚠️ Use Databricks managed storage |
| R / Scala | ❌ Not supported | ✅ Stack is Python + PySpark |
| University Alliance | Classroom support, cert exam discounts | ✅ Apply via institution |
| Student Fellows program | Leadership + networking | Optional |

> **Why primary:** No billing surprises. Compute quota resets daily. Data is never deleted when quota runs out. Best choice for a semester-long academic project.

---

#### Platform 2 — Microsoft Azure for Students

**Reference:** https://azure.microsoft.com/en-us/free/students/

| Property | Details |
|---|---|
| Credit | **$100 USD** free — valid 12 months, renewable annually |
| Credit card required | ❌ No |
| Blob Storage (free) | 5 GB Locally Redundant Storage |
| Azure Databricks | Deployable with student credits |
| Azure Data Factory | Available for orchestration within credit budget |
| Also available via | GitHub Student Developer Pack |

> **FinOps risk:** Set a $10 billing alert (early warning) and $80 hard-stop alert immediately on signup. VM instances and Azure Databricks clusters consume credits quickly if left running.

---

#### Platform 3 — Google Cloud (BigQuery Free Tier + Education Credits)

**Reference:** https://cloud.google.com/edu/students  
**BigQuery free tier:** https://cloud.google.com/bigquery/pricing

| Property | Details |
|---|---|
| BigQuery free tier | **1 TB of query data/month** — permanent, no expiry |
| Education credits | Additional billing credits via faculty application |
| New account free trial | $300 credit for 90 days |
| Skills Boost credits | 200 free lab credits for hands-on training |
| PyPI BigQuery dataset | `bigquery-public-data.pypi.file_downloads` — free to query |

> **Best use in this project:** PyPI download statistics already live in BigQuery. Our filtered queries use < 1 GB/month — well within the 1 TB free tier. GCP eliminates the need to store the 440 TB raw dataset locally.

---

#### Platform 4 — AWS Educate + AWS Student Rewards

**Reference:** https://www.awseducate.com / https://builder.aws.com

| Property | Details |
|---|---|
| AWS Educate | Free learning platform, hands-on EC2 and S3 labs |
| Student Rewards | Credits ($10–$100) earned by completing learning badges |
| AWS Free Tier | 12 months of free S3, EC2, RDS within usage limits |
| Fit for this project | ⚠️ Indirect — Spark on EMR is expensive even at small scale |

> **Note:** AWS is viable for S3 raw data storage but not recommended as the primary Spark processing platform. EMR (managed Spark on AWS) consumes credits significantly faster than Databricks Free Edition.

---

#### Platform 5 — GitHub Student Developer Pack *(Apply Immediately)*

**Reference:** https://education.github.com/pack

| Property | Details |
|---|---|
| Cost | Free — verify with .edu email or student ID |
| Total offers | 80+ partner tools and credits |
| GitHub Copilot | ✅ Free for verified students |
| JetBrains IDEs | ✅ PyCharm, IntelliJ, etc. — full suite free |
| Azure credits | ✅ Included via Microsoft partnership |
| DataCamp | ✅ Free premium access |
| GitHub Actions | ✅ Free CI/CD minutes for private repos |
| Domain name | ✅ Free .me or .tech domain via Namecheap/Name.com |

> **Recommended first step:** Apply for the GitHub Student Developer Pack before any other platform. It unlocks Azure credits, Copilot, and JetBrains at no cost — all useful for this project.

---

#### Recommended Platform Stack

| Layer | Primary Platform | Alternative | Student Offer |
|---|---|---|---|
| Spark Processing | **Databricks Free Edition** | Azure Databricks | Free Edition / $100 Azure credit |
| Bronze Storage | Databricks managed storage | Azure Blob (5 GB free) | Free tier |
| Silver / Gold (Parquet) | Databricks managed storage | Azure Data Lake Storage Gen2 | $100 Azure credit |
| PyPI Queries | **Google BigQuery** | — | 1 TB/month free tier |
| Version Control | **GitHub** | — | GitHub Student Pack |
| CI/CD | GitHub Actions | — | Student Pack (free minutes) |
| Dashboard | Power BI Desktop (free) | Tableau Public (free) | Free desktop tools |

---

#### Cost Guardrails (Enforced Across All Platforms)

1. **Early universe filter** — applied at Bronze ingest before data is stored. Single biggest cost lever: reduces processed data by ~99.99%.
2. **Date-bounded development** — all development uses 7 days of data max. Full backfill runs once, after validation.
3. **Parquet + Snappy compression** — reduces Silver/Gold footprint 60–80% versus raw JSON.
4. **Partition pruning** — Gold queries always include `year/month` filters. No full-table scans in production.
5. **BigQuery filtered queries** — always include `WHERE project IN (...)` and `WHERE DATE BETWEEN ...` to stay within 1 TB/month free tier.
6. **Billing alerts** — if using Azure or GCP: $10 early warning, $80 hard-stop.
7. **No always-on clusters** — all clusters are job-scoped with auto-termination. Zero idle compute cost.
8. **Monthly FinOps review** — review actual vs estimated consumption at start of each month.

---

#### Estimated Monthly Cost at Full Operational Scale

| Resource | Platform | Monthly Cost |
|---|---|---|
| Spark — daily GH Archive ingest | Databricks Free Edition | **$0** |
| Spark — daily HF snapshot | Databricks Free Edition | **$0** |
| Storage — Bronze + Silver + Gold | Databricks managed / Azure Blob | **< $1** |
| PyPI queries — BigQuery (< 1 GB/month) | GCP free tier | **$0** |
| Version control + CI/CD | GitHub Student Pack | **$0** |
| **Total** | | **$0 – $1/month** |

> This project is **designed to cost nothing** for a full semester when run within Databricks Free Edition + GCP BigQuery free tier + GitHub Student Pack. No cloud billing is required during development or normal operation.

---



### Volume Summary & Viability

| Source | Raw Volume (Unfiltered) | Filtered Project Volume | Databricks Fit |
|---|---|---|---|
| GH Archive — Full Load (3 months) | 216 GB – 1 TB | **~216 MB – 10.8 GB** | ✅ Manageable |
| GH Archive — Daily Incremental | 2.4–12 GB/day | **~2.4–120 MB/day** | ✅ Manageable |
| HF Hub — Full Snapshot | 3–30 GB | **~5–50 MB** | ✅ Trivially small |
| HF Hub — Daily Incremental | 2–50 MB/day | **< 1 MB/day** | ✅ Trivially small |
| PyPI — Full Load (180d) | 440 TB (BigQuery) | **< 10 MB** | ✅ Trivially small |
| PyPI — Daily Incremental | N/A | **< 1 MB/day** | ✅ Trivially small |

**Why this remains a genuine Spark / distributed processing project despite the filtered volumes:**  
The GH Archive backfill produces up to 10.8 GB of Bronze data. Parsing compressed hourly JSON, applying schema enforcement, flattening nested structures, deduplicating events, computing aggregations, and writing partitioned Parquet across this volume is a legitimate distributed Spark workload. The technology universe filter makes the project feasible on free-tier infrastructure without compromising the data engineering depth.

**Volume mitigation strategy (applied in this order):**

1. **Early filter at Bronze ingest** — apply the technology universe filter before writing to Bronze, not after
2. **Date-bounded backfill** — initial full load covers 3 months, not 12; incremental extends coverage forward
3. **Parquet + Snappy compression** — reduces Silver/Gold storage 60–80% versus raw JSON
4. **Partition pruning** — Gold queries use `year/month` partitions to avoid full-table scans
5. **BigQuery for PyPI** — use BigQuery's 1 TB/month free tier instead of local 440 TB storage

---

## 22. Storage Format

Preferred analytical storage: **Parquet**

Reasons:

- columnar
- compressed
- efficient analytical scans
- partitionable
- interoperable with Spark
- suitable for large datasets

Raw source files may remain in their original format when useful.

Example:

- Bronze: JSON / JSON.GZ
- Silver: Parquet
- Gold: Parquet + SQL tables

---

## 23. Processing Engine

The project should use Apache Spark / PySpark for the core distributed transformation workload.

Why:

- large event volumes
- distributed processing
- DataFrame transformations
- partitioning
- joins
- aggregations
- scalable Parquet processing
- demonstrates course-relevant distributed data engineering

Spark should not be used merely because it is available. The pipeline should contain workloads where distributed processing is meaningful.

---

## 24. Recommended Technology Stack

Initial recommendation:

- **Language:** Python
- **Distributed processing:** Apache Spark / PySpark
- **Storage:** Parquet
- **Architecture:** Medallion
- **Warehouse/query layer:** SQL-compatible analytical engine
- **Orchestration:** Apache Airflow / equivalent
- **Containerization:** Docker
- **Version control:** Git
- **Data quality:** Great Expectations / custom validation / equivalent
- **Dashboard:** Power BI / Apache Superset / Streamlit / equivalent

The exact tools are implementation decisions and may change during Phase 1.

Do not introduce tools solely to increase the technology list.

---

## 25. Repository Structure

Recommended starting structure:

```text
ai-stack-evolution/
│
├── README.md
├── PROJECT.md
├── CHANGELOG.md
├── ROADMAP.md
├── CONTRIBUTING.md
├── .env.example
├── .gitignore
├── docker-compose.yml
├── Makefile
│
├── docs/
│   ├── architecture/
│   │   ├── overview.md
│   │   ├── bronze.md
│   │   ├── silver.md
│   │   ├── gold.md
│   │   └── incremental.md
│   │
│   ├── sources/
│   │   ├── github.md
│   │   ├── huggingface.md
│   │   ├── pypi.md
│   │   └── arxiv.md
│   │
│   ├── data-model/
│   │   ├── dimensions.md
│   │   ├── facts.md
│   │   └── crosswalk.md
│   │
│   └── decisions/
│       ├── ADR-001-storage.md
│       ├── ADR-002-incremental-strategy.md
│       └── ...
│
├── config/
│   ├── sources.yaml
│   ├── technologies.yaml
│   └── pipeline.yaml
│
├── src/
│   ├── ingestion/
│   │   ├── github/
│   │   ├── huggingface/
│   │   ├── pypi/
│   │   └── arxiv/
│   │
│   ├── bronze/
│   ├── silver/
│   ├── gold/
│   ├── quality/
│   ├── common/
│   └── cli/
│
├── tests/
│   ├── unit/
│   ├── integration/
│   └── data_quality/
│
├── data/
│   ├── bronze/
│   ├── silver/
│   └── gold/
│
├── notebooks/
│   └── exploration/
│
└── dashboard/
```

---

## 26. Configuration-Driven Design

Do not hardcode the technology universe into Python.

Use configuration:

```yaml
technologies:
  - id: TECH-0001
    name: PyTorch
    category: framework
    github:
      repository: pytorch/pytorch
    huggingface:
      namespace: pytorch
    pypi:
      package: torch

  - id: TECH-0002
    name: Transformers
    category: model-tooling
    github:
      repository: huggingface/transformers
    huggingface:
      namespace: huggingface
    pypi:
      package: transformers
```

This allows the project to grow without rewriting ingestion code.

---

## 27. CLI Design

The project should eventually expose a CLI.

Example:

- `ase init` — Initialize configuration and local structure.
- `ase source list` — List configured sources.
- `ase source test github` — Validate connectivity.
- `ase ingest github --date 2026-09-27 --hour 10` — Download one GH Archive partition.
- `ase ingest github --since 2026-09-01` — Run historical/backfill ingestion.
- `ase ingest huggingface --snapshot` — Create an HF snapshot.
- `ase diff huggingface` — Compare current and previous snapshot.
- `ase ingest pypi --date 2026-09-27` — Load PyPI statistics.
- `ase transform silver` — Build Silver.
- `ase transform gold` — Build Gold.
- `ase quality run` — Run all quality checks.
- `ase pipeline daily` — Run the daily pipeline.
- `ase pipeline backfill --source github --start 2026-01-01 --end 2026-09-01` — Backfill historical data.
- `ase status` — Show pipeline state.
- `ase lineage technology TECH-0001` — Show where a technology's data came from.

---

## 28. Pipeline Orchestration

Conceptual DAG:

```text
                   DAILY PIPELINE
                        │
        ┌───────────────┼────────────────┐
        │               │                │
        ▼               ▼                ▼
     GitHub            HF              PyPI
      ingest          snapshot         ingest
        │               │                │
        ▼               ▼                ▼
     Bronze           Bronze           Bronze
        │               │                │
        └───────────────┼────────────────┘
                        ▼
                  Silver transforms
                        │
                        ▼
                Identity resolution
                        │
                        ▼
                  Gold transforms
                        │
                        ▼
                 Data quality tests
                        │
                        ▼
                 Metrics / lineage
                        │
                        ▼
                    Dashboard
```

---

## 29. Failure Handling

Pipelines must fail loudly.

If ingestion fails: `FAILED`

Do not advance the checkpoint.

If transformation fails:

- Bronze remains available
- Silver/Gold are not partially promoted

Recommended lifecycle:

```text
STARTED
  ↓
EXTRACTED
  ↓
VALIDATED
  ↓
PERSISTED
  ↓
TRANSFORMED
  ↓
QUALITY_PASSED
  ↓
COMPLETED
```

Failure can occur at any stage and must be recorded.

---

## 30. Backfill Strategy

Historical ingestion must be separate from daily incremental ingestion.

Example:

```text
ase ingest github --start 2026-01-01 --end 2026-09-27
```

The backfill process should:

1. Generate source partitions.
2. Check manifest.
3. Skip completed partitions.
4. Download missing partitions.
5. Validate.
6. Persist Bronze.
7. Record metadata.
8. Process Silver in batches.
9. Build Gold incrementally.

Do not attempt to load an enormous historical source entirely into memory.

---

## 31. Partitioning Strategy

Potential partitions:

- **GitHub:** year/month/day/hour
- **Hugging Face:** snapshot_date
- **PyPI:** date
- **Gold:** usually year/month, or another partition chosen after query benchmarking.

Avoid excessive partition counts.

Partition strategy must be based on actual query and file-size behavior.

---

## 32. Performance Requirements

The system should be designed to:

- process data in batches;
- avoid collecting large datasets to the driver;
- use distributed Spark transformations where appropriate;
- use predicate pushdown;
- use Parquet column pruning;
- avoid unnecessary shuffles;
- broadcast only genuinely small dimensions;
- partition data intentionally;
- cache only when justified;
- monitor execution plans for expensive joins.

The project should document major optimization decisions.

---

## 33. Data Lineage

Every Gold metric should be traceable.

Example:

```text
Gold:
github_activity_fact
       ↓
Silver:
silver_github_events
       ↓
Bronze:
github/events/year=2026/month=09/day=27/hour=10
       ↓
Source:
GH Archive hourly archive
```

For cross-source metrics:

```text
Technology
   ↓
Crosswalk
   ↓
GitHub repository
   +
HF namespace
   +
PyPI package
```

---

## 34. Analytics Layer & Dashboard Specification

The dashboard is the final consumer of the Gold layer. Every visual must read exclusively from Gold fact and dimension tables. No dashboard visual should read directly from Bronze or Silver.

The dashboard is organized into **5 pages**, each answering a distinct analytical question.

---

### Dashboard Architecture Rule

```text
Bronze  →  never queried by dashboard
Silver  →  never queried by dashboard
Gold    →  only source for all dashboard visuals
ingestion_manifest  →  source for pipeline health page only
```

---

### Page 1 — Executive Overview

Purpose: High-level AI ecosystem status at a glance, no filters required.

#### Chart 1 — AI Stack Activity Heatmap
- **Type:** Calendar Heatmap (colour intensity = daily event volume)
- **Gold table:** `fact_github_activity`
- **Fields:** `date_key`, `SUM(event_count)` across all technologies
- **Question:** On which days was the AI ecosystem most and least active?
- **Insight:** Identifies weekends, holidays, and release-driven spikes across all monitored technologies simultaneously.

#### Chart 2 — Technology Category Breakdown
- **Type:** Treemap
- **Gold tables:** `fact_github_activity` JOIN `dim_technology` JOIN `dim_category`
- **Fields:** `category`, `subcategory`, `event_count`
- **Question:** Which AI technology categories dominate current engineering activity?
- **Insight:** Shows whether the AI ecosystem is currently dominated by Frameworks, LLM Tooling, Inference/Serving, or Agent frameworks.

#### Chart 3 — Top 10 Technologies by Combined Ecosystem Score
- **Type:** Horizontal Bar Chart (sorted descending)
- **Gold table:** `fact_technology_daily_snapshot`
- **Fields:** `technology_name`, normalized composite score from `github_events + hf_model_growth + pypi_downloads`
- **Question:** Which technologies lead across all measured ecosystem layers simultaneously?
- **Insight:** A normalized multi-source ranking that requires activity across GitHub, HF, and PyPI — not just a single dimension.

---

### Page 2 — Technology Deep Dive

Purpose: Full historical profile of one technology across all data sources.

Filters: `dim_technology.technology_name` (single-select) + date range

#### Chart 4 — Dual-Axis Engineering vs Adoption Timeline
- **Type:** Dual-Axis Line Chart
- **Gold tables:** `fact_github_activity` + `fact_hf_adoption` JOIN `dim_date` JOIN `dim_technology`
- **Fields:** `date`, `event_count` (left Y), `downloads` (right Y), monthly grain
- **Question:** Do engineering activity and model ecosystem adoption for this technology move together or diverge?
- **Insight:** The primary cross-source view. PyTorch may show sustained engineering activity while HF downloads spike months later as derivative fine-tuned models proliferate.

#### Chart 5 — Engineering Activity Breakdown
- **Type:** Stacked Area Chart
- **Gold table:** `fact_github_activity`
- **Fields:** `date_key`, `event_type`, `event_count`
- **Question:** What types of engineering actions are contributors performing on this technology over time?
- **Insight:** Shows whether the community is in active development (push/PR), maintenance (issues), or outreach (forks/stars). A shift from push-heavy to issue-heavy indicates maturity.

#### Chart 6 — HF Model Growth Waterfall
- **Type:** Waterfall / Cumulative Line Chart
- **Gold table:** `fact_hf_model_activity`
- **Fields:** `date_key`, `new_models`, `modified_models`, `missing_models`
- **Question:** How is the HF model ecosystem for this technology actually growing — net vs gross?
- **Insight:** Distinguishes real ecosystem growth (net new models) from churn (many updates to existing models without net creation).

---

### Page 3 — Ecosystem Comparison

Purpose: Side-by-side comparison of up to 6 technologies.

Filters: `dim_technology.technology_name` (multi-select, max 6) + `dim_category.category`

#### Chart 7 — Technology Lifecycle Quadrant
- **Type:** Bubble Scatter Plot
- **Gold table:** `fact_technology_daily_snapshot`
- **Fields:** `growth_rate_90d` (X-axis), `activity_30d` (Y-axis), bubble size = `hf_model_count`
- **Quadrants:**
  - Top-right → **Rising** (high activity, high growth)
  - Top-left → **Mature** (high activity, slowing growth)
  - Bottom-right → **Emerging** (low activity, accelerating growth)
  - Bottom-left → **Declining** (low activity, low growth)
- **Question:** At what lifecycle stage is each AI technology positioned today?
- **Insight:** The most powerful single analytical view. A time-lapse animation shows full lifecycle arcs.

#### Chart 8 — Activity Race Chart
- **Type:** Animated Bar Race Chart (or grouped bar for static output)
- **Gold table:** `fact_github_activity` JOIN `dim_technology`
- **Fields:** `date_key` (monthly), `technology_name`, `event_count`
- **Question:** How has the relative engineering activity ranking of AI technologies changed month by month?
- **Insight:** Answers "which technologies overtook others" — e.g., did `vllm` surpass `TGI` in 2025?

#### Chart 9 — Ecosystem Balance Radar
- **Type:** Radar / Spider Chart (one axis per ecosystem dimension)
- **Gold tables:** `fact_github_activity` + `fact_hf_adoption` + `fact_pypi_downloads` JOIN `dim_technology`
- **Axes:** GitHub Events, Unique Contributors, HF Model Count, HF Downloads, PyPI Downloads
- **Question:** How balanced is a technology's ecosystem across all five measured dimensions?
- **Insight:** A technology dominant only in downloads but weak in contributors and HF model count may have fragile adoption. A balanced polygon indicates a multi-dimensional, healthy ecosystem.

---

### Page 4 — Cross-Source Lag Analysis

Purpose: Test the core research hypothesis that engineering activity is a leading indicator of ecosystem adoption.

#### Chart 10 — Lead-Lag Correlation Timeline
- **Type:** Dual-Line Chart with highlighted correlation band
- **Gold tables:** `fact_github_activity` + `fact_hf_adoption` (weekly grain)
- **Fields:** `week_start_date`, `github_events_normalized`, `hf_downloads_normalized`
- **Question:** By how many weeks does a rise in GitHub engineering activity precede a rise in HF model downloads?
- **Insight:** If GitHub activity consistently peaks 4–8 weeks before HF downloads, this validates the research hypothesis. This is a novel finding unique to this multi-source architecture.

#### Chart 11 — Release-Driven Spike Analysis
- **Type:** Annotated Line Chart
- **Gold tables:** `fact_github_activity` + `fact_hf_model_activity`
- **Fields:** `date`, `event_count`, `new_models`, with vertical annotations at `ReleaseEvent` dates
- **Question:** Do major software releases trigger measurable spikes in HF model creation?
- **Insight:** Makes the release → ecosystem expansion pattern visually explicit. Annotations mark release dates on the timeline without requiring statistical inference from the viewer.

---

### Page 5 — Pipeline Health Monitor

Purpose: Operational visibility into data pipeline reliability and freshness.

#### Chart 12 — Pipeline Status & Data Freshness Table
- **Type:** Status Table with conditional formatting (🟢 green / 🟡 amber / 🔴 red)
- **Source table:** `ingestion_manifest` (pipeline metadata, not analytical Gold)
- **Fields:** `source`, `last_successful_partition`, `records_read`, `records_rejected`, `status`, `completed_at`
- **Question:** Is the pipeline running? Is the data fresh? Are there quality failures?
- **Insight:** Demonstrates that the project monitors its own data reliability — a key data engineering maturity signal. Rejected record counts surface data quality issues before they corrupt analytical results.

---

### Dashboard Data Flow

```text
Gold Layer
│
├── fact_github_activity            → Charts 1, 2, 3, 4, 5, 8, 9, 10, 11
├── fact_hf_adoption                → Charts 3, 4, 9, 10
├── fact_hf_model_activity          → Charts 6, 11
├── fact_pypi_downloads             → Charts 3, 9
├── fact_technology_daily_snapshot  → Charts 3, 7
├── dim_technology                  → All charts (filter + label)
├── dim_date                        → All charts (time axis)
├── dim_category                    → Charts 2, 3, 8
│
└── ingestion_manifest              → Chart 12 (pipeline health only)
```

---

### Dashboard Summary Table

| # | Page | Chart Name | Chart Type | Primary Gold Table | Business Question |
|---|---|---|---|---|---|
| 1 | Overview | AI Stack Activity Heatmap | Calendar Heatmap | `fact_github_activity` | When was the ecosystem most active? |
| 2 | Overview | Category Breakdown | Treemap | `fact_github_activity` | Which categories dominate? |
| 3 | Overview | Top 10 Technologies | Horizontal Bar | `fact_technology_daily_snapshot` | Who leads across all layers? |
| 4 | Deep Dive | Engineering vs Adoption Timeline | Dual-Axis Line | `fact_github_activity` + `fact_hf_adoption` | Do they move together or diverge? |
| 5 | Deep Dive | Engineering Activity Breakdown | Stacked Area | `fact_github_activity` | What actions are contributors taking? |
| 6 | Deep Dive | HF Model Growth Waterfall | Waterfall / Line | `fact_hf_model_activity` | Is the model ecosystem growing? |
| 7 | Comparison | Technology Lifecycle Quadrant | Bubble Scatter | `fact_technology_daily_snapshot` | What lifecycle stage is each technology in? |
| 8 | Comparison | Activity Race Chart | Bar Race | `fact_github_activity` | How has ranking changed over time? |
| 9 | Comparison | Ecosystem Balance Radar | Radar / Spider | All fact tables | How balanced is each ecosystem? |
| 10 | Lag Analysis | Lead-Lag Correlation Timeline | Dual-Line | `fact_github_activity` + `fact_hf_adoption` | How long before activity drives adoption? |
| 11 | Lag Analysis | Release-Driven Spike Analysis | Annotated Line | `fact_github_activity` + `fact_hf_model_activity` | Do releases cause model ecosystem spikes? |
| 12 | Health | Pipeline Status & Freshness | Status Table | `ingestion_manifest` | Is the pipeline healthy and fresh? |

## 35. Analytical Metrics

Metrics must be clearly defined.

Examples:

**Engineering activity**

- events_per_day
- active_contributors
- active_repositories
- pull_requests
- issues
- releases

**HF ecosystem activity**

- models_created
- models_modified
- unique_model_authors
- downloads
- likes

**PyPI adoption**

- daily_downloads
- weekly_downloads
- monthly_downloads
- growth_rate

**Temporal metrics**

- day-over-day change
- week-over-week change
- month-over-month change
- rolling averages
- growth rate
- activity volatility

Never combine metrics with incompatible grains without explicitly aggregating them.

---

## 36. Derived Technology Lifecycle Signals

Potential derived fields:

- first_seen_date
- latest_seen_date
- active_days
- activity_30d
- activity_90d
- adoption_30d
- adoption_90d
- growth_rate
- contributor_growth
- model_growth
- package_growth

A lifecycle classifier can be added later.

Example:

- emerging
- growing
- mature
- declining

The classification methodology must be documented and versioned.

---

## 37. Data Retention

**Bronze:** Preserve as much source history as storage and project constraints allow.

**Silver:** Preserve normalized historical data.

**Gold:** Preserve historical analytical snapshots where required.

Do not overwrite historical observations merely because a source now reports a different value.

---

## 38. Reproducibility

A fresh machine should be able to reproduce the pipeline.

Required:

- requirements.txt / pyproject.toml
- Dockerfile
- docker-compose.yml
- .env.example
- configuration
- schema definitions
- pipeline documentation

Every dataset generation should record:

- pipeline_version
- schema_version
- source_version / retrieval timestamp

---

## 39. Secrets

Never commit:

- API tokens
- passwords
- private keys
- database credentials

Use:

- .env
- environment variables
- secret manager

Commit: `.env.example` with placeholders.

---

## 40. Agile Project Management

The project is intentionally not frozen.

Use:

- PROJECT.md
- ROADMAP.md
- CHANGELOG.md
- docs/decisions/

Requirements may be added, modified, deferred, or removed.

Every meaningful architectural change should produce an ADR.

---

## 41. Phase Structure

### Phase 0 — Discovery & Feasibility

**Goal:** Prove that the selected sources provide the fields and volume required.

**Tasks:**

- inspect GH Archive samples;
- inspect HF API responses;
- inspect PyPI Stats;
- estimate data volume;
- test rate limits;
- identify source fields;
- define initial technology universe;
- build first crosswalk sample.

**Deliverables:**

- source documentation
- sample raw files
- initial schema
- technology universe v0.1
- volume estimate
- risk register

**Exit criteria:**

- all Phase 1 sources can be accessed;
- sample data successfully stored;
- initial schemas documented;
- incremental strategy demonstrated on small samples.

---

## 42. Phase 1 — Foundation

**Goal:** Build reliable Bronze ingestion.

**Tasks:**

*GitHub*

- historical downloader;
- hourly incremental downloader;
- manifest;
- checkpoint;
- retries;
- checksum/hash;
- partitioned storage.

*Hugging Face*

- model listing;
- metadata extraction;
- snapshot storage;
- snapshot manifest.

**Deliverables:**

- Bronze GitHub pipeline
- Bronze HF pipeline
- manifest
- checkpoint system
- raw schemas
- initial CLI

---

## 43. Phase 2 — Silver

**Goal:** Produce normalized, queryable entities.

**Tasks:**

- normalize timestamps;
- normalize IDs;
- flatten required nested structures;
- deduplicate;
- validate;
- create technology crosswalk;
- create conformed date dimension;
- create source-specific Silver tables.

**Deliverables:**

- Silver datasets
- crosswalk
- data-quality suite
- schema documentation

---

## 44. Phase 3 — Gold & Analytics

**Goal:** Build the analytical warehouse.

**Tasks:**

- fact tables;
- dimensions;
- daily technology snapshots;
- historical metrics;
- cross-source joins;
- analytical views.

**Deliverables:**

- Gold warehouse
- SQL views
- metric definitions
- initial dashboard

---

## 45. Phase 4 — PyPI Extension

**Goal:** Add package-level adoption.

**Tasks:**

- PyPI ingestion;
- package crosswalk;
- daily fact;
- incremental processing;
- quality tests;
- Gold integration.

**Deliverables:**

- PyPI Bronze
- PyPI Silver
- PyPI Gold
- package dimension
- adoption analytics

---

## 46. Phase 5 — Advanced Analytics

Possible additions:

- lifecycle classification;
- cross-source lag analysis;
- contribution concentration;
- technology emergence detection;
- ecosystem growth;
- dependency analysis;
- anomaly detection.

These are optional and must not compromise pipeline reliability.

---

## 47. Phase 6 — arXiv Extension

**Goal:** Add the research layer.

Architecture:

```text
arXiv
  ↓
Research activity
  ↓
Technology mapping
  ↓
GitHub
  ↓
HF / PyPI
```

Potential analysis:

```text
Research
   ↓
Engineering implementation
   ↓
Package availability
   ↓
Model ecosystem adoption
```

This phase is explicitly optional.

---

## 48. Agile Backlog

Backlog items should use IDs.

Example epics:

- EPIC-01 Source Ingestion
- EPIC-02 Bronze Architecture
- EPIC-03 Silver Modeling
- EPIC-04 Gold Warehouse
- EPIC-05 Incremental Processing
- EPIC-06 Data Quality
- EPIC-07 Technology Identity
- EPIC-08 Analytics
- EPIC-09 Dashboard
- EPIC-10 Observability

Stories:

- GH-001 Download hourly archive
- GH-002 Validate archive
- GH-003 Persist Bronze
- GH-004 Create checkpoint
- HF-001 Retrieve model metadata
- HF-002 Create snapshot
- HF-003 Compare snapshots
- HF-004 Detect modified models
- XW-001 Create technology dimension
- XW-002 Map GitHub repository
- XW-003 Map HF namespace
- XW-004 Map PyPI package

---

## 49. Definition of Done

A story is complete only when:

- implementation exists;
- tests exist;
- documentation exists;
- failure behavior is defined;
- logging exists;
- data quality checks pass;
- reproducibility is verified;
- code is committed;
- relevant ADR is updated if architecture changed.

---

## 50. Acceptance Criteria for the Core System

The core system is complete when:

- GH Archive historical data can be ingested.
- GH Archive hourly data can be ingested incrementally.
- HF model metadata can be ingested.
- HF snapshots can be compared.
- New/updated/unchanged records can be identified.
- Raw data is preserved in Bronze.
- Silver data is normalized.
- Cross-source technology identities exist.
- Gold facts/dimensions exist.
- Pipelines are idempotent.
- Failed jobs do not corrupt checkpoints.
- Data-quality tests execute automatically.
- Pipeline runs are recorded.
- At least one large distributed Spark transformation is demonstrated.
- Analytical queries can be executed against Gold.
- Dashboard views are generated from Gold rather than raw source data.

---

## 51. Risk Register

### Risk 1 — Source API changes

Mitigation:

- schema versioning;
- validation;
- contract tests;
- source adapters.

### Risk 2 — GH Archive completeness

Mitigation:

- explicitly document coverage;
- monitor partition availability;
- do not claim complete GitHub activity;
- keep source-quality metrics.

### Risk 3 — Hugging Face rate limits

Mitigation:

- pagination;
- controlled concurrency;
- caching;
- retries with backoff;
- incremental snapshots;
- optional authentication.

The Hugging Face documentation states that API calls are subject to Hub-wide rate limits.

### Risk 4 — Dataset becomes too large

Mitigation:

- technology universe;
- date-bounded backfill;
- partitioning;
- Parquet;
- Spark;
- configurable scope.

### Risk 5 — Cross-source matching errors

Mitigation:

- crosswalk table;
- mapping method;
- mapping confidence;
- manual verification for critical technologies.

### Risk 6 — Scope creep

Mitigation:

- Phase gates;
- backlog;
- explicit non-goals;
- stretch goals.

---

## 52. Architecture Decision Records

Use ADRs for major decisions.

Example:

- ADR-001: Use Medallion Architecture
- ADR-002: Use Parquet for analytical storage
- ADR-003: Use Spark for distributed transformations
- ADR-004: Use snapshot-diff for HF incremental processing
- ADR-005: Use a curated technology universe
- ADR-006: Use a technology crosswalk
- ADR-007: Add PyPI as an adoption source
- ADR-008: Treat arXiv as optional extension

Each ADR should contain:

- Status
- Context
- Decision
- Alternatives
- Consequences
- Date

---

## 53. Change Management

Never silently change the architecture.

If a requirement changes:

```text
Old requirement
      ↓
Reason for change
      ↓
Impact analysis
      ↓
Decision
      ↓
Documentation update
```

Maintain: `CHANGELOG.md`

Example:

```markdown
## [0.2.0]

Added:
- PyPI source
- package dimension

Changed:
- HF snapshot frequency from daily to configurable

Deferred:
- arXiv ingestion
```

---

## 54. Data Contracts

Each source should have a data contract.

Example:

```yaml
source: huggingface
dataset: models

required:
  - id
  - createdAt
  - lastModified

optional:
  - downloads
  - likes
  - tags
  - pipeline_tag
```

When a required field disappears or changes type, the pipeline should fail validation rather than silently corrupting Silver.

---

## 55. Observability

The pipeline should expose:

- records read
- records written
- records rejected
- processing time
- throughput
- latest partition
- last successful run
- failed runs
- data quality failures
- source freshness

A simple status command should eventually show:

```text
StackTrack AI Pipeline

GitHub
  Last partition: 2026-09-27 11:00
  Status: HEALTHY
  Records: 482,391

Hugging Face
  Last snapshot: 2026-09-27
  Status: HEALTHY
  Models: 2,xxx,xxx

PyPI
  Last date: 2026-09-27
  Status: HEALTHY

Silver
  Status: HEALTHY

Gold
  Status: HEALTHY
```

---

## 56. Testing Strategy

**Unit tests**

Test:

- parsers;
- transformations;
- hashing;
- mapping;
- date handling.

**Integration tests**

Test:

```text
source
 ↓
Bronze
 ↓
Silver
 ↓
Gold
```

using small fixtures.

**Data-quality tests**

Test:

- nulls;
- duplicates;
- invalid values;
- referential integrity;
- freshness;
- volume anomalies.

**Regression tests**

Keep representative source payloads.

If a source changes schema, tests should expose the change.

---

## 57. Development Workflow

Recommended workflow:

```text
Issue
  ↓
Design
  ↓
ADR if needed
  ↓
Implementation
  ↓
Unit tests
  ↓
Integration test
  ↓
Data-quality test
  ↓
Documentation
  ↓
Commit
```

Use small commits.

Example:

```text
feat(github): add hourly archive downloader
feat(hf): add model snapshot ingestion
feat(hf): add snapshot diff engine
feat(silver): normalize github events
feat(crosswalk): add technology mapping
```

---

## 58. Initial Milestone Plan

- **Milestone M0** — Source exploration complete
- **Milestone M1** — GitHub Bronze working
- **Milestone M2** — Hugging Face Bronze working
- **Milestone M3** — Incremental processing working
- **Milestone M4** — Silver models working
- **Milestone M5** — Gold warehouse working
- **Milestone M6** — Analytics working
- **Milestone M7** — Dashboard + presentation
- **Milestone M8** — Optional PyPI / arXiv extension

---

## 59. First Implementation Sprint

Do NOT start by building the entire system.

Start with a vertical slice.

**Step 1** — Create project repository.

**Step 2** — Implement:

```bash
ase source test github
```

**Step 3** — Download exactly one GH Archive hour.

**Step 4** — Store it in Bronze.

**Step 5** — Read it with Spark.

**Step 6** — Create a tiny Silver table.

**Step 7** — Create a manifest record.

**Step 8** — Repeat the same hour.

**Step 9** — Prove that no duplicate data appears.

**Step 10** — Move to Hugging Face. Implement:

```bash
ase ingest huggingface --snapshot
```

**Step 11** — Store the snapshot.

**Step 12** — Run the same snapshot twice.

**Step 13** — Implement snapshot diff.

At that point, the architecture has been proven end-to-end.

---

## 60. First Vertical Slice

The first demonstrable pipeline should be:

```text
GH Archive
     ↓
Downloader
     ↓
Bronze JSON.GZ
     ↓
Spark
     ↓
Silver events
     ↓
Technology filter
     ↓
Gold daily activity
     ↓
SQL query
```

Then:

```text
Hugging Face
     ↓
API
     ↓
Bronze snapshot
     ↓
Snapshot diff
     ↓
Silver model changes
     ↓
Gold adoption
```

Then combine:

```text
GitHub activity
       +
HF adoption
       ↓
Technology daily view
```

This gives a working project before adding complexity.

---

## 61. Example Gold View

A conceptual analytical view: `technology_daily_metrics`

- date
- technology_id
- technology_name
- category
- github_events
- github_contributors
- github_repositories
- github_prs
- github_issues
- github_releases
- hf_models
- hf_new_models
- hf_modified_models
- hf_downloads
- hf_likes
- pypi_downloads

This becomes the primary dashboard dataset.

---

## 62. Important Grain Rule

Every fact table must explicitly document its grain.

Example:

`fact_hf_model_snapshot`

Grain: One HF model at one snapshot date.

Not: One model.

Likewise:

`fact_pypi_downloads`

Grain: One package × one date × one download metric scope.

This rule should be enforced throughout the project.

---

## 63. Semantic Layer

Metric definitions should be centralized.

Example:

- `github_activity` = count(valid GitHub events)
- `active_contributors` = count(distinct actor_id)
- `new_hf_models` = count(models where first_seen_date = analysis_date)
- `hf_adoption` = sum(download metric at defined grain)

Do not implement the same metric differently in different dashboard queries.

---

## 64. Documentation Requirements

The repository should eventually contain:

- README
- PROJECT
- ROADMAP
- CHANGELOG
- Source documentation
- Architecture documentation
- Data dictionary
- Schema documentation
- Metric definitions
- ADR records
- Runbook
- Troubleshooting guide
- CLI documentation

---

## 65. Final Architecture

The intended mature architecture is:

```text
                         PUBLIC DATA SOURCES
                                │
              ┌─────────────────┼─────────────────┐
              │                 │                 │
              ▼                 ▼                 ▼
         GH Archive        Hugging Face          PyPI
         Engineering        AI ecosystem       Packages
              │                 │                 │
              └─────────────────┼─────────────────┘
                                │
                         INGESTION LAYER
                                │
                    ┌───────────┴───────────┐
                    │                       │
               Full/Backfill            Incremental
                    │                       │
                    └───────────┬───────────┘
                                ▼
                           BRONZE
                     Immutable raw storage
                                │
                                ▼
                           SILVER
                  Clean + normalized entities
                                │
                       ┌────────┴────────┐
                       │                 │
                 Crosswalk          Data Quality
                       │                 │
                       └────────┬────────┘
                                ▼
                            GOLD
                  Facts + Dimensions + Views
                                │
                ┌───────────────┼───────────────┐
                │               │               │
                ▼               ▼               ▼
             SQL/API       Analytics        Dashboard
                                │
                                ▼
                     STACKTRACK AI
```

---

## 66. Project Success Definition

The project succeeds if it demonstrates that the team can take large, heterogeneous, continuously changing public datasets and turn them into a reliable historical analytical system.

The final demonstration should emphasize:

```text
Large-scale ingestion
        ↓
Multiple source types
        ↓
Incremental processing
        ↓
Data quality
        ↓
Identity resolution
        ↓
Distributed transformation
        ↓
Historical modeling
        ↓
Analytical warehouse
        ↓
Useful ecosystem analysis
```

The dashboard is the last layer, not the project itself.

---

## 67. Current Version / Starting Point

### Version 0.1.0 — Committed

- Project name: StackTrack AI
- Medallion architecture
- GitHub/GH Archive source
- Hugging Face source
- Historical + incremental processing
- Technology crosswalk
- Spark/PySpark processing
- Parquet-oriented storage
- Gold analytical warehouse
- Agile phase structure
- CLI-first implementation
- Data-quality and observability requirements

### Planned

- PyPI integration
- Advanced lifecycle analytics
- More comprehensive dependency modeling

### Optional

- arXiv research layer
- Research → engineering → adoption analysis
- Dependency graph
- Advanced anomaly detection

### Explicitly deferred

- Any feature that makes the core ingestion pipeline unreliable.
- Any source whose availability/rate limits cannot be validated.
- Unbounded collection of the entire public AI ecosystem.

---

## 68. Immediate Next Actions

The CLI/implementation agent should NOT begin by implementing the entire architecture.

It should perform this sequence:

1. Inspect this PROJECT.md.
2. Convert requirements into an implementation backlog.
3. Inspect the environment.
4. Propose the repository/tooling setup.
5. Validate GitHub/GH Archive access.
6. Validate Hugging Face API access.
7. Collect small representative samples.
8. Measure actual record counts and payload sizes.
9. Confirm source schemas.
10. Produce a concrete Phase 0 report.
11. Only after Phase 0 approval, implement Bronze.

The first implementation goal is therefore not the dashboard.

It is: **Prove that the source data, storage strategy, incremental mechanisms, and processing engine work together on real data.**

---

## 69. Living-Document Rule

This document is the project's starting contract, not a permanent frozen specification.

Future changes must:

1. Identify the requirement being changed.
2. Explain why.
3. Record technical consequences.
4. Update the relevant architecture/schema.
5. Update the backlog.
6. Update the version/changelog.
7. Add an ADR when the change is architectural.

The project should evolve through evidence from actual source data rather than assumptions.

---

## 70. Source References

Official source documentation used to establish the current source capabilities:

- GH Archive: https://www.gharchive.org/
- Hugging Face Hub documentation: https://huggingface.co/docs/hub/
- Hugging Face Hub API: https://huggingface.co/docs/hub/api
- Hugging Face Python client: https://huggingface.co/docs/huggingface_hub/
- PyPI Stats: https://pypistats.org/api

The Hugging Face documentation currently describes the Hub as hosting over 2M models, 1.5M datasets, and 1.5M Spaces, and documents API access plus webhook support for incremental repository information. The project must re-check source documentation during implementation because APIs, limits, schemas, and available fields can change.

---

*End of Project Specification*
