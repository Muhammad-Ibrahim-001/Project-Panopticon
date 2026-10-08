# Project Panopticon - AI Agent Operational Guidelines

## 1. Mandatory Pre-Flight Directives

### 1.1 Documentation Reading Protocol
- **Strict Format Filter**: When reading documentation in `Docs/`, **ONLY** read `.md` (Markdown) files. **DO NOT** read or parse `.docx` files (located under `Docs/.docx/` or elsewhere); they are binary/academic submission duplicates of the `.md` files.
- **Pre-Action Review**: Before writing code, modifying notebooks, or altering lakehouse tables:
  1. Inspect `panopticon_adversarial_review.md` (to verify adversarial threat models, attack vectors, and validation assumptions).
  2. Inspect the relevant markdown files in `Docs/Phase 1/`, `Docs/Phase 2/`, or `Docs/Future & Reference/` corresponding to the task.

### 1.2 Cross-File Impact & Dependency Header Standard
Whenever creating or editing a script, notebook, SQL file, or pipeline component that affects, depends on, or is consumed by other files:
- **You MUST include/update a structured Dependency Comment Block at the top of the file** (or immediately below the module docstring / first cell):

```python
# ==============================================================================
# PIPELINE DEPENDENCY & IMPACT TRACKING
# ==============================================================================
# Upstream Dependencies:
#   - [File/Table/API]: <e.g., bronze_ooni_raw / s3://ooni-data-eu-fra/>
#   - [File]: <e.g., notebooks/01_ingestion_bronze.py>
# Downstream Impact:
#   - [Affects]: <e.g., notebooks/03_silver_enrichment.py, gold_censorship_metrics>
#   - [Schema/State Changes]: <e.g., adds 'normalized_asn' column, alters partition key>
# Risk & Side Effects:
#   - <e.g., Requires full Silver re-ingestion if raw schema mapping is modified>
# ==============================================================================
```

If modifying an existing file with an existing header, **always update** this block to reflect newly added or altered dependencies.

---

## 2. Core Engineering Practices for Lakehouse Pipeline

### 2.1 Medallion Invariants (Bronze -> Silver -> Gold)
- **Bronze (Raw / Append-Only)**: Ingest raw OONI telemetry with ingestion timestamps, input file provenance, and raw JSON payload preservation. Zero destructive transformations.
- **Silver (Cleaned / Conformed)**: Strict schema enforcement, deduplication via surrogate keys (`SHA-256` of composite natural keys), and late-arriving data resolution via Delta `MERGE INTO`.
- **Gold (Aggregated / Consumption)**: Business metrics, dimensional models (Star/Snowflake), and dashboard-ready analytical views.

### 2.2 Databricks Community Edition Memory Safeguards
The project executes under **15 GB total RAM / ~9.5 GB usable Spark heap / 2 vCPUs**:
- **Zero Dangerous Collections**: Never use `.collect()` or `.toPandas()` on unbounded or high-cardinality DataFrames. Always use `.take(n)` or aggregate beforehand.
- **Broadcast Joins Guard**: Broadcast joins (`broadcast(df)`) are strictly permitted only for small, static lookup tables (< 10 MB, e.g., Citizen Lab country taxonomy or AS name maps). Never broadcast dynamic measurement streams.
- **Partition Sizing**: Explicitly control shuffle partitions (`spark.sql.shuffle.partitions` tuned to 4–8 for Community Edition, never the Spark default of 200).
- **Explicit Schema Enforcement**: Always supply explicit `StructType` schemas when loading raw nested `.jsonl.gz` payloads. Avoid automatic schema inference (`inferSchema=True`), which forces multi-pass scans and triggers OOMs.

### 2.3 Idempotency & Deterministic Pipeline Design
- Every data write or table merge must be **idempotent**: re-running the pipeline on the same data batch must yield an identical lakehouse state without duplicate rows.
- Use explicit watermarking / lookback windows (e.g., rolling 3-day lookback: $T-3$ to $T$) for late-arriving OONI telemetry.

### 2.4 Notebook & Code Modularity
- Avoid monolithic 2,000-line notebooks. Separate concerns into:
  1. Configuration & Parameterization (Widgets / Environment variables)
  2. Extraction & Sanitization
  3. Transformation & Enrichment
  4. Delta Lake Persistence & Data Quality Validation

### 2.5 Teacher & Infrastructure Mandate: Pure API Ingestion (No Manual Uploads)
- **Zero Manual Data Uploads**: Under no circumstances should data be uploaded manually via the Databricks UI or local upload buttons.
- **Automated Ingestion via Code/API**: All telemetry and taxonomy must be ingested programmatically within Databricks using REST APIs or direct public HTTP streams:
  - OONI Telemetry: OONI Measurement API (`https://api.ooni.io/api/v1/...`) or direct public HTTPS S3 endpoints.
  - Citizen Lab Taxonomy: Raw GitHub REST/HTTPS endpoint (`https://raw.githubusercontent.com/citizenlab/test-lists/...`).
- **Target Runtime**: The entire pipeline must be runnable end-to-end directly on **Databricks Community Edition**.

---

## 3. Agent Execution Protocol
1. **Analyze First**: Read the relevant `.md` documentation and active code before suggesting or making changes.
2. **Explicit Impact Declaration**: In your plan or response, explicitly state which upstream and downstream files are impacted by your planned change.
3. **Preserve Integrity**: Do not remove existing comments, academic metadata, or validation checks.
