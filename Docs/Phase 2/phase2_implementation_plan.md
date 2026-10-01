# Phase 2: Lakehouse Bronze & Silver Engineering Plan
## Project Panopticon: Global Cyber Warfare & Internet Censorship Lakehouse

---

## 1. Instructor Requirements & Deliverables

### Context & Objective
With the data source approved and project structure planned, Phase 2 focuses on hands-on engineering. We will build the foundational layers of the Lakehouse (**Bronze** and **Silver**) using **Apache Spark**.

This phase transitions the project from a basic script to an enterprise-grade data pipeline prioritizing software engineering best practices: explicit contract enforcement, idempotency, fault tolerance, and comprehensive auditing.

### Strict Technical Requirements
1. **Infrastructure & Environment Setup**:
   * **Workspace**: Initialize project using Databricks Community Edition or Microsoft Azure (student credits).
   * **Version Control**: All PySpark notebooks or scripts must be continuously committed to the GitHub repository.
2. **Data Modeling & Schema Enforcement (Bronze & Silver)**:
   * **Data Dictionary**: Provide full, documented data models for both Bronze and Silver layers, specifying column names, data types, and primary keys.
   * **Strict Schema-on-Read**: Must **not** use Spark's built-in schema inference (`inferSchema=True`). Define schemas explicitly using PySpark's `StructType` and `StructField` APIs before reading raw files.
   * **Data Types & Casting**: Enforce strict data types. Perform necessary casting operations (e.g., converting strings to timestamps, or stringified numbers to doubles) as data moves into the Silver layer.
   * **Metadata Integration**: Every record in every table (Bronze and Silver) must include a `load_timestamp` column indicating exactly when that specific record was ingested or processed.
3. **Pipeline Robustness & Idempotency**:
   * **Idempotent Execution**: Fully idempotent pipeline. Multiple executions on the exact same raw data must not impact the Silver layer (zero duplicate rows). Must demonstrate the use of Delta Lake `MERGE INTO` (Upsert) patterns.
   * **Parameterized Backfills**: No rigid, hardcoded file paths that only process "today's" data. Functions/notebooks must accept parameters (e.g., specific date, batch ID, or folder path) so backfills from raw files to Bronze, or Bronze to Silver, can be re-executed at will for any historical timeframe.
   * **Schema Drift Handling**: Implement logic to handle schema drift. If the source system unexpectedly adds a new column or changes a data type, the pipeline should either gracefully evolve the schema (`mergeSchema`) or quarantine non-conforming records without crashing the entire batch.
4. **Audit & Execution Logging**:
   * **Dedicated Logging Tables**: Establish a robust logging framework with separate operational tables (e.g., `pipeline_execution_logs`) recording metadata about every pipeline run.
   * **Audit Metrics**: Every time a file or table is processed (for both Full and Incremental loads across every layer), write a log entry containing:
     * Layer being processed (e.g., `Raw-to-Bronze`, `Bronze-to-Silver`).
     * Parameter/file processed.
     * Execution start and end times.
     * Status (`Success` / `Failure`).
     * Number of rows inserted/updated.

### 1.4 Instructor Feedback Resolution & Empirical Re-Alignment
During Phase 2 review, instructor feedback identified two critical architectural questions:
1. **"Your data only gets added, never updated. OONI never changes old records, so MERGE has nothing to update. Explain what will actually get updated."**
   * **Root Cause & OONI Truth**: Raw OONI network measurements are immutable append-only event logs. Probes test a URL at a discrete timestamp and record the outcome; upstream OONI never mutates historical raw JSON in S3.
   * **Engineering Solution**: 
     - **3-Day Rolling Lookback Window**: OONI probes frequently operate on mobile devices (Android/iOS) over throttled or disconnected networks in censored regimes. Measurements are stored locally and synced days late. The incremental pipeline ingests a rolling 3-day window ($T-3$ to $T$). Truly late measurements take the `WHEN NOT MATCHED INSERT` branch.
     - **Dimensional Re-Enrichment**: Silver is an enriched layer. If Citizen Lab releases updated URL category mappings (e.g., a URL changes from `MISC` to `CIRCUMVENTION_TOOLS`) or tampering classification heuristics are refined, Delta `MERGE INTO` updates the enriched columns (`content_category`, `tampering_vector`, `human_rights_risk_tier`) for existing `measurement_id` records without dropping historical data.
     - **Pipeline Idempotency**: Multiple runs over the exact same date range never produce duplicate records; `MERGE INTO` reconciles audit metadata without row multiplication.
2. **"Data size is too large, I don't think it will run on free edition. Have you guys tried uploading the data yet?"**
   * **Root Cause & Databricks CE Reality**: Databricks Community Edition provides a single-node instance with **15 GB total RAM** (~9.5 GB usable Spark JVM heap), 2 CPU cores, and a **2 GB hard upload limit** on the DBFS web interface. In our empirical testing on live OONI archives, compressed `.json.gz` expands by **5.44x** into raw JSON text, and raw JSON expands by another 2x–3x into JVM objects. A 10 GB baseline would consume ~100 GB heap during array explodes and crashes the single-node driver with `OutOfMemoryError`.
   * **Engineering Solution**:
     - **Scoped Focus Baseline**: Right-sizing the baseline to ~400 MB – 600 MB compressed `.json.gz` (~2.2 GB – 3.2 GB raw text, ~35,000 to 60,000 deep telemetry records) focused on strategic censorship zones (Pakistan `PK`, Iran `IR`, Russia `RU`).
     - **Direct Cloud Read / Sample Mounting**: Rather than uploading multi-gigabyte files via the web UI, data is read directly from AWS Open Data (`s3://ooni-data-eu-fra/`) or staged locally in chunked micro-batches.
     - **Deterministic Memory Bounds**: `spark.sql.shuffle.partitions = 16` and explicit `StructType` Schema-on-Read ensure driver heap stays under 4 GB, running flawlessly on Databricks Community Edition ($0.00 cost).

### Submission Deliverables
* Updated GitHub repository link.
* PySpark notebooks/scripts executing the pipeline.
* Updated `README.md` containing the Bronze and Silver data models.
* Brief execution guide explaining how to pass parameters to trigger a backfill versus a standard incremental load.

---

## 2. Panopticon Feasibility Assessment

| Requirement Area | Feasible? | Panopticon Solution Mapping |
| :--- | :---: | :--- |
| **Databricks / Cloud Workspace** | **YES** | Compatible with **Databricks Community Edition** (DBR 14.x/15.x with native Delta Lake) within 15 GB RAM ceiling via right-sized 500 MB baseline. |
| **Strict Schema-on-Read** | **YES** | Explicit `StructType` hierarchies for OONI JSON and Citizen Lab CSV without schema inference overhead. |
| **Bronze & Silver Data Models** | **YES** | Formal Data Dictionaries with column names, PySpark data types, nullability, and primary key (`measurement_id`). |
| **Strict Typing & Casting** | **YES** | Standardized UTC timestamps, integer port/ASN casting, double latency metrics, boolean anomaly flags. |
| **Metadata Ingestion Columns** | **YES** | Ingestion timestamp (`load_timestamp`), source file (`_source_file`), and batch ID (`_batch_id`) on all rows. |
| **Idempotency & Late Arrivals (MERGE INTO)** | **YES** | Delta Lake `DeltaTable.merge()` on `target.measurement_id = source.measurement_id` with 3-day rolling lookback ($T-3$ to $T$) and dimensional re-enrichment. |
| **Parameterized Backfills** | **YES** | Parameter-driven execution via Databricks Widgets (`dbutils.widgets`) and Python function arguments (`batch_date`, `load_type`, `lookback_days`). |
| **Schema Drift & Quarantine** | **YES** | Dual-strategy: `mergeSchema=true` for safe column evolution + PERMISSIVE mode routing malformed rows to `quarantine_ooni_raw`. |
| **Dedicated Audit Logging** | **YES** | Operational table `pipeline_execution_logs` tracking layer, execution time, duration, status, and rows inserted/updated. |
| **Documentation & Execution Guide**| **YES** | Updated `README.md`, `Docs/Phase 2/06_data_dictionary.md`, and `Docs/Phase 2/07_execution_guide.md`. |

---

## 3. Architecture & Data Flow

```mermaid
flowchart TD
    subgraph Raw_Layer["Raw Storage (S3 / FileStore)"]
        RawJSON["OONI Network Telemetry (.json / .jsonl)"]
        RefCSV["Citizen Lab Category Taxonomy (.csv)"]
    end

    subgraph Bronze_Layer["Bronze Layer (Raw Storage Landing)"]
        SchemaRead["Strict Schema-on-Read (StructType)"]
        DriftRouter{"Schema Drift / Malformed?"}
        BronzeDelta[("bronze_ooni_raw (Delta Table)")]
        QuarantineDelta[("quarantine_ooni_raw (Delta Table)")]
    end

    subgraph Silver_Layer["Silver Layer (Cleansed, Sanitized, Enriched)"]
        PIIMasking["Cryptographic PII Masking (HMAC-SHA256)"]
        Flattening["Array Exploding & Normalization"]
        VectorEngine["Tampering Vector Classification Engine"]
        RefJoin["Broadcast Join with Citizen Lab Categories"]
        DeltaMerge["Idempotent Upsert (Delta MERGE INTO)"]
        SilverDelta[("silver_network_measurements (Delta Table)")]
    end

    subgraph Operational_Layer["Operational Governance & Auditing"]
        AuditLogger["Pipeline Auditor Context Manager"]
        AuditTable[("pipeline_execution_logs (Delta Table)")]
    end

    RawJSON --> SchemaRead
    SchemaRead --> DriftRouter
    DriftRouter -- Valid Records --> BronzeDelta
    DriftRouter -- Corrupt Records --> QuarantineDelta

    BronzeDelta --> PIIMasking
    RefCSV --> RefJoin
    PIIMasking --> Flattening
    Flattening --> VectorEngine
    VectorEngine --> RefJoin
    RefJoin --> DeltaMerge
    DeltaMerge --> SilverDelta

    SchemaRead -. Log Audit .-> AuditLogger
    DeltaMerge -. Log Audit .-> AuditLogger
    AuditLogger --> AuditTable
```

---

## 4. Proposed Data Models (Data Dictionaries)

### 4.1 Bronze Layer: `bronze_ooni_raw`
Stores raw, unmanipulated records landed with audit metadata.

| Column Name | PySpark Data Type | Nullable | Primary Key | Description |
| :--- | :--- | :---: | :---: | :--- |
| `report_id` | `StringType` | No | No | Unique test report identifier from OONI probe |
| `input` | `StringType` | Yes | No | Target test URL or network domain |
| `test_name` | `StringType` | No | No | Name of the test (`web_connectivity`, `echcheck`, etc.) |
| `test_start_time` | `StringType` | No | No | Unparsed start time string from source payload |
| `probe_asn` | `StringType` | No | No | Autonomous System Number of probe (e.g., `AS9541`) |
| `probe_cc` | `StringType` | No | No | ISO country code of testing probe (e.g., `PK`) |
| `probe_ip` | `StringType` | Yes | No | Raw client IP address (to be sanitized in Silver) |
| `probe_network_name` | `StringType` | Yes | No | ISP / Telecom operator name |
| `resolver_asn` | `StringType` | Yes | No | Resolver Autonomous System Number |
| `resolver_ip` | `StringType` | Yes | No | Local DNS resolver IP |
| `resolver_network_name`| `StringType` | Yes | No | Resolver network name |
| `test_runtime` | `DoubleType` | Yes | No | Total execution duration of test in seconds |
| `software_name` | `StringType` | Yes | No | Testing agent client software |
| `software_version` | `StringType` | Yes | No | Client version |
| `test_keys` | `StructType` | Yes | No | Nested payload containing queries, handshakes, failures |
| `load_timestamp` | `TimestampType` | No | No | **[Audit]** Exact UTC timestamp of pipeline ingestion |
| `_source_file` | `StringType` | No | No | **[Audit]** Input file name / S3 path |
| `_batch_id` | `StringType` | No | No | **[Audit]** Execution batch parameter / date |

### 4.2 Silver Layer: `silver_network_measurements`
Cleansed, flattened, typed, PII-sanitized, and enriched with Citizen Lab categories.

| Column Name | PySpark Data Type | Nullable | Primary Key | Description |
| :--- | :--- | :---: | :---: | :--- |
| `measurement_id` | `StringType` | No | **PK** | Deterministic hash (`sha2(report_id \|\| input \|\| test_start_time)`) |
| `event_timestamp` | `TimestampType` | No | No | Parsed UTC timestamp of test execution |
| `target_url` | `StringType` | Yes | No | Sanitized target URL (query credentials scrubbed) |
| `target_domain` | `StringType` | Yes | No | Extracted second-level domain name |
| `probe_asn` | `LongType` | No | No | Cleaned numeric Autonomous System Number |
| `probe_cc` | `StringType` | No | No | ISO country code (e.g., `PK`, `IR`, `RU`) |
| `probe_network_name` | `StringType` | Yes | No | Telecommunications provider / ISP |
| `masked_probe_subnet`| `StringType` | Yes | No | Truncated `/24` subnet masking client IP |
| `hashed_resolver_ip` | `StringType` | Yes | No | Cryptographically salted HMAC-SHA256 resolver hash |
| `test_name` | `StringType` | No | No | Network test classification |
| `tampering_vector` | `StringType` | No | No | Attack label (`DNS_TAMPERING`, `TCP_RESET`, `TLS_DROP`, `BENIGN`) |
| `is_anomaly` | `BooleanType` | No | No | Binary anomaly flag indicating censorship activity |
| `dns_experiment_failure` | `StringType` | Yes | No | DNS-level error message if present |
| `http_experiment_failure`| `StringType` | Yes | No | HTTP-level error message if present |
| `duration_seconds` | `DoubleType` | Yes | No | Standardized numeric execution runtime |
| `content_category` | `StringType` | Yes | No | Citizen Lab category code (`NEWS`, `CULTR`, `POL`) |
| `category_description` | `StringType` | Yes | No | Full descriptive label from category taxonomy |
| `human_rights_risk_tier`| `StringType`| Yes | No | Assigned vulnerability level (`HIGH`, `MEDIUM`, `LOW`) |
| `load_timestamp` | `TimestampType` | No | No | **[Audit]** Exact UTC timestamp when record was upserted |
| `_batch_id` | `StringType` | No | No | **[Audit]** Ingestion batch identifier |

### 4.3 Operational Table: `pipeline_execution_logs`
Tracks audit metrics for every ingestion and transformation run.

| Column Name | PySpark Data Type | Nullable | Primary Key | Description |
| :--- | :--- | :---: | :---: | :--- |
| `log_id` | `StringType` | No | **PK** | Unique execution run UUID |
| `pipeline_layer` | `StringType` | No | No | Processing stage (`Raw-to-Bronze`, `Bronze-to-Silver`) |
| `batch_parameter` | `StringType` | No | No | Input parameter (date, directory, batch key) |
| `load_type` | `StringType` | No | No | Mode (`Full`, `Incremental`, `Backfill`) |
| `start_time` | `TimestampType` | No | No | Pipeline execution start timestamp |
| `end_time` | `TimestampType` | No | No | Pipeline execution completion timestamp |
| `duration_seconds` | `DoubleType` | No | No | Total elapsed runtime in seconds |
| `status` | `StringType` | No | No | Execution state (`Success`, `Failure`, `Partial_Quarantine`) |
| `rows_read` | `LongType` | No | No | Total raw records scanned from source |
| `rows_inserted` | `LongType` | No | No | Net new records inserted into Delta table |
| `rows_updated` | `LongType` | No | No | Existing records updated via Delta MERGE |
| `rows_quarantined` | `LongType` | No | No | Corrupt records diverted to quarantine table |
| `error_message` | `StringType` | Yes | No | Detailed stack trace if status is `Failure` |

---

## 5. Technical Implementation Blueprint

### 5.1 Strict Schema-on-Read Pattern
```python
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, ArrayType, BooleanType

test_keys_schema = StructType([
    StructField("dns_experiment_failure", StringType(), True),
    StructField("http_experiment_failure", StringType(), True),
    StructField("control_failure", StringType(), True),
    StructField("queries", ArrayType(
        StructType([
            StructField("failure", StringType(), True),
            StructField("query_type", StringType(), True),
            StructField("answers", ArrayType(
                StructType([
                    StructField("answer_type", StringType(), True),
                    StructField("ipv4", StringType(), True)
                ])
            ), True)
        ])
    ), True),
    StructField("tcp_connect", ArrayType(
        StructType([
            StructField("ip", StringType(), True),
            StructField("port", LongType(), True),
            StructField("status", StructType([
                StructField("failure", StringType(), True),
                StructField("success", BooleanType(), True)
            ]), True)
        ])
    ), True)
])

bronze_schema = StructType([
    StructField("report_id", StringType(), True),
    StructField("input", StringType(), True),
    StructField("test_name", StringType(), True),
    StructField("test_start_time", StringType(), True),
    StructField("probe_asn", StringType(), True),
    StructField("probe_cc", StringType(), True),
    StructField("probe_ip", StringType(), True),
    StructField("probe_network_name", StringType(), True),
    StructField("resolver_asn", StringType(), True),
    StructField("resolver_ip", StringType(), True),
    StructField("resolver_network_name", StringType(), True),
    StructField("test_runtime", DoubleType(), True),
    StructField("software_name", StringType(), True),
    StructField("software_version", StringType(), True),
    StructField("test_keys", test_keys_schema, True),
    StructField("_corrupt_record", StringType(), True)
])

# Read with strict schema enforcement (PERMISSIVE mode routes corrupted lines)
df_raw = (
    spark.read
    .schema(bronze_schema)
    .option("mode", "PERMISSIVE")
    .option("columnNameOfCorruptRecord", "_corrupt_record")
    .json(f"{source_dir}/{batch_param}/*.json")
)
```

### 5.2 Idempotent Delta MERGE Pattern (Late Arrivals & Dimensional Re-Enrichment)
Because raw OONI measurements are immutable event logs, the `MERGE INTO` operation explicitly serves three operational purposes:
1. **Late-Arriving Measurements**: Ingesting a rolling 3-day lookback window ($T-3$ to $T$) catches delayed uploads from offline/mobile probes; records with net-new `measurement_id` execute `whenNotMatchedInsertAll()`.
2. **Dimensional Re-Enrichment**: If upstream Citizen Lab taxonomies update a domain's categorization or tampering classification heuristics are refined, `whenMatchedUpdate` updates enriched attributes without altering raw test keys.
3. **Pipeline Idempotency**: Re-running a batch updates `load_timestamp` and `_batch_id` without creating duplicate rows.

```python
from delta.tables import DeltaTable

delta_silver = DeltaTable.forPath(spark, silver_delta_path)

delta_silver.alias("target").merge(
    source=df_silver_staged.alias("source"),
    condition="target.measurement_id = source.measurement_id"
).whenMatchedUpdate(
    condition="target.content_category != source.content_category OR target.tampering_vector != source.tampering_vector OR target._batch_id != source._batch_id",
    set={
        "event_timestamp": "source.event_timestamp",
        "tampering_vector": "source.tampering_vector",
        "is_anomaly": "source.is_anomaly",
        "dns_experiment_failure": "source.dns_experiment_failure",
        "http_experiment_failure": "source.http_experiment_failure",
        "duration_seconds": "source.duration_seconds",
        "content_category": "source.content_category",
        "category_description": "source.category_description",
        "human_rights_risk_tier": "source.human_rights_risk_tier",
        "load_timestamp": "source.load_timestamp",
        "_batch_id": "source._batch_id"
    }
).whenNotMatchedInsertAll().execute()
```

### 5.3 Parameterized Backfill vs. Incremental Execution Pattern
```python
# Databricks Widgets or CLI Parameterization
dbutils.widgets.text("batch_date", "2026-09-27", "Batch Date (YYYY-MM-DD)")
dbutils.widgets.dropdown("load_type", "incremental", ["full", "incremental", "backfill"], "Load Type")
dbutils.widgets.text("lookback_days", "3", "Lookback Days (Rolling Window)")

batch_date = dbutils.widgets.get("batch_date")
load_type = dbutils.widgets.get("load_type")
lookback_days = int(dbutils.widgets.get("lookback_days"))

# Calculate sliding lookback dates for incremental recovery:
# Ingests [batch_date - lookback_days, batch_date]
```

---

## 6. Repository Layout for Phase 2 Submission

```text
Panopticon/
├── notebooks/
│   ├── 00_audit_logger.py          # Operational logging utility & table setup
│   ├── 01_bronze_ingestion.py       # Strict schema-on-read ingestion + quarantine + metadata
│   ├── 02_silver_transformation.py  # Cleansing, casting, PII masking, flattening & Delta MERGE
│   └── run_pipeline.py              # Parameterized runner (Incremental vs Backfill)
├── data/
│   ├── samples/                     # Existing sample datasets
│   ├── bronze/                      # Delta Lake Bronze storage location
│   ├── silver/                      # Delta Lake Silver storage location
│   ├── quarantine/                  # Delta Lake Quarantine storage location
│   └── logs/                        # Operational audit log Delta storage location
├── Docs/
│   ├── Phase 2/
│   │   ├── phase2_implementation_plan.md  # Complete requirements & implementation guide
│   │   ├── 06_data_dictionary.md          # Comprehensive Bronze & Silver Data Dictionaries
│   │   └── 07_execution_guide.md          # Parameterized execution manual
└── README.md                        # Updated with data models & submission links
```
