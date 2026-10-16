# Deployment Guide, Troubleshooting & Known Failure Modes
## Project Panopticon: Production Operations Manual

---

### Overview
This guide provides the exact step-by-step instructions to deploy, configure, and execute the Project Panopticon Lakehouse across **Databricks Community Edition**, **Local PySpark**, and **Azure Cloud**, along with solutions to known edge cases and failure modes.

---

## 1. Quickstart: Databricks Community Edition Setup ($0.00 Cost)

### Step 1: Account Creation & Cluster Configuration
1. Navigate to [https://community.cloud.databricks.com/](https://community.cloud.databricks.com/) and register for a free account.
2. Click **Compute** -> **Create Cluster**:
   * **Cluster Name**: `panopticon-spark-node`
   * **Databricks Runtime Version**: `Runtime 14.3 LTS (Apache Spark 3.5.0, Scala 2.12)`
   * **Instance**: 15 GB Memory, 2 Cores.
3. In **Advanced Options** -> **Spark Config**, paste these memory safeguards:
   ```properties
   spark.sql.shuffle.partitions 4
   spark.databricks.delta.optimizeWrite.enabled true
   spark.databricks.delta.autoCompact.enabled true
   ```
4. Click **Create Cluster**.

### Step 2: Secret Scope & Privacy Configuration
To protect the salt used for hashing resolver IPs, create a Databricks Secret:
```python
# In Databricks Notebook cell
dbutils.secrets.put(scope="panopticon_vault", key="ip_salt", string="FAST_NUCES_SECRET_PEPPER_2026")
```

### Step 3: Run Ingestion Notebooks in Sequence
Import the notebooks from the repository into your Databricks workspace:
1. `notebooks/01_bronze_ingestion.py` -> Ingests baseline raw JSONL to Delta Bronze.
2. `notebooks/02_silver_cleaning_anonymization.py` -> Hashes PII, explodes arrays, joins Citizen Lab, upserts to Silver.
3. `notebooks/03_gold_dimensional_modeling.py` -> Builds Star Schema facts, dimensions, and aggregated marts.

---

## 2. Known Technical Setbacks & Troubleshooting Guide

### Failure Mode 1: `java.lang.OutOfMemoryError: Java heap space` (Community Edition Heap Exhaustion)
* **Symptoms**: Cluster driver terminates abruptly; logs show executor heap exhaustion during JSON read or wide transformations.
* **Root Cause**: Attempting to read multi-gigabyte raw JSON files on Databricks Community Edition (15 GB total RAM, ~9.5 GB usable Spark heap). Production `.jsonl.gz` expands by **5.44x** into raw JSON text, and raw JSON expands by another 2x–3x into JVM objects during array exploding.
* **Fix**:
  1. Scope baseline to strategic focus countries (~400 MB – 600 MB compressed / ~35k–60k deep records).
  2. Define an explicit `StructType` schema (never use `inferSchema=True`).
  3. Ingest via bounded micro-batches:
     ```python
     df = spark.readStream.format("json").schema(explicit_schema).option("maxBytesPerTrigger", "512mb").load("...")
     ```
  4. Ensure `spark.sql.shuffle.partitions` is set to `16` (not default 200).

### Failure Mode 2: AWS S3 503 "SlowDown" / Rate Limiting Errors
* **Symptoms**: Read job fails with `AmazonS3Exception: 503 Slow Down`.
* **Root Cause**: Reading thousands of small gzipped JSON files simultaneously across the same S3 prefix exceeds Amazon's request rate limits.
* **Fix**:
  * Add retry backoff in the Spark configuration:
    ```properties
    spark.hadoop.fs.s3a.retry.limit 10
    spark.hadoop.fs.s3a.retry.interval 500ms
    ```

### Failure Mode 3: Schema Drift / Polymorphic Key Mismatches
* **Symptoms**: Columns in `test_keys` return all `NULL` values for certain rows.
* **Root Cause**: Different OONI tests have different key names (e.g., `web_connectivity` has `queries`, while `http_requests` has `requests`).
* **Fix**:
  * Filter by `test_name` before parsing test-specific keys:
    ```python
    df_web = df_bronze.filter(col("test_name") == "web_connectivity")
    ```

### Failure Mode 4: Power BI Direct Query Refresh Latency
* **Symptoms**: Power BI visuals take > 30 seconds to render when cross-filtering.
* **Root Cause**: Dashboard is querying un-compacted Silver Delta tables containing hundreds of small transaction files.
* **Fix**:
  * Run the `OPTIMIZE` command with `Z-ORDER BY` on the Gold tables before connecting Power BI:
    ```sql
    OPTIMIZE fact_network_measurement ZORDER BY (country_code, date_key, asn_id);
    ```

### Failure Mode 5: Delta MERGE Redundancy (`rows_updated = 0`) on Immutable Telemetry
* **Symptoms**: Incremental batch runs always report `rows_updated = 0`, and late-arriving measurements from mobile probes are lost or ignored.
* **Root Cause**: OONI measurements are immutable event logs. When an incremental batch only reads single-day partition $T$, all records are net-new, making `WHEN MATCHED UPDATE` dead code. Furthermore, mobile probes that synced days late are missed.
* **Fix**:
  * Implement a **3-day rolling lookback window ($T-3$ to $T$)** in the pipeline runner (`--lookback-days 3`).
  * In the Delta `MERGE INTO` statement, specify conditional `whenMatchedUpdate` to update enriched attributes (`content_category`, `tampering_vector`, `human_rights_risk_tier`) when Citizen Lab taxonomies evolve, while using `whenNotMatchedInsertAll()` to ingest late-arriving records.

### Failure Mode 6: Databricks Serverless / Spark Connect `[JVM_ATTRIBUTE_NOT_SUPPORTED]` & Managed Configs
* **Symptoms**: Runtime crashes during initialization with `[JVM_ATTRIBUTE_NOT_SUPPORTED] Attribute 'sparkContext' is not supported on Spark Connect` or `AnalysisException: Cannot modify the value of a spark config: spark.databricks.delta.optimizeWrite.enabled`.
* **Root Cause**: Databricks Serverless Compute utilizes the remote Spark Connect client protocol, strictly prohibiting direct JVM handles (`sc._jsc.hadoopConfiguration()`). Furthermore, Serverless manages write optimizations and compaction at the infrastructure level, locking down manual `conf.set()` overrides.
* **Fix**:
  * Centralize runtime configuration in `notebooks/00_config.py` using `_safe_conf_set()`.
  * Wrap each Spark setting in an isolated try-except block to safely apply tunable parameters (e.g. `spark.sql.shuffle.partitions = 4`) while quietly ignoring immutable Serverless properties.
  * Eliminate all JVM `_jsc` calls from notebooks.

### Failure Mode 7: Photon Engine `[UNSUPPORTED_FEATURE.QUERY_ONLY_CORRUPT_RECORD_COLUMN]` on Corrupt Trap
* **Symptoms**: Reading raw JSON with `_corrupt_record` fails on Serverless with `AnalysisException: [UNSUPPORTED_FEATURE.QUERY_ONLY_CORRUPT_RECORD_COLUMN] Querying only the column '_corrupt_record' from raw JSON is not supported`.
* **Root Cause**: The Databricks Photon vectorized engine disallows filtering directly on `_corrupt_record` against streaming unmaterialized JSON files, and DataFrame `.cache()` is disabled on Serverless compute.
* **Fix**:
  * Materialize `df_raw` to a temporary Delta staging path (`<bronze_path>_temp_staging`).
  * Read back the materialized Delta DataFrame before filtering `df_valid` (`col("_corrupt_record").isNull()`) and `df_quarantine` (`col("_corrupt_record").isNotNull()`).

### Failure Mode 8: Cross-Country & Historical Date Partition Overwrite
* **Symptoms**: Re-running Bronze ingestion for Pakistan (`PK`) on `2026-09-27` wipes out previously ingested measurements for Iran (`IR`) on that date; or executing an incremental write with lookback deletes historical dates `[T-lookback, T-1]`.
* **Root Cause**: Partitioning solely by `measurement_date` means a predicate `measurement_date = '2026-09-27'` targets the entire date directory, erasing all other countries. Furthermore, using a range predicate `replaceWhere: measurement_date >= 'T-3' AND measurement_date <= 'T'` when the incoming batch only contains single-day data deletes all historical partitions in the range that are absent from the incoming DataFrame.
* **Fix**:
  1. Re-partition Bronze physically by **both** country and date: `PARTITIONED BY (probe_cc, measurement_date)`.
  2. Scope the partition replacement predicate strictly to both dimensions:
     ```python
     df_valid_staged.write.format("delta").mode("overwrite") \
         .option("replaceWhere", f"probe_cc = '{country_code}' AND measurement_date = '{target_date}'") \
         .save(bronze_path)
     ```
  3. Alternatively, use keyed Delta Lake `MERGE INTO` matching on composite natural keys (`report_id`, `input`, `measurement_start_time`) with ANSI NULL-safe `<=>` comparison.

### Failure Mode 9: Databricks Serverless `CANNOT_DETERMINE_TYPE` & `DBFS_DISABLED`
* **Symptoms**: Initializing empty mock DataFrames during tests or audit initialization crashes with `[CANNOT_DETERMINE_TYPE] Some schema types cannot be determined from data: []`; or writing to `/tmp` / `/FileStore` fails with `[DBFS_DISABLED] DBFS is disabled in Unity Catalog Serverless`.
* **Root Cause**: On Databricks Serverless Compute (Spark Connect), `spark.createDataFrame([])` cannot infer schemas from empty collections without explicit `StructType` definitions. Additionally, Serverless restricts public root DBFS paths.
* **Fix**:
  1. Always supply explicit `StructType` schemas to `createDataFrame([], schema=EXPLICIT_SCHEMA)`.
  2. Centralize storage paths to Unity Catalog Volumes (`/Volumes/workspace/default/panopticon/`) rather than `/tmp` or `/dbfs/FileStore`.

### Failure Mode 10: Multi-Country Directory Staging Bleed
* **Symptoms**: Running ingestion for Pakistan (`PK`) crashes during pre-write validation: `[Country Contract Violation] Staged DataFrame contains 498 records with probe_cc != 'PK'`.
* **Root Cause**: The raw staging directory `raw/<date>/` contains JSON files for multiple countries (e.g. `ooni_ir_...json` and `ooni_pk_...json`). Calling `spark.read.json(landing_dir)` loads all JSON files in the folder, blending countries into a single batch.
* **Fix**:
  * Scope the Spark read path with an explicit country file glob:
    ```python
    target_read_path = f"{landing_dir}/ooni_{clean_cc}_*.json"
    df_raw = spark.read.schema(target_schema).json(target_read_path)
    ```

### Failure Mode 11: Ingestion Manifest Validation Bypass on Empty Tables
* **Symptoms**: First-time ingestion batches bypass manifest checksum and date scope validations, potentially landing corrupt files on day zero.
* **Root Cause**: Manifest validation logic placed inside `if table_exists:` blocks was skipped when `bronze_ooni_raw` had not yet been created.
* **Fix**:
  * Hoist manifest existence, hash verification, and date scope validation to execute **before** the table existence check.

### Failure Mode 12: `DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE` (Silver Grain Conflict)
* **Symptoms**: Silver Delta MERGE crashes with `DeltaUnsupportedOperationException: Cannot perform Merge as multiple source rows matched and attempted to modify the same target row in the Delta table in possibly conflicting ways`.
* **Root Cause**: Exploding multi-valued arrays (`queries`, `tcp_connect`) directly inside the DataFrame destined for `silver_network_measurements` produces multiple rows sharing the same `measurement_id`, violating Delta Lake's target uniqueness invariant during `MERGE INTO`.
* **Fix**:
  * Enforce a **strictly 1:1 measurement event grain** on `silver_network_measurements`. Never explode arrays into the parent conformed table.
  * Normalize arrays into dedicated child tables (`silver_dns_experiments`, `silver_tcp_experiments`) via direct 1-pass `explode_outer()` projections.

### Failure Mode 13: Polymorphic HTTP Response Body Deserialization Corruption (SILVER-11)
* **Symptoms**: Spark's `from_json()` returns all `NULL` for the HTTP response struct, nullifying network measurements on confirmed blockpages.
* **Root Cause**: Per OONI `df-001-httpt`, binary or compressed HTTP response bodies are serialized as JSON objects `{"data": "<base64>", "format": "base64"}`. Defining `body` as `StringType` inside a static `StructType` creates a polymorphic union-type conflict under Spark `from_json()`.
* **Fix**:
  * Omit raw HTTP `body` from the Silver schema entirely. Only parse status codes, headers, and failure strings.

### Failure Mode 14: Corrupted Timestamp Silent Fallback & Temporal Partition Bleed (SILVER-37)
* **Symptoms**: Invalid or garbage probe timestamps silently pass validation and land under the execution `batch_date` partition instead of being flagged as corrupt.
* **Root Cause**: Including a fallback expression `to_date(lit(batch_date))` inside the timestamp coalesce chain masks invalid dates, bypassing pre-write null checks.
* **Fix**:
  * Remove `batch_date` from the coalesce chain. If timestamp parsing fails, `measurement_date` evaluates to `NULL` and routes to `quarantine_ooni_raw` with `rejection_reason = 'INVALID_MEASUREMENT_TIMESTAMP'`.

---

## 3. Resource & Dependency Requirements

| Component | Minimum Specification | Recommended Specification |
| :--- | :--- | :--- |
| **Operating System** | Windows 10/11, macOS, or Ubuntu 22.04 | Windows 11 with WSL2 / Native Python 3.10+ |
| **Local Memory (RAM)** | 8 GB RAM | 16 GB RAM |
| **Cloud Runtime** | Databricks Community Edition (Free) | Databricks CE or Serverless UC (`/Volumes/...`) |
| **Python Version** | Python 3.10 | Python 3.11 / 3.12 |
| **Primary Libraries** | `pyspark==3.5.0`, `delta-spark==3.0.0` | `pyspark`, `delta-spark`, `python-docx`, `requests` |
| **BI Tool** | Power BI Desktop (Free) | Power BI Desktop (Latest Version) |
