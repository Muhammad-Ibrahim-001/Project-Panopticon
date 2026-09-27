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
   spark.sql.shuffle.partitions 16
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

### Failure Mode 1: `java.lang.OutOfMemoryError: Java heap space`
* **Symptoms**: Cluster driver terminates abruptly; logs show executor heap exhaustion during JSON read or wide transformations.
* **Root Cause**: Attempting to run `spark.read.json()` directly on 10 GB of uncompressed data with default `inferSchema=True`.
* **Fix**:
  1. Define an explicit `StructType` schema.
  2. Ingest via micro-batches:
     ```python
     df = spark.readStream.format("json").schema(explicit_schema).option("maxBytesPerTrigger", "512mb").load("...")
     ```
  3. Ensure `spark.sql.shuffle.partitions` is set to `16` (not 200).

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
    OPTIMIZE fact_network_anomaly ZORDER BY (country_code, date_key, asn_id);
    ```

---

## 3. Resource & Dependency Requirements

| Component | Minimum Specification | Recommended Specification |
| :--- | :--- | :--- |
| **Operating System** | Windows 10/11, macOS, or Ubuntu 22.04 | Windows 11 with WSL2 / Native Python 3.10+ |
| **Local Memory (RAM)** | 8 GB RAM | 16 GB RAM |
| **Cloud Runtime** | Databricks Community Edition (Free) | Databricks CE or Azure Databricks (Standard_D4ds_v5) |
| **Python Version** | Python 3.10 | Python 3.11 / 3.12 |
| **Primary Libraries** | `pyspark==3.5.0`, `delta-spark==3.0.0` | `pyspark`, `delta-spark`, `python-docx`, `requests` |
| **BI Tool** | Power BI Desktop (Free) | Power BI Desktop (Latest Version) |
