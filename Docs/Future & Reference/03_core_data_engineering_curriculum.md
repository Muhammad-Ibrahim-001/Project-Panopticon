# Core Data Engineering Curriculum & Essential Features
## The 8 Industry Superpowers Mastered in Project Panopticon

---

### Purpose
To stand out in technical interviews and achieve top marks, a data engineering project cannot simply be "running SQL queries on CSVs." It must demonstrate mastery of **distributed computing, data lakehouse internals, streaming, and governance**. 

This document outlines the **8 core engineering features** you will build, accompanied by production PySpark code patterns.

---

## 1. Semi-Structured JSON Parsing & Polymorphic Schema Enforcement
* **The Problem**: Raw OONI network logs are deeply nested JSONL files. Different tests (`web_connectivity`, `whatsapp`, `dnscheck`) have different structures inside `test_keys`. Naive schema inference (`inferSchema=True`) will trigger a full file scan, exhaust cluster memory, or produce corrupted columns.
* **The Core DE Feature**: Two-tier schema enforcement: storing polymorphic `test_keys` as raw `StringType` at Bronze, and applying test-specific `StructType` schemas via `from_json()` at Silver without risking union-type crashes or driver OOM.
* **PySpark Code Pattern**:
```python
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, ArrayType, DoubleType, LongType
from pyspark.sql.functions import col, from_json

# 1. Bronze: Strict Schema-on-Read stores polymorphic test_keys as raw StringType
base_schema = StructType([
    StructField("measurement_uid", StringType(), True),  # Global UUID from API index (SILVER-04)
    StructField("report_id", StringType(), False),
    StructField("input", StringType(), True),
    StructField("test_name", StringType(), False),
    StructField("measurement_start_time", StringType(), False),
    StructField("probe_asn", StringType(), False),
    StructField("probe_cc", StringType(), False),
    StructField("resolver_ip", StringType(), True),
    StructField("test_runtime", DoubleType(), True),
    StructField("extensions", StringType(), True),        # Preserved protocol extensions (SILVER-05)
    StructField("api_anomaly", BooleanType(), True),      # Upstream ground truth (SILVER-41)
    StructField("api_confirmed", BooleanType(), True),
    StructField("test_keys", StringType(), True),         # Raw JSON string prevents union-type crashes
    StructField("_corrupt_record", StringType(), True)
])

# Read Bronze without inferSchema overhead
df_bronze = spark.read.schema(base_schema).option("mode", "PERMISSIVE").json("s3a://ooni-data-eu-fra/raw/20260920/*/*/*/*.jsonl.gz")

# 2. Silver: Test-specific schema applied via from_json() per test_name
# Raw response body is omitted to avoid union-type string|object crashes (SILVER-11)
web_test_keys_schema = StructType([
    StructField("blocking", StringType(), True),
    StructField("accessible", BooleanType(), True),
    StructField("dns_experiment_failure", StringType(), True),
    StructField("http_experiment_failure", StringType(), True),
    StructField("tcp_connect", ArrayType(
        StructType([
            StructField("ip", StringType(), True),
            StructField("port", LongType(), True),
            StructField("status", StructType([
                StructField("blocked", BooleanType(), True),  # OONI confirmed blocking signal (SILVER-09)
                StructField("failure", StringType(), True),
                StructField("success", BooleanType(), True)
            ]), True)
        ])
    ), True)
])

df_parsed = df_bronze.filter(col("test_name") == "web_connectivity").withColumn(
    "parsed_keys", from_json(col("test_keys"), web_test_keys_schema)
)
```

---

## 2. Normalized Protocol Child Table Projection (Cartesian Guard)
* **The Problem**: Chaining multiple `explode()` operations on a single DataFrame (`queries` $\times$ `tcp_connect` $\times$ `network_events`) creates an intra-row Cartesian cross-product ($36 \times 34 \times 379 = 463,896$ rows per record), ballooning 50K records into 60M+ rows and triggering driver heap exhaustion on Databricks Community Edition (SILVER-13). Furthermore, standard `explode()` silently deletes 100% of DNS-censored records because downstream arrays are empty (SILVER-14).
* **The Core DE Feature**: Projecting arrays into isolated child tables using `explode_outer()` while preserving a strict 1:1 grain on the primary measurement table.
* **PySpark Code Pattern**:
```python
from pyspark.sql.functions import col, posexplode_outer, sha2, concat_ws, lit

# Isolated child table projection: deterministic keying using array position (SILVER-12, SILVER-13, SILVER-52)
# posexplode_outer() preserves array order and generates deterministic keys across task re-execution
df_dns_experiments = df_parsed.select(
    col("measurement_id"),
    col("measurement_date"),
    posexplode_outer(col("parsed_keys.queries")).alias("query_pos", "query")
).filter(col("query").isNotNull()).select(
    sha2(concat_ws("||", col("measurement_id"), col("query_pos").cast("string")), 256).alias("dns_experiment_id"),
    col("measurement_id"),
    col("measurement_date"),
    col("query.query_type"),
    col("query.hostname").alias("query_hostname"),
    col("query.failure").alias("dns_failure"),
    col("query.answers")
)
```

---

## 3. Late Arrivals, Re-Enrichment & Zero-Write-Amplification Delta `MERGE INTO`
* **The Problem**: Raw telemetry observations are immutable event logs—upstream OONI never alters a historical measurement. However:
  1. *Late Arrivals*: Probes in censored regimes frequently store test results offline and sync days later.
  2. *Dimensional Re-Enrichment*: As Citizen Lab updates domain categories or tampering classification heuristics improve, enriched columns in the Silver layer must be refreshed without re-ingesting raw files.
  3. *Zero Write Amplification (SILVER-20)*: Including `_batch_id` in the `WHEN MATCHED` trigger causes 100% false updates on every run, forcing full Copy-on-Write partition rewrites.
* **The Core DE Feature**: Idempotent upserting using Delta Lake's ACID `MERGE INTO` with partition pruning and NULL-safe `<=>` change detection.
* **PySpark Code Pattern**:
```python
from delta.tables import DeltaTable

silver_delta = DeltaTable.forPath(spark, "/mnt/lakehouse/silver/network_measurements")

# Upsert rolling 3-day window into Silver layer with partition pruning
silver_delta.alias("target").merge(
    source=df_staged_lookback_batch.alias("source"),
    condition="target.measurement_id = source.measurement_id AND target.measurement_date = source.measurement_date"
).whenMatchedUpdate(
    # NULL-safe equality operator <=> detects actual business content changes.
    # Excludes _batch_id to prevent 100% false update write amplification (SILVER-20).
    condition="""
        NOT (target.content_category <=> source.content_category)
        OR NOT (target.tampering_vector <=> source.tampering_vector)
        OR NOT (target.is_anomaly <=> source.is_anomaly)
    """,
    set={
        "content_category": "source.content_category",
        "category_description": "source.category_description",
        "human_rights_risk_tier": "source.human_rights_risk_tier",
        "tampering_vector": "source.tampering_vector",
        "is_anomaly": "source.is_anomaly",
        "load_timestamp": "current_timestamp()",
        "_batch_id": "source._batch_id"
    }
).whenNotMatchedInsertAll().execute()
```

---

## 4. Cryptographic PII Governance & Data Minimization
* **The Problem**: Public OONI archives redact `probe_ip` to `127.0.0.1` (SILVER-02), but expose sensitive network metadata (`resolver_ip`, persistent `probe_id` hashes, and raw HTTP request headers inside nested `test_keys`) that could endanger volunteer activists under hostile regimes (SILVER-43).
* **The Core DE Feature**: Enforcing one-way salted cryptographic hashing (`HMAC-SHA256`), dropping persistent installation IDs, and purging raw `test_keys` JSON strings from Silver for GDPR data minimization.
* **PySpark Code Pattern**:
```python
from pyspark.sql.functions import sha2, concat, lit, regexp_replace

PEPPER_SALT = dbutils.secrets.get(scope="panopticon_vault", key="ip_salt")

df_silver = df_bronze.withColumn(
    # One-way salted cryptographic hash of public ISP resolver IP
    "hashed_resolver_ip", sha2(concat(col("resolver_ip"), lit(PEPPER_SALT)), 256)
).withColumn(
    # Strip sensitive authentication tokens and session keys from tested URLs
    "target_url", regexp_replace(col("input"), "([?&](session_id|token|auth)=)[^&]+", "$1REDACTED")
).drop("resolver_ip", "probe_ip", "probe_id", "test_keys")  # Drop PII & raw payloads (SILVER-43)
```

---

## 5. Broadcast Joins for Multi-Source Context Enrichment
* **The Problem**: Joining tens of thousands of forensic telemetry records with the Citizen Lab URL taxonomy across a distributed Spark cluster causes expensive shuffle exchanges across network sockets if executed via default sort-merge join.
* **The Core DE Feature**: Utilizing Spark's `broadcast()` hint to distribute the small dimension table (~2 MB) to all executors, eliminating data shuffling entirely.
* **PySpark Code Pattern**:
```python
from pyspark.sql.functions import broadcast

# Broadcast join: O(1) memory transfer, 0 shuffle partitions created
df_enriched = df_silver.join(
    broadcast(df_citizenlab_categories),
    on=df_silver.target_domain == df_citizenlab_categories.domain,
    how="left"
).fillna({"category_code": "UNCATEGORIZED"})
```

---

## 6. Time-Series Anomaly Detection with Spark Window Functions
* **The Problem**: Distinguishing between normal network packet jitter and deliberate censorship requires evaluating rolling baseline failure rates over time.
* **The Core DE Feature**: Implementing distributed Window functions to compute rolling failure rates and lag differences.
* **PySpark Code Pattern**:
```python
from pyspark.sql.window import Window
from pyspark.sql.functions import avg, stddev, lag, col

# Partition by ISP and order by hourly timestamp
window_spec = Window.partitionBy("probe_asn").orderBy("measurement_hour").rowsBetween(-24, 0)

df_anomaly = (
    df_silver
    .withColumn("rolling_avg_failure", avg("failure_flag").over(window_spec))
    .withColumn("rolling_stddev", stddev("failure_flag").over(window_spec))
    .withColumn("z_score", (col("failure_flag") - col("rolling_avg_failure")) / col("rolling_stddev"))
)
```

---

## 7. Lakehouse Storage Optimization & Compaction
* **The Problem**: Continuous streaming and daily incremental loads create thousands of small Parquet files (the "Small File Problem"), degrading query performance in Power BI.
* **The Core DE Feature**: Automated file compaction, vacuuming, and multi-dimensional clustering (`Z-ORDER`).
* **Delta SQL Commands**:
```sql
-- Compact small files into optimal 128MB-1GB sizes and cluster by query keys
OPTIMIZE silver_network_measurements
ZORDER BY (probe_cc, measurement_date, probe_asn);

-- Clean up tombstoned files older than 7 days to manage storage limits
VACUUM silver_network_measurements RETAIN 168 HOURS;
```

---

## 8. Data Quality Testing & Quarantine Logic
* **The Problem**: Malformed JSON records or network connection drops produce corrupted timestamps or impossible coordinates.
* **The Core DE Feature**: Enforcing quality constraints and segregating bad data into a quarantine table rather than aborting the pipeline.
* **PySpark Code Pattern**:
```python
# Validation conditions
is_valid_date = col("measurement_start_time").isNotNull() & (col("measurement_start_time") <= current_timestamp())
is_valid_asn = col("probe_asn").rlike("^AS[0-9]+$")

# Valid records progress to Silver
df_clean = df_silver.filter(is_valid_date & is_valid_asn)

# Corrupted records route to Quarantine for debugging
df_quarantine = df_silver.filter(~(is_valid_date & is_valid_asn))
df_quarantine.write.format("delta").mode("append").save("/mnt/lakehouse/quarantine/bad_records")
```
