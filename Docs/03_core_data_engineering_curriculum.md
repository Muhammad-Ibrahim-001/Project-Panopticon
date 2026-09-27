# Core Data Engineering Curriculum & Essential Features
## The 8 Industry Superpowers Mastered in Project Panopticon

---

### Purpose
To stand out in technical interviews and achieve top marks, a data engineering project cannot simply be "running SQL queries on CSVs." It must demonstrate mastery of **distributed computing, data lakehouse internals, streaming, and governance**. 

This document outlines the **8 core engineering features** you will build, accompanied by production PySpark code patterns.

---

## 1. Semi-Structured JSON Parsing & Polymorphic Schema Enforcement
* **The Problem**: Raw OONI network logs are deeply nested JSONL files. Different tests (`web_connectivity`, `whatsapp`, `dnscheck`) have different structures inside `test_keys`. Naive schema inference (`inferSchema=True`) will trigger a full file scan, exhaust cluster memory, or produce corrupted columns.
* **The Core DE Feature**: Explicit `StructType` schema definition and safe parsing.
* **PySpark Code Pattern**:
```python
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, ArrayType, DoubleType, LongType

test_keys_schema = StructType([
    StructField("blocking", StringType(), True),
    StructField("dns_experiment_failure", StringType(), True),
    StructField("http_experiment_failure", StringType(), True),
    StructField("control_failure", StringType(), True),
    StructField("queries", ArrayType(
        StructType([
            StructField("query_type", StringType(), True),
            StructField("failure", StringType(), True),
            StructField("answers", ArrayType(
                StructType([
                    StructField("answer_type", StringType(), True),
                    StructField("ipv4", StringType(), True)
                ])
            ), True)
        ])
    ), True)
])

# Read without inferSchema overhead
df_bronze = spark.read.schema(base_schema).json("s3a://ooni-data-eu-fra/raw/20260920/*/*/*/*.jsonl.gz")
```

---

## 2. Array Exploding & Relational Flattening
* **The Problem**: A single measurement contains multiple DNS queries and TCP handshake attempts stored as nested JSON arrays. Analysts cannot run SQL `GROUP BY` or calculate latency across arrays.
* **The Core DE Feature**: Normalizing multi-valued nested structures into atomic relational records using `explode()`.
* **PySpark Code Pattern**:
```python
from pyspark.sql.functions import col, explode

# Explode nested DNS queries to measure individual resolver performance
df_exploded_dns = df_bronze.select(
    col("measurement_uid"),
    col("probe_asn"),
    col("input").alias("target_url"),
    explode(col("test_keys.queries")).alias("dns_query")
).select(
    col("measurement_uid"),
    col("probe_asn"),
    col("target_url"),
    col("dns_query.query_type"),
    col("dns_query.failure").alias("dns_failure")
)
```

---

## 3. Change Data Capture (CDC) via Delta Lake `MERGE INTO` (SCD Type 2)
* **The Problem**: Network blocks are not static. A website blocked on Monday may be unblocked on Tuesday or experience DNS tampering followed by full TCP RST injection. Appending data naively creates duplicate rows; overwriting historical tables destroys longitudinal evidence.
* **The Core DE Feature**: Idempotent upserting using Delta Lake's ACID `MERGE INTO`.
* **PySpark Code Pattern**:
```python
from delta.tables import DeltaTable

silver_delta = DeltaTable.forPath(spark, "/mnt/lakehouse/silver/network_incidents")

# Upsert incremental batch into Silver layer
silver_delta.alias("target").merge(
    source=df_incremental_batch.alias("source"),
    condition="target.measurement_uid = source.measurement_uid"
).whenMatchedUpdate(
    set={
        "verification_status": "source.verification_status",
        "tampering_vector": "source.tampering_vector",
        "updated_at": "current_timestamp()"
    }
).whenNotMatchedInsertAll(
).execute()
```

---

## 4. Cryptographic PII Governance & Data Pseudonymization
* **The Problem**: Probe telemetry exposes sensitive network metadata (`resolver_ip`, `probe_asn`, and URL session tokens) that could endanger volunteer activists under hostile regimes.
* **The Core DE Feature**: Enforcing one-way cryptographic hashing (`HMAC-SHA256`) and subnet truncation before writing to Silver.
* **PySpark Code Pattern**:
```python
from pyspark.sql.functions import sha2, concat, lit, regexp_replace, split

PEPPER_SALT = dbutils.secrets.get(scope="panopticon_vault", key="ip_salt")

df_silver = df_bronze.withColumn(
    # One-way salted cryptographic hash of resolver IP
    "anonymized_resolver_id", sha2(concat(col("resolver_ip"), lit(PEPPER_SALT)), 256)
).withColumn(
    # Truncate IPv4 to /24 subnet for regional analytics without user tracking
    "resolver_subnet", concat(split(col("resolver_ip"), "\.")[0], lit("."),
                              split(col("resolver_ip"), "\.")[1], lit("."),
                              split(col("resolver_ip"), "\.")[2], lit(".0/24"))
).withColumn(
    # Strip sensitive authentication tokens from tested URLs
    "sanitized_url", regexp_replace(col("input"), "([?&](session_id|token|auth)=)[^&]+", "$1REDACTED")
).drop("resolver_ip", "probe_ip")
```

---

## 5. Broadcast Joins for Multi-Source Context Enrichment
* **The Problem**: Joining 15 million telemetry records with the Citizen Lab URL taxonomy across a distributed Spark cluster causes expensive shuffle exchanges across network sockets.
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
from pyspark.sql.functions import avg, stddev, lag

# Partition by ISP and order by hourly timestamp
window_spec = Window.partitionBy("probe_asn").orderBy("measurement_hour").rowsBetween(-24, 0)

df_anomaly = df_silver.withColumn("rolling_avg_failure", avg("failure_flag").over(window_spec))                       .withColumn("rolling_stddev", stddev("failure_flag").over(window_spec))                       .withColumn("z_score", (col("failure_flag") - col("rolling_avg_failure")) / col("rolling_stddev"))
```

---

## 7. Lakehouse Storage Optimization & Compaction
* **The Problem**: Continuous streaming and daily incremental loads create thousands of small Parquet files (the "Small File Problem"), degrading query performance in Power BI.
* **The Core DE Feature**: Automated file compaction, vacuuming, and multi-dimensional clustering (`Z-ORDER`).
* **Delta SQL Commands**:
```sql
-- Compact small files into optimal 128MB-1GB sizes and cluster by query keys
OPTIMIZE silver_network_incidents
ZORDER BY (country_code, measurement_date, probe_asn);

-- Clean up tombstoned files older than 7 days to manage storage limits
VACUUM silver_network_incidents RETAIN 168 HOURS;
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
