# Phase 5: Delta Lake Persistence, MERGE Optimization & Memory Tuning
**Priority**: **P1 · High**  
**Target Files**: [`notebooks/02_silver_transformation.py`](file:///d:/Semester%20Projects/Panopticon/notebooks/02_silver_transformation.py), [`notebooks/00_config.py`](file:///d:/Semester%20Projects/Panopticon/notebooks/00_config.py)  
**Downstream Impact**: Delta Lake storage engine, file layout, merge performance, and driver memory stability on Databricks Community Edition.  
**Associated Audit Findings**: `SILVER-20`, `SILVER-21`, `SILVER-39`, `SILVER-49`

```text
==============================================================================
PIPELINE DEPENDENCY & IMPACT TRACKING
==============================================================================
Upstream Dependencies:
  - Enriched, classified Silver DataFrames from Phases 1-4
  - Active lookback window [T-lookback, T] from config
Downstream Impact:
  - Idempotently merges silver_network_measurements
  - Idempotently merges child tables (DNS, TCP, App)
  - Eliminates 100% false update write amplification
  - Prunes partition scans via physical partition keys
Risk & Side Effects:
  - Omitting partition key from MERGE ON clause forces full historical table scans.
==============================================================================
```

---

## 1. Problem Diagnosis & Architectural Vulnerabilities

### Problem 1.1: `target._batch_id != source._batch_id` 100% Write Amplification (SILVER-20)
* **Root Cause**: Earlier code and documentation included `_batch_id` in the `WHEN MATCHED` trigger condition:
  ```sql
  -- DEFECTIVE MERGE CONDITION:
  WHEN MATCHED AND (target._batch_id != source._batch_id) THEN UPDATE SET *
  ```
* **Impact**:
  - Every pipeline run generates a new unique `_batch_id`.
  - When re-running the pipeline on existing data or running lookbacks, **100% of rows match as updated**!
  - Delta Lake triggers full Copy-on-Write parquet file rewrites for every unchanged partition.
  - Audit logs record 100% false update counts (`num_target_rows_updated == total_rows`), completely distorting operational metrics.
* **Remediation**: Remove `_batch_id` from the `WHEN MATCHED` predicate. Trigger updates **strictly** when substantive business attributes change (e.g., taxonomy re-classification, ground truth flag correction).

### Problem 1.2: Inconsistent Partition Pruning in MERGE INTO (SILVER-21)
* **Root Cause**: `07_execution_guide.md` omitted `target.measurement_date = source.measurement_date` from the `MERGE ON` clause, joining solely on `target.measurement_id = source.measurement_id`.
* **Impact**:
  - Without the physical partition column (`measurement_date`) in the join predicate, Delta Lake's optimizer cannot prune partition directories.
  - Spark executes a full table scan across **all historical partitions** on every incremental run. As the lakehouse grows, incremental runs become exponentially slower and crash with driver timeouts.
* **Remediation**: Always include `target.measurement_date = source.measurement_date` in the `ON` condition.

### Problem 1.3: Databricks Community Edition Memory Crash (Safeguards)
* **Runtime Constraints**: Databricks CE provides **15 GB total RAM / ~9.5 GB usable Spark heap / 2 vCPUs**.
* **Vulnerability**: Default Spark settings create 200 shuffle partitions (`spark.sql.shuffle.partitions=200`), generating thousands of tiny files and overwhelming driver memory with metadata.
* **Remediation**: Explicitly tune shuffle partitions to `4–8` and enable Delta Lake's `optimizeWrite` and `autoCompact`.

---

## 2. Target Implementation Blueprint

### Step 2.1: Delta Community Edition Session Tuning
In `notebooks/02_silver_transformation.py`:
```python
def configure_spark_for_community_edition(spark: SparkSession) -> None:
    """
    Applies strict Databricks Community Edition memory safeguards:
      - 4-8 shuffle partitions (replaces wasteful 200 default).
      - Enables automatic write optimization and bin-packing compaction.
    """
    spark.conf.set("spark.sql.shuffle.partitions", "4")
    spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
    spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
    print("[Performance Tuning] Configured shuffle.partitions=4 and optimizeWrite=True.")
```

### Step 2.2: Scoped Idempotent MERGE for `silver_network_measurements`
```python
def merge_silver_network_measurements(
    spark: SparkSession,
    df_silver: DataFrame,
    silver_path: Optional[str] = None
) -> Dict[str, int]:
    """
    Performs conditional Delta MERGE INTO with strict partition pruning:
      1. Partition Pruning: ON target.measurement_id = source.measurement_id
                            AND target.measurement_date = source.measurement_date.
      2. Zero Write Amplification: Updates trigger ONLY when business taxonomy
         or anomaly classifications change (using null-safe <=>).
      3. Idempotent: Re-running identical data results in 0 updates, 0 inserts.
    """
    from delta.tables import DeltaTable

    target_path = silver_path or config.PATHS["silver"]
    
    if not DeltaTable.isDeltaTable(spark, target_path):
        print(f"[Silver Persistence] Initializing table at {target_path}...")
        df_silver.write.format("delta") \
            .partitionBy("measurement_date") \
            .mode("overwrite") \
            .save(target_path)
        count = df_silver.count()
        return {"num_target_rows_inserted": count, "num_target_rows_updated": 0}

    delta_target = DeltaTable.forPath(spark, target_path)

    # Business change condition using null-safe equality (<=>)
    business_update_condition = """
        (target.classification_status <=> source.classification_status = FALSE) OR
        (target.tampering_vector <=> source.tampering_vector = FALSE) OR
        (target.content_category <=> source.content_category = FALSE) OR
        (target.taxonomy_version <=> source.taxonomy_version = FALSE) OR
        (target.ooni_anomaly <=> source.ooni_anomaly = FALSE) OR
        (target.ooni_confirmed <=> source.ooni_confirmed = FALSE)
    """

    merge_builder = delta_target.alias("target").merge(
        source=df_silver.alias("source"),
        condition="target.measurement_id = source.measurement_id AND target.measurement_date = source.measurement_date"
    ).whenMatchedUpdate(
        condition=business_update_condition,
        set={
            "classification_status": "source.classification_status",
            "tampering_vector": "source.tampering_vector",
            "is_anomaly": "source.is_anomaly",
            "ooni_anomaly": "source.ooni_anomaly",
            "ooni_confirmed": "source.ooni_confirmed",
            "dns_anomaly_flag": "source.dns_anomaly_flag",
            "tcp_anomaly_flag": "source.tcp_anomaly_flag",
            "tls_anomaly_flag": "source.tls_anomaly_flag",
            "http_anomaly_flag": "source.http_anomaly_flag",
            "content_category": "source.content_category",
            "category_description": "source.category_description",
            "category_match_type": "source.category_match_type",
            "taxonomy_version": "source.taxonomy_version",
            "human_rights_risk_tier": "source.human_rights_risk_tier",
            "load_timestamp": "current_timestamp()"
        }
    ).whenNotMatchedInsertAll()

    merge_builder.execute()

    # Capture execution metrics from Delta history
    history = delta_target.history(1).collect()[0]
    metrics = history["operationMetrics"]
    return {
        "num_target_rows_inserted": int(metrics.get("numTargetRowsInserted", 0)),
        "num_target_rows_updated": int(metrics.get("numTargetRowsUpdated", 0)),
    }
```

### Step 2.3: Idempotent Child Table MERGE Implementation
Apply identical partition-pruned merges to child tables:
```python
def merge_child_delta_table(
    spark: SparkSession,
    df_child: DataFrame,
    child_path: str,
    key_column: str,
    partition_column: str = "measurement_date"
) -> Dict[str, int]:
    """
    Standardized, idempotent merger for child diagnostic tables
    (silver_dns_experiments, silver_tcp_experiments, silver_app_anomaly_measurements).
    """
    from delta.tables import DeltaTable

    if not DeltaTable.isDeltaTable(spark, child_path):
        df_child.write.format("delta") \
            .partitionBy(partition_column) \
            .mode("overwrite") \
            .save(child_path)
        return {"num_target_rows_inserted": df_child.count(), "num_target_rows_updated": 0}

    delta_target = DeltaTable.forPath(spark, child_path)

    merge_cond = f"target.{key_column} = source.{key_column} AND target.{partition_column} = source.{partition_column}"

    delta_target.alias("target").merge(
        source=df_child.alias("source"),
        condition=merge_cond
    ).whenNotMatchedInsertAll().execute()

    history = delta_target.history(1).collect()[0]
    metrics = history["operationMetrics"]
    return {
        "num_target_rows_inserted": int(metrics.get("numTargetRowsInserted", 0)),
        "num_target_rows_updated": int(metrics.get("numTargetRowsUpdated", 0)),
    }
```

---

## 3. Verification Protocol & Acceptance Criteria

1. **Partition Pruning Verification**: Inspect the Spark physical plan via `explain()`; confirm `PartitionFilters: [isnotnull(measurement_date), ...]`.
2. **Zero Write Amplification Test**: Run the Silver merge twice on the exact same dataset:
   - Run 1: `num_target_rows_inserted == N`, `num_target_rows_updated == 0`.
   - Run 2: `num_target_rows_inserted == 0`, `num_target_rows_updated == 0`.
3. **Business Update Trigger Test**: Update the `category_code` of an existing URL in `ref_citizenlab_categories` and re-run Silver; verify that **only the affected measurement rows** are updated (`num_target_rows_updated > 0`).
4. **Memory Stability**: Verify memory consumption stays under 60% of the Spark driver heap during execution.
