# Phase 0: Upstream Bronze Contract Hardening & Ground Truth Integrity
**Priority**: **P0 · Critical Blocker**  
**Task Completion Status**:
- [x] **Problem 1.1**: Missing `api_anomaly` & `api_confirmed` Injection in `_resolve_full_payload` (SILVER-45) — **COMPLETED**
- [x] **Problem 1.2**: `CITIZENLAB_REFERENCE_SCHEMA` Missing `taxonomy_version` (SILVER-46) — **COMPLETED**
- [x] **Problem 1.3**: 24-Hour Fetch vs 3-Day Lookback Disconnect (SILVER-19) — **COMPLETED**
- [x] **Problem 1.4**: Ingestion Manifest Completeness Over-Assertion (SILVER-36, SILVER-50) — **COMPLETED**
- [x] **Problem 1.5**: Masking Invalid Timestamps with `to_date(lit(batch_date))` (SILVER-37, SILVER-51) — **COMPLETED**
- [x] **Problem 1.6**: Bronze MERGE Violating Raw Immutability (SILVER-49) — **COMPLETED**

**Target Files**: [`notebooks/01_bronze_ingestion.py`](file:///d:/Semester%20Projects/Panopticon/notebooks/01_bronze_ingestion.py), [`notebooks/00_config.py`](file:///d:/Semester%20Projects/Panopticon/notebooks/00_config.py)  
**Downstream Impact**: [`bronze_ooni_raw`](file:///d:/Semester%20Projects/Panopticon/data/schemas/bronze_ooni_schema.json), [`ref_citizenlab_categories`](file:///d:/Semester%20Projects/Panopticon/data/schemas/ref_citizenlab_categories_schema.json), [`notebooks/02_silver_transformation.py`](file:///d:/Semester%20Projects/Panopticon/notebooks/02_silver_transformation.py)  
**Associated Audit Findings**: `SILVER-05`, `SILVER-19`, `SILVER-36`/`SILVER-50`, `SILVER-37`/`SILVER-51`, `SILVER-45`, `SILVER-46`, `SILVER-49`

```text
==============================================================================
PIPELINE DEPENDENCY & IMPACT TRACKING
==============================================================================
Upstream Dependencies:
  - OONI Measurement API (/api/v1/measurements) & Stage 2 measurement_url
  - Citizen Lab GitHub Raw Lists
Downstream Impact:
  - Populates 'api_anomaly' & 'api_confirmed' on bronze_ooni_raw
  - Attaches 'taxonomy_version' to ref_citizenlab_categories
  - Routes corrupt timestamps to quarantine instead of masking with batch_date
  - Converts Bronze incremental merge to append-only (whenNotMatchedInsertAll)
Risk & Side Effects:
  - Requires clean re-run of Bronze ingestion to populate new columns
    prior to running Silver transformation.
==============================================================================
```

---

## 1. Problem Diagnosis & Architectural Vulnerabilities

### Problem 1.1: Missing `api_anomaly` & `api_confirmed` Injection in `_resolve_full_payload` (SILVER-45)
* **Root Cause**: In [`notebooks/01_bronze_ingestion.py:L673-691`](file:///d:/Semester%20Projects/Panopticon/notebooks/01_bronze_ingestion.py#L673-L691), `_resolve_full_payload()` fetches Stage 2 raw JSON from `measurement_url`. Stage 2 files **do not contain** index-level flags. The code only copies `measurement_uid` from the Stage 1 `item` dict, discarding `anomaly` and `confirmed`:
  ```python
  # CURRENT DEFECTIVE CODE:
  if "measurement_uid" not in payload and item.get("measurement_uid"):
      payload["measurement_uid"] = item.get("measurement_uid")
  ```
* **Downstream Silver Impact**: Both columns evaluate to `NULL` on 100% of rows in `bronze_ooni_raw`. In Silver, SQL 3-valued logic evaluates `NULL == False` as `UNKNOWN`, causing **100% of benign measurements to misclassify as `UNCLASSIFIED`**.

### Problem 1.2: `CITIZENLAB_REFERENCE_SCHEMA` Missing `taxonomy_version` (SILVER-46)
* **Root Cause**: [`notebooks/01_bronze_ingestion.py:L424-431`](file:///d:/Semester%20Projects/Panopticon/notebooks/01_bronze_ingestion.py#L424-L431) defines a 6-field schema omitting `taxonomy_version`.
* **Downstream Silver Impact**: The Silver two-tier enrichment query executes `.select(col("taxonomy_version"))`. Spark immediately crashes with:
  `AnalysisException: [UNRESOLVED_COLUMN] Cannot resolve column name 'taxonomy_version'`.

### Problem 1.3: 24-Hour Fetch vs 3-Day Lookback Disconnect (SILVER-19)
* **Root Cause**: In [`notebooks/01_bronze_ingestion.py:L565-569`](file:///d:/Semester%20Projects/Panopticon/notebooks/01_bronze_ingestion.py#L565-L569), `fetch_raw_ooni_telemetry()` hardcodes a single 24-hour window (`since = batch_date`, `until = batch_date + 1 day`). The parameter `lookback_days` is resolved in `resolve_ingestion_parameters()` but **never passed** to the API fetcher.
* **Downstream Silver Impact**: Late-arriving measurements ($T-2$ uploaded on $T$) are never fetched into Bronze. When Silver scans $[T-3, T]$, it re-reads static Bronze rows without finding any late-arriving data.

### Problem 1.4: Ingestion Manifest Completeness Over-Assertion (SILVER-36, SILVER-50)
* **Root Cause**: In `fetch_raw_ooni_telemetry()`, when the loop hits `len(index_items) >= max_records`, it breaks and sets `api_success = True` and `is_complete = True`.
* **Downstream Impact**: A truncated batch (e.g. 5,000 records fetched out of 40,000 available) is marked `is_complete = True`. If partition overwrite is triggered downstream, the complete partition is replaced with a partial slice, causing silent permanent data loss.

### Problem 1.5: Masking Invalid Timestamps with `to_date(lit(batch_date))` (SILVER-37, SILVER-51)
* **Root Cause**: In `stage_bronze_dataframe()`, if `measurement_start_time` is corrupt or unparseable, the code falls back to `coalesce(to_date(col("measurement_start_time")), to_date(lit(batch_date)))`.
* **Downstream Impact**: Malformed records with garbage timestamps enter `bronze_ooni_raw` with fabricated partition dates instead of routing to `quarantine_ooni_raw`.

### Problem 1.6: Bronze MERGE Violating Raw Immutability (SILVER-49)
* **Root Cause**: Bronze incremental MERGE contains `WHEN MATCHED AND (target._batch_id != source._batch_id) THEN UPDATE SET *`.
* **Downstream Impact**: Because every run generates a new `_batch_id`, 100% of rows match as updated, rewriting unchanged historical parquet files and violating Medallion Architecture raw append-only immutability. Switch to `whenNotMatchedInsertAll`.

---

## 2. Target Implementation Blueprint

### Step 2.1: Fix Payload Resolution & Sample Fallback in `01_bronze_ingestion.py`
Update `_resolve_full_payload()` to inject Stage 1 flags:
```python
def _resolve_full_payload(item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    raw_url = item.get("measurement_url")
    if not raw_url:
        return None
    try:
        r = http_session.get(raw_url, timeout=config.API_REQUEST_TIMEOUT_SECONDS)
        if r.status_code == 200:
            payload = r.json()
            # Inject Stage 1 metadata into payload
            if "measurement_uid" not in payload and item.get("measurement_uid"):
                payload["measurement_uid"] = item.get("measurement_uid")
            if "api_anomaly" not in payload:
                payload["api_anomaly"] = item.get("anomaly")
            if "api_confirmed" not in payload:
                payload["api_confirmed"] = item.get("confirmed")
            # Stringify polymorphic dictionary fields
            for field_name in ("test_keys", "annotations", "test_helpers", "extensions"):
                if field_name in payload and isinstance(payload[field_name], (dict, list)):
                    payload[field_name] = json.dumps(payload[field_name])
            return payload
    except Exception:
        pass
    return None
```
Also in the local sample fallback loop (`01_bronze_ingestion.py`), inject boolean defaults:
```python
for rec in raw_sample:
    rec.setdefault("api_anomaly", False)
    rec.setdefault("api_confirmed", False)
```

### Step 2.2: Add `taxonomy_version` to `CITIZENLAB_REFERENCE_SCHEMA`
Update `CITIZENLAB_REFERENCE_SCHEMA` in `notebooks/01_bronze_ingestion.py`:
```python
CITIZENLAB_REFERENCE_SCHEMA = StructType([
    StructField("url", StringType(), False),
    StructField("category_code", StringType(), False),
    StructField("category_description", StringType(), True),
    StructField("date_added", StringType(), True),
    StructField("source", StringType(), True),
    StructField("notes", StringType(), True),
    StructField("taxonomy_version", StringType(), False),
    StructField("ingestion_timestamp", TimestampType(), False),
])
```
In `ingest_citizenlab_taxonomy()`, capture the Git commit SHA or default to `v2026.1`:
```python
taxonomy_version_str = "commit-841804b"  # Pinned Citizen Lab commit SHA
df_clean = df_raw.withColumn("taxonomy_version", lit(taxonomy_version_str)) \
                 .withColumn("ingestion_timestamp", current_timestamp())
```

### Step 2.3: Parameterize Multi-Day Rolling Lookback in `fetch_raw_ooni_telemetry`
Accept `lookback_days: int = 0` and expand date range:
```python
dt_target = datetime.strptime(batch_date, "%Y-%m-%d")
dt_start = dt_target - timedelta(days=lookback_days)
dt_end = dt_target + timedelta(days=1)
since_str = dt_start.strftime("%Y-%m-%d")
until_str = dt_end.strftime("%Y-%m-%d")
```

### Step 2.4: Tighten Ingestion Manifest State
Distinguish between fully complete vs capped batches:
```python
if is_sample_fallback:
    manifest_status = "PARTIAL_FAILURE"
    is_complete = False
elif genuinely_empty:
    manifest_status = "GENUINELY_EMPTY"
    is_complete = True
elif len(index_items) >= max_records:
    manifest_status = "SUCCESS_CAPPED"
    is_complete = False  # DO NOT permit destructive replaceWhere on capped batches
elif api_success:
    manifest_status = "SUCCESS_COMPLETE"
    is_complete = True
```

### Step 2.5: Route Corrupt Timestamps to Quarantine
In `stage_bronze_dataframe()`:
```python
# Unparseable timestamps evaluate to null
df_staged = df_raw.withColumn(
    "measurement_date",
    to_date(to_timestamp(col("measurement_start_time"), "yyyy-MM-dd HH:mm:ss"))
)
# Quarantine records where measurement_date is null
df_valid = df_staged.filter(col("measurement_date").isNotNull() & col("report_id").isNotNull())
df_quarantine = df_staged.filter(col("measurement_date").isNull() | col("report_id").isNull())
```

### Step 2.6: Convert Bronze Incremental Merge to Insert-Only
In `write_bronze_delta_table()`:
```python
delta_target.alias("target").merge(
    source=df_bronze.alias("source"),
    condition="""
        target.probe_cc = source.probe_cc AND
        target.measurement_date = source.measurement_date AND
        target.report_id = source.report_id AND
        target.measurement_start_time = source.measurement_start_time AND
        target.test_name = source.test_name AND
        coalesce(target.input, 'NO_INPUT') = coalesce(source.input, 'NO_INPUT')
    """
).whenNotMatchedInsertAll().execute()
# No WHEN MATCHED update clause: Bronze raw measurements are strictly immutable!
```

---

## 3. Verification Protocol & Acceptance Criteria

1. **Unit Test**: Validate `_resolve_full_payload()` injects `api_anomaly` and `api_confirmed` from Stage 1 dictionary.
2. **Schema Test**: Validate `ref_citizenlab_categories` contains `taxonomy_version`.
3. **Partition Test**: Ingesting $T-2$ records during a lookback run lands records in $T-2$ partition without overwriting existing data.
4. **Idempotency Test**: Re-running Bronze on the same batch results in `num_target_rows_inserted = 0` and `num_target_rows_updated = 0`.
5. **Quality Gate**: Execute `pytest tests/test_phase1_bronze_isolation.py` and confirm all 27 tests pass.
