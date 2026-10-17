# Phase 1: Silver Core Infrastructure, Configurations & Schema Contracts
**Priority**: **P0 · Critical Foundation**  
**Target Files**: [`notebooks/02_silver_transformation.py`](file:///d:/Semester%20Projects/Panopticon/notebooks/02_silver_transformation.py), [`notebooks/00_config.py`](file:///d:/Semester%20Projects/Panopticon/notebooks/00_config.py), [`data/schemas/silver_conformed_schema.json`](file:///d:/Semester%20Projects/Panopticon/data/schemas/silver_conformed_schema.json)  
**Downstream Impact**: Primary conformed table [`silver_network_measurements`](file:///d:/Semester%20Projects/Panopticon/data/schemas/silver_conformed_schema.json), downstream Gold Fact and Dimensions.  
**Associated Audit Findings**: `SILVER-01`, `SILVER-02`, `SILVER-03`, `SILVER-04`, `SILVER-12`, `SILVER-16`, `SILVER-17`, `SILVER-18`, `SILVER-43`, `SILVER-47`, `SILVER-53`

```text
==============================================================================
PIPELINE DEPENDENCY & IMPACT TRACKING
==============================================================================
Upstream Dependencies:
  - bronze_ooni_raw (Clean Bronze data from Phase 0)
  - notebooks/00_config.py (STORAGE_ROOT, PATHS, PEPPER_SALT)
Downstream Impact:
  - Creates silver_network_measurements Delta Lake table
  - Enforces strict 1:1 measurement event grain
  - Scrubs activist identifiers (probe_id, probe_ip, raw test_keys)
  - Establishes canonical deterministic measurement_id primary key
Risk & Side Effects:
  - Changing measurement_id formula requires full Silver re-ingestion
==============================================================================
```

---

## 1. Problem Diagnosis & Architectural Vulnerabilities

### Problem 1.1: Bronze vs Silver Surrogate Key Grain Mismatch (SILVER-16, SILVER-17, SILVER-18)
* **Root Cause**: Bronze deduplicates on 6 natural keys (`probe_cc`, `measurement_date`, `report_id`, `measurement_start_time`, `test_name`, `input`). Early Silver specs defined `measurement_id` by hashing only 3 fields (`report_id`, `input`, `measurement_start_time`), and ignored the globally unique `measurement_uid` already injected in Bronze.
* **Downstream Silver Impact**: Multi-test reports with null inputs collide in Silver. Rapid retries within the same second share timestamps and collide. When Delta `MERGE INTO` runs, Spark crashes with:
  `DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE`.

### Problem 1.2: Grain Conflict & Intra-Row Cartesian Explosion (SILVER-12, SILVER-13)
* **Root Cause**: Exploding multi-valued protocol arrays (`queries`, `tcp_connect`) directly inside the parent table multiplies rows per measurement ($36 \times 34 = 1,224$ rows per test).
* **Downstream Silver Impact**: Exploding rows in the parent table breaks the 1:1 measurement event grain, corrupts the Censorship Aggression Index (CAI) denominator in Gold, and causes fatal merge collisions.

### Problem 1.3: Activist Privacy Vulnerabilities & Useless Subnet Masking (SILVER-01, SILVER-02, SILVER-43)
* **Root Cause**: Upstream OONI already scrubs `probe_ip` to loopback `127.0.0.1`. Deriving `/24` subnet masks produces constant `127.0.0.0/24`. Meanwhile, persisting `probe_id` across longitudinal runs enables tracking activist vantage points, and persisting raw `test_keys` JSON leaks HTTP authorization tokens and cookies.
* **Remediation**: Drop `probe_id`, raw `probe_ip`, and raw `test_keys` string completely at the Silver boundary.

### Problem 1.4: `probe_asn` Nullability Crash on AS0 or Failed Geolocation (SILVER-03)
* **Root Cause**: If ASN resolution fails (`AS0`, empty, or null), casting regex strings to `LongType` yields `NULL`. If the schema declares `nullable: false`, Delta Lake raises an invariant violation and crashes the batch.
* **Remediation**: Use `coalesce(regexp_extract(col("probe_asn"), r"AS(\d+)", 1).cast(LongType()), lit(0L))`.

---

## 2. Target Implementation Blueprint

### Step 2.1: Canonical `measurement_id` Primary Key Generation
In `notebooks/02_silver_transformation.py`:
```python
def generate_measurement_surrogate_key(df: DataFrame) -> DataFrame:
    """
    Generates deterministic, collision-resistant primary key:
      1. Uses globally unique measurement_uid when present (from OONI API metadata).
      2. Falls back to SHA-256 hash of all 6 Bronze natural key components.
    """
    fallback_hash = sha2(
        concat_ws(
            "||",
            coalesce(col("probe_cc"), lit("XX")),
            coalesce(col("measurement_date").cast("string"), lit("1970-01-01")),
            coalesce(col("report_id"), lit("NO_REPORT")),
            coalesce(col("measurement_start_time"), lit("1970-01-01 00:00:00")),
            coalesce(col("test_name"), lit("unknown_test")),
            coalesce(col("input"), lit("NO_INPUT")),
        ),
        256
    )
    return df.withColumn(
        "measurement_id",
        coalesce(col("measurement_uid"), fallback_hash)
    )
```

### Step 2.2: Cryptographic Peppered SHA-256 for Resolver IP
Secure DNS resolver pseudonymization using a pepper/salt stored in Databricks secrets or environment variable:
```python
def hash_resolver_ip(df: DataFrame, pepper_salt: str) -> DataFrame:
    """
    Applies salted cryptographic SHA-256 pseudonymization to resolver_ip.
    Fails securely if salt is absent in production.
    """
    if not pepper_salt:
        raise ValueError("[Security Guard] PEPPER_HASH_SALT cannot be empty in production.")
    
    return df.withColumn(
        "hashed_resolver_ip",
        when(
            col("resolver_ip").isNotNull() & (col("resolver_ip") != "") & (col("resolver_ip") != "127.0.0.1"),
            sha2(concat(col("resolver_ip"), lit(f"::{pepper_salt}")), 256)
        ).otherwise(lit(None).cast("string"))
    )
```

### Step 2.3: Robust ASN Extraction & Safe Fallbacks
```python
def clean_network_identifiers(df: DataFrame) -> DataFrame:
    """
    Safely extracts numeric ASNs with 0L fallback and cleans provider names.
    """
    return df.withColumn(
        "probe_asn",
        coalesce(
            regexp_extract(col("probe_asn"), r"(?:AS)?(\d+)", 1).cast("long"),
            lit(0L)
        )
    ).withColumn(
        "resolver_asn",
        coalesce(
            regexp_extract(col("resolver_asn"), r"(?:AS)?(\d+)", 1).cast("long"),
            lit(0L)
        )
    ).withColumn(
        "isp_name",
        coalesce(col("probe_network_name"), lit("Unknown ISP"))
    )
```

### Step 2.4: Activist Privacy Boundary Sanitization
Drop all PII and raw payload blobs at the Silver boundary:
```python
COLUMNS_TO_DROP_AT_SILVER = [
    "probe_id",        # Dropped: enables longitudinal activist tracking
    "probe_ip",        # Dropped: redundant loopback / potential local leak
    "test_keys",       # Dropped: unparsed JSON string; metrics extracted into columns
    "annotations",     # Dropped: hardware/kernel metadata
    "test_helpers",    # Dropped: backend endpoint IPs
    "extensions",      # Dropped: raw extension payloads
]

df_sanitized = df_raw_bronze.drop(*COLUMNS_TO_DROP_AT_SILVER)
```

---

## 3. Verification Protocol & Acceptance Criteria

1. **Surrogate Key Uniqueness**: `df.groupBy("measurement_id").count().filter("count > 1").count() == 0`.
2. **Privacy Audit**: Ensure zero occurrences of `probe_id` or `probe_ip` in the persisted Silver schema.
3. **Pepper Salt Verification**: Changing `PEPPER_HASH_SALT` produces completely different `hashed_resolver_ip` hashes; empty salt throws `ValueError`.
4. **Schema Compliance**: Persisted Delta table matches `silver_conformed_schema.json` 100%.
