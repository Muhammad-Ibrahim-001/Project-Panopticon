# Phase 4: Normalized Child Tables & Multi-Test Polymorphism
**Priority**: **P1 · High**  
**Target Files**: [`notebooks/02_silver_transformation.py`](file:///d:/Semester%20Projects/Panopticon/notebooks/02_silver_transformation.py), [`data/schemas/silver_dns_experiments_schema.json`](file:///d:/Semester%20Projects/Panopticon/data/schemas/silver_dns_experiments_schema.json), [`data/schemas/silver_tcp_experiments_schema.json`](file:///d:/Semester%20Projects/Panopticon/data/schemas/silver_tcp_experiments_schema.json), [`data/schemas/silver_app_anomaly_schema.json`](file:///d:/Semester%20Projects/Panopticon/data/schemas/silver_app_anomaly_schema.json)  
**Downstream Impact**: Creates 3 normalized Delta Lake child tables supporting diagnostic drill-downs and application-level censorship analytics.  
**Associated Audit Findings**: `SILVER-07`, `SILVER-08`, `SILVER-09`, `SILVER-10`, `SILVER-11`, `SILVER-13`, `SILVER-14`, `SILVER-15`, `SILVER-48`, `SILVER-52`

```text
==============================================================================
PIPELINE DEPENDENCY & IMPACT TRACKING
==============================================================================
Upstream Dependencies:
  - Raw test_keys JSON strings from bronze_ooni_raw
  - Canonical measurement_id from Phase 1
Downstream Impact:
  - Generates: silver_dns_experiments (Exploded queries, 1:N)
  - Generates: silver_tcp_experiments (Exploded tcp_connect, 1:N)
  - Generates: silver_app_anomaly_measurements (App nettests: WhatsApp, Signal, etc.)
Risk & Side Effects:
  - Chaining sequential explode() triggers 463K-row intra-row Cartesian explosion
    and driver OOM. Projections MUST be executed independently via posexplode_outer().
==============================================================================
```

---

## 1. Problem Diagnosis & Architectural Vulnerabilities

### Problem 1.1: Intra-Row Cartesian Explosion & Driver OOM (SILVER-13, SILVER-15)
* **Root Cause**: A single rich multimedia measurement contains up to 36 DNS queries, 34 TCP handshakes, and 379 network events.
* **Impact**:
  - Chaining sequential `explode()` calls in a single projection creates an intra-row Cartesian cross product:
    $$36 \times 34 \times 379 = 463,896 \text{ rows per parent measurement}$$
  - A modest batch of 50,000 raw measurements explodes into **23 Billion rows**, instantly exhausting Spark heap memory and crashing Databricks Community Edition with `OutOfMemoryError: Java heap space`.

### Problem 1.2: Silent Deletion of Censored Records via Standard `explode()` (SILVER-14)
* **Root Cause**: When a censor injects DNS hijacking or NXDOMAIN errors (`dns_nxdomain_error`), the probe cannot resolve the hostname. Consequently, **no TCP connection or TLS handshake is ever attempted**. The arrays `tcp_connect` and `tls_handshakes` are `null` or empty.
* **Impact**: Standard Spark `explode()` **drops rows where the array is empty or null**. Chaining standard `explode()` across protocol arrays silently **deletes 100% of DNS censorship events**, turning the lakehouse into an engine that reports zero censorship precisely where censorship was strongest!
* **Remediation**: Always use `posexplode_outer()`. Never filter parent rows based on empty child arrays.

### Problem 1.3: Multi-Test Polymorphism Trap (SILVER-07, SILVER-52)
* **Root Cause**: Earlier code contained `filter(col("test_name") == "web_connectivity")`.
* **Impact**: This filter silently discards all measurements for WhatsApp (`whatsapp`), Telegram (`telegram`), Signal (`signal`), Tor (`tor`), and Psiphon (`psiphon`), completely breaking the human rights project mandate and Phase 1 proposal Visual 4.
* **Remediation**: Implement polymorphic schema routing: web tests enrich the parent table, while instant messaging and circumvention tests populate `silver_app_anomaly_measurements`.

### Problem 1.4: Base64 HTTP Response Body Type Corruption (SILVER-11)
* **Root Cause**: Per OONI `df-001-httpt`, binary response bodies (common on custom government blockpages with inline GIF/PNG banners) are serialized as JSON objects: `{"data": "...", "format": "base64"}`.
* **Impact**: Declaring `body: StringType` in the schema corrupts or nullifies the entire HTTP struct when parsing blockpages.
* **Remediation**: Do NOT parse raw HTTP `body` in the Silver schema. Extract only `status_code`, `headers`, and `failure` codes.

---

## 2. Target Implementation Blueprint

### Step 2.1: Independent 1-Pass Projection for `silver_dns_experiments`
Project exploded DNS queries with deterministic `query_id` surrogate key:
```python
def project_silver_dns_experiments(df_bronze: DataFrame) -> DataFrame:
    """
    Extracts and normalizes DNS queries into silver_dns_experiments:
      - Uses posexplode_outer to prevent dropping failed DNS queries.
      - Generates deterministic surrogate key: sha2(measurement_id || query_pos).
      - Drops raw IP addresses to protect activist privacy.
    """
    from pyspark.sql.functions import posexplode_outer, size, expr

    DNS_PARSE_SCHEMA = """
        queries ARRAY<STRUCT<
            engine: STRING,
            query_type: STRING,
            hostname: STRING,
            failure: STRING,
            answers: ARRAY<STRUCT<
                answer_type: STRING,
                ipv4: STRING,
                ipv6: STRING,
                cname: STRING,
                ttl: INT
            >>
        >>,
        resolver_address: STRING
    """

    df_parsed = df_bronze.filter(col("test_keys").isNotNull()) \
                         .withColumn("parsed_dns", from_json(col("test_keys"), DNS_PARSE_SCHEMA))

    # Explode queries array with outer join and position index
    df_queries = df_parsed.select(
        col("measurement_id"),
        col("measurement_date"),
        col("probe_cc"),
        col("parsed_dns.resolver_address").alias("raw_resolver"),
        posexplode_outer(col("parsed_dns.queries")).alias("query_pos", "query_struct")
    ).filter(col("query_struct").isNotNull())

    return df_queries.select(
        sha2(concat_ws("||", col("measurement_id"), col("query_pos").cast("string")), 256).alias("query_id"),
        col("measurement_id"),
        col("measurement_date"),
        col("probe_cc"),
        col("query_pos"),
        col("query_struct.query_type").alias("query_type"),
        col("query_struct.hostname").alias("query_hostname"),
        col("query_struct.engine").alias("query_engine"),
        col("query_struct.failure").alias("query_failure"),
        when(col("query_struct.failure").isNotNull(), lit(True)).otherwise(lit(False)).alias("is_dns_failure"),
        size(col("query_struct.answers")).alias("total_answers_count"),
        expr("size(filter(query_struct.answers, a -> a.ipv4 is not null))").alias("ipv4_answers_count"),
        expr("size(filter(query_struct.answers, a -> a.cname is not null))").alias("cname_answers_count"),
        current_timestamp().alias("load_timestamp")
    )
```

### Step 2.2: Independent 1-Pass Projection for `silver_tcp_experiments`
```python
def project_silver_tcp_experiments(df_bronze: DataFrame) -> DataFrame:
    """
    Extracts and normalizes TCP connect handshakes into silver_tcp_experiments:
      - Uses posexplode_outer to preserve TCP failures.
      - Generates deterministic surrogate key: sha2(measurement_id || tcp_pos).
    """
    TCP_PARSE_SCHEMA = """
        tcp_connect ARRAY<STRUCT<
            ip: STRING,
            port: INT,
            status: STRUCT<
                blocked: BOOLEAN,
                failure: STRING,
                success: BOOLEAN
            >
        >>
    """
    df_parsed = df_bronze.filter(col("test_keys").isNotNull()) \
                         .withColumn("parsed_tcp", from_json(col("test_keys"), TCP_PARSE_SCHEMA))

    df_tcp = df_parsed.select(
        col("measurement_id"),
        col("measurement_date"),
        col("probe_cc"),
        posexplode_outer(col("parsed_tcp.tcp_connect")).alias("tcp_pos", "tcp_struct")
    ).filter(col("tcp_struct").isNotNull())

    return df_tcp.select(
        sha2(concat_ws("||", col("measurement_id"), col("tcp_pos").cast("string")), 256).alias("tcp_connect_id"),
        col("measurement_id"),
        col("measurement_date"),
        col("probe_cc"),
        col("tcp_pos"),
        col("tcp_struct.ip").alias("destination_ip"),
        col("tcp_struct.port").alias("destination_port"),
        coalesce(col("tcp_struct.status.blocked"), lit(False)).alias("is_blocked"),
        col("tcp_struct.status.failure").alias("failure_reason"),
        coalesce(col("tcp_struct.status.success"), lit(False)).alias("is_success"),
        current_timestamp().alias("load_timestamp")
    )
```

### Step 2.3: Multi-Test Polymorphic Routing: `silver_app_anomaly_measurements`
Extract non-web circumvention and messaging tests:
```python
def project_silver_app_anomaly_measurements(df_bronze: DataFrame) -> DataFrame:
    """
    Routes specialized instant messaging and circumvention nettests
    (WhatsApp, Telegram, Signal, Tor, Psiphon) into a dedicated analytical table.
    """
    APP_TEST_NAMES = ["whatsapp", "telegram", "signal", "tor", "psiphon", "facebook_messenger"]

    APP_KEYS_SCHEMA = """
        whatsapp_endpoints_status STRING,
        whatsapp_web_status STRING,
        telegram_http_blocking BOOLEAN,
        telegram_tcp_blocking BOOLEAN,
        signal_backend_status STRING,
        tor_bootstrap_status STRING,
        psiphon_bootstrap_time DOUBLE
    """

    df_apps = df_bronze.filter(col("test_name").isin(APP_TEST_NAMES)) \
                       .withColumn("app_keys", from_json(col("test_keys"), APP_KEYS_SCHEMA))

    return df_apps.select(
        col("measurement_id").alias("app_measurement_id"),
        col("measurement_date"),
        col("probe_cc"),
        col("probe_asn"),
        col("test_name").alias("app_name"),
        coalesce(col("api_anomaly"), lit(False)).alias("is_anomaly"),
        col("app_keys.whatsapp_web_status").alias("whatsapp_web_status"),
        col("app_keys.whatsapp_endpoints_status").alias("whatsapp_endpoints_status"),
        col("app_keys.telegram_http_blocking").alias("telegram_http_blocked"),
        col("app_keys.telegram_tcp_blocking").alias("telegram_tcp_blocked"),
        col("app_keys.signal_backend_status").alias("signal_backend_status"),
        col("app_keys.tor_bootstrap_status").alias("tor_bootstrap_status"),
        col("app_keys.psiphon_bootstrap_time").alias("psiphon_bootstrap_time"),
        current_timestamp().alias("load_timestamp")
    )
```

---

## 3. Verification Protocol & Acceptance Criteria

1. **Zero Cartesian Explosion**: Verify driver memory remains stable under a 10,000-record batch.
2. **DNS Blocking Preservation**: Confirm that measurements where DNS failed (`failure != null` and `tcp_connect is null`) are present in `silver_dns_experiments` and NOT deleted from parent `silver_network_measurements`.
3. **App Telemetry Routing**: Confirm WhatsApp, Telegram, Signal, and Tor records populate `silver_app_anomaly_measurements`.
4. **Primary Key Uniqueness**:
   - `silver_dns_experiments`: `query_id` is 100% unique.
   - `silver_tcp_experiments`: `tcp_connect_id` is 100% unique.
   - `silver_app_anomaly_measurements`: `app_measurement_id` is 100% unique.
