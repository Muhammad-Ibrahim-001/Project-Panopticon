# Phase 3: Epistemic Multi-Signal Classification Engine
**Priority**: **P1 · High**  
**Target Files**: [`notebooks/02_silver_transformation.py`](file:///d:/Semester%20Projects/Panopticon/notebooks/02_silver_transformation.py), [`data/schemas/silver_conformed_schema.json`](file:///d:/Semester%20Projects/Panopticon/data/schemas/silver_conformed_schema.json)  
**Downstream Impact**: Classifies network interference vectors and epistemic statuses on [`silver_network_measurements`](file:///d:/Semester%20Projects/Panopticon/data/schemas/silver_conformed_schema.json); feeds Gold CAI and censorship metric rollups.  
**Associated Audit Findings**: `SILVER-06`, `SILVER-24`, `SILVER-40`, `SILVER-41`, `SILVER-45`, `SILVER-47`, `SILVER-51`

```text
==============================================================================
PIPELINE DEPENDENCY & IMPACT TRACKING
==============================================================================
Upstream Dependencies:
  - Stage 1 API flags (api_anomaly, api_confirmed) preserved from Phase 0
  - Parsed test_keys JSON structs (queries, tcp_connect, tls_handshakes, http_requests)
Downstream Impact:
  - Generates: ooni_anomaly, ooni_confirmed, is_anomaly
  - Generates: classification_status (CONFIRMED_BLOCKED, HEURISTIC_ANOMALY, BENIGN, UNCLASSIFIED)
  - Generates: tampering_vector (DNS_TAMPERING, TCP_RESET, TLS_DROP, HTTP_BLOCK, etc.)
  - Generates: dns_anomaly_flag, tcp_anomaly_flag, tls_anomaly_flag, http_anomaly_flag
  - Computes: test_runtime_seconds (renamed from response_latency_ms)
Risk & Side Effects:
  - Flawed classification rules directly distort Gold CAI metrics.
==============================================================================
```

---

## 1. Problem Diagnosis & Architectural Vulnerabilities

### Problem 1.1: OONI `blocking` HTTPS Censorship Blind Spot (SILVER-06, SILVER-24)
* **Root Cause**: In OONI's `ts-003-http-requests` specification, the `blocking` summary field only outputs: `"dns"`, `"tcp-ip"`, `"http-failure"`, `"http-diff"`. **It never outputs `"tls"`**.
* **Impact**:
  - Legacy probes and SNI-filtering middleboxes that inject TLS `connection_reset` or TLS timeouts have `blocking: false`.
  - Relying exclusively on OONI's `blocking` field produces `tls_anomaly_flag = 0` on 100% of rows, completely blinding the pipeline to modern TLS SNI censorship (e.g., Iranian and Russian TSPU deep packet inspection systems).

### Problem 1.2: Epistemic Integrity & Anomaly Conflation (SILVER-41, SILVER-47)
* **Root Cause**: Earlier designs calculated `is_anomaly = (tampering_vector != 'BENIGN')`.
* **Impact**:
  - If a probe experienced local Wi-Fi packet drop or an unparseable test payload, the classifier set `tampering_vector = 'UNCLASSIFIED'`.
  - Under `tampering_vector != 'BENIGN'`, every unclassified test and transient packet drop was falsely counted as **government network censorship**!
  - In scientific censorship measurement, **an anomaly is not necessarily censorship**. Evidence (protocol failure codes) must be strictly decoupled from epistemic confidence ratings (`CONFIRMED_BLOCKED` vs `HEURISTIC_ANOMALY` vs `BENIGN` vs `UNCLASSIFIED`).

### Problem 1.3: Empirical Scientific Rigor: Latency vs Test Runtime (SILVER-40)
* **Root Cause**: Earlier documentation labeled `test_runtime` (the 2–15 second total execution duration of the probe's test harness) as `response_latency_ms`.
* **Impact**: Presenting 15-second total runtimes as 15,000 ms HTTP response latencies destroys academic credibility during project defense. It must be explicitly named `test_runtime_seconds`.

### Problem 1.4: SQL 3-Valued Logic Classification Trap (SILVER-45)
* **Root Cause**: In Spark SQL, evaluating `col("ooni_anomaly") == False` when `ooni_anomaly` is `NULL` evaluates to `UNKNOWN` (falsy in `when()`).
* **Impact**: Without explicit `coalesce()` handling, 100% of benign rows were pushed to the fallback `UNCLASSIFIED` state.

---

## 2. Target Implementation Blueprint

### Step 2.1: Extract Protocol Diagnostic Signals from Parsed `test_keys`
In `notebooks/02_silver_transformation.py`:
```python
def extract_protocol_diagnostic_signals(df: DataFrame) -> DataFrame:
    """
    Parses core test_keys JSON elements and computes discrete protocol anomaly flags:
      1. DNS Anomaly: dns_experiment_failure != null or answers empty with failure.
      2. TCP Anomaly: any tcp_connect failure in (connection_refused, connection_reset, timeout).
      3. TLS Anomaly: tls_handshakes.failure in (connection_reset, ssl_error, timeout).
      4. HTTP Anomaly: http_experiment_failure != null or status_code in (403, 451).
    """
    from pyspark.sql.functions import col, lit, when, coalesce, from_json, size, expr

    # Schema for extracting diagnostic signals from test_keys JSON string
    TEST_KEYS_EXTRACT_SCHEMA = """
        client_resolver STRING,
        dns_experiment_failure STRING,
        control_failure STRING,
        http_experiment_failure STRING,
        blocking STRING,
        queries ARRAY<STRUCT<failure: STRING, answers: ARRAY<STRUCT<answer_type: STRING, ipv4: STRING>>>>,
        tcp_connect ARRAY<STRUCT<status: STRUCT<blocked: BOOLEAN, failure: STRING, success: BOOLEAN>>>,
        tls_handshakes ARRAY<STRUCT<failure: STRING, version: STRING>>,
        requests ARRAY<STRUCT<response: STRUCT<code: INT>>>
    """

    df_parsed = df.withColumn("parsed_keys", from_json(col("test_keys"), TEST_KEYS_EXTRACT_SCHEMA))

    # Compute protocol diagnostic flags
    return df_parsed.withColumn(
        # DNS Anomaly Flag
        "dns_anomaly_flag",
        when(
            col("parsed_keys.dns_experiment_failure").isNotNull() &
            (col("parsed_keys.dns_experiment_failure") != "null"),
            lit(True)
        ).when(
            expr("exists(parsed_keys.queries, q -> q.failure is not null and q.failure != 'null')"),
            lit(True)
        ).otherwise(lit(False))
    ).withColumn(
        # TCP Anomaly Flag
        "tcp_anomaly_flag",
        when(
            expr("exists(parsed_keys.tcp_connect, t -> t.status.blocked == true or (t.status.failure is not null and t.status.failure != 'null'))"),
            lit(True)
        ).otherwise(lit(False))
    ).withColumn(
        # TLS Anomaly Flag (Solves HTTPS Censorship Blind Spot!)
        "tls_anomaly_flag",
        when(
            expr("exists(parsed_keys.tls_handshakes, tls -> tls.failure is not null and tls.failure != 'null')"),
            lit(True)
        ).otherwise(lit(False))
    ).withColumn(
        # HTTP Anomaly Flag
        "http_anomaly_flag",
        when(
            col("parsed_keys.http_experiment_failure").isNotNull() &
            (col("parsed_keys.http_experiment_failure") != "null"),
            lit(True)
        ).when(
            expr("exists(parsed_keys.requests, r -> r.response.code in (403, 451))"),
            lit(True)
        ).otherwise(lit(False))
    )
```

### Step 2.2: Multi-Signal Epistemic Classification Engine
```python
def classify_censorship_epistemics(df: DataFrame) -> DataFrame:
    """
    Applies rigorous multi-signal epistemic classification:
      - Preserves upstream OONI ground truth: ooni_anomaly, ooni_confirmed
      - Computes epistemic status: CONFIRMED_BLOCKED, HEURISTIC_ANOMALY, BENIGN, UNCLASSIFIED
      - Assigns hierarchical tampering_vector based on protocol network stack layers
    """
    # Upstream ground truth flags
    df_ground = df.withColumn(
        "ooni_anomaly",
        coalesce(col("api_anomaly"), lit(False))
    ).withColumn(
        "ooni_confirmed",
        coalesce(col("api_confirmed"), lit(False))
    )

    # Multi-signal interference boolean
    df_anomaly = df_ground.withColumn(
        "is_anomaly",
        when(col("ooni_confirmed") == True, lit(True))
        .when(col("ooni_anomaly") == True, lit(True))
        .when(col("dns_anomaly_flag") | col("tcp_anomaly_flag") |
              col("tls_anomaly_flag") | col("http_anomaly_flag"), lit(True))
        .otherwise(lit(False))
    )

    # Epistemic Classification Status
    df_status = df_anomaly.withColumn(
        "classification_status",
        when(col("ooni_confirmed") == True, lit("CONFIRMED_BLOCKED"))
        .when(col("is_anomaly") == True, lit("HEURISTIC_ANOMALY"))
        .when(
            (col("ooni_anomaly") == False) & (col("is_anomaly") == False),
            lit("BENIGN")
        )
        .otherwise(lit("UNCLASSIFIED"))
    )

    # Hierarchical Protocol Tampering Vector Assignment
    return df_status.withColumn(
        "tampering_vector",
        when(col("ooni_confirmed") == True, lit("HTTP_BLOCK"))  # Blockpage detected
        .when(col("dns_anomaly_flag") == True, lit("DNS_TAMPERING"))  # Layer 3/DNS precedes transport
        .when(col("tcp_anomaly_flag") == True, lit("TCP_RESET"))     # Layer 4 precedes TLS
        .when(col("tls_anomaly_flag") == True, lit("TLS_DROP"))      # Layer 5/TLS precedes HTTP
        .when(col("http_anomaly_flag") == True, lit("HTTP_BLOCK"))   # Layer 7 Application
        .when(col("is_anomaly") == True, lit("ANOMALY_UNCLASSIFIED"))# Anomaly without clear vector
        .when(col("classification_status") == "BENIGN", lit("BENIGN"))
        .otherwise(lit("UNCLASSIFIED"))
    ).withColumn(
        # Standardized runtime duration (seconds)
        "test_runtime_seconds",
        coalesce(col("test_runtime").cast("double"), lit(0.0))
    )
```

---

## 3. Verification Protocol & Acceptance Criteria

1. **HTTPS Blind Spot Test**: Pass fixture with TLS handshake reset (`failure: "connection_reset"`); verify `tls_anomaly_flag == True`, `tampering_vector == "TLS_DROP"`, and `classification_status == "HEURISTIC_ANOMALY"`.
2. **Benign Test**: Pass clean measurement; verify `is_anomaly == False`, `classification_status == "BENIGN"`, and `tampering_vector == "BENIGN"` (confirming SQL 3-valued logic does NOT produce `UNCLASSIFIED`).
3. **Confirmed Blockpage Test**: Pass record with `api_confirmed = True`; verify `classification_status == "CONFIRMED_BLOCKED"`.
4. **Hierarchical Precedence Test**: Pass record with both DNS failure and HTTP 403; verify `tampering_vector == "DNS_TAMPERING"` (DNS takes precedence over application layer).
