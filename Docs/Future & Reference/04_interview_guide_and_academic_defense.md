# Interview Guide, Resume Storytelling & Academic Defense
## How to Present Project Panopticon to Evaluators, Teachers & Recruiters

---

### Executive Positioning
When interviewers or professors ask about your semester project, **do not say**:
> *"We analyzed some internet data using Spark and built a dashboard."*

**Instead, position it like a Senior Data Engineer**:
> *"We engineered an end-to-end Big Data Lakehouse on Apache Spark and Delta Lake called Project Panopticon. It ingests multi-gigabyte streams of real-world internet censorship telemetry from the AWS Open Data Registry, sanitizes sensitive activist PII using salted HMAC-SHA256 hashing, enriches raw network events against international threat catalogs via broadcast joins, and executes ACID upserts to track state-sponsored cyber warfare in Power BI."*

---

## 1. Resume Bullet Points (Ready to Copy-Paste into your CV)

```text
Project Panopticon — Global Cyber Warfare & Internet Censorship Lakehouse
Technologies: Apache Spark 3.5, PySpark, Delta Lake 3.0, Databricks, AWS S3, Power BI, Python
• Architected a 3-tier Medallion Lakehouse on Apache Spark ingesting telemetry archives from the AWS Open Data Registry (`s3://ooni-data-eu-fra/`) and Citizen Lab threat catalogs.
• Engineered a rolling 3-day lookback CDC engine utilizing Delta Lake `MERGE INTO`, reconciling late-arriving measurements from offline probes and updating dimensional threat categorizations with zero row duplication and zero write amplification.
• Engineered privacy-preserving governance pipelines, applying salted HMAC-SHA256 pseudonymization to sanitize thousands of resolver IP addresses and dropping persistent installation IDs and raw HTTP headers under GDPR compliance.
• Optimized Spark shuffle partitions (`shuffle.partitions=4-8`), columnar compression, and broadcast joins, executing multi-stage transformations on Databricks Community Edition (15 GB RAM single-node) with zero OutOfMemory failures.
• Built an executive Power BI Command Center delivering digital forensic metrics, including an automated Censorship Aggression Index (CAI) with verified 1:1 measurement event denominators and rolling blackout predictors.
```

---

## 2. The STAR Story for Behavioral & Technical Rounds

* **Situation**: Authoritarian regimes actively weaponize Deep Packet Inspection (DPI) and DNS poisoning during civil unrest, yet monitoring organizations lack centralized Big Data pipelines to distinguish between general telecom outages and targeted digital censorship at scale.
* **Task**: Our team was tasked with building an automated, production-grade distributed data pipeline using Apache Spark, capable of handling multi-gigabyte semi-structured JSON telemetry, enforcing strict privacy safeguards, handling late-arriving edge reports, and serving real-time analytics.
* **Action**:
  1. Built an ingestion engine on Databricks reading compressed JSONL archives (`s3://ooni-data-eu-fra/`) in bounded micro-batches.
  2. Designed an explicit schema parser to unpack deeply nested polymorphic test keys (DNS queries, TCP handshakes, TLS negotiation records).
  3. Engineered an idempotent Delta Lake `MERGE INTO` pipeline scanning a 3-day sliding window to reconcile late-arriving mobile probe measurements and update Citizen Lab threat taxonomies.
  4. Enforced data governance by hashing sensitive resolver IPs, scrubbing URL query parameters, and purging raw JSON payload strings before persisting Silver Delta tables.
  5. Maintained a strict 1:1 event fact grain while normalizing granular protocol hops into dedicated child tables (`silver_dns_experiments`, `silver_tcp_experiments`), preventing driver OOM and Delta MERGE crashes.
  6. Modeled an analytical Star Schema in Gold with precomputed Censorship Aggression rollups.
* **Result**: Successfully processed tens of thousands of forensic network events across high-surveillance regions, achieved deterministic 2-tier matching with Citizen Lab threat taxonomies, and built an interactive Power BI dashboard that renders complex geospatial queries in under 1.5 seconds.

---

## 3. Defense Against Tough Technical Questions

### Q1: "Why did you use Apache Spark instead of DuckDB or PostgreSQL for this dataset?"
* **Model Answer**: "While DuckDB is fantastic for in-memory local analytics on flat files, OONI telemetry consists of deeply nested, polymorphic JSON with variable-length arrays (`network_events`, `queries`, `tcp_connects`). Spark SQL's native distributed array exploding (`explode_outer`), explicit `StructType` handling, and seamless integration with Delta Lake ACID transactions make it the enterprise standard for this workload. Furthermore, our architecture is designed to scale horizontally across multi-node clusters without rewriting the pipeline."

### Q2: "Raw OONI telemetry is immutable event logs. Old records never change upstream. Why did you use Delta MERGE INTO, and what actually gets updated?"
* **Model Answer**: "That was a critical architectural realization in our design:
  1. *Late-Arriving Probe Data*: Probes in censored regions frequently operate on mobile devices (Android/iOS) over throttled or severed internet connections. They store tests locally and sync days later. By running a 3-day rolling lookback window ($T-3$ to $T$), `WHEN NOT MATCHED INSERT` captures delayed measurements while ignoring duplicates.
  2. *Dimensional Re-Enrichment*: While raw probe packets are immutable, the Silver layer is enriched with Citizen Lab categories and ISP registries. When Citizen Lab updates a URL's classification (e.g. from uncategorized to circumvention tools) or when our tampering vector heuristics are refined, `WHEN MATCHED UPDATE` updates the enriched columns for existing records.
  3. *Idempotency*: Re-running a batch updates audit metadata (`load_timestamp`, `_batch_id`) without producing duplicate rows."

### Q3: "Why did you scope your baseline to ~500 MB rather than a 10+ GB global dump on Databricks Community Edition?"
* **Model Answer**: "In production data engineering, FinOps and compute memory budgeting are paramount. We empirically measured that OONI's `.jsonl.gz` archives expand by **5.44x** into raw JSON text, and raw JSON expands by another 2x–3x when instantiated as JVM objects during Spark array explodes. A 10 GB compressed archive would require ~100 GB heap memory, immediately crashing the 15 GB RAM ceiling of Databricks Community Edition (which only allocates ~9.5 GB usable Spark heap and caps file uploads at 2 GB). By scoping the dataset to ~500 MB compressed (~35k–60k deep measurements across key censorship focus countries like Pakistan and Iran), we proved enterprise-grade schema enforcement, Z-Ordering, and Delta MERGE while guaranteeing zero OutOfMemory crashes."

### Q4: "How does your pipeline ensure idempotency and prevent duplicate records during incremental runs?"
* **Model Answer**: "Network measurements can be re-sent during probe retries. In our Silver layer, instead of blind appends, we execute Delta Lake's `MERGE INTO` keyed on a deterministic surrogate key: `coalesce(measurement_uid, sha2(concat_ws('||', probe_cc, measurement_date, report_id, measurement_start_time, test_name, coalesce(input, 'NO_INPUT')), 256))`. This harmonizes with all 6 natural key columns from Bronze while prioritizing OONI's global UUID when available. Combined with the partition predicate `target.measurement_date = source.measurement_date`, it guarantees partition-pruned idempotent execution with zero duplicate rows."

### Q5: "How did you join Citizen Lab URLs when web addresses have variations (http vs https, subdomains, trailing slashes)?"
* **Model Answer**: "In the Silver layer, we engineered a **two-tier enrichment engine**:
  1. *Stage 1 (Exact Normalized URL Match)*: Strips default port numbers (80/443) and trailing slashes to match the full path against Citizen Lab entries (preserving path-specific categorizations like `twitter.com/amnesty`).
  2. *Stage 2 (Deduplicated Domain Fallback)*: For URLs not explicitly listed in Citizen Lab, we extract the lowercased registered domain (e.g., `pmln.org.pk`) and join against a window-ranked deduplicated domain catalog (prioritizing sensitive civil society categories like `POL` and `NEWS`).
  This achieved a 100% deterministic join match while preserving match provenance (`category_match_type = 'EXACT_URL' | 'DOMAIN_FALLBACK'`)."

### Q6: "Is `probe_ip` really in the public OONI dataset?"
* **Model Answer**: "That was an essential technical discovery during our exploratory analysis. In public OONI dumps, upstream collectors redact `probe_ip` to `127.0.0.1` on 100% of records to protect mobile users. Attempting to mask `probe_ip` into a `/24` subnet produces constant `127.0.0.0/24`. However, `resolver_ip` (the actual public IP address of the local ISP DNS resolver, e.g., `202.163.69.18`), `probe_asn`, and URL query strings are fully present. Our governance framework specifically targets `resolver_ip` using salted HMAC-SHA256 hashing and drops persistent `probe_id` hashes to prevent longitudinal activist tracking."

### Q7: "Why didn't you explode queries, TCP connections, and network events directly into your primary Silver measurement table?"
* **Model Answer**: "Chaining multiple array explodes on the same measurement row generates a nested Cartesian cross-product in Spark SQL Catalyst ($36 \text{ queries} \times 34 \text{ TCP connects} \times 379 \text{ events} = \mathbf{463,896}\text{ rows per single test}$). Across 50,000 measurements, that balloons into 60 million JVM rows, instantly crashing the 15 GB RAM ceiling of Databricks Community Edition with Java heap exhaustion. Furthermore:
  1. Standard `explode()` silently deletes 100% of DNS-censored records because downstream TCP and TLS arrays are empty.
  2. Writing exploded rows into the primary table creates duplicate primary keys, crashing Delta Lake MERGE with `DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE`.
  3. Exploded rows distort the Censorship Aggression Index (CAI) by artificially weighting complex multimedia sites with 70+ connection rows while blocked sites have only 1 row.
  We solved this by enforcing a strict 1:1 grain on `silver_network_measurements` and projecting array diagnostics into isolated child tables (`silver_dns_experiments`, `silver_tcp_experiments`) via direct 1-pass `explode_outer()`."

### Q8: "Why did you remove `_batch_id` from the Delta MERGE `WHEN MATCHED` trigger condition?"
* **Model Answer**: "In early pipeline iterations, the condition included `OR target._batch_id != source._batch_id`. Because every incremental run generates a new batch ID, 100% of matched records evaluated to true, forcing Delta Lake to execute full Copy-on-Write partition rewrites on every run. This caused massive write amplification and reported 100% false update metrics. We eliminated `_batch_id` from the trigger condition, using NULL-safe `<=>` operators to detect actual business and taxonomy content changes, while still updating `_batch_id` and `load_timestamp` in the SET clause."

### Q9: "Is `test_runtime` equivalent to HTTP response latency in OONI telemetry?"
* **Model Answer**: "No! `test_runtime` is the total wall-clock execution duration of the probe test runner (typically 2.0 to 15.0 seconds), including resolver discovery, DNS queries, TCP attempts, TLS handshakes, HTTP transactions, and JSON serialization. Mislabelling 8.5 seconds as 'HTTP response latency' would be an empirical error. We preserved `test_runtime_seconds` as experiment runner duration and extract true socket latency from specific network event timestamps when needed."

### Q10: "How do you distinguish between network anomalies and state censorship?"
* **Model Answer**: "Upstream OONI explicitly emphasizes that an anomaly represents unusual network behavior (such as transient timeouts, DNS churn, or packet loss), not proof of censorship. Conflating parser errors or unclassified tests with state censorship manufactures false certainty and inflates the Censorship Aggression Index. We maintain epistemic separation: preserving upstream OONI flags (`ooni_anomaly`, `ooni_confirmed`) while classifying heuristic vectors independently and gating `is_anomaly` strictly on confirmed or verified protocol tampering (`DNS_TAMPERING`, `TCP_RESET`, `TLS_DROP`, `HTTP_BLOCK`)."
