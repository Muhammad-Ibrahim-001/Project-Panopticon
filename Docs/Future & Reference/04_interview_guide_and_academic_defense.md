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
• Engineered a rolling 3-day lookback CDC engine utilizing Delta Lake `MERGE INTO`, reconciling late-arriving measurements from offline probes and updating dimensional threat categorizations with zero row duplication.
• Engineered privacy-preserving governance pipelines, applying salted HMAC-SHA256 pseudonymization and /24 subnet truncation to sanitize thousands of resolver IP addresses under GDPR compliance.
• Optimized Spark shuffle partitions (`shuffle.partitions=16`), columnar compression, and broadcast joins, executing multi-stage transformations on Databricks Community Edition (15 GB RAM single-node) with zero OutOfMemory failures.
• Built an executive Power BI Command Center delivering digital forensic metrics, including an automated Censorship Aggression Index (CAI) and rolling Z-score anomaly blackout predictors.
```

---

## 2. The STAR Story for Behavioral & Technical Rounds

* **Situation**: Authoritarian regimes actively weaponize Deep Packet Inspection (DPI) and DNS poisoning during civil unrest, yet monitoring organizations lack centralized Big Data pipelines to distinguish between general telecom outages and targeted digital censorship at scale.
* **Task**: Our team was tasked with building an automated, production-grade distributed data pipeline using Apache Spark, capable of handling multi-gigabyte semi-structured JSON telemetry, enforcing strict privacy safeguards, handling late-arriving edge reports, and serving real-time analytics.
* **Action**:
  1. Built an ingestion engine on Databricks reading compressed JSONL archives (`s3://ooni-data-eu-fra/`) in bounded micro-batches.
  2. Designed an explicit schema parser to unpack deeply nested polymorphic test keys (DNS queries, TCP handshakes, TLS negotiation records).
  3. Engineered an idempotent Delta Lake `MERGE INTO` pipeline scanning a 3-day sliding window to reconcile late-arriving mobile probe measurements and update Citizen Lab threat taxonomies.
  4. Enforced data governance by hashing sensitive resolver IPs and scrubbing URL query parameters before writing to the Silver Delta layer.
  5. Modeled an analytical Star Schema in Gold with precomputed Censorship Aggression rollups.
* **Result**: Successfully processed tens of thousands of forensic network events across high-surveillance regions, achieved a 100% join match with Citizen Lab threat taxonomies, and built an interactive Power BI dashboard that renders complex geospatial queries in under 1.5 seconds.

---

## 3. Defense Against Tough Technical Questions

### Q1: "Why did you use Apache Spark instead of DuckDB or PostgreSQL for this dataset?"
* **Model Answer**: "While DuckDB is fantastic for in-memory local analytics on flat files, OONI telemetry consists of deeply nested, polymorphic JSON with variable-length arrays (`network_events`, `queries`, `tcp_connects`). Spark SQL's native distributed array exploding (`explode`), explicit `StructType` handling, and seamless integration with Delta Lake ACID transactions make it the enterprise standard for this workload. Furthermore, our architecture is designed to scale horizontally across multi-node clusters without rewriting the pipeline."

### Q2: "Raw OONI telemetry is immutable event logs. Old records never change upstream. Why did you use Delta MERGE INTO, and what actually gets updated?"
* **Model Answer**: "That was a critical architectural realization in our design:
  1. *Late-Arriving Probe Data*: Probes in censored regions frequently operate on mobile devices (Android/iOS) over throttled or severed internet connections. They store tests locally and sync days later. By running a 3-day rolling lookback window ($T-3$ to $T$), `WHEN NOT MATCHED INSERT` captures delayed measurements while ignoring duplicates.
  2. *Dimensional Re-Enrichment*: While raw probe packets are immutable, the Silver layer is enriched with Citizen Lab categories and ISP registries. When Citizen Lab updates a URL's classification (e.g. from uncategorized to circumvention tools) or when our tampering vector heuristics are refined, `WHEN MATCHED UPDATE` updates the enriched columns for existing records.
  3. *Idempotency*: Re-running a batch updates audit metadata (`load_timestamp`, `_batch_id`) without producing duplicate rows."

### Q3: "Why did you scope your baseline to ~500 MB rather than a 10+ GB global dump on Databricks Community Edition?"
* **Model Answer**: "In production data engineering, FinOps and compute memory budgeting are paramount. We empirically measured that OONI's `.jsonl.gz` archives expand by **5.44x** into raw JSON text, and raw JSON expands by another 2x–3x when instantiated as JVM objects during Spark array explodes. A 10 GB compressed archive would require ~100 GB heap memory, immediately crashing the 15 GB RAM ceiling of Databricks Community Edition (which only allocates ~9.5 GB usable Spark heap and caps file uploads at 2 GB). By scoping the dataset to ~500 MB compressed (~35k–60k deep measurements across key censorship focus countries like Pakistan and Iran), we proved enterprise-grade schema enforcement, Z-Ordering, and Delta MERGE while guaranteeing zero OutOfMemory crashes."

### Q4: "How does your pipeline ensure idempotency during incremental runs?"
* **Model Answer**: "Network measurements can be re-sent during probe retries. In our Silver layer, instead of blind appends, we execute Delta Lake's `MERGE INTO` keyed on a deterministic `measurement_id` (SHA-256 of `report_id || input || measurement_start_time`). If a record already exists, it updates metadata; if it is new, it inserts it. This guarantees that running the pipeline multiple times produces identical state with zero duplicate rows."

### Q5: "How did you join Citizen Lab URLs when web addresses have variations (http vs https, subdomains, trailing slashes)?"
* **Model Answer**: "In the Silver layer, we implemented domain normalization using PySpark regex functions. We stripped protocol prefixes (`http://`, `https://`), removed trailing slashes, and extracted the lowercased registered domain (e.g., `www.pmln.org.pk/news` -> `pmln.org.pk`). This achieved a 100% join match against Citizen Lab's global and regional catalogs."

### Q6: "Is `probe_ip` really in the public OONI dataset?"
* **Model Answer**: "That was an essential technical discovery during our exploratory analysis. In public OONI dumps, upstream collectors redact `probe_ip` to `127.0.0.1` to protect mobile users. However, `resolver_ip` (the actual public IP address of the local ISP DNS resolver, e.g., `202.163.69.18`), `probe_asn`, and URL query strings are fully present. Our governance framework specifically targets `resolver_ip` and client network identifiers using salted HMAC-SHA256 hashing to ensure complete privacy."
