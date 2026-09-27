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
• Architected a 3-tier Medallion Lakehouse on Apache Spark ingesting a 10+ GB baseline and 1.8 GB daily incremental telemetry streams from the AWS Open Data Registry across 200+ countries.
• Implemented distributed Change Data Capture (CDC) utilizing Delta Lake MERGE INTO to track temporal censorship state transitions idempotently, reducing batch processing times by 65%.
• Engineered privacy-preserving governance pipelines, applying salted HMAC-SHA256 pseudonymization and /24 subnet truncation to sanitize thousands of resolver IP addresses under GDPR compliance.
• Optimized Spark shuffle partitions and memory utilization (Z-Ordering, partition pruning, and broadcast joins), executing multi-million row aggregations on a single-node 15 GB RAM Databricks cluster without OOM failures.
• Built an executive Power BI Command Center delivering digital forensic metrics, including an automated Censorship Aggression Index (CAI) and rolling Z-score anomaly blackout predictors.
```

---

## 2. The STAR Story for Behavioral & Technical Rounds

* **Situation**: Authoritarian regimes actively weaponize Deep Packet Inspection (DPI) and DNS poisoning during civil unrest, yet monitoring organizations lack centralized Big Data pipelines to distinguish between general telecom outages and targeted digital censorship at scale.
* **Task**: Our team was tasked with building an automated, production-grade distributed data pipeline using Apache Spark, capable of handling multi-gigabyte semi-structured JSON telemetry, enforcing strict privacy safeguards, and serving real-time analytics.
* **Action**:
  1. Built a streaming ingestion engine on Databricks reading compressed JSONL files in 512 MB micro-batches from AWS S3.
  2. Designed an explicit schema parser to unpack deeply nested polymorphic test keys (DNS queries, TCP handshakes, TLS negotiation records).
  3. Enforced data governance by hashing sensitive resolver IPs and scrubbing URL query parameters before writing to the Silver Delta layer.
  4. Modeled an analytical Star Schema in Gold with precomputed Censorship Aggression rollups.
* **Result**: Successfully processed over 15 million network events, achieved a 100% join match with Citizen Lab threat taxonomies, and built an interactive Power BI dashboard that renders complex geospatial queries in under 1.5 seconds.

---

## 3. Defense Against Tough Technical Questions

### Q1: "Why did you use Apache Spark instead of DuckDB or PostgreSQL for this dataset?"
* **Model Answer**: "While DuckDB is fantastic for in-memory local analytics on flat files, OONI telemetry consists of deeply nested, polymorphic JSON with variable-length arrays (`network_events`, `queries`, `tcp_connects`). Spark SQL's native distributed array exploding (`explode`), explicit `StructType` handling, and seamless integration with Delta Lake ACID transactions make it the enterprise standard for this workload. Furthermore, our architecture is designed to scale horizontally from 10 GB to 100+ GB without rewriting the pipeline."

### Q2: "How did you process 10 GB of raw JSON on a 15 GB single-node Databricks Community cluster without crashing from OutOfMemoryError?"
* **Model Answer**: "We implemented three specific FinOps and memory safeguards:
  1. *Chunked Ingestion*: Instead of loading 10 GB into memory at once with `spark.read.json()`, we used Spark Structured Streaming with `maxBytesPerTrigger = 512mb` to process data in smooth, bounded micro-batches.
  2. *Shuffle Partition Tuning*: We reduced `spark.sql.shuffle.partitions` from the 200 default down to 16, eliminating thread scheduling overhead and thousands of small task objects on our 2-core node.
  3. *Delta Snappy Compression*: Writing directly to Delta Lake compressed the raw JSON text by ~78%, reducing memory pressure during joins."

### Q3: "How does your pipeline ensure idempotency during incremental runs?"
* **Model Answer**: "Network measurements can be re-sent during network retries. In our Silver layer, instead of blind appends, we execute Delta Lake's `MERGE INTO` keyed on `measurement_uid`. If a record already exists, it updates status metadata (`whenMatchedUpdate`); if it is new, it inserts it (`whenNotMatchedInsert`). This guarantees that running the pipeline multiple times produces identical results with zero duplicate rows."

### Q4: "How did you join Citizen Lab URLs when web addresses have variations (http vs https, subdomains, trailing slashes)?"
* **Model Answer**: "In the Silver layer, we implemented domain normalization using PySpark regex functions. We stripped protocol prefixes (`http://`, `https://`), removed trailing slashes, and extracted the lowercased registered domain (e.g., `www.pmln.org.pk/news` -> `pmln.org.pk`). This achieved a 100% join match against Citizen Lab's global and regional catalogs."

### Q5: "Is `probe_ip` really in the public OONI dataset?"
* **Model Answer**: "That was an essential technical discovery during our exploratory analysis. In public OONI dumps, upstream collectors redact `probe_ip` to `127.0.0.1` to protect mobile users. However, `resolver_ip` (the actual public IP address of the local ISP DNS resolver, e.g., `202.163.69.18`), `probe_asn`, and URL query strings are fully present. Our governance framework specifically targets `resolver_ip` and client network identifiers using salted HMAC-SHA256 hashing to ensure complete privacy."
