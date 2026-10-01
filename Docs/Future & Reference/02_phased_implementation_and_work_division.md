# Phased Implementation Plan & Work Division
## Project Panopticon: Global Cyber Warfare & Internet Censorship Lakehouse

---

### Project Timeline & Hard Deadlines
* **Phase 1**: Submission & Project Proposal Approval (Completed)
* **Phase 2**: Automated Ingestion Engine, Bronze/Silver Delta Pipelines & PII Masking (**Due: 10 Oct 2026**)
* **Phase 3**: Gold Star Schema, Power BI Dashboard Integration & Final Project Report (**Due: 24 Oct 2026**)

---

## 1. Work Division Between Teammates

To ensure balanced accountability and maximum learning across distributed systems, data modeling, and visualization, work is divided across two functional leads:

```text
+-------------------------------------------------------+-------------------------------------------------------+
| LEAD 1: Muhammad Ibrahim (24L-2602)                  | LEAD 2: Safee Akmal (23L-2556)                        |
| Role: Distributed Systems & Lakehouse Architect       | Role: Data Governance & Analytics Engineer            |
+-------------------------------------------------------+-------------------------------------------------------+
| • OONI S3 Ingestion Engine & Streaming Setup          | • Citizen Lab & ASN Reference Pipeline Ingestion      |
| • Bronze Delta Lake Storage & Metadata Injection      | • PII Cryptographic Masking (HMAC-SHA256)             |
| • JSON Schema Enforcement & Polymorphic Parser        | • Tampering Vector Classification Engine (PySpark)    |
| • Incremental CDC Engine (Delta Lake MERGE INTO)      | • Star Schema Data Modeling (Fact & Dimensions)       |
| • Lakehouse Optimization (Z-Order, Vacuum, Partitions)| • Power BI Dashboard Design & Measure Formulation     |
| • FinOps Management on Databricks Community Edition   | • Data Quality Testing & Anomaly Threshold Rules      |
+-------------------------------------------------------+-------------------------------------------------------+
```

---

## 2. Phase-by-Phase Sprint Deliverables

### Phase 1: Foundations, Architecture & Data Sourcing (Completed)
* [x] Domain selection and problem definition (Digital Human Rights & Cyber Censorship).
* [x] Verification of S3 source partitioning, file schemas, and incremental update mechanics.
* [x] Acquisition and validation of sample payloads in version control (`data/samples/`).
* [x] Design of high-level Medallion architecture and Star Schema.
* [x] Formal proposal document (`Docs/phase1_proposal.md` and `.docx`).
* [x] GitHub repository initialization and setup.

### Phase 2: Core Ingestion, Transformations & Silver Delta Layer (Due: 10 Oct 2026)
* **Sprint 2.1: Bronze Ingestion Pipeline**:
  * Implement `01_bronze_ingestion_streaming.py` in PySpark.
  * Ingest the scoped ~500 MB baseline using explicit `StructType` Schema-on-Read in bounded micro-batches (`maxBytesPerTrigger = 512MB`).
  * Attach ingestion audit columns (`_ingestion_timestamp`, `_source_file`, `_batch_id`).
  * Save to `bronze_ooni_raw` Delta table partitioned by date.
* **Sprint 2.2: Cleansing & PII Cryptographic Sanitization**:
  * Implement `02_silver_cleaning_anonymization.py`.
  * Hash resolver IPs using `sha2(concat(resolver_ip, salt), 256)` and truncate client subnets.
  * Sanitize target URL query parameters using PySpark regex functions.
* **Sprint 2.3: Exploding & Relational Flattening**:
  * Unpack nested arrays: `test_keys.queries`, `test_keys.tcp_connect`, and `test_keys.network_events`.
  * Classify tampering attacks into clean labels (`DNS_TAMPERING`, `TCP_RST_INJECTION`, `TLS_DROP`, `HTTP_BLOCK`).
* **Sprint 2.4: Reference Joins & Incremental Rolling Lookback CDC Engine**:
  * Broadcast join with Citizen Lab URL categories and ASN lookup tables.
  * Implement Delta Lake `MERGE INTO` with a 3-day rolling lookback window ($T-3$ to $T$) to reconcile late-arriving mobile probe measurements and update enriched taxonomy idempotently.
  * Phase 2 Deliverable: Working Databricks notebook with reproducible Bronze and Silver pipeline runs.

### Phase 3: Gold Layer Modeling, BI Dashboards & Final Defense (Due: 24 Oct 2026)
* **Sprint 3.1: Gold Star Schema & Analytical Marts**:
  * Implement `03_gold_dimensional_modeling.py`.
  * Populate `fact_network_anomaly` and dimension tables (`dim_country_region`, `dim_isp_telecom`, `dim_content_category`, `dim_date_time`).
  * Precompute `agg_daily_censorship_severity` calculating the Censorship Aggression Index (CAI).
* **Sprint 3.2: Advanced Analytical Measures**:
  * Implement the 5 complex business logic queries (Regulatory Enforcement Latency, Collateral Damage Ratio, Evasion Feasibility Matrix, Rolling Z-score Blackout Predictor).
* **Sprint 3.3: Power BI / Tableau Dashboard Construction**:
  * Connect Power BI to the exported Gold Delta/Parquet tables.
  * Build the 4 core visuals: Global Choropleth Map, 100% Stacked Tampering Bar, Category Vulnerability Bar, and Crisis Timeline Spike Detector.
  * Construct executive KPI cards (Total Tests, Block Rate %, Active Blackouts, Dominant Attack Vector).
* **Sprint 3.4: Final Project Report & Presentation**:
  * Draft comprehensive Phase 3 report with execution metrics, cost summaries, and query benchmarks.
  * Record a 5-minute video demonstration walking through the end-to-end pipeline and interactive dashboard.

---

## 3. Weekly Task Matrix & Acceptance Criteria

| Week | Target Focus | Lead Assigned | Deliverable Artifact | Acceptance Criteria |
| :--- | :--- | :--- | :--- | :--- |
| **Week 1** (Sep 28 - Oct 04) | Bronze Ingestion & Streaming Setup | Muhammad Ibrahim | `notebooks/01_bronze_ingestion.py` | Successfully reads scoped baseline from S3 without OOM errors. |
| **Week 1** (Sep 28 - Oct 04) | PII Masking & Reference Load | Safee Akmal | `notebooks/02_silver_cleaning.py` | 100% of resolver IPs hashed; 0 raw PII leaked into Silver; Citizen Lab 100% join match. |
| **Week 2** (Oct 05 - Oct 10) | Silver Transformations & CDC | Both Leads | `notebooks/02_silver_cdc_merge.py` | Delta `MERGE INTO` reconciles 3-day lookback window in < 60 seconds with zero duplicates. |
| **Week 3** (Oct 11 - Oct 17) | Gold Dimensional Model & Marts | Muhammad Ibrahim | `notebooks/03_gold_modeling.py` | Star Schema generated; Gold aggregation tables query in < 2 seconds. |
| **Week 4** (Oct 18 - Oct 24) | Power BI Dashboard & Video Defense | Safee Akmal | `dashboards/Panopticon_Dashboard.pbix` | Interactive cross-filtering on country click; all 4 visual charts fully operational. |
