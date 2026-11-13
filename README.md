# Project Panopticon: Global Cyber Warfare & Internet Censorship Lakehouse

[![Apache Spark](https://img.shields.io/badge/Apache_Spark-3.5-E25A1C?logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Delta Lake](https://img.shields.io/badge/Delta_Lake-3.0-00ADD8?logo=delta&logoColor=white)](https://delta.io/)
[![Databricks](https://img.shields.io/badge/Databricks-Community-FF3621?logo=databricks&logoColor=white)](https://community.cloud.databricks.com/)
[![Python](https://img.shields.io/badge/Python-3.10%2B-3776AB?logo=python&logoColor=white)](https://www.python.org/)

---

## 🏛️ Academic Information
* **Institution**: National University of Computer and Emerging Sciences (**FAST-NUCES**)
* **Department**: Department of Data Science | Fall 2026
* **Course**: **DS3001: Design Analysis and Visualization**
* **Section**: **BDS-5B**
* **Team Members**:
  * **Muhammad Ibrahim** (`24L-2602`)
  * **Safee Akmal** (`23L-2556`)
* **Repository**: [https://github.com/Muhammad-Ibrahim-001/Project-Panopticon](https://github.com/Muhammad-Ibrahim-001/Project-Panopticon)

---

## 🌐 Project Overview
**Project Panopticon** is an enterprise-scale distributed data engineering pipeline designed on the **Medallion Architecture (Bronze $\rightarrow$ Silver $\rightarrow$ Gold)** using **Apache Spark 3.5** and **Delta Lake 3.0**. 

The system continuously ingests, cryptographically sanitizes, normalizes, and analyzes real-world internet interference telemetry from the **Open Observatory of Network Interference (OONI)** hosted on the **AWS Open Data Registry**. Enriched with target taxonomy from **Citizen Lab (University of Toronto)** and autonomous network routing topologies from **RIPE / MaxMind**, Panopticon detects and visualizes state-sponsored cyber warfare, Deep Packet Inspection (DPI) attacks, DNS tampering, and nationwide internet blackouts across strategic surveillance focus regions.

### 🔗 Live Production Data Sources & Access Points
* **Primary Telemetry Stream**: AWS Open Data Registry S3 Bucket **[`s3://ooni-data-eu-fra/`](https://ooni-data-eu-fra.s3.eu-central-1.amazonaws.com/)** (`--no-sign-request`, key structure: `jsonl/<test_name>/<probe_cc>/<YYYYMMDD>/<HH>/<filename>.jsonl.gz`) and [OONI Measurement API](https://api.ooni.io/api/v1/measurements).
* **Target Classification Taxonomy**: [Citizen Lab Test Lists Repository](https://github.com/citizenlab/test-lists) (Standardized CSV catalogs per ISO country code, e.g. `lists/pk.csv`).
* **Autonomous System Routing Context**: S3 Open Data directory `s3://ooni-data-eu-fra/ip2country-as/` (MaxMind/IP2Location binary and tabular mappings).

### ⚙️ Free Tier Memory Safeguards & Sizing
Empirically benchmarked on live OONI archives: raw `.jsonl.gz` files expand by **5.44x** into raw JSON text, and raw JSON expands by another 2x–3x into JVM objects. To run reliably on **Databricks Community Edition** (15 GB total RAM, ~9.5 GB usable Spark heap, 2 vCPUs) without OutOfMemory crashes:
* **Full Load Baseline**: **~400 MB to 600 MB compressed `.json.gz`** (~2.2 GB – 3.2 GB raw JSON, ~35,000 to 60,000 deep network events for focus countries: Pakistan `PK`, Iran `IR`, Russia `RU`).
* **Daily Incremental Load**: **~25 MB to 50 MB compressed / day** (~140 MB – 270 MB raw text, ~2,000 to 5,000 events / day).
* **Late-Arriving Reconciliation**: Incremental loads scan a **3-day rolling lookback window ($T-3$ to $T$)** using Delta Lake `MERGE INTO`, reconciling late submissions from offline mobile probes and updating enriched taxonomy with zero duplicate rows.
* **Instructor Runtime Mandate (Zero Manual Uploads)**: Under course specifications, no data is manually uploaded via the Databricks UI or local upload widgets. All raw telemetry and target taxonomy are fetched programmatically within Databricks notebooks using public REST endpoints (OONI Measurement API and Citizen Lab GitHub raw API).
* **Empirical Validation**: The complete end-to-end Medallion pipeline (Bronze $\rightarrow$ Silver $\rightarrow$ Gold) has been executed and verified on live Databricks Serverless Compute (Photon Engine) under Unity Catalog Volumes (`/Volumes/workspace/default/panopticon/`) across **63 logged executions** (Master Orchestration Run `976063331961030`). The pipeline ingested 1,000 raw OONI probe measurements, parsed and conformed 5,535 records across Silver child tables, and built 1,717 Gold dimensional records (Star Schema fact/dims and CAI severity marts) with **82.5% strict ACID commit success** and **$0.00 student cloud spend**.

---

## 📂 Repository Structure

```text
Project-Panopticon/
├── README.md                                  # Executive lakehouse guide & project overview
├── .gitignore                                 # Production environment & secret ignore rules
├── AGENTS.md                                  # Operational guidelines & developer practices
├── panopticon_remediation_plan.md             # Phased engineering roadmap & compliance tracking
├── panopticon_adversarial_review.md           # Senior staff architectural review & threat models
├── bronze_layer_engineering_audit.md          # 12-dimension technical audit of Bronze ingestion
├── silver_layer_engineering_audit.md          # 53-dimension technical audit of Silver layer
├── dashboards/                                # Production Power BI Watchtower developer project
│   ├── Panopticon_Dashboard.pbip              # Power BI Desktop Project entry point
│   ├── Panopticon_Dashboard.Report/           # Visual layout, containers & executive theme
│   │   ├── definition.pbir                    # Report definition binding
│   │   └── report.json                        # 4-page executive surveillance dashboard layout
│   └── Panopticon_Dashboard.SemanticModel/    # VertiPaq Star Schema data model & DAX metrics
│       ├── definition.pbism                   # Semantic model binding
│       └── model.bim                          # Tabular schema, M partitions, relationships & DAX
├── Docs/                                      # Structured documentation suite
│   ├── .docx/                                 # Word documents for academic submission
│   │   └── phase1_proposal.docx               # Executive Word Document Version
│   ├── Phase 1/                               # Phase 1: Inception & Domain Proposal
│   │   └── phase1_proposal.md                 # Formal Phase 1 Project Proposal (with Figures 3a–3d)
│   ├── Phase 2/                               # Phase 2: Ingestion, Silver & Governance
│   │   ├── phase2_implementation_plan.md      # Technical blueprint & instructor requirements
│   │   ├── 06_data_dictionary.md              # Comprehensive Bronze, Silver & Gold Data Dictionaries
│   │   └── 07_execution_guide.md              # Parameterized execution & backfill manual
│   ├── Future & Reference/                    # Architecture, Curriculum & Defense Guides
│   │   ├── 01_architectures_and_tech_stacks.md # Architecture & Cloud Trade-Off Evaluation
│   │   ├── 02_phased_implementation_and_work_division.md # Sprint Deliverables & Work Division
│   │   ├── 03_core_data_engineering_curriculum.md # 8 Core DE Lakehouse Superpowers
│   │   ├── 04_interview_guide_and_academic_defense.md # STAR Stories & Defense Q&A
│   │   └── 05_deployment_guide_and_troubleshooting.md # Operations Manual & OOM Mitigation
│   ├── finops_cost_evaluation.md              # Final FinOps, Architecture & Spark Execution Report
│   └── assets/                                # Architectural & schema diagram assets
│       ├── architecture_diagram.png           # End-to-end Medallion architecture visual
│       ├── star_schema_diagram.png            # Dimensional Star Schema ER diagram
│       ├── dashboard_mockup.png               # Production Power BI Watchtower capture
│       ├── powerbi_page1_executive_watchtower.png # Page 1: Sovereign Executive Watchtower
│       ├── powerbi_page2_protocol_forensics.png   # Page 2: Protocol Interdiction & DPI Forensics
│       ├── powerbi_page3_circadian_curfew.png     # Page 3: Circadian Temporal Anomaly & Curfew
│       └── powerbi_page4_lakehouse_finops.png     # Page 4: Delta Lakehouse FinOps & Audit Ledger
├── data/
│   ├── powerbi_export/                        # Refreshed Gold & Audit CSV data for Power BI
│   │   ├── fact_network_measurement.csv       # Complete 1:1 measurement event fact (1,000 rows)
│   │   ├── fact_network_anomaly.csv           # Filtered anomalous interdictions (529 rows)
│   │   ├── agg_daily_censorship_severity.csv  # Precomputed daily CAI mart
│   │   ├── agg_hourly_censorship_severity.csv # Precomputed hourly diurnal mart
│   │   ├── dim_country_region.csv             # UN Geoscheme & RSF Press Freedom index
│   │   ├── dim_isp_telecom.csv                # Autonomous System ISP classification
│   │   ├── dim_content_category.csv           # Citizen Lab content categories & risk tiers
│   │   ├── dim_date.csv                       # Conformed calendar dimension
│   │   ├── dim_date_time.csv                  # 24-hour diurnal grid dimension
│   │   └── pipeline_execution_logs.csv        # Centralized audit trail (63 logged batches)
│   ├── samples/                               # Lightweight local fixtures for testing & CI
│   │   ├── sample_full_load_ooni.json         # Raw telemetry fixture (~222 KB, 15 records)
│   │   ├── sample_incremental_ooni.json       # Incremental telemetry fixture (~865 KB, 10 records)
│   │   └── sample_citizenlab_categories.csv   # Citizen Lab URL taxonomy (~68 KB, 670+ records)
│   └── schemas/                               # Machine-readable PySpark StructType contracts
│       ├── bronze_ooni_schema.json            # Bronze strict Schema-on-Read contract
│       ├── silver_conformed_schema.json       # Silver conformed 1:1 measurement contract
│       ├── silver_dns_experiments_schema.json # Silver normalized DNS queries & answers
│       ├── silver_tcp_experiments_schema.json # Silver normalized TCP connect attempts
│       ├── silver_app_anomaly_schema.json     # Silver normalized app probe telemetry
│       └── ref_citizenlab_categories_schema.json # Citizen Lab reference taxonomy contract
├── notebooks/                                 # Implementation PySpark pipelines
│   ├── 00_config.py                           # Central configuration, DBFS/local paths & API endpoints
│   ├── 00_audit_logger.py                     # Context manager & operational Delta logger
│   ├── 01_bronze_ingestion.py                 # Schema-on-read ingestion + quarantine + metadata
│   ├── 02_silver_transformation.ipynb         # PII masking, array flattening & Delta MERGE
│   ├── 03_gold_dimensional_modeling.ipynb     # Star Schema dimensional modeling & Z-ORDER marts
│   ├── 04_master_pipeline.ipynb               # Master end-to-end pipeline orchestrator
│   └── run_pipeline.py                        # Standalone CLI pipeline runner
└── tests/                                     # Comprehensive testing & validation suite
    ├── test_phase1_bronze_isolation.py        # 33 unit/integration tests (local & Databricks runnable)
    └── troubleshooting/                       # Databricks Genie audit & serverless migration notes
        └── serverless-test-migration.md       # Root-cause analysis & Genie test enhancements
```

---

## 📊 Medallion Data Models Summary

### Bronze Layer: `bronze_ooni_raw` (Raw Ingestion Landing)
* **Storage**: Delta Lake (Snappy Compressed) | `PARTITIONED BY (probe_cc, measurement_date)` | Target path: `/Volumes/workspace/default/panopticon/bronze/bronze_ooni_raw` (Databricks Serverless UC) or `/FileStore/tables/panopticon/bronze/bronze_ooni_raw` (Community Edition DBFS)
* **Idempotency & Isolation**:
  - **Partition Overwrite**: Scoped `replaceWhere (probe_cc = '<country_code>' AND measurement_date = '<target_date>')` guarantees that historical dates and other countries are never accidentally erased during incremental writes.
  - **Delta MERGE INTO**: Keyed merge with ANSI NULL-safe `<=>` matching on composite natural keys (`report_id`, `input`, `measurement_start_time`) with deterministic window deduplication (`_panopticon_rank`).
  - **Staging Isolation**: Raw JSON reading is explicitly scoped by country glob (`ooni_{clean_cc}_*.json`) preventing cross-country file bleeding when staging multiple countries under the same date partition.
  - **Ingestion Manifest**: Programmatic REST API fetcher produces an authoritative `IngestionManifest` sidecar (`_manifest.json`) recording payload SHA-256 checksums, byte sizes, and record counts. Manifest validation is hoisted before table existence checks to guarantee strict data integrity.
* **Core Columns (29 total)**:
  - Natural Telemetry (25): `measurement_uid` (String), `report_id` (String), `input` (String), `test_name` (String), `measurement_start_time` (String), `test_start_time` (String), `probe_asn` (String), `probe_cc` (String - Partition Key), `probe_id` (String), `probe_ip` (String), `probe_network_name` (String), `resolver_asn` (String), `resolver_ip` (String), `resolver_network_name` (String), `test_runtime` (Double), `software_name` (String), `software_version` (String), `test_version` (String), `data_format_version` (String), `annotations` (String), `test_helpers` (String), `api_anomaly` (Boolean), `api_confirmed` (Boolean), `extensions` (String), `test_keys` (**StringType — raw JSON string**; polymorphic union type parsed per `test_name` in Silver).
  - Derived Partition Key (1): `measurement_date` (Date - Partition Key).
  - Ingestion Lineage & Audit (3): `load_timestamp` (Timestamp - Audit), `_source_file` (String - Audit), `_batch_id` (String - Audit).
  - Permissive Trap Column (Pre-Quarantine): `_corrupt_record` (String; diverted to `quarantine_ooni_raw`).

### Silver Layer: `silver_network_measurements` (Cleansed, Sanitized, Enriched)
* **Storage**: Delta Lake (Snappy Compressed) | `PARTITIONED BY (measurement_date)` | `ZORDER BY (probe_cc, probe_asn, tampering_vector)`
* **Primary Key**: `measurement_id` (`coalesce(measurement_uid, sha2(concat_ws('||', probe_cc, measurement_date, report_id, measurement_start_time, test_name, coalesce(input, 'NO_INPUT')), 256))`).
* **Strict 1:1 Grain**: Exactly one row per network measurement probe. Multi-query DNS arrays, TCP connects, and messaging endpoints are projected into dedicated child tables (`silver_dns_experiments`, `silver_tcp_experiments`, `silver_app_anomaly_measurements`) to eliminate Cartesian fan-out and prevent `DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE` crashes.
* **CDC & Reconciliation**: Maintained via idempotent Delta Lake `MERGE INTO` operations scanning a 3-day rolling lookback window ($T-3$ to $T$) with partition pruning (`target.measurement_date = source.measurement_date`). Inserts late-arriving measurements (`WHEN NOT MATCHED`) and updates enriched taxonomy attributes (`WHEN MATCHED`, using NULL-safe `<=>` comparisons on `content_category`, `tampering_vector`, `is_anomaly`, `classification_status`) with zero write amplification and zero row duplication.
* **Core Columns (32 total)**: `measurement_id` (String - PK), `measurement_uid` (String), `report_id` (String), `event_timestamp` (Timestamp UTC), `measurement_date` (Date - Partition Key), `target_url` (String - Sanitized), `target_domain` (String), `probe_asn` (Long - `0L` fallback), `probe_cc` (String), `probe_network_name` (String), `hashed_resolver_ip` (String - Peppered SHA-256), `resolver_asn` (Long), `test_name` (String), `ooni_anomaly` (Boolean), `ooni_confirmed` (Boolean), `is_anomaly` (Boolean - Multi-Signal), `classification_status` (String: `CONFIRMED_BLOCKED`, `HEURISTIC_ANOMALY`, `BENIGN`, `UNCLASSIFIED`), `tampering_vector` (String: `DNS_TAMPERING`, `TCP_RESET`, `TLS_DROP`, `HTTP_BLOCK`, `BENIGN`, `ANOMALY_UNCLASSIFIED`, `UNCLASSIFIED`), `dns_anomaly_flag` (Integer), `tcp_anomaly_flag` (Integer), `tls_anomaly_flag` (Integer), `http_anomaly_flag` (Integer), `dns_experiment_failure` (String), `http_experiment_failure` (String), `test_runtime_seconds` (Double), `content_category` (String), `category_description` (String), `category_match_type` (String: `EXACT_URL`, `DOMAIN_FALLBACK`, `UNMATCHED`), `taxonomy_version` (String), `human_rights_risk_tier` (String: `HIGH`, `MEDIUM`, `LOW`), `load_timestamp` (Timestamp - Audit), `_batch_id` (String - Audit).
* **Child Tables**:
  - `silver_dns_experiments`: Exploded DNS queries, answers, bogon detection, and resolver tamper flags (`dns_experiment_id` PK).
  - `silver_tcp_experiments`: Exploded TCP connection attempts, destination ports, and reset flags (`tcp_experiment_id` PK).
  - `silver_app_anomaly_measurements`: Exploded non-web app probes (WhatsApp, Telegram, Signal, Tor, Psiphon) (`app_measurement_id` PK).

### Gold Layer: Star Schema & Censorship Aggression Marts
* **Storage**: Delta Lake (Snappy Compressed) | Co-located and clustered via `OPTIMIZE ... ZORDER BY (country_code, date_key, asn_id)`
* **Fact Tables**:
  - `fact_network_measurement`: Complete 1:1 grain per probe measurement (1,000 rows in primary benchmark load), preserving benign baseline measurements for un-distorted Censorship Aggression Index (CAI) calculation.
  - `fact_network_anomaly`: Filtered anomalous interdiction events (529 rows), enriched with DPI attack vectors, HTTP status codes, and test runtimes.
* **Dimension Tables**:
  - `dim_country_region`: ISO 3166-1 alpha-2 country codes, UN geoschemes, and RSF Press Freedom Index tiers.
  - `dim_isp_telecom`: Autonomous System Numbers (ASN), provider brand names, and state-owned vs. private ownership classification.
  - `dim_content_category`: 31 Citizen Lab category codes (`NEWS`, `POLITICAL_OPPOSITION`, `VPN`), sensitivity descriptions, and legal risk tiers (`HIGH`, `MEDIUM`, `LOW`).
  - `dim_date`: Conformed calendar dimension with ISO dates, quarters, months, and weekend indicators.
  - `dim_date_time`: Full 24-hour diurnal grid dimension (`dates × 24 hours`) ensuring blackout periods register as absence of signal rather than missing rows.
* **Pre-Aggregated Analytical Marts**:
  - `agg_daily_censorship_severity`: Daily rollups computing the Censorship Aggression Index ($\text{CAI} = \frac{\text{Tampered}}{\text{Total}} \times 100$) sliced by Country, Carrier, and Content Category.
  - `agg_hourly_censorship_severity`: Hourly diurnal rollups capturing state-enforced nocturnal curfews and anomaly spikes.

### Operational Logs: `pipeline_execution_logs` (Governance & Auditing)
* **Storage**: Delta Lake (**MERGE-based write pattern** — not append-only; each pipeline run opens with a `status='Running'` insert, then MERGEs final metrics on `log_id` at completion, ensuring exactly one row per run)
* **Primary Key**: `log_id` (UUID)
* **Audit Columns**: `log_id` (String), `pipeline_layer` (String), `batch_parameter` (String), `load_type` (String: `Full`, `Incremental`, `Backfill`), `start_time` (Timestamp), `end_time` (Timestamp), `duration_seconds` (Double), `status` (String: `Success`, `Failure`, `Partial_Quarantine`), `rows_read` (Long), `rows_inserted` (Long), `rows_updated` (Long), `rows_quarantined` (Long), `error_message` (String).

> *For complete field-by-field definitions, constraints, and lineages, refer to [Docs/Phase 2/06_data_dictionary.md](Docs/Phase%202/06_data_dictionary.md).*

---

## 🔐 Data Security & PII Governance
Because probe runners include investigative journalists, civil dissidents, and human rights defenders under authoritarian regimes, privacy is enforced at the **Bronze $\rightarrow$ Silver boundary**:
1. **Cryptographic Peppered Hashing**: DNS resolver IP addresses (`resolver_ip`) are pseudonymized using `sha2(concat(resolver_ip, PEPPER_SALT), 256)` (peppered SHA-256 hash, not HMAC) to preserve longitudinal ISP resolver correlation while guaranteeing zero client PII leakage.
2. **PII Data Minimization & Identifier Purging**: OONI client `probe_id`, loopback IP (`probe_ip` — redacted by OONI to `127.0.0.1`), and raw protocol dump strings (`test_keys`) are explicitly excluded from Silver tables to minimize threat exposure. Raw `resolver_address` is also excluded from child DNS tables.
3. **URL Query-String & Token Cleansing**: Target URLs are scrubbed with regex validators to strip authentication tokens, session cookies, and tracking credentials (`?token=...`, `&session_id=...`).
4. **GDPR Right-to-be-Forgotten**: Delta Lake ACID deletion vectors allow targeted removal of individual anonymized probe records without rebuilding entire historical partitions.

---

## 📈 Production Power BI Surveillance Watchtower Suite

The Gold dimensional lakehouse directly powers a 4-page interactive Power BI Watchtower suite ([dashboards/Panopticon_Dashboard.pbip](dashboards/Panopticon_Dashboard.pbip)) configured with an executive high-contrast theme:

1. **Page 1: Sovereign Executive Watchtower**: Live CAI Defcon Meter (9.90%), Carrier Strategic Intentionality Scatter Matrix (Surgical Censorship vs. Collateral Disruption), Protocol Interdiction Vector Donut, and Societal Vulnerability Treemap.
2. **Page 2: Protocol Interdiction & DPI Forensics**: 100% Stacked DPI Fingerprint across Carriers, Human Rights Risk Tier Funnel (282 Low $\rightarrow$ 176 High $\rightarrow$ 71 Medium), Attack Matrix, and Atomic Fact Evidence Stream.
3. **Page 3: Circadian Temporal Anomaly & Curfew Waveform**: Dual-axis Diurnal Waveform (Total Probes vs. Tampered Interdictions) and Nocturnal Cumulative Anomaly Curve exposing state-ordered curfew spikes (21:00–02:00 PKT).
4. **Page 4: Delta Lakehouse Architecture & FinOps Audit**: Pipeline Latency Waterfall (Bronze $\rightarrow$ Silver $\rightarrow$ Gold), Medallion Layer Ingestion Throughput (3K Bronze, 5.5K Silver, 1.7K Gold), ACID Commit Verification (82.5%), and $0.00 Serverless Cost Governance.

---

## 📚 Comprehensive Project Documentation

All formal technical documentation and implementation manuals are organized in the [Docs/](Docs/) directory:

| Folder | Document | Description & Contents |
| :--- | :--- | :--- |
| **Phase 1** | [**Docs/Phase 1/phase1_proposal.md**](Docs/Phase%201/phase1_proposal.md) | **Formal Phase 1 Academic Proposal** (Course requirements, Sizing, Medallion modeling, Figures 3a–3d). |
| **Classroom** | [**Docs/.docx/phase1_proposal.docx**](Docs/.docx/phase1_proposal.docx) | **Executive Word Document Version** with high-resolution visual diagrams and tables. |
| **Phase 2** | [**Docs/Phase 2/phase2_implementation_plan.md**](Docs/Phase%202/phase2_implementation_plan.md) | **Phase 2 Technical Blueprint** (Instructor deliverables, engineering milestones). |
| **Phase 2** | [**Docs/Phase 2/06_data_dictionary.md**](Docs/Phase%202/06_data_dictionary.md) | **Enterprise Data Dictionary** (Bronze, Silver, Gold, Quarantine & Log schema contracts). |
| **Phase 2** | [**Docs/Phase 2/07_execution_guide.md**](Docs/Phase%202/07_execution_guide.md) | **Parameterized Execution & Backfill Runbook** (CLI/Widget parameters & Delta MERGE). |
| **FinOps** | [**Docs/finops_cost_evaluation.md**](Docs/finops_cost_evaluation.md) | **Final Architecture, Spark Execution & FinOps Report** (63-run empirical audit & $0.00 spend). |
| **Reference** | [**Docs/Future & Reference/01_architectures_and_tech_stacks.md**](Docs/Future%20&%20Reference/01_architectures_and_tech_stacks.md) | **Architecture & Tech Stack Evaluation** (Databricks vs. Local vs. Azure trade-offs). |
| **Reference** | [**Docs/Future & Reference/02_phased_implementation_and_work_division.md**](Docs/Future%20&%20Reference/02_phased_implementation_and_work_division.md) | **Phased Sprint Plan & Teammate Work Division** (Tasks for Muhammad & Safee). |
| **Reference** | [**Docs/Future & Reference/03_core_data_engineering_curriculum.md**](Docs/Future%20&%20Reference/03_core_data_engineering_curriculum.md) | **8 Core Data Engineering Superpowers** (PySpark JSON explode, Delta CDC, Z-Order). |
| **Reference** | [**Docs/Future & Reference/04_interview_guide_and_academic_defense.md**](Docs/Future%20&%20Reference/04_interview_guide_and_academic_defense.md) | **Interview & Defense Guide** (STAR story, resume bullet points, 5 tough Q&A answers). |
| **Reference** | [**Docs/Future & Reference/05_deployment_guide_and_troubleshooting.md**](Docs/Future%20&%20Reference/05_deployment_guide_and_troubleshooting.md) | **Production Operations Manual** (Databricks setup, OOM mitigation, S3 503 fix). |

---

## ⚡ Quickstart Execution

### 1. Interactive Master Pipeline (Databricks Serverless / Community Edition)
Open `notebooks/04_master_pipeline.ipynb` in your Databricks workspace and click **Run All**:
- Configurable interactive widgets: `batch_date`, `load_type` (`full`, `incremental`, `backfill`), `country_code` (`PK`, `IR`, `RU`), `max_records`, and `lookback_days`.
- Automatically orchestrates Stage 1 (Bronze Ingestion), Stage 2 (Silver Transformation), and Stage 3 (Gold Dimensional Modeling) with centralized audit logging in `pipeline_execution_logs`.

### 2. Standalone CLI Execution (Local & Automation)
Pipelines can also be triggered via terminal CLI arguments using the standalone runner:

```bash
# Standard Daily Incremental Execution
python notebooks/run_pipeline.py \
    --batch-date "2026-09-28" \
    --load-type "incremental" \
    --country-code "PK"

# Parameterized Historical Backfill
python notebooks/run_pipeline.py \
    --batch-date "2026-09-22" \
    --load-type "backfill" \
    --country-code "IR"
```

### 3. Running the Test Suite (33 Unit & Integration Tests)
The test suite validates schema enforcement, partition safety, manifest integrity, and isolation contracts:
```bash
# Run offline unit tests locally without PySpark (< 0.05s)
python -m unittest tests/test_phase1_bronze_isolation.py

# On Databricks Community Edition / Serverless:
# Open tests/test_phase1_bronze_isolation.py directly as a notebook and click "Run All"
```

> *For complete step-by-step instructions on Databricks Community Edition and local run environments, see [Docs/Phase 2/07_execution_guide.md](Docs/Phase%202/07_execution_guide.md).*
