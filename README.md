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

---

## 📂 Repository Structure

```text
Project-Panopticon/
├── README.md                                  # Executive lakehouse guide & project overview
├── .gitignore                                 # Production environment & secret ignore rules
├── Docs/                                      # Structured documentation suite
│   ├── .docx/                                 # Word documents for academic submission
│   │   └── phase1_proposal.docx               # Executive Word Document Version
│   ├── Phase 1/                               # Phase 1: Inception & Domain Proposal
│   │   └── phase1_proposal.md                 # Formal Phase 1 Project Proposal
│   ├── Phase 2/                               # Phase 2: Ingestion, Silver & Governance
│   │   ├── phase2_implementation_plan.md      # Technical blueprint & instructor requirements
│   │   ├── 06_data_dictionary.md              # Comprehensive Bronze & Silver Data Dictionaries
│   │   └── 07_execution_guide.md              # Parameterized execution & backfill manual
│   ├── Future & Reference/                    # Architecture, Curriculum & Defense Guides
│   │   ├── 01_architectures_and_tech_stacks.md # Architecture & Cloud Trade-Off Evaluation
│   │   ├── 02_phased_implementation_and_work_division.md # Sprint Deliverables & Work Division
│   │   ├── 03_core_data_engineering_curriculum.md # 8 Core DE Lakehouse Superpowers
│   │   ├── 04_interview_guide_and_academic_defense.md # STAR Stories & Defense Q&A
│   │   └── 05_deployment_guide_and_troubleshooting.md # Operations Manual & OOM Mitigation
│   └── assets/                                # Architectural & schema diagram assets
│       ├── architecture_diagram.png           # End-to-end Medallion architecture visual
│       └── star_schema_diagram.png            # Dimensional Star Schema ER diagram
├── data/
│   ├── samples/                               # Lightweight local fixtures for testing & CI
│   │   ├── sample_full_load_ooni.json         # Raw telemetry fixture (~222 KB, 15 records)
│   │   ├── sample_incremental_ooni.json       # Incremental telemetry fixture (~865 KB, 10 records)
│   │   └── sample_citizenlab_categories.csv   # Citizen Lab URL taxonomy (~68 KB, 670+ records)
│   └── schemas/                               # Machine-readable PySpark StructType contracts
│       ├── bronze_ooni_schema.json            # Bronze strict Schema-on-Read contract
│       └── silver_conformed_schema.json       # Silver cleansed & enriched schema contract
└── notebooks/                                 # Implementation PySpark pipelines
    ├── 00_audit_logger.py                     # Context manager & operational Delta logger
    ├── 01_bronze_ingestion.py                 # Schema-on-read ingestion + quarantine + metadata
    ├── 02_silver_transformation.py            # PII masking, array flattening & Delta MERGE
    ├── 03_gold_dimensional_modeling.py       # Star Schema dimensional modeling & rollups
    └── run_pipeline.py                        # Parameterized runner (Incremental vs Backfill)
```

---

## 📊 Medallion Data Models Summary

### Bronze Layer: `bronze_ooni_raw` (Raw Ingestion Landing)
* **Storage**: Delta Lake (Snappy Compressed) | `PARTITIONED BY (measurement_date)`
* **Primary Key**: None (append-only immutable raw landing)
* **Core Columns**: `report_id` (String), `input` (String), `test_name` (String), `measurement_start_time` (String), `probe_asn` (String), `probe_cc` (String), `probe_ip` (String), `probe_network_name` (String), `resolver_asn` (String), `resolver_ip` (String), `test_runtime` (Double), `test_keys` (StructType), `load_timestamp` (Timestamp - Audit), `_source_file` (String - Audit), `_batch_id` (String - Audit), `_corrupt_record` (String - Quarantine).

### Silver Layer: `silver_network_measurements` (Cleansed, Sanitized, Enriched)
* **Storage**: Delta Lake (Snappy Compressed) | `PARTITIONED BY (measurement_date)` | `ZORDER BY (probe_cc, probe_asn, tampering_vector)`
* **Primary Key**: `measurement_id` (Deterministic SHA-256 of `report_id || input || measurement_start_time`)
* **CDC & Reconciliation**: Maintained via idempotent Delta Lake `MERGE INTO` operations scanning a 3-day rolling lookback window ($T-3$ to $T$). Inserts late-arriving measurements from offline/mobile probes (`WHEN NOT MATCHED`) and updates enriched taxonomy attributes (`WHEN MATCHED`) with zero row duplication.
* **Core Columns**: `measurement_id` (String - PK), `report_id` (String), `event_timestamp` (Timestamp UTC), `measurement_date` (Date), `target_url` (String - Sanitized), `target_domain` (String), `probe_asn` (Long), `probe_cc` (String), `probe_network_name` (String), `masked_probe_subnet` (String - `/24` Mask), `hashed_resolver_ip` (String - Salted HMAC-SHA256), `resolver_asn` (Long), `test_name` (String), `tampering_vector` (String: `DNS_TAMPERING`, `TCP_RESET`, `TLS_DROP`, `HTTP_BLOCK`, `BENIGN`), `is_anomaly` (Boolean), `dns_anomaly_flag` (Integer), `tcp_anomaly_flag` (Integer), `tls_anomaly_flag` (Integer), `http_anomaly_flag` (Integer), `duration_seconds` (Double), `content_category` (String), `category_description` (String), `human_rights_risk_tier` (String: `HIGH`, `MEDIUM`, `LOW`), `load_timestamp` (Timestamp - Audit), `_batch_id` (String - Audit).

### Operational Logs: `pipeline_execution_logs` (Governance & Auditing)
* **Storage**: Delta Lake (Append-Only Operational Audit Log)
* **Primary Key**: `log_id` (UUID)
* **Audit Columns**: `log_id` (String), `pipeline_layer` (String), `batch_parameter` (String), `load_type` (String: `Full`, `Incremental`, `Backfill`), `start_time` (Timestamp), `end_time` (Timestamp), `duration_seconds` (Double), `status` (String: `Success`, `Failure`, `Partial_Quarantine`), `rows_read` (Long), `rows_inserted` (Long), `rows_updated` (Long), `rows_quarantined` (Long), `error_message` (String).

> *For complete field-by-field definitions, constraints, and lineages, refer to [Docs/Phase 2/06_data_dictionary.md](Docs/Phase%202/06_data_dictionary.md).*

---

## 🔐 Data Security & PII Governance
Because probe runners include investigative journalists, civil dissidents, and human rights defenders under authoritarian regimes, privacy is enforced at the **Bronze $\rightarrow$ Silver boundary**:
1. **Cryptographic Salted Hashing**: DNS resolver IP addresses (`resolver_ip`) are pseudonymized using `HMAC-SHA256(resolver_ip + PEPPER_SALT)` to preserve longitudinal analysis while guaranteeing zero PII leakage.
2. **Subnet Masking**: Client IPv4 addresses (`probe_ip`) are truncated to `/24` subnets (`192.168.1.0/24`) to eliminate household identification.
3. **URL Query-String Cleansing**: Target URLs are scrubbed with regex validators to strip authentication tokens, session cookies, and tracking IDs (`?token=...`, `&session_id=...`).
4. **GDPR Right-to-be-Forgotten**: Delta Lake ACID deletion vectors allow targeted removal of individual anonymized probe records without rebuilding entire historical partitions.

---

## 📈 Key Dashboards & Business Questions
1. **Regulatory Enforcement Latency**: Which ISPs comply immediately with state ban directives vs. which ones delay, resist, or lack technical DPI infrastructure?
2. **Surgical Censorship vs. Collateral Damage**: Measuring the empirical ratio of targeted political blocks to unintended disruptions in domestic banking and e-commerce infrastructure.
3. **Attack Vector Fingerprints**: Categorizing Layer 3/4 TCP reset injections vs. Layer 7 DNS/TLS tampering to reveal the sophistication of the national censorship apparatus.
4. **Crisis Timeline & Blackout Predictor**: Rolling Z-score anomaly detection to forecast nationwide blackouts before telecommunications go completely dark.

---

## 📚 Comprehensive Project Documentation

All formal technical documentation and implementation manuals are organized in the [Docs/](Docs/) directory:

| Folder | Document | Description & Contents |
| :--- | :--- | :--- |
| **Phase 1** | [**Docs/Phase 1/phase1_proposal.md**](Docs/Phase%201/phase1_proposal.md) | **Formal Phase 1 Academic Proposal** (Course requirements, Sizing, Medallion modeling). |
| **Classroom** | [**Docs/.docx/phase1_proposal.docx**](Docs/.docx/phase1_proposal.docx) | **Executive Word Document Version** with high-resolution visual diagrams and tables. |
| **Phase 2** | [**Docs/Phase 2/phase2_implementation_plan.md**](Docs/Phase%202/phase2_implementation_plan.md) | **Phase 2 Technical Blueprint** (Instructor deliverables, engineering milestones). |
| **Phase 2** | [**Docs/Phase 2/06_data_dictionary.md**](Docs/Phase%202/06_data_dictionary.md) | **Enterprise Data Dictionary** (Bronze, Silver, Quarantine & Log schema contracts). |
| **Phase 2** | [**Docs/Phase 2/07_execution_guide.md**](Docs/Phase%202/07_execution_guide.md) | **Parameterized Execution & Backfill Runbook** (CLI/Widget parameters & Delta MERGE). |
| **Reference** | [**Docs/Future & Reference/01_architectures_and_tech_stacks.md**](Docs/Future%20&%20Reference/01_architectures_and_tech_stacks.md) | **Architecture & Tech Stack Evaluation** (Databricks vs. Local vs. Azure trade-offs). |
| **Reference** | [**Docs/Future & Reference/02_phased_implementation_and_work_division.md**](Docs/Future%20&%20Reference/02_phased_implementation_and_work_division.md) | **Phased Sprint Plan & Teammate Work Division** (Tasks for Muhammad & Safee). |
| **Reference** | [**Docs/Future & Reference/03_core_data_engineering_curriculum.md**](Docs/Future%20&%20Reference/03_core_data_engineering_curriculum.md) | **8 Core Data Engineering Superpowers** (PySpark JSON explode, Delta CDC, Z-Order). |
| **Reference** | [**Docs/Future & Reference/04_interview_guide_and_academic_defense.md**](Docs/Future%20&%20Reference/04_interview_guide_and_academic_defense.md) | **Interview & Defense Guide** (STAR story, resume bullet points, 5 tough Q&A answers). |
| **Reference** | [**Docs/Future & Reference/05_deployment_guide_and_troubleshooting.md**](Docs/Future%20&%20Reference/05_deployment_guide_and_troubleshooting.md) | **Production Operations Manual** (Databricks setup, OOM mitigation, S3 503 fix). |

---

## ⚡ Quickstart Execution

### Triggering Daily Incremental Run vs. Historical Backfill
Pipelines support execution via **Databricks Widgets** or terminal **CLI arguments**:

```bash
# Standard Daily Incremental Execution
python notebooks/run_pipeline.py \
    --batch-date "2026-09-28" \
    --load-type "incremental"

# Parameterized Historical Backfill
python notebooks/run_pipeline.py \
    --batch-date "2026-09-22" \
    --load-type "backfill"
```

> *For complete step-by-step instructions on Databricks Community Edition and local run environments, see [Docs/Phase 2/07_execution_guide.md](Docs/Phase%202/07_execution_guide.md).*
