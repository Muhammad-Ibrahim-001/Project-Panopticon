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
  * **Muhammad Ibrahim** (24L-2602)
  * **Safee Akmal** (23L-2556)

---

## 🌐 Project Overview
**Project Panopticon** is an enterprise-scale distributed data engineering pipeline designed on the **Medallion Architecture (Bronze -> Silver -> Gold)** using **Apache Spark** and **Delta Lake**. 

The system continuously ingests, sanitizes, and analyzes multi-gigabyte streams of global internet censorship telemetry to identify state-sponsored cyber warfare, Deep Packet Inspection (DPI) attacks, DNS tampering, and internet blackouts across 200+ countries.

---

## 📂 Repository Structure

`	ext
├── README.md
├── Docs/
│   ├── phase1_proposal.md                   # Formal Phase 1 Project Proposal
│   └── phase1_proposal_panopticon.md        # Detailed Proposal & Architecture
└── data/
    └── samples/
        ├── sample_full_load_ooni.json       # Full Load raw telemetry payload (~222 KB, 15 records)
        ├── sample_incremental_ooni.json    # Incremental Load delta payload (~865 KB, 10 records)
        └── sample_citizenlab_categories.csv # Citizen Lab Pakistan & Global URL taxonomy (~68 KB)
`

---

## 🔐 Data Security & PII Governance
Because probe runners include investigative journalists and citizens under restrictive regimes, privacy is paramount:
1. **Cryptographic Salted Hashing**: Client IP addresses (probe_ip) and local DNS resolvers (
esolver_ip) are pseudonymized using HMAC-SHA256 before writing to the Silver layer.
2. **Subnet Masking**: IPv4 addresses are truncated to /24 subnets (192.168.1.0/24).
3. **URL Cleansing**: Session tokens and credentials in tested URLs are scrubbed with regex validators.

---

## 📈 Key Dashboards & Business Questions
1. **Regulatory Enforcement Latency**: Which ISPs comply immediately with state ban directives vs. which ones delay or resist?
2. **Surgical Censorship vs. Collateral Damage**: Measuring the ratio of targeted political blocks to unintended disruptions in domestic banking and e-commerce.
3. **Attack Vector Fingerprints**: Categorizing Layer 3/4 TCP reset injections vs. Layer 7 DNS/TLS tampering.
4. **Crisis Timeline & Blackout Predictor**: Rolling Z-score anomaly detection to forecast nationwide blackouts before communications go dark.

---

## 📚 Comprehensive Project Documentation

All formal technical documentation and implementation manuals are organized in the [Docs/](Docs/) directory:

| Document | Description & Contents |
| :--- | :--- |
| [**Docs/phase1_proposal.md**](Docs/phase1_proposal.md) | **Formal Phase 1 Academic Proposal** (Course requirements, Sizing, Medallion modeling). |
| [**Docs/phase1_proposal.docx**](Docs/phase1_proposal.docx) | **Executive Word Document Version** with high-resolution visual diagrams and tables. |
| [**Docs/01_architectures_and_tech_stacks.md**](Docs/01_architectures_and_tech_stacks.md) | **Architecture & Tech Stack Evaluation** (Databricks vs. Local vs. Azure trade-offs). |
| [**Docs/02_phased_implementation_and_work_division.md**](Docs/02_phased_implementation_and_work_division.md) | **Phased Sprint Plan & Teammate Work Division** (Tasks for Muhammad & Safee). |
| [**Docs/03_core_data_engineering_curriculum.md**](Docs/03_core_data_engineering_curriculum.md) | **8 Core Data Engineering Superpowers** (PySpark JSON explode, Delta CDC, Z-Order). |
| [**Docs/04_interview_guide_and_academic_defense.md**](Docs/04_interview_guide_and_academic_defense.md) | **Interview & Defense Guide** (STAR story, resume bullet points, 5 tough Q&A answers). |
| [**Docs/05_deployment_guide_and_troubleshooting.md**](Docs/05_deployment_guide_and_troubleshooting.md) | **Production Operations Manual** (Databricks setup, OOM mitigation, S3 503 fix). |

