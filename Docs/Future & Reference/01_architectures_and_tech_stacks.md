# Architecture Options & Technology Stack Evaluation
## Project Panopticon: Global Cyber Warfare & Internet Censorship Lakehouse

---

### Executive Overview
Choosing the right architecture and technology stack is the most critical decision in a modern data engineering pipeline. A poor architecture leads to cluster crashes, runaway cloud costs, and high latency. This document formally evaluates **three distinct architectural blueprints**, compares their trade-offs, and provides the technical justification for the chosen design.

---

## 1. Architectural Options Comparison

```text
+---------------------------------------------------------------------------------------------------------+
| ARCHITECTURE 1: Cloud-Native Databricks Delta Lakehouse (Recommended & Selected)                        |
|   Storage: AWS S3 Open Data / DBFS  -->  Compute: Apache Spark 3.5  -->  Format: Delta Lake ACID       |
+---------------------------------------------------------------------------------------------------------+
| ARCHITECTURE 2: Fully Local / Self-Hosted Open-Source Stack                                             |
|   Storage: MinIO (S3 API)  -->  Compute: Local PySpark / DuckDB  -->  Format: Apache Iceberg / Parquet  |
+---------------------------------------------------------------------------------------------------------+
| ARCHITECTURE 3: Enterprise Azure Modern Data Platform (MDP)                                             |
|   Storage: ADLS Gen2  -->  Compute: Azure Synapse / Databricks  -->  Orchestrator: Azure Data Factory   |
+---------------------------------------------------------------------------------------------------------+
```

---

### Option 1: Cloud-Native Databricks Delta Lakehouse (Selected Blueprint)
* **Storage**: Amazon S3 Open Data Bucket (s3://ooni-data-eu-fra/) directly mounted or streamed via Delta Lake on DBFS / Cloud Storage.
* **Compute Engine**: Apache Spark 3.5 running on **Databricks Community Edition** (1 node, 15 GB RAM, 2 vCPUs).
* **Storage Layer**: **Delta Lake 3.0** with ACID transactions, Schema Enforcement, and Time Travel.
* **Serving**: Databricks SQL Analytics / Direct Parquet Export to Power BI Desktop.
* **Pros**:
  * **Zero Cost ($0.00)**: Operates 100% within Databricks Community Edition free tier.
  * **Native Delta Lake Optimizations**: Built-in support for MERGE INTO, OPTIMIZE, and Z-ORDER BY.
  * **No Cloud Billing Risks**: Prevents student credit exhaustion.
  * **Industry Relevance**: Databricks is the dominant enterprise Lakehouse platform.
* **Cons**:
  * Single-node memory constraints (requires disciplined partition pruning and micro-batching).
  * 2-hour idle timeout on free clusters.

---

### Option 2: Fully Local Open-Source Containerized Stack
* **Storage**: Local MinIO container (emulating S3 object storage).
* **Compute Engine**: Standalone PySpark cluster in Docker + DuckDB for fast local OLAP querying.
* **Storage Layer**: Apache Iceberg or Delta Lake on local storage.
* **Orchestration**: Lightweight Prefect / Cron.
* **Serving**: Apache Superset or Streamlit running in Docker.
* **Pros**:
  * 100% offline development capability; no dependency on internet stability.
  * Complete control over cluster memory allocation and JVM tuning.
* **Cons**:
  * Consumes 10–16 GB of RAM on the local development laptop, competing with IDEs and browser tabs.
  * Does not demonstrate cloud-native deployment patterns to prospective employers.

---

### Option 3: Enterprise Azure Modern Data Platform (MDP)
* **Storage**: Azure Data Lake Storage (ADLS) Gen2 with hierarchical namespace.
* **Compute**: Azure Databricks (Standard_D4ds_v5) or Azure Synapse Analytics Serverless SQL.
* **Orchestration**: Azure Data Factory (ADF) trigger pipelines.
* **Serving**: Power BI Service with DirectQuery connection.
* **Pros**:
  * True enterprise production setup simulating a Fortune 500 data platform.
  * Native integration with Microsoft Entra ID (Azure AD) and Power BI Service.
* **Cons**:
  * **Cost Burn Risk**: A multi-node Spark cluster burns through the $100 Azure for Students credit in 2–3 weeks if auto-termination is improperly configured.
  * Over-engineering overhead for Phase 1 and Phase 2 milestones.

---

## 2. Technology Stack Evaluation Matrix

| Architectural Layer | Selected Technology | Alternative Evaluated | Why the Selected Tech Wins |
| :--- | :--- | :--- | :--- |
| **Distributed Compute** | **Apache Spark 3.5 (PySpark)** | DuckDB / Polars | Handles multi-gigabyte polymorphic JSON with distributed array exploding; standard for enterprise data engineering. |
| **Table Storage Format** | **Delta Lake 3.0** | Apache Iceberg / Plain Parquet | ACID compliance, native MERGE INTO for rolling lookback reconciliation (late arrivals & dimensional re-enrichment), and Z-ORDER clustering out-of-the-box on Databricks. |
| **Data Governance / PII** | **PySpark Crypto (sha2, HMAC)** | Manual Python Regex | Operates at distributed scale across worker partitions without collecting data to the driver node. |
| **Orchestration** | **Databricks Workflows / Cron** | Apache Airflow | Native zero-cost scheduling on Databricks without maintaining an external Airflow webserver and metadata DB. |
| **Serving & BI** | **Power BI Desktop** | Tableau / Superset | Native direct connector to Parquet/Delta tables, superior choropleth geospatial mapping, and industry standard in enterprise BI. |

---

## 3. Chosen Production Blueprint Specification

```text
[Raw Sources: OONI S3 + Citizen Lab GitHub + RIPE ASN]
                       |
                       v
         +----------------------------+
         | BRONZE: Append-Only Delta  |
         | Raw JSONL, Ingest Metadata |
         +----------------------------+
                       |
     PySpark: Explode Arrays, Schema Enforcement,
     HMAC-SHA256 IP Masking, Broadcast Reference Join
                       v
         +----------------------------+
         | SILVER: Cleansed & Conformed
         | Normalized Events, Cleaned |
         | Delta MERGE (Lookback+SCD) |
         +----------------------------+
                       |
     Spark SQL: Star Schema Aggregations,
     Censorship Severity Rollups, Window Z-Scores
                       v
         +----------------------------+
         | GOLD: Analytical Marts     |
         | Star Schema (Fact + Dims)  |
         | Z-Ordered for Fast BI      |
         +----------------------------+
                       |
                       v
         [Power BI Executive Watchtower]
```
