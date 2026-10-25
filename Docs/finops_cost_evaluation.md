# Cost & FinOps Evaluation — Project Panopticon

**Course context:** Cloud Computing (Azure for Students / Databricks) — Cost & FinOps awareness requirement
**Environment:** Azure Databricks workspace, serverless compute (Spark Connect) for pipeline notebooks, serverless SQL / AI/BI dashboards for serving
**Pipeline:** OONI censorship telemetry lakehouse — Bronze (raw) → Silver (conformed) → Gold (dimensional) on Unity Catalog volumes (`/Volumes/workspace/default/panopticon/`)
**Date of evaluation:** 2026-10-25

---

## 1. Observed Compute Consumption (Evidence Base)

All figures below come from the project's own audit framework — the `pipeline_execution_logs` table at `/Volumes/workspace/default/panopticon/logs/pipeline_execution_logs` (27 runs logged as of 2026-10-25) and the "Pipeline Operations & Audit" dashboard page.

| Metric (per audit-logged stage) | Observed value |
| --- | --- |
| Stage duration — successful runs | **10.8–14.5 s** (median ≈ 12.5 s) |
| Stage duration — failed runs | 16.9–19.8 s (wasted compute) |
| Rows read per stage (academic sample scale) | 4–36 rows |
| Full daily incremental cycle (Bronze→Silver 4 child-table steps + Silver→Gold agg/dim/fact steps) | ≈ **2–3 minutes of engine time** total |
| End-to-end wall-clock per daily cycle (incl. serverless session start, notebook parse, API pull) | ≈ 5–10 minutes |
| Pipeline success rate | ≈ 85% of logged runs (failures were early-development PATH_NOT_FOUND and sample-contamination contract violations) |

Key FinOps observation at academic scale: **cost is driven by the number of executions, not data volume.** Each incremental run of the full cycle costs roughly the same whether it processes 15 rows or 15,000, because fixed overhead (serverless session spin-up, plan analysis, notebook interpretation, Delta commit) dominates the ~12 s per-stage runtime.

## 2. Typical Daily Incremental Run — Cost Estimate

Assumptions (verify against the current Azure Databricks pricing page before quoting externally):

* Serverless compute for jobs/notebooks: ≈ **$0.55–$0.70 per DBU-hour** (list price, tier-dependent)
* Effective DBU burn while a small serverless step is actively running: ≈ 1–2 DBU
* Engine time per full daily cycle: ≈ 3 minutes (from audit logs)

| Item | Calculation | Estimate |
| --- | --- | --- |
| Engine time per daily run | ~15 stages × ~12.5 s | ≈ 0.05–0.10 DBU-hour |
| **Cost per daily incremental run** | 0.05–0.10 DBU-hr × $0.55–$0.70 | **≈ $0.03–$0.07** |
| Monthly (one daily incremental cycle) | 30 runs | **< $2.50** |
| Dashboard serving (serverless SQL, sample-scale data) | ad-hoc query bursts | < $1.00/month |
| Development/debug runs (the real student cost driver) | dozens of interactive notebook sessions | a few $ over the project |

**Verdict:** the entire project lifecycle consumed only a small fraction of the $100 Azure for Students credit. The dominant student-spend category was *interactive development and re-runs*, not the production-style incremental loads — the runs themselves are effectively free at this data volume. The early "Bronze Persistence CONTRACT VIOLATION" guardrails also proved to be cost guardrails: they abort in ~17–20 s and prevent expensive writes of contaminated data into production tables.

## 3. Enterprise Hourly Extrapolation — Architectural Cost

Scenario: the same architecture promoted to an enterprise censorship-monitoring service over full OONI global telemetry (~1–2M measurements/day ≈ 50k–100k/hour), running **hourly** → **8,760 pipeline cycles/year**.

| Cost component | Assumption | Estimated annual cost (USD) |
| --- | --- | --- |
| Pipeline compute (Jobs / job cluster) | 3–8 DBU/hr cluster, 10–20 min per hourly cycle, classic jobs-compute rates | $2,000 – $9,000 |
| Ingestion (OONI API / S3 pulls) | bundled into pipeline compute above | (included) |
| Storage — Delta tables in UC | 1–5 TB/yr incl. Bronze retention + quarantine | $300 – $1,500 |
| Dashboard serving (serverless SQL warehouse) | hourly refreshes + concurrent viewers, ~$14/DBU-hr | $1,500 – $8,000 |
| Monitoring, alerting, audit logging | system tables + budget alerts | < $500 |
| **Total architectural estimate** | | **≈ $4,000 – $18,000 / year** |

Two structural insights for the enterprise design:

1. **The cost driver inverts at scale.** At student scale cost ≈ per-run overhead; at enterprise scale cost ≈ (data volume × frequency) + serving. The SQL warehouse serving the dashboard can rival or exceed pipeline compute if dashboards aggregate raw-scale data on every view.
2. **Frequency is the most expensive knob.** Hourly (8,760 runs) vs daily (365 runs) is a 24× multiplier on fixed per-run overhead. Freshness SLA must be priced, not assumed.

## 4. Optimization Strategy

1. **Consolidate executions.** Replace notebook-driven runs with a single Lakeflow Job per cycle (or Spark Declarative Pipeline) using one job cluster per cycle (or serverless jobs) — amortizes startup/plan overhead across all Silver child tables and Gold steps instead of paying it per notebook.
2. **Event-driven ingestion.** Use Auto Loader / file-arrival triggers against the OONI public data endpoint instead of a fixed hourly poll — pay only when new data lands; idle hours cost $0.
3. **Keep every stage truly incremental.** MERGE-based upserts with partition pruning / liquid clustering on date keys so an hourly run reads only that hour's slice, not a rolling scan of Bronze.
4. **Pre-aggregate in Gold, materialize for serving.** Push aggregations into Gold materialized views and enable dashboard dataset materialization on publish, so viewer traffic hits pre-computed tables and the warehouse runs short, cheap queries with aggressive auto-stop.
5. **Storage hygiene.** Scheduled OPTIMIZE/VACUUM, quarantine purge policy, and archival of aged raw JSON to cool storage tiers.
6. **Price the freshness SLA.** Moving from hourly to 4-hourly cuts pipeline compute ~4×; offer a streaming/real-time tier only for focus countries if a use case demands it.
7. **FinOps observability.** Tag workloads for cost allocation, track spend in `system.billing.usage`, set budget policies and spend alerts, and review cost-per-run monthly against the audit logs (the project's `pipeline_execution_logs` already provides per-run duration telemetry to detect drift).

## 5. Assumptions & Caveats

* DBU rates are Azure list-price approximations and vary by pricing tier, region, and commitment discounts — the dollar figures are order-of-magnitude estimates, not quotes.
* Stage durations are measured from the project's audit logs at academic sample scale; enterprise durations are extrapolated (real OONI-scale hourly volumes would extend per-cycle runtime from ~12 s/stage to minutes/stage, which is reflected in the ranges above).
* The 27-run log includes development/debug failures; the "typical daily incremental run" figure uses only the successful 2026-09-27 batch cycle as the reference workload.

## Dependency & Impact Note

Documentation only — no code, notebook, or lakehouse state is affected by this file. Evidence sources: `pipeline_execution_logs` (audit table), gold-layer dashboard "Pipeline Operations & Audit" page, and `Docs/` project documentation.