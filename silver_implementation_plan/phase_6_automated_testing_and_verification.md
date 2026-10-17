# Phase 6: Automated Testing, Quality Verification & CI Gate
**Priority**: **P2 · Quality Gate**  
**Target Files**: [`tests/test_phase2_silver_transformation.py`](file:///d:/Semester%20Projects/Panopticon/tests), [`notebooks/run_pipeline.py`](file:///d:/Semester%20Projects/Panopticon/notebooks)  
**Downstream Impact**: Automated test harness protecting all Silver invariant contracts against regressions.  
**Associated Audit Findings**: `SILVER-38`, `SILVER-53`

```text
==============================================================================
PIPELINE DEPENDENCY & IMPACT TRACKING
==============================================================================
Upstream Dependencies:
  - notebooks/02_silver_transformation.py
  - data/schemas/*.json (Authoritative JSON contracts)
  - tests/conftest.py (Shared local PySpark test fixture)
Downstream Impact:
  - Validates Silver pipeline end-to-end
  - Blocks deployments if invariants, schemas, or idempotency fail
Risk & Side Effects:
  - None. Non-destructive test harness running on temporary directories.
==============================================================================
```

---

## 1. Problem Diagnosis & Architectural Vulnerabilities

### Problem 1.1: Schema Validation Logic Checking Names Only (SILVER-38)
* **Root Cause**: Earlier versions of `validate_bronze_schema_compliance()` claimed to validate types, but in practice only checked column names via `set(df.columns) == set(expected_columns)`.
* **Impact**: Silent schema drift. If a column changed from `LongType` to `StringType` or `BooleanType` to `IntegerType`, the validator returned `True`, leading to downstream runtime crashes during Delta Lake ingestion or queries.
* **Remediation**: Implement comprehensive dictionary-level type equality checks matching both name and Spark `dataType`.

### Problem 1.2: Lack of Standalone Automated Test Suite (SILVER-53)
* **Root Cause**: Academic evaluators noted that while Bronze had 27 tests in `tests/test_phase1_bronze_isolation.py`, Silver lacked a standalone pytest suite. Procedural self-tests in notebooks are insufficient for CI/CD pipelines.
* **Remediation**: Build `tests/test_phase2_silver_transformation.py` containing modular unit tests (URL normalization, multi-signal classifier, peppered hashing) and Delta Lake integration tests (idempotency, partition pruning, child table normalization).

---

## 2. Target Implementation Blueprint

### Step 2.1: Robust Schema Type Equality Validator
In `notebooks/02_silver_transformation.py`:
```python
def validate_silver_schema_contract(
    df: DataFrame,
    expected_schema_json_path: str
) -> Tuple[bool, List[str]]:
    """
    Validates that a DataFrame strictly adheres to the authoritative JSON schema contract:
      1. All required fields are present.
      2. No unexpected columns are present.
      3. Data types match exactly (e.g. LongType vs IntegerType).
    """
    with open(expected_schema_json_path, "r", encoding="utf-8") as f:
        contract = json.load(f)

    expected_fields = {field["name"]: field["type"] for field in contract["fields"]}
    actual_fields = {field.name: field.dataType.typeName() for field in df.schema.fields}

    errors = []
    # Check for missing columns
    for name, expected_type in expected_fields.items():
        if name not in actual_fields:
            errors.append(f"Missing column: '{name}' (expected {expected_type})")
        elif actual_fields[name].lower() != expected_type.lower():
            errors.append(f"Type mismatch on '{name}': actual '{actual_fields[name]}' != expected '{expected_type}'")

    # Check for unexpected extra columns
    for name in actual_fields:
        if name not in expected_fields:
            errors.append(f"Unexpected extra column: '{name}'")

    is_valid = len(errors) == 0
    return is_valid, errors
```

### Step 2.2: Automated Test Matrix (`tests/test_phase2_silver_transformation.py`)
Implement the test suite covering both Unit and Delta Integration tests:

```python
# tests/test_phase2_silver_transformation.py
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

class TestSilverUnitLogic:
    """Fast, offline unit tests for business logic transformations."""

    def test_url_normalization_exact_and_domain(self, spark):
        """Validates that URLs strip trailing slashes and correctly isolate domains."""
        ...

    def test_multi_signal_classifier_truth_table(self, spark):
        """Verifies CONFIRMED_BLOCKED, HEURISTIC_ANOMALY, and BENIGN truth table."""
        ...

    def test_https_blind_spot_resolution(self, spark):
        """Ensures TLS handshake failures produce tls_anomaly_flag=True and TLS_DROP."""
        ...

    def test_peppered_sha256_resolver_hashing(self, spark):
        """Ensures resolver IP is cryptographically pseudonymized and empty salt fails."""
        ...

class TestSilverDeltaIntegration:
    """Delta Lake persistence, idempotency, and partition isolation tests."""

    def test_silver_1_to_1_measurement_grain(self, spark, tmp_path):
        """Ensures no row duplication or multiple-match merge crashes in parent table."""
        ...

    def test_child_projections_posexplode_outer(self, spark, tmp_path):
        """Verifies DNS and TCP child tables correctly explode with unique surrogate keys."""
        ...

    def test_merge_idempotency_zero_write_amplification(self, spark, tmp_path):
        """Re-running identical data must yield 0 inserts and 0 updates."""
        ...

    def test_business_change_conditional_update(self, spark, tmp_path):
        """Changing category or classification must update affected rows only."""
        ...
```

---

## 3. Verification Protocol & Evaluator 2's 22-Item Checklist

To certify the Phase 2 Silver pipeline as production-ready per [`silver_layer_engineering_audit.md:§5.3`](file:///d:/Semester%20Projects/Panopticon/silver_layer_engineering_audit.md#L2222), every test below must pass:

```markdown
#### Bronze Contract & Ingestion
- [ ] 1. A lookback fetch requests all intended dates [T-lookback, T] and preserves each measurement's actual date (SILVER-19).
- [ ] 2. A capped or partially failed API response is marked PARTIAL and never marked SUCCESS_COMPLETE (SILVER-50).
- [ ] 3. Invalid measurement timestamps are routed to quarantine instead of silently assigned batch_date (SILVER-51).
- [ ] 4. Runtime schemas (BRONZE_OONI_SCHEMA) match checked-in schemas in field names, types, and nullability (SILVER-38).
- [ ] 5. API anomaly and confirmation signals (api_anomaly, api_confirmed) survive Bronze ingestion (SILVER-45).

#### Identity & Idempotency
- [ ] 6. Re-running the same batch leaves Silver row counts and business content completely unchanged (SILVER-20, SILVER-49).
- [ ] 7. Two distinct measurements with the same timestamp remain distinct when their natural keys differ (SILVER-17).
- [ ] 8. Null, empty, and delimiter-containing key components cannot collide under composite hashing (SILVER-16).
- [ ] 9. Duplicate source measurement IDs within an incoming batch are detected and resolved before MERGE (SILVER-12).
- [ ] 10. A genuine taxonomy or heuristics change updates the relevant row, while a changed batch ID alone does not (SILVER-20).

#### Privacy & Classification
- [ ] 11. No disallowed raw resolver (resolver_address), probe (probe_ip, probe_id), or test_keys fields enter Silver (SILVER-43, SILVER-48).
- [ ] 12. A missing production secret stops the pipeline (fail-closed) rather than selecting a hard-coded fallback (SILVER-53).
- [ ] 13. Every canonical tampering vector maps consistently to the appropriate binary measure flags (100% non-zero) (SILVER-06, SILVER-24).
- [ ] 14. Unknown labels, malformed payloads, and unclassified tests do not count as confirmed censorship (is_anomaly=False) (SILVER-41).

#### Reference & Child Tables
- [ ] 15. Exact URL, domain fallback, and unmatched Citizen Lab joins behave strictly as documented (SILVER-42).
- [ ] 16. The taxonomy version is present, non-null, and traceable to a reproducible reference snapshot (SILVER-44, SILVER-46).
- [ ] 17. Child-table keys are 100% deterministic and stable across replay and repartitioning (no monotonically_increasing_id) (SILVER-52).
- [ ] 18. Exploding child arrays does not multiply parent measurement rows or create null-only child records (SILVER-13, SILVER-14).
- [ ] 19. DNS child table cleanly represents non-IP answer values (CNAME, PTR) without type corruption (SILVER-48).

#### Audit & Operational Recovery
- [ ] 20. Every execution records start/end times, status, parameters, and Delta operational insert/update metrics (Course Req P2-2d).
- [ ] 21. A failed Silver merge produces an explicit failure log and does not falsely mark the run successful.
- [ ] 22. All persisted Bronze and Silver tables contain the mandatory load_timestamp (Course Requirement P2-2d).
```

---

## 4. Acceptance Criteria & Quality Gates

1. **Test Execution**: Run `pytest tests/test_phase2_silver_transformation.py -v`.
2. **Success Criteria**:
   - 100% of the 22 checklist items are implemented and tested.
   - Zero Delta Lake multiple-match errors.
   - Zero write amplification on re-runs (`num_target_rows_updated == 0`).
   - Execution time under 45 seconds on local developer environments.
3. **Pipeline Runner**: Execute `python notebooks/run_pipeline.py --mode incremental --country PK` and confirm end-to-end execution logging in `pipeline_execution_logs`.
