# Serverless Test Migration — Errors & Solutions

**Date:** 2026-10-14  
**Context:** Migrating `test_phase1_bronze_isolation` test suite to Serverless interactive compute (Spark Connect)  
**Result:** 24/27 tests pass, 3 failures identified as production code bugs

---

## Error 1: Git Checkout Conflict

**Error:**
```
GIT_CONFLICT: error: Your local changes to the following files would be overwritten by checkout:
    notebooks/01_bronze_ingestion.py
```

**Cause:** Staged (uncommitted) changes to `01_bronze_ingestion.py` on `main` branch conflicted with the `feat/phase1-4-bronze-hardening` branch during checkout.

**Solution:** Stashed the local changes, then switched branches:
```sh
git stash push -m "serverless-compat-fixes" -- notebooks/01_bronze_ingestion.py
git checkout feat/phase1-4-bronze-hardening
```

---

## Error 2: `NameError: name '__file__' is not defined`

**Error:**
```
NameError: name '__file__' is not defined
```

**Location:** Cell 1, line 67 — bootstrapping `workspace_root` for module imports.

**Cause:** `__file__` is a standard Python variable available when running `.py` files as scripts, but it does **not** exist in Databricks notebook environments (notebook code runs in an exec context, not a file).

**Solution:** Added a `try/except NameError` fallback that uses the known workspace path:
```python
try:
    workspace_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
except NameError:
    # In Databricks notebooks, __file__ is not defined
    workspace_root = "/Workspace/Repos/Project Panopticon/Project-Panopticon"
```

**Notes:**
- `dbutils.notebook.getContext().notebookPath()` → fails on Serverless (`'NotebookHandler' object has no attribute 'getContext'`)
- `spark.conf.get("spark.databricks.notebook.path")` → fails on Serverless (`CONFIG_NOT_AVAILABLE`)
- `dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()` → returns conversation path, not notebook path, when run from `executeCode`

---

## Error 3: `NotebookImportException` — Workspace `.py` Files Treated as Notebooks

**Error:**
```
NotebookImportException: Unable to import module `notebooks.01_bronze_ingestion`.
The following file appears to be a notebook:
  /Workspace/Repos/Project Panopticon/Project-Panopticon/notebooks/01_bronze_ingestion.py
Importing notebooks directly is not supported.
```

**Location:** Cell 1, line 76 — `importlib.import_module("notebooks.01_bronze_ingestion")`.

**Cause:** On Serverless compute, Databricks treats workspace `.py` files as notebooks (they have cell markers). The standard `importlib.import_module()` is intercepted by the Databricks import hook, which raises `NotebookImportException` instead of importing the module.

**Solution:** Read the `.py` file directly and `exec` it into a synthetic `types.ModuleType` namespace:
```python
import types
_mod_path = os.path.join(workspace_root, "notebooks", "01_bronze_ingestion.py")
bronze_mod = types.ModuleType("01_bronze_ingestion")
bronze_mod.__file__ = _mod_path
_notebooks_dir = os.path.join(workspace_root, "notebooks")
if _notebooks_dir not in sys.path:
    sys.path.insert(0, _notebooks_dir)
with open(_mod_path) as _f:
    exec(compile(_f.read(), _mod_path, "exec"), bronze_mod.__dict__)
```

**Why other approaches failed:**
- `dbutils.import_notebook()` → `ModuleNotFoundError` for all path formats tried
- The `except ModuleNotFoundError` fallback also caught the wrong exception type (`NotebookImportException` is not a subclass of `ModuleNotFoundError`)

---

## Error 4: `PySparkValueError: [CANNOT_DETERMINE_TYPE]` — Spark Connect Type Inference

**Error:**
```
pyspark.errors.exceptions.base.PySparkValueError: [CANNOT_DETERMINE_TYPE]
Some of types cannot be determined after inferring.
```

**Location:** Cell 5, `_create_sample_batch()` — `self.spark.createDataFrame(rows)`.

**Cause:** On Spark Connect (Serverless), `spark.createDataFrame(rows)` cannot infer column types from a list of `Row` objects. Classic Spark does local type inference; Spark Connect delegates to the server, which requires an explicit schema.

**Solution:** Added explicit `StructType` schemas as class attributes and passed them to every `createDataFrame` call:
```python
_SAMPLE_SCHEMA = StructType([
    StructField("report_id", StringType(), True),
    StructField("measurement_start_time", StringType(), True),
    # ... 19 fields total
])

return self.spark.createDataFrame(rows, schema=self._SAMPLE_SCHEMA)
```

Three schemas were defined:
- `_SAMPLE_SCHEMA` (19 fields) — standard test batch rows
- `_CONFLICT_SCHEMA` (19 fields) — test_07 rows with `test_start_time` in a different position
- `_TIE_SCHEMA` (19 fields) — test_16 rows with `test_start_time` in yet another position

**Note:** Field order in the schema must match the `Row` kwargs order because Spark matches by position, not by name.

---

## Error 5: `DBFS_DISABLED` — `/tmp` Unavailable on Serverless

**Error:**
```
UnsupportedOperationException: [DBFS_DISABLED] Public DBFS root is disabled.
Access is denied on path: /tmp/panopticon_test_bronze_xxx/test_cross_country/_delta_log
```

**Location:** Cell 5, `setUpClass()` — `tempfile.mkdtemp(prefix="panopticon_test_bronze_")`.

**Cause:** `tempfile.mkdtemp()` creates directories under `/tmp/`, which on Serverless compute resolves to a DBFS path. DBFS is disabled on Serverless; all file I/O must use Unity Catalog volumes.

**Solution:** Detect Serverless via the `IS_SERVERLESS` environment variable and use the UC volume path instead:
```python
_is_serverless = os.environ.get("IS_SERVERLESS", "").upper() == "TRUE"
if _is_serverless:
    _vol_base = "/Volumes/workspace/default/panopticon"
    os.makedirs(_vol_base, exist_ok=True)
    cls.test_dir = tempfile.mkdtemp(prefix="test_bronze_", dir=_vol_base)
else:
    cls.test_dir = tempfile.mkdtemp(prefix="panopticon_test_bronze_")
```

**Note:** The UC volume `workspace.default.panopticon` was already created by `00_config.py` during earlier pipeline runs.

---

## Error 6: `UNRESOLVED_COLUMN: measurement_uid` — Missing Dedup Columns

**Error:**
```
AnalysisException: [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function
parameter with name `measurement_uid` cannot be resolved.
```

**Location:** Cell 5 — inside `write_bronze_delta_table()` at the deduplication window function (source line 1206).

**Cause:** The `write_bronze_delta_table` function's deterministic deduplication window function references 6 columns that the test sample data did not include:

| Missing Column | Type | Used For |
|---|---|---|
| `measurement_uid` | StringType | Provenance ranking (prefer records with OONI UID) |
| `test_start_time` | StringType | Source test session time tie-breaker |
| `test_runtime` | DoubleType | Execution duration tie-breaker |
| `annotations` | StringType | SHA-256 payload hash component |
| `probe_ip` | StringType | SHA-256 payload hash component |
| `resolver_ip` | StringType | SHA-256 payload hash component |

**Solution:** Added all 6 columns as nullable fields (value `None`) to:
- All 3 schema definitions (`_SAMPLE_SCHEMA`, `_CONFLICT_SCHEMA`, `_TIE_SCHEMA`)
- All `Row()` constructors in `_create_sample_batch`, `test_07`, `test_08`, `test_09`, `test_16`
- Also added `DoubleType` to the cell 1 imports

---

## Remaining Failures: 3 Production Code Bugs (Unfixed)

After all Serverless compatibility fixes, 24/27 tests pass. The remaining 3 failures are **bugs in the production code** (`01_bronze_ingestion.py`), not in the test code.

### Bug 1: test_07 — Dedup Skipped on Initial Table Creation

**Failure:**
```
AssertionError: 2 != 1 : Duplicate key was not deduplicated!
```

**Root Cause:** At source line 1171, when the Delta table doesn't exist yet (first write), the function writes the raw DataFrame with `mode("overwrite")` and returns immediately — **without applying the deduplication window function**. The dedup logic (window ranking by `measurement_uid`, `test_start_time`, SHA-256 hash) only runs in the incremental MERGE branch (line 1190+). When duplicate keys arrive in the first batch, both rows are written.

**Verification:** The window function itself works correctly on Spark Connect — tested in isolation, it ranks 2 rows → 1 row with proper ordering.

### Bug 2: test_12 — Manifest Scope Not Checked on Initial Table Creation

**Failure:**
```
AssertionError: ValueError not raised
```

**Root Cause:** Same initial-creation branch (line 1171). The unauthorized-dates check (`fetched_dates - manifest.authorized_replacement_dates`) at line 1278 is only reached when the table already exists. On first write, the function writes all data without checking the manifest's authorized date scope.

### Bug 3: test_16 — Tie-Breaker Skipped on Initial Table Creation

**Failure:**
```
AssertionError: 2 != 1 : Tie was not resolved into single deterministic row!
```

**Root Cause:** Same as Bug 1. The SHA-256 tie-breaker window function is only applied in the incremental MERGE branch, not during initial table creation.

### Recommended Fix for All 3 Bugs

Move the deduplication window function and manifest scope validation **before** the initial table creation branch (line 1171), so they apply to all write paths — not only incremental MERGE.

---

## Summary Table

| # | Error | Layer | Fixed | Approach |
|---|---|---|---|---|
| 1 | Git checkout conflict | Git | Yes | Stash + checkout |
| 2 | `__file__` NameError | Notebook env | Yes | Hardcoded workspace path fallback |
| 3 | NotebookImportException | Spark Connect | Yes | Direct file read + `exec` into `types.ModuleType` |
| 4 | CANNOT_DETERMINE_TYPE | Spark Connect | Yes | Explicit `StructType` schemas for all `createDataFrame` calls |
| 5 | DBFS_DISABLED | Serverless | Yes | UC volume path for test temp directories |
| 6 | UNRESOLVED_COLUMN | Schema mismatch | Yes | Added 6 missing dedup columns to test data |
| 7 | Dedup skipped on initial write | Production code | No | Needs fix in `01_bronze_ingestion.py` line 1171 |
| 8 | Manifest scope skipped on initial write | Production code | No | Needs fix in `01_bronze_ingestion.py` line 1171 |

**Final test result: 24 pass, 3 fail (production code bugs), 0 errors.**