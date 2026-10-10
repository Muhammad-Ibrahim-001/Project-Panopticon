# ==============================================================================
# PIPELINE DEPENDENCY & IMPACT TRACKING
# ==============================================================================
# Upstream Dependencies:
#   - None (Root configuration and parameter specification module)
# Downstream Impact:
#   - [Affects]: notebooks/00_audit_logger.py
#   - [Affects]: notebooks/01_bronze_ingestion.py
#   - [Affects]: notebooks/02_silver_transformation.py
#   - [Affects]: notebooks/run_pipeline.py
#   - [Schema/State Changes]:
#       * Centralizes DBFS and local storage paths (Raw Landing, Bronze, Silver, Quarantine, Logs)
#       * Defines public API endpoints (OONI Measurement API, Citizen Lab GitHub taxonomy)
#       * Tunes Spark session configurations for Databricks Community Edition (shuffle partitions, memory caps)
#       * Defines target country codes, default test names, and rolling lookback parameters
# Risk & Side Effects:
#   - Altering storage path prefixes or table names impacts all persistence layers across the lakehouse.
# ==============================================================================

"""
Project Panopticon: Central Pipeline Configuration
Defines environment detection, storage paths, external API endpoints,
Spark performance profiles, and runtime parameter resolvers.
"""

import os
from typing import Dict, Any, List

# ------------------------------------------------------------------------------
# 1. Environment Detection & Storage Architecture
# ------------------------------------------------------------------------------

def is_databricks_environment() -> bool:
    """
    Detect whether code is running within a Databricks runtime environment.
    """
    return "DATABRICKS_RUNTIME_VERSION" in os.environ or "DB_HOME" in os.environ


# Determine root storage base path based on execution environment
if is_databricks_environment() and os.environ.get("IS_SERVERLESS", "FALSE").upper() == "TRUE":
    # Serverless compute: DBFS is disabled, use Unity Catalog volume instead
    STORAGE_ROOT = "/Volumes/workspace/default/panopticon"
elif is_databricks_environment():
    # Databricks Community Edition DBFS storage prefix
    STORAGE_ROOT = "/FileStore/tables/panopticon"
else:
    # Local project repository root fallback
    STORAGE_ROOT = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "data")
    )

# Dedicated storage paths for each architectural tier
PATHS: Dict[str, str] = {
    "raw_landing": f"{STORAGE_ROOT}/raw",
    "bronze":      f"{STORAGE_ROOT}/bronze/bronze_ooni_raw",
    "silver":      f"{STORAGE_ROOT}/silver/silver_network_measurements",
    "quarantine":  f"{STORAGE_ROOT}/quarantine/quarantine_ooni_raw",
    "audit_logs":  f"{STORAGE_ROOT}/logs/pipeline_execution_logs",
    "reference":   f"{STORAGE_ROOT}/reference/citizenlab_categories",
}

# Registered Delta Lake table names (Metastore / Catalog)
TABLE_NAMES: Dict[str, str] = {
    "bronze":      "bronze_ooni_raw",
    "silver":      "silver_network_measurements",
    "quarantine":  "quarantine_ooni_raw",
    "audit_logs":  "pipeline_execution_logs",
    "reference":   "ref_citizenlab_categories",
}

# ------------------------------------------------------------------------------
# 2. External API Endpoints (Teacher Mandate: Zero Manual Uploads)
# ------------------------------------------------------------------------------

# OONI Public REST API (Measurement extraction)
OONI_API_BASE_URL: str = "https://api.ooni.io/api/v1"
OONI_MEASUREMENTS_ENDPOINT: str = f"{OONI_API_BASE_URL}/measurements"
OONI_RAW_MEASUREMENT_ENDPOINT: str = f"{OONI_API_BASE_URL}/raw_measurement"

# Citizen Lab Test Lists Repository (Taxonomy and URL categorization)
CITIZEN_LAB_RAW_BASE_URL: str = (
    "https://raw.githubusercontent.com/citizenlab/test-lists/master/lists"
)

# AWS Open Data Public Gateway (Optional direct S3 HTTPS fallback)
OONI_S3_PUBLIC_GATEWAY: str = "https://ooni-data-eu-fra.s3.eu-central-1.amazonaws.com"

# ------------------------------------------------------------------------------
# 3. Surveillance Scope & Pipeline Defaults
# ------------------------------------------------------------------------------

# Strategic Focus Surveillance Regions
FOCUS_COUNTRIES: List[str] = ["PK", "IR", "RU"]
DEFAULT_COUNTRY: str = "PK"

# Supported Network Test Types
SUPPORTED_TESTS: List[str] = [
    "web_connectivity",
    "echcheck",
    "whatsapp",
    "telegram",
    "signal",
    "tor",
    "psiphon"
]
DEFAULT_TEST: str = "web_connectivity"

# Lookback & Pagination Defaults
DEFAULT_LOOKBACK_DAYS: int = 3       # Rolling window for delayed probe sync (T-3 to T)
DEFAULT_API_PAGE_LIMIT: int = 50     # Batch page size for OONI API calls to prevent timeouts
API_REQUEST_TIMEOUT_SECONDS: int = 30

# ------------------------------------------------------------------------------
# 4. Databricks Community Edition Spark Safeguards
# ------------------------------------------------------------------------------

def _safe_conf_set(conf, key: str, value: str) -> None:
    """
    Wraps spark.conf.set() so that Serverless-incompatible keys are silently skipped
    instead of aborting the entire configuration block.
    """
    try:
        conf.set(key, value)
    except Exception:
        pass  # Config key not available on this compute tier — safe no-op


def configure_spark_runtime(spark_session) -> None:
    """
    Applies strict memory safeguards, partition tuning, and Delta Lake optimizations
    tailored specifically for Databricks Community Edition (15 GB RAM, 2 vCPUs),
    Serverless Compute, and Dedicated single-node clusters.
    
    Prevents OutOfMemory (OOM) errors during wide transformations and shuffles.
    All config sets are wrapped individually so one failure does not block others.
    Serverless-incompatible keys (optimizeWrite, autoCompact, Hadoop S3 credentials)
    are silently skipped on Serverless/Shared clusters.
    """
    conf = spark_session.conf

    # Each config wrapped individually — some keys are not available on Serverless
    _safe_conf_set(conf, "spark.sql.shuffle.partitions", "4")
    _safe_conf_set(conf, "spark.databricks.delta.optimizeWrite.enabled", "true")
    _safe_conf_set(conf, "spark.databricks.delta.autoCompact.enabled", "true")
    _safe_conf_set(
        conf,
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider",
    )


# ------------------------------------------------------------------------------
# 5. Security & PII Salt Provider
# ------------------------------------------------------------------------------

def get_pii_salt() -> str:
    """
    Retrieves the pepper salt used for HMAC-SHA256 resolver and IP anonymization.
    Tries Databricks Secret Vault first, then environment variable, then fallback.
    """
    # 1. Try Databricks Secrets Vault
    try:
        import IPython
        dbutils = IPython.get_ipython().user_ns.get("dbutils")
        if dbutils:
            return dbutils.secrets.get(scope="panopticon_vault", key="ip_salt")
    except Exception:
        pass

    # 2. Try OS Environment variable or return academic fallback
    return os.environ.get("PANOPTICON_SALT", "FAST_NUCES_PANOPTICON_SALT_2026")


# ------------------------------------------------------------------------------
# 6. Parameter Resolver (Widgets / CLI / Defaults)
# ------------------------------------------------------------------------------

def get_pipeline_parameter(param_name: str, default_value: Any) -> Any:
    """
    Resolves a pipeline runtime parameter with priority:
    1. Databricks Widget (if interactive notebook)
    2. OS Environment Variable
    3. Provided default value
    """
    # Try Databricks widget
    try:
        import IPython
        dbutils = IPython.get_ipython().user_ns.get("dbutils")
        if dbutils:
            val = dbutils.widgets.get(param_name)
            if val is not None and str(val).strip() != "":
                return val
    except Exception:
        pass

    # Try Environment variable
    env_key = f"PANOPTICON_{param_name.upper()}"
    if env_key in os.environ:
        return os.environ[env_key]

    return default_value


if __name__ == "__main__":
    print("=" * 60)
    print(" PROJECT PANOPTICON: PIPELINE CONFIGURATION DIAGNOSTIC")
    print("=" * 60)
    print(f" Environment       : {'Databricks CE' if is_databricks_environment() else 'Local Standalone'}")
    print(f" Storage Root      : {STORAGE_ROOT}")
    print(f" Focus Regions     : {FOCUS_COUNTRIES}")
    print(f" Default Country   : {DEFAULT_COUNTRY}")
    print(f" Default Test      : {DEFAULT_TEST}")
    print(f" OONI API Endpoint : {OONI_MEASUREMENTS_ENDPOINT}")
    print(f" Citizen Lab URL   : {CITIZEN_LAB_RAW_BASE_URL}")
    print("\nStorage Paths:")
    for tier, path in PATHS.items():
        print(f"  - {tier:<14}: {path}")
    print("=" * 60)

