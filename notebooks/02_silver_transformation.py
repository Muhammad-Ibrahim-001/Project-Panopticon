# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# ==============================================================================
# PIPELINE DEPENDENCY & IMPACT TRACKING
# ==============================================================================
# Upstream Dependencies:
#   - [Module]: notebooks/00_config.py
#       * PATHS, TABLE_NAMES, FOCUS_COUNTRIES, DEFAULT_LOOKBACK_DAYS, get_pii_salt(),
#         get_pipeline_parameter(), configure_spark_runtime()
#   - [Module]: notebooks/00_audit_logger.py
#       * PipelineAuditor (layer = "Bronze-to-Silver"), AuditStatus
#   - [Delta Table]: bronze_ooni_raw            (written by notebooks/01_bronze_ingestion.py)
#   - [Delta Table]: ref_citizenlab_categories  (written by notebooks/01_bronze_ingestion.py)
# Downstream Impact:
#   - [Generates Delta Table]: silver_network_measurements
#       (PARTITIONED BY measurement_date, Snappy, ZORDER BY probe_cc/probe_asn/tampering_vector)
#   - [Appends Delta Table]: quarantine_ooni_raw  (rows tagged _source_file = 'BRONZE_TO_SILVER')
#   - [Appends Delta Table]: pipeline_execution_logs (layer = 'Bronze-to-Silver')
#   - [Affects]: Gold-layer notebooks / dashboards that read silver_network_measurements
#   - [Schema Contract]: data/schemas/silver_conformed_schema.json (SILVER_SCHEMA below must match)
#   - [Schema/State Changes]: none to Bronze. Silver PK = measurement_id (SHA-256).
# Risk & Side Effects:
#   - Changing tampering heuristics / RISK_TIER_MAP is SAFE to re-run: the MERGE updates the
#     enriched columns of existing rows (no re-ingestion of raw files needed).
#   - Changing the measurement_id formula, PII salt or SILVER_SCHEMA requires a full Silver rebuild.
#   - PII Guardrail: probe_ip / resolver_ip are read here ONLY to derive masked_probe_subnet and
#     hashed_resolver_ip. Raw IPs are never printed, logged, quarantined or written to Silver.
# ==============================================================================

"""
Project Panopticon: Silver Transformation Engine (Bronze -> Silver)

Cleanses, type-casts, PII-sanitizes, classifies (tampering vector) and enriches
(Citizen Lab taxonomy) Bronze OONI telemetry, then upserts it into
`silver_network_measurements` with an idempotent Delta Lake MERGE INTO.

Grain: ONE Silver row per OONI measurement. test_keys arrays (queries, tcp_connect,
tls_handshakes, ...) are deliberately NOT exploded (see adversarial review, Dim. 4.1).
"""

import importlib
import hashlib
import hmac
import sys
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

try:
    from pyspark.sql import SparkSession, DataFrame, Window
    from pyspark.sql import functions as F
    from pyspark.sql.types import (
        StructType, StructField, StringType, LongType, DoubleType,
        IntegerType, BooleanType, TimestampType, DateType,
    )
    from delta.tables import DeltaTable
    HAS_PYSPARK = True
except (ImportError, ModuleNotFoundError):  # allows importing constants without Spark
    SparkSession = DataFrame = DeltaTable = Any  # type: ignore
    HAS_PYSPARK = False

# ------------------------------------------------------------------------------
# Dynamic module import (Databricks %run / standalone compatibility), same as Bronze
# ------------------------------------------------------------------------------
config = None
for _mod_name in ("notebooks.00_config", "00_config"):
    try:
        config = importlib.import_module(_mod_name)
        break
    except (ImportError, ModuleNotFoundError):
        continue
if config is None:
    raise ImportError("[02_silver_transformation] Could not import '00_config'.")

audit_logger = None
for _mod_name in ("notebooks.00_audit_logger", "00_audit_logger"):
    try:
        audit_logger = importlib.import_module(_mod_name)
        break
    except (ImportError, ModuleNotFoundError):
        continue


# COMMAND ----------

# ==============================================================================
# STEP 0: CONTRACTS (explicit schemas - inferSchema is never used)
# ==============================================================================

# --- 0.1 Silver target schema (mirrors data/schemas/silver_conformed_schema.json) ---
SILVER_SCHEMA = StructType([
    StructField("measurement_id",          StringType(),    False),  # PK
    StructField("report_id",               StringType(),    False),
    StructField("event_timestamp",         TimestampType(), False),  # UTC
    StructField("measurement_date",        DateType(),      False),  # partition column
    StructField("target_url",              StringType(),    True),   # sanitized
    StructField("target_domain",           StringType(),    True),
    StructField("probe_asn",               LongType(),      False),
    StructField("probe_cc",                StringType(),    False),
    StructField("probe_network_name",      StringType(),    True),
    StructField("masked_probe_subnet",     StringType(),    True),   # /24 (IPv4) or /48 (IPv6)
    StructField("hashed_resolver_ip",      StringType(),    True),   # HMAC-SHA256
    StructField("resolver_asn",            LongType(),      True),
    StructField("test_name",               StringType(),    False),
    StructField("tampering_vector",        StringType(),    False),
    StructField("is_anomaly",              BooleanType(),   False),
    StructField("dns_anomaly_flag",        IntegerType(),   False),
    StructField("tcp_anomaly_flag",        IntegerType(),   False),
    StructField("tls_anomaly_flag",        IntegerType(),   False),
    StructField("http_anomaly_flag",       IntegerType(),   False),
    StructField("dns_experiment_failure",  StringType(),    True),
    StructField("http_experiment_failure", StringType(),    True),
    StructField("duration_seconds",        DoubleType(),    True),
    StructField("content_category",        StringType(),    False),
    StructField("category_description",    StringType(),    True),
    StructField("human_rights_risk_tier",  StringType(),    False),
    StructField("load_timestamp",          TimestampType(), False),  # audit
    StructField("_batch_id",               StringType(),    False),  # audit
])
SILVER_COLUMNS: List[str] = [f.name for f in SILVER_SCHEMA.fields]

# --- 0.2 Bronze input contract: the ONLY columns Silver reads, with expected types ---
# Anything else in Bronze (e.g. a newly added OONI field) is ignored -> schema drift safe.
BRONZE_INPUT_CONTRACT: Dict[str, str] = {
    "report_id": "string", "input": "string", "test_name": "string",
    "measurement_start_time": "string", "probe_asn": "string", "probe_cc": "string",
    "probe_ip": "string", "probe_network_name": "string", "resolver_asn": "string",
    "resolver_ip": "string", "test_runtime": "double", "test_keys": "string",
    "measurement_date": "date", "load_timestamp": "timestamp",
}

# --- 0.3 test_keys: every extracted field is parsed as STRING on purpose ---
# OONI fields are union-typed (e.g. web_connectivity.blocking is false | "dns" | ...).
# Parsing as string means a type change upstream (bool -> string, int -> string, ...)
# can never null-out the whole struct. Typed interpretation happens in classify_tampering().
TEST_KEYS_FIELDS: List[str] = [
    "blocking", "accessible", "dns_experiment_failure", "http_experiment_failure",
    "control_failure", "dns_consistency",
    "telegram_http_blocking", "telegram_tcp_blocking", "telegram_web_status",
    "whatsapp_endpoints_status", "whatsapp_endpoints_dns_inconsistent",
    "whatsapp_web_status", "registration_server_status",
    "signal_backend_status",
]
TEST_KEYS_SCHEMA = StructType([StructField(n, StringType(), True) for n in TEST_KEYS_FIELDS])

# --- 0.4 Human-rights risk tiers keyed by REAL Citizen Lab category codes ---
# NOTE: Docs/Phase 2/06_data_dictionary.md §3.2 lists POL / HR / VPN, which are not Citizen Lab
# codes (the real ones are POLR / HUMR / ANON). Legacy aliases are kept so either spelling works.
# Tiers for codes the data dictionary does not mention (GRP, COMT, MMED, ...) are an ASSUMPTION
# - edit here and re-run; the MERGE will update existing rows.
RISK_TIER_MAP: Dict[str, List[str]] = {
    "HIGH":   ["NEWS", "POLR", "HUMR", "ANON", "POL", "HR", "VPN"],
    "MEDIUM": ["REL", "LGBT", "GRP", "COMT", "MMED", "PUBH", "XED", "HOST"],
    # every other category (CULTR, COMM, GOVT, ECON, ...) and UNCATEGORIZED -> LOW
}

QUARANTINE_SOURCE_TAG = "BRONZE_TO_SILVER"


# COMMAND ----------

# ==============================================================================
# STEP 1: PARAMETERS (nothing hard-coded to "today")
# ==============================================================================

def init_silver_widgets() -> None:
    """Creates Databricks widgets. Safe no-op outside an interactive notebook."""
    try:
        import IPython
        ipy = IPython.get_ipython()
        if ipy and "dbutils" in ipy.user_ns:
            w = ipy.user_ns["dbutils"].widgets
            yesterday = (datetime.now(timezone.utc).date() - timedelta(days=1)).isoformat()
            w.text("batch_date", yesterday, "1. Batch Date T (YYYY-MM-DD)")
            w.dropdown("load_type", "incremental", ["full", "incremental", "backfill"], "2. Load Type")
            w.text("lookback_days", str(config.DEFAULT_LOOKBACK_DAYS), "3. Rolling Lookback Days")
            w.text("backfill_start", "", "4. Backfill Start (YYYY-MM-DD, optional)")
            w.text("backfill_end", "", "5. Backfill End (YYYY-MM-DD, optional)")
            w.text("country_filter", "ALL", "6. Country Filter (ALL or PK/IR/RU)")
    except Exception:
        pass


def _parse_date(value: Any, name: str) -> date:
    try:
        return datetime.strptime(str(value).strip(), "%Y-%m-%d").date()
    except ValueError:
        raise ValueError(f"[Parameter Error] '{name}' must be YYYY-MM-DD, got '{value}'.")


def resolve_silver_parameters(
    batch_date: Optional[str] = None,
    load_type: Optional[str] = None,
    lookback_days: Optional[int] = None,
    backfill_start: Optional[str] = None,
    backfill_end: Optional[str] = None,
    country_filter: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Resolves runtime parameters (explicit args > widgets > env PANOPTICON_* > defaults) and
    derives the Bronze partition window to process.

      incremental : [batch_date - lookback_days, batch_date]       (default 3-day lookback)
      backfill    : [backfill_start, backfill_end] if given, else the same lookback window
                    (set lookback_days=0 to re-process exactly one historical date)
      full        : the entire Bronze table (no date filter)
    """
    get = config.get_pipeline_parameter
    yesterday = (datetime.now(timezone.utc).date() - timedelta(days=1)).isoformat()

    r_load = str(load_type or get("load_type", "incremental")).lower()
    if r_load not in ("full", "incremental", "backfill"):
        raise ValueError(f"[Parameter Error] load_type must be full|incremental|backfill, got '{r_load}'.")

    r_batch = _parse_date(batch_date or get("batch_date", yesterday), "batch_date")
    r_lookback = int(lookback_days if lookback_days is not None else get("lookback_days", config.DEFAULT_LOOKBACK_DAYS))
    if r_lookback < 0:
        raise ValueError("[Parameter Error] lookback_days must be >= 0.")

    r_country = str(country_filter or get("country_filter", "ALL")).strip().upper() or "ALL"
    r_bf_start = str(backfill_start or get("backfill_start", "") or "").strip()
    r_bf_end = str(backfill_end or get("backfill_end", "") or "").strip()

    if r_load == "full":
        win_start = win_end = None
        batch_id = f"SILVER_FULL_{r_country}"
    else:
        if r_load == "backfill" and r_bf_start:
            win_start = _parse_date(r_bf_start, "backfill_start")
            win_end = _parse_date(r_bf_end, "backfill_end") if r_bf_end else win_start
        else:
            win_end = r_batch
            win_start = r_batch - timedelta(days=r_lookback)
        if win_start > win_end:
            raise ValueError(f"[Parameter Error] window start {win_start} is after end {win_end}.")
        batch_id = f"SILVER_{win_start:%Y%m%d}_{win_end:%Y%m%d}_{r_country}_{r_load.upper()}"

    return {
        "batch_date": r_batch.isoformat(), "load_type": r_load, "lookback_days": r_lookback,
        "window_start": win_start.isoformat() if win_start else None,
        "window_end": win_end.isoformat() if win_end else None,
        "country_filter": r_country, "batch_id": batch_id,
    }


def resolve_spark_session(spark: Optional[Any] = None) -> Any:
    """Active SparkSession + Community-Edition safeguards + UTC timestamp semantics."""
    session = spark or SparkSession.builder.getOrCreate()
    config.configure_spark_runtime(session)
    config._safe_conf_set(session.conf, "spark.sql.session.timeZone", "UTC")
    config._safe_conf_set(session.conf, "spark.sql.parquet.compression.codec", "snappy")
    # Allows MERGE ... INSERT * to evolve the Silver schema if a new Silver column is added later.
    config._safe_conf_set(session.conf, "spark.databricks.delta.schema.autoMerge.enabled", "true")
    return session


# COMMAND ----------

# ==============================================================================
# STEP 2: READ BRONZE (partition-pruned) + SCHEMA-DRIFT GUARD
# ==============================================================================

class SchemaDriftError(RuntimeError):
    """Raised when Bronze lacks a column Silver cannot work without."""


def detect_and_absorb_schema_drift(df_bronze: "DataFrame") -> Tuple["DataFrame", Dict[str, Any]]:
    """
    Compares the live Bronze schema with BRONZE_INPUT_CONTRACT.
      * missing column    -> SchemaDriftError (fail fast, nothing is written)
      * type changed      -> value is coerced with try_cast (bad values become NULL and are
                             then quarantined by the data-quality rules, not crashing the batch)
      * extra column      -> ignored (explicit projection), reported for visibility
    Returns the projected DataFrame (contract columns only, contract types) and a drift report.
    """
    actual = {f.name: f.dataType.simpleString() for f in df_bronze.schema.fields}
    missing = sorted(set(BRONZE_INPUT_CONTRACT) - set(actual))
    if missing:
        raise SchemaDriftError(f"Bronze is missing required columns: {missing}")

    changed = {c: (actual[c], t) for c, t in BRONZE_INPUT_CONTRACT.items() if actual[c] != t}
    known = set(BRONZE_INPUT_CONTRACT) | {"_source_file", "_batch_id", "measurement_uid", "test_start_time",
                                          "probe_id", "resolver_network_name", "software_name",
                                          "software_version", "test_version", "data_format_version",
                                          "annotations", "test_helpers", "extensions"}
    unexpected = sorted(set(actual) - known)

    projected = df_bronze.select(*[
        (F.col(c) if actual[c] == t else F.expr(f"try_cast(`{c}` AS {t})")).alias(c)
        for c, t in BRONZE_INPUT_CONTRACT.items()
    ])
    report = {"type_changes": changed, "unexpected_columns": unexpected}
    if changed or unexpected:
        print(f"[Schema Drift] type_changes={changed} unexpected_columns={unexpected} (absorbed, batch continues)")
    return projected, report


def read_bronze_window(spark, bronze_path: str, window_start: Optional[str],
                       window_end: Optional[str], country_filter: str) -> "DataFrame":
    """Reads Bronze with partition pruning on measurement_date (and optional country filter)."""
    df = spark.read.format("delta").load(bronze_path)
    if window_start and window_end:
        df = df.filter((F.col("measurement_date") >= F.lit(window_start).cast("date")) &
                       (F.col("measurement_date") <= F.lit(window_end).cast("date")))
    if country_filter != "ALL":
        df = df.filter(F.upper(F.col("probe_cc")) == country_filter)
    return df


def read_taxonomy(spark) -> "DataFrame":
    """Citizen Lab reference table. Degrades to empty (everything UNCATEGORIZED) with a warning;
    a later run re-enriches existing rows through the MERGE once the taxonomy exists."""
    path = config.PATHS["reference"]
    try:
        if DeltaTable.isDeltaTable(spark, path):
            return spark.read.format("delta").load(path)
    except Exception:
        pass
    print(f"[Taxonomy Warning] No reference table at {path}. All rows -> UNCATEGORIZED/LOW "
          f"until taxonomy is ingested and Silver is re-run.")
    schema = StructType([
        StructField("url", StringType()), StructField("category_code", StringType()),
        StructField("category_description", StringType()), StructField("country_code", StringType()),
    ])
    return spark.createDataFrame([], schema)


# COMMAND ----------

# ==============================================================================
# STEP 3: PURE TRANSFORMATION FUNCTIONS (unit-testable, no I/O)
# ==============================================================================

# ---- 3.1 Parsing helpers ------------------------------------------------------

def parse_utc_timestamp(c: "F.Column") -> "F.Column":
    """Multi-format, ANSI-safe parse (try_to_timestamp returns NULL instead of raising)."""
    return F.coalesce(
        F.try_to_timestamp(c, F.lit("yyyy-MM-dd HH:mm:ss")),
        F.try_to_timestamp(c, F.lit("yyyy-MM-dd'T'HH:mm:ss'Z'")),
        F.try_to_timestamp(c, F.lit("yyyy-MM-dd'T'HH:mm:ss")),
        F.try_to_timestamp(c, F.lit("yyyy-MM-dd HH:mm:ss.SSS")),
    )


def parse_asn(c: "F.Column") -> "F.Column":
    """'AS9541' / 'as9541' / '9541' -> 9541 (Long); anything else (incl. overflow) -> NULL."""
    digits = F.regexp_extract(F.upper(F.trim(c)), r"^(?:AS)?(\d+)$", 1)
    return F.when((F.length(digits) > 0) & (F.length(digits) <= 18), digits).cast("long")  # <=18 digits: no overflow


# ---- 3.2 PII sanitation -------------------------------------------------------

_SENSITIVE_QUERY_PARAMS = (
    r"(?:token|access_token|refresh_token|id_token|auth|authorization|apikey|api_key|key|"
    r"password|passwd|pwd|secret|session|session_id|sessionid|sid|jwt|signature|sig|code|"
    r"fbclid|gclid|utm_[a-z0-9_]+)"
)


def sanitize_url(c: "F.Column") -> "F.Column":
    """Strips userinfo (user:pass@) and redacts credential / tracking query parameter values."""
    no_userinfo = F.regexp_replace(c, r"^([A-Za-z][A-Za-z0-9+.\-]*://)[^/?#@]*@", "$1")
    return F.regexp_replace(no_userinfo, r"(?i)([?&]" + _SENSITIVE_QUERY_PARAMS + r"=)[^&#]*", "$1REDACTED")


def extract_domain(url_col: "F.Column") -> "F.Column":
    """Lower-cased host without scheme, userinfo, port, path or leading 'www.'. NULL for IPv6 literals.
    (Host-level, not public-suffix-aware: 'news.bbc.co.uk' stays 'news.bbc.co.uk'.)"""
    host = F.regexp_extract(
        F.lower(F.trim(url_col)),
        r"^(?:[a-z][a-z0-9+.\-]*://)?(?:[^/?#@]*@)?(?:www\.)?([^/:?#]+)", 1)
    return F.when((F.length(host) > 0) & ~host.startswith("["), host)


def mask_probe_subnet(ip_col: "F.Column") -> "F.Column":
    """IPv4 -> a.b.c.0/24 ; IPv6 (>=3 explicit groups) -> first three hextets ::/48 ; else NULL."""
    ip = F.trim(ip_col)
    v4 = F.when(ip.rlike(r"^\d{1,3}(\.\d{1,3}){3}$"),
                F.regexp_replace(ip, r"^(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.\d{1,3}$", "$1.$2.$3.0/24"))
    v6 = F.when(ip.rlike(r"^[0-9a-fA-F]{1,4}(:[0-9a-fA-F]{1,4}){2,}"),
                F.regexp_replace(ip, r"^([0-9a-fA-F]{1,4}):([0-9a-fA-F]{1,4}):([0-9a-fA-F]{1,4}).*$", "$1:$2:$3::/48"))
    return F.coalesce(v4, v6)


def make_hmac_udf(salt: str):
    """Real HMAC-SHA256 (key = secret salt, message = IP). Deterministic -> joinable across runs."""
    key = salt.encode("utf-8")

    def _hmac(ip: Optional[str]) -> Optional[str]:
        if ip is None or not ip.strip():
            return None
        return hmac.new(key, ip.strip().encode("utf-8"), hashlib.sha256).hexdigest()

    return F.udf(_hmac, StringType())


# ---- 3.3 Tampering classification --------------------------------------------

def _b(col: "F.Column") -> "F.Column":
    """NULL-safe boolean: NULL -> False."""
    return F.coalesce(col, F.lit(False))


def classify_tampering(df: "DataFrame") -> "DataFrame":
    """
    Adds dns/tcp/tls/http anomaly flags (int), is_anomaly and tampering_vector.
    Expects columns: test_name, input, tk (parsed test_keys struct).

    HEURISTICS (documented in the data dictionary; refine here and re-run - MERGE updates rows):
      web_connectivity  blocking='dns'                       -> DNS
                        blocking='tcp_ip' OR (http-failure + connection_reset) -> TCP
                        blocking='http-failure', https input, ssl/tls/eof/certificate failure -> TLS
                        blocking='http-diff' OR remaining http-failure          -> HTTP
      telegram          tcp_blocking=true -> TCP ; http_blocking=true OR web_status='blocked' -> HTTP
      whatsapp          endpoints_status='blocked' -> TCP ; non-empty dns_inconsistent -> DNS ;
                        registration_server_status='failed' OR web_status='blocked' -> HTTP
      signal            signal_backend_status='blocked' -> TLS
      anything else     BENIGN (no OONI blocking signal defined for that test)
    Inconclusive web_connectivity results (blocking NULL) are BENIGN / is_anomaly=false.
    Precedence when several layers fire: DNS > TCP > TLS > HTTP.
    """
    tn = F.col("test_name")
    low = lambda name: F.lower(F.col(f"tk.{name}"))  # noqa: E731

    # web_connectivity
    blocking = low("blocking")
    http_fail = low("http_experiment_failure")
    https_target = _b(F.lower(F.col("input")).startswith("https://"))
    wc_dns = _b(blocking == "dns")
    wc_tcp = _b(blocking == "tcp_ip") | (_b(blocking == "http-failure") & _b(http_fail.rlike("connection_reset")))
    wc_tls = (_b(blocking == "http-failure") & https_target &
              _b(http_fail.rlike("ssl|tls|eof_error|certificate")) & ~wc_tcp)
    wc_http = _b(blocking == "http-diff") | (_b(blocking == "http-failure") & ~wc_tcp & ~wc_tls)

    # telegram / whatsapp / signal
    tg_tcp = _b(low("telegram_tcp_blocking") == "true")
    tg_http = _b(low("telegram_http_blocking") == "true") | _b(low("telegram_web_status") == "blocked")
    wa_tcp = _b(low("whatsapp_endpoints_status") == "blocked")
    wa_dns = _b(~low("whatsapp_endpoints_dns_inconsistent").isin("", "[]"))
    wa_http = _b(low("registration_server_status") == "failed") | _b(low("whatsapp_web_status") == "blocked")
    sig_tls = _b(low("signal_backend_status") == "blocked")

    def pick(wc, tg, wa, sig):
        return (F.when(tn == "web_connectivity", wc).when(tn == "telegram", tg)
                 .when(tn == "whatsapp", wa).when(tn == "signal", sig).otherwise(F.lit(False)))

    F_FALSE = F.lit(False)
    dns = _b(pick(wc_dns, F_FALSE, wa_dns, F_FALSE))
    tcp = _b(pick(wc_tcp, tg_tcp, wa_tcp, F_FALSE))
    tls = _b(pick(wc_tls, F_FALSE, F_FALSE, sig_tls))
    http = _b(pick(wc_http, tg_http, wa_http, F_FALSE))

    return (df
        .withColumn("dns_anomaly_flag", dns.cast("int"))
        .withColumn("tcp_anomaly_flag", tcp.cast("int"))
        .withColumn("tls_anomaly_flag", tls.cast("int"))
        .withColumn("http_anomaly_flag", http.cast("int"))
        .withColumn("is_anomaly", dns | tcp | tls | http)
        .withColumn("tampering_vector",
                    F.when(dns, "DNS_TAMPERING").when(tcp, "TCP_RESET")
                     .when(tls, "TLS_DROP").when(http, "HTTP_BLOCK").otherwise("BENIGN")))


# ---- 3.4 Citizen Lab enrichment ----------------------------------------------

def risk_tier(category_col: "F.Column") -> "F.Column":
    return (F.when(category_col.isin(*RISK_TIER_MAP["HIGH"]), "HIGH")
             .when(category_col.isin(*RISK_TIER_MAP["MEDIUM"]), "MEDIUM")
             .otherwise("LOW"))


def prepare_taxonomy(df_ref: "DataFrame") -> "DataFrame":
    """
    Citizen Lab stores FULL URLs; Silver joins on host. Normalise the reference side with the
    SAME extract_domain() and keep one row per (domain, country scope) so the join cannot fan out.
    """
    base = (df_ref
        .select(extract_domain(F.col("url")).alias("ref_domain"),
                F.upper(F.trim(F.col("category_code"))).alias("ref_category"),
                F.col("category_description").alias("ref_description"),
                F.upper(F.trim(F.col("country_code"))).alias("ref_country"))
        .filter(F.col("ref_domain").isNotNull() & F.col("ref_category").isNotNull()))
    w = Window.partitionBy("ref_domain", "ref_country").orderBy(F.col("ref_category"))
    return base.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")


def enrich_with_taxonomy(df: "DataFrame", df_ref_prepared: "DataFrame") -> "DataFrame":
    """
    Left-join (broadcast: the reference is a few thousand rows, < 10 MB - allowed by AGENTS.md 2.2).
    A probe-country-specific entry wins over a GLOBAL entry; ties break on category code.
    """
    from pyspark.sql.functions import broadcast
    joined = df.join(
        broadcast(df_ref_prepared),
        (df["target_domain"] == df_ref_prepared["ref_domain"]) &
        ((df_ref_prepared["ref_country"] == df["probe_cc"]) | (df_ref_prepared["ref_country"] == "GLOBAL")),
        "left")
    w = Window.partitionBy("measurement_id").orderBy(
        (F.col("ref_country") == F.col("probe_cc")).desc_nulls_last(), F.col("ref_category").asc_nulls_last())
    best = joined.withColumn("_rk", F.row_number().over(w)).filter(F.col("_rk") == 1).drop("_rk")
    return (best
        .withColumn("content_category", F.coalesce(F.col("ref_category"), F.lit("UNCATEGORIZED")))
        .withColumn("category_description", F.coalesce(F.col("ref_description"), F.lit("Uncategorized")))
        .withColumn("human_rights_risk_tier", risk_tier(F.col("content_category")))
        .drop("ref_domain", "ref_category", "ref_description", "ref_country"))


# ---- 3.5 End-to-end Bronze -> Silver frame ------------------------------------

def transform_bronze_to_silver(
    df_bronze: "DataFrame", df_taxonomy: "DataFrame", salt: str, batch_id: str, load_ts: datetime,
) -> Tuple["DataFrame", "DataFrame"]:
    """
    Returns (df_valid, df_invalid).
      df_valid   : conformed to SILVER_SCHEMA, de-duplicated on measurement_id
      df_invalid : rows violating data-quality rules, with a `_dq_reason` (-> quarantine)
    """
    df = df_bronze.select(
        "report_id", "input", "test_name", "measurement_start_time", "probe_asn", "probe_cc",
        "probe_ip", "probe_network_name", "resolver_asn", "resolver_ip", "test_runtime",
        "test_keys", F.col("load_timestamp").alias("_bronze_load_ts"))

    # Casting & normalisation (ANSI-safe: invalid values become NULL, never raise)
    df = (df
        .withColumn("event_timestamp", parse_utc_timestamp(F.col("measurement_start_time")))
        .withColumn("measurement_date", F.to_date(F.col("event_timestamp")))
        .withColumn("probe_asn_n", parse_asn(F.col("probe_asn")))
        .withColumn("resolver_asn_n", F.when(parse_asn(F.col("resolver_asn")) > 0, parse_asn(F.col("resolver_asn"))))
        .withColumn("probe_cc_n", F.upper(F.trim(F.col("probe_cc"))))
        .withColumn("duration_seconds", F.col("test_runtime").cast("double")))

    dq_reason = (
        F.when(F.col("report_id").isNull() | (F.trim(F.col("report_id")) == ""), "MISSING_REPORT_ID")
         .when(F.col("test_name").isNull() | (F.trim(F.col("test_name")) == ""), "MISSING_TEST_NAME")
         .when(F.col("event_timestamp").isNull(), "UNPARSEABLE_MEASUREMENT_START_TIME")
         .when(~_b(F.col("probe_cc_n").rlike("^[A-Z]{2}$")), "INVALID_PROBE_CC")
         .when(F.col("probe_asn_n").isNull() | (F.col("probe_asn_n") <= 0), "INVALID_PROBE_ASN"))
    df = df.withColumn("_dq_reason", dq_reason)

    # Quarantine payload: non-PII columns only (no IPs, no test_keys)
    df_invalid = (df.filter(F.col("_dq_reason").isNotNull())
        .select(F.to_json(F.struct("report_id", "input", "test_name", "measurement_start_time",
                                   "probe_asn", "probe_cc", "probe_network_name", "resolver_asn",
                                   "test_runtime")).alias("raw_record"),
                F.col("_dq_reason")))

    valid = df.filter(F.col("_dq_reason").isNull())

    # Primary key: SHA-256( report_id || input || measurement_start_time ) over the RAW strings
    valid = valid.withColumn("measurement_id", F.sha2(F.concat_ws(
        "||", F.col("report_id"), F.coalesce(F.col("input"), F.lit("NO_INPUT")),
        F.col("measurement_start_time")), 256))

    # De-duplicate source on the PK (a MERGE with duplicate source keys would fail)
    w_dedup = Window.partitionBy("measurement_id").orderBy(
        F.col("_bronze_load_ts").desc_nulls_last(), F.col("test_runtime").desc_nulls_last())
    valid = valid.withColumn("_dr", F.row_number().over(w_dedup)).filter(F.col("_dr") == 1).drop("_dr")

    # Sanitise PII, derive dimensions, parse + classify test_keys
    hmac_udf = make_hmac_udf(salt)
    valid = (valid
        .withColumn("target_url", sanitize_url(F.col("input")))
        .withColumn("target_domain", extract_domain(F.col("target_url")))
        .withColumn("masked_probe_subnet", mask_probe_subnet(F.col("probe_ip")))
        .withColumn("hashed_resolver_ip", hmac_udf(F.col("resolver_ip")))
        .withColumn("tk", F.from_json(F.col("test_keys"), TEST_KEYS_SCHEMA, {"mode": "PERMISSIVE"}))
        .withColumn("dns_experiment_failure",
                    F.when(F.col("test_name") == "web_connectivity", F.col("tk.dns_experiment_failure")))
        .withColumn("http_experiment_failure",
                    F.when(F.col("test_name") == "web_connectivity", F.col("tk.http_experiment_failure"))))
    valid = classify_tampering(valid)
    valid = enrich_with_taxonomy(valid, prepare_taxonomy(df_taxonomy))

    df_valid = (valid
        .withColumn("probe_asn", F.col("probe_asn_n"))
        .withColumn("resolver_asn", F.col("resolver_asn_n"))
        .withColumn("probe_cc", F.col("probe_cc_n"))
        .withColumn("load_timestamp", F.lit(load_ts).cast("timestamp"))
        .withColumn("_batch_id", F.lit(batch_id))
        .select(*[F.col(f.name).cast(f.dataType).alias(f.name) for f in SILVER_SCHEMA.fields]))
    return df_valid, df_invalid


# COMMAND ----------

# ==============================================================================
# STEP 4: DELTA PERSISTENCE (idempotent MERGE INTO) + QUARANTINE
# ==============================================================================

def ensure_silver_table(spark, silver_path: str) -> None:
    """Creates the empty, explicitly-typed, partitioned Silver Delta table on first run."""
    if not DeltaTable.isDeltaTable(spark, silver_path):
        print(f"[Silver Persistence] Creating Silver Delta table at {silver_path}")
        (spark.createDataFrame([], SILVER_SCHEMA).write.format("delta")
              .partitionBy("measurement_date").save(silver_path))
    try:
        spark.sql(f"CREATE TABLE IF NOT EXISTS {config.TABLE_NAMES['silver']} USING DELTA LOCATION '{silver_path}'")
    except Exception:
        pass  # metastore registration is best-effort (same as Bronze)


# Enrichment columns that MERGE keeps in sync. NULL-safe <=> so NULL -> value changes are detected.
_ENRICHED_COLUMNS = [
    "content_category", "category_description", "human_rights_risk_tier", "tampering_vector",
    "is_anomaly", "dns_anomaly_flag", "tcp_anomaly_flag", "tls_anomaly_flag", "http_anomaly_flag",
]


def merge_into_silver(spark, df_silver: "DataFrame", silver_path: str,
                      window_start: Optional[str], window_end: Optional[str]) -> Dict[str, int]:
    """
    Idempotent upsert on measurement_id.
      WHEN NOT MATCHED            -> INSERT  (new + late-arriving measurements)
      WHEN MATCHED AND <changed>  -> UPDATE enriched columns + load_timestamp
    Re-running the same data changes nothing: no duplicates, no spurious updates.
    """
    ensure_silver_table(spark, silver_path)
    target = DeltaTable.forPath(spark, silver_path)

    cond = "target.measurement_id = source.measurement_id AND target.measurement_date = source.measurement_date"
    if window_start and window_end:   # literal predicate -> static partition pruning of the target
        cond += (f" AND target.measurement_date >= '{window_start}'"
                 f" AND target.measurement_date <= '{window_end}'")

    changed = " OR ".join(f"NOT (target.{c} <=> source.{c})" for c in _ENRICHED_COLUMNS)
    update_set = {c: f"source.{c}" for c in _ENRICHED_COLUMNS}
    update_set.update({"load_timestamp": "source.load_timestamp", "_batch_id": "source._batch_id"})

    (target.alias("target")
        .merge(df_silver.alias("source"), cond)
        .whenMatchedUpdate(condition=changed, set=update_set)
        .whenNotMatchedInsertAll()
        .execute())

    inserted = updated = 0
    try:
        m = target.history(1).select("operationMetrics").first()["operationMetrics"]
        inserted, updated = int(m.get("numTargetRowsInserted", 0)), int(m.get("numTargetRowsUpdated", 0))
    except Exception as err:
        print(f"[Silver Persistence Warning] Could not read MERGE metrics: {err}")
    print(f"[Silver Persistence] MERGE complete: inserted={inserted}, updated={updated}")
    return {"rows_inserted": inserted, "rows_updated": updated}


def quarantine_invalid_rows(spark, df_invalid: "DataFrame", batch_id: str, quarantine_path: str) -> int:
    """Idempotent: MERGE on (raw_record, _source_file) so re-runs never duplicate quarantine rows."""
    n = df_invalid.count()
    if n == 0:
        return 0
    staged = (df_invalid.select(
        F.col("raw_record"), F.lit(QUARANTINE_SOURCE_TAG).alias("_source_file"),
        F.lit(batch_id).alias("_batch_id"), F.current_timestamp().alias("quarantine_timestamp"),
        F.concat(F.lit("Silver DQ violation: "), F.col("_dq_reason")).alias("quarantine_reason"))
        .dropDuplicates(["raw_record"]))
    if DeltaTable.isDeltaTable(spark, quarantine_path):
        (DeltaTable.forPath(spark, quarantine_path).alias("t")
            .merge(staged.alias("s"), "t.raw_record = s.raw_record AND t._source_file = s._source_file")
            .whenNotMatchedInsertAll().execute())
    else:
        staged.write.format("delta").mode("append").save(quarantine_path)
    print(f"[Quarantine Router] {n} rows violated Silver data-quality rules -> {quarantine_path}")
    return n


def optimize_silver(spark, silver_path: str, window_start: Optional[str], window_end: Optional[str]) -> None:
    """ZORDER (probe_cc, probe_asn, tampering_vector), scoped to the processed partitions only."""
    try:
        opt = DeltaTable.forPath(spark, silver_path).optimize()
        if window_start and window_end:
            opt = opt.where(f"measurement_date >= '{window_start}' AND measurement_date <= '{window_end}'")
        opt.executeZOrderBy("probe_cc", "probe_asn", "tampering_vector")
        print("[Silver Optimize] ZORDER BY (probe_cc, probe_asn, tampering_vector) complete.")
    except Exception as err:   # never fail the pipeline on file-layout maintenance
        print(f"[Silver Optimize Warning] OPTIMIZE/ZORDER skipped: {err}")


# COMMAND ----------

# ==============================================================================
# STEP 5: ORCHESTRATION + AUDIT LOGGING
# ==============================================================================

class _NullAuditor:
    """Fallback if 00_audit_logger cannot be imported; keeps the pipeline runnable."""
    def __enter__(self): return self
    def __exit__(self, *exc): return False
    def set_metrics(self, **kw): pass


def run_bronze_to_silver(
    spark: Optional[Any] = None,
    batch_date: Optional[str] = None,
    load_type: Optional[str] = None,
    lookback_days: Optional[int] = None,
    backfill_start: Optional[str] = None,
    backfill_end: Optional[str] = None,
    country_filter: Optional[str] = None,
    bronze_path: Optional[str] = None,
    silver_path: Optional[str] = None,
    quarantine_path: Optional[str] = None,
    run_optimize: bool = True,
) -> Dict[str, Any]:
    """
    Bronze -> Silver for any date window. Re-runnable at will:

      incremental :  run_bronze_to_silver(batch_date="2026-09-28")                 # T-3 .. T
      backfill    :  run_bronze_to_silver(load_type="backfill", backfill_start="2026-09-10",
                                          backfill_end="2026-09-14")
      one-day fix :  run_bronze_to_silver(load_type="backfill", batch_date="2026-09-22", lookback_days=0)
      full        :  run_bronze_to_silver(load_type="full")
    """
    p = resolve_silver_parameters(batch_date, load_type, lookback_days, backfill_start, backfill_end, country_filter)
    session = resolve_spark_session(spark)
    bronze_path = bronze_path or config.PATHS["bronze"]
    silver_path = silver_path or config.PATHS["silver"]
    quarantine_path = quarantine_path or config.PATHS["quarantine"]
    load_ts = datetime.now(timezone.utc).replace(tzinfo=None)   # one exact processing instant per batch

    auditor = (audit_logger.PipelineAuditor(layer="Bronze-to-Silver", parameter=p["batch_id"],
                                            load_type=p["load_type"], spark=session)
               if audit_logger and hasattr(audit_logger, "PipelineAuditor") else _NullAuditor())

    with auditor as aud:
        print(f"[Silver] batch_id={p['batch_id']} window=[{p['window_start']} .. {p['window_end']}] "
              f"country={p['country_filter']}")

        df_raw = read_bronze_window(session, bronze_path, p["window_start"], p["window_end"], p["country_filter"])
        df_in, drift = detect_and_absorb_schema_drift(df_raw)
        rows_read = df_in.count()

        df_valid, df_invalid = transform_bronze_to_silver(
            df_in, read_taxonomy(session), config.get_pii_salt(), p["batch_id"], load_ts)

        quarantined = quarantine_invalid_rows(session, df_invalid, p["batch_id"], quarantine_path)
        rows_staged = df_valid.count()
        dup_dropped = max(rows_read - quarantined - rows_staged, 0)
        if dup_dropped:
            print(f"[Silver] {dup_dropped} duplicate measurement_id rows collapsed before MERGE.")

        merge_metrics = (merge_into_silver(session, df_valid, silver_path, p["window_start"], p["window_end"])
                         if rows_staged > 0 else {"rows_inserted": 0, "rows_updated": 0})
        if run_optimize and rows_staged > 0:
            optimize_silver(session, silver_path, p["window_start"], p["window_end"])

        aud.set_metrics(rows_read=rows_read, rows_inserted=merge_metrics["rows_inserted"],
                        rows_updated=merge_metrics["rows_updated"], rows_quarantined=quarantined)

    return {**p, "rows_read": rows_read, "rows_staged": rows_staged, "duplicates_collapsed": dup_dropped,
            "rows_quarantined": quarantined, **merge_metrics, "schema_drift": drift,
            "silver_path": silver_path,
            "status": "Success" if quarantined == 0 else "Partial_Quarantine"}


# COMMAND ----------

if __name__ == "__main__":
    print("=" * 75)
    print(" PROJECT PANOPTICON: SILVER TRANSFORMATION ENGINE")
    print("=" * 75)
    init_silver_widgets()
    summary = run_bronze_to_silver()          # parameters come from widgets / env / defaults
    for k, v in summary.items():
        print(f"  * {k:<20}: {v}")
