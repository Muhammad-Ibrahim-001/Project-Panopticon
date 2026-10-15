"""
Local unit tests for notebooks/02_silver_transformation.py (pure transformation layer).

Runs on plain PySpark with ANSI mode ON (mirrors Databricks Serverless, where invalid casts raise).
The Delta MERGE itself needs Delta JARs and is verified on Databricks (see Docs/Phase 2/07_execution_guide.md).

    pip install pyspark==3.5.1 delta-spark==3.0.0 pytest
    pytest tests/test_silver_transformations.py -v
"""
import csv, hashlib, hmac, importlib.util, json, os, sys
from datetime import datetime

import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(ROOT, "notebooks"))

from pyspark.sql import SparkSession
from pyspark.sql.types import (StructType, StructField, StringType, DoubleType,
                               DateType, TimestampType)

_spec = importlib.util.spec_from_file_location("silver", os.path.join(ROOT, "notebooks", "02_silver_transformation.py"))
silver = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(silver)

SALT = "unit-test-salt"
LOAD_TS = datetime(2026, 10, 11, 12, 0, 0)
BRONZE_SCHEMA = StructType([
    StructField("report_id", StringType()), StructField("input", StringType()),
    StructField("test_name", StringType()), StructField("measurement_start_time", StringType()),
    StructField("probe_asn", StringType()), StructField("probe_cc", StringType()),
    StructField("probe_ip", StringType()), StructField("probe_network_name", StringType()),
    StructField("resolver_asn", StringType()), StructField("resolver_ip", StringType()),
    StructField("test_runtime", DoubleType()), StructField("test_keys", StringType()),
    StructField("measurement_date", DateType()), StructField("load_timestamp", TimestampType()),
])


@pytest.fixture(scope="module")
def spark():
    s = (SparkSession.builder.master("local[2]").appName("silver-tests")
         .config("spark.sql.shuffle.partitions", "2").config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.ansi.enabled", "true").config("spark.ui.enabled", "false").getOrCreate())
    yield s
    s.stop()


def _row(report_id="R1", inp="https://example.org/", test="web_connectivity", start="2026-09-27 10:00:00",
         asn="AS9541", cc="PK", pip="203.0.113.77", resolver_ip="202.163.69.18", rasn="AS9541",
         runtime=1.5, tk=None, bronze_ts=datetime(2026, 9, 27, 11, 0, 0)):
    return (report_id, inp, test, start, asn, cc, pip, "ISP", rasn, resolver_ip, runtime,
            json.dumps(tk if tk is not None else {"blocking": False, "accessible": True}),
            datetime(2026, 9, 27).date(), bronze_ts)


def _taxonomy(spark):
    schema = StructType([StructField(n, StringType()) for n in ("url", "category_code", "category_description", "country_code")])
    rows = [("https://example.org/", "NEWS", "News Media", "PK"),
            ("http://www.blocked.net/page", "ANON", "Anonymization and circumvention tools", "GLOBAL"),
            ("http://www.blocked.net/", "CULTR", "Culture", "PK"),       # country-specific entry must win
            ("https://faith.org/", "REL", "Religion", "GLOBAL"),
            ("https://shop.pk/", "COMM", "E-commerce", "PK")]
    return spark.createDataFrame(rows, schema)


def _run(spark, rows):
    df = spark.createDataFrame(rows, BRONZE_SCHEMA)
    valid, invalid = silver.transform_bronze_to_silver(df, _taxonomy(spark), SALT, "B1", LOAD_TS)
    return valid, invalid


def _by_report(valid):
    return {r["report_id"]: r.asDict() for r in valid.collect()}


def test_schema_matches_contract_json(spark):
    valid, _ = _run(spark, [_row()])
    assert [(f.name, f.dataType) for f in valid.schema.fields] == [(f.name, f.dataType) for f in silver.SILVER_SCHEMA.fields]
    contract = json.load(open(os.path.join(ROOT, "data", "schemas", "silver_conformed_schema.json")))
    assert [f["name"] for f in contract["fields"]] == silver.SILVER_COLUMNS
    json_to_spark = {"string": "string", "timestamp": "timestamp", "date": "date", "long": "bigint",
                     "integer": "int", "boolean": "boolean", "double": "double"}
    for f, c in zip(silver.SILVER_SCHEMA.fields, contract["fields"]):
        assert f.dataType.simpleString() == json_to_spark[c["type"]], (f.name, c["type"])
        assert f.nullable == c["nullable"], f.name


def test_primary_key_is_deterministic_sha256(spark):
    valid, _ = _run(spark, [_row(), _row(inp=None, report_id="R2")])
    got = _by_report(valid)
    expect = hashlib.sha256("R1||https://example.org/||2026-09-27 10:00:00".encode()).hexdigest()
    assert got["R1"]["measurement_id"] == expect
    assert got["R2"]["measurement_id"] == hashlib.sha256("R2||NO_INPUT||2026-09-27 10:00:00".encode()).hexdigest()


def test_duplicates_collapse_to_one_row(spark):
    older, newer = datetime(2026, 9, 27, 11, 0), datetime(2026, 9, 27, 12, 0)
    valid, _ = _run(spark, [_row(bronze_ts=older, runtime=1.0), _row(bronze_ts=newer, runtime=2.0)])
    rows = valid.collect()
    assert len(rows) == 1 and rows[0]["duration_seconds"] == 2.0          # newest Bronze copy wins


def test_pii_is_sanitised_and_no_raw_ip_survives(spark):
    valid, _ = _run(spark, [_row(inp="https://user:pw@example.org/p?token=abc123&q=1&utm_source=x")])
    r = valid.collect()[0].asDict()
    assert r["target_url"] == "https://example.org/p?token=REDACTED&q=1&utm_source=REDACTED"
    assert r["masked_probe_subnet"] == "203.0.113.0/24"
    assert r["hashed_resolver_ip"] == hmac.new(SALT.encode(), b"202.163.69.18", hashlib.sha256).hexdigest()
    blob = json.dumps([str(v) for v in r.values()])
    assert "203.0.113.77" not in blob and "202.163.69.18" not in blob


def test_ipv6_and_missing_ip_handling(spark):
    valid, _ = _run(spark, [_row(report_id="V6", pip="2001:db8:abcd:12:1:2:3:4"), _row(report_id="NOIP", pip=None, resolver_ip=None)])
    g = _by_report(valid)
    assert g["V6"]["masked_probe_subnet"] == "2001:db8:abcd::/48"
    assert g["NOIP"]["masked_probe_subnet"] is None and g["NOIP"]["hashed_resolver_ip"] is None


def test_type_casting(spark):
    valid, _ = _run(spark, [_row(asn="as9541", rasn="AS0")])
    r = valid.collect()[0]
    assert r["probe_asn"] == 9541 and r["resolver_asn"] is None
    assert r["event_timestamp"] == datetime(2026, 9, 27, 10, 0, 0) and str(r["measurement_date"]) == "2026-09-27"


@pytest.mark.parametrize("tk,inp,test,vector,flags", [
    ({"blocking": False, "accessible": True}, "https://a.org/", "web_connectivity", "BENIGN", (0, 0, 0, 0)),
    ({"blocking": None, "accessible": None, "http_experiment_failure": "host_unreachable"}, "https://a.org/", "web_connectivity", "BENIGN", (0, 0, 0, 0)),
    ({"blocking": "dns", "dns_experiment_failure": "dns_nxdomain_error"}, "https://a.org/", "web_connectivity", "DNS_TAMPERING", (1, 0, 0, 0)),
    ({"blocking": "tcp_ip"}, "https://a.org/", "web_connectivity", "TCP_RESET", (0, 1, 0, 0)),
    ({"blocking": "http-failure", "http_experiment_failure": "connection_reset"}, "https://a.org/", "web_connectivity", "TCP_RESET", (0, 1, 0, 0)),
    ({"blocking": "http-failure", "http_experiment_failure": "ssl_invalid_hostname"}, "https://a.org/", "web_connectivity", "TLS_DROP", (0, 0, 1, 0)),
    ({"blocking": "http-failure", "http_experiment_failure": "ssl_invalid_hostname"}, "http://a.org/", "web_connectivity", "HTTP_BLOCK", (0, 0, 0, 1)),
    ({"blocking": "http-diff"}, "http://a.org/", "web_connectivity", "HTTP_BLOCK", (0, 0, 0, 1)),
    ({"blocking": "dns", "http_experiment_failure": "connection_reset"}, "https://a.org/", "web_connectivity", "DNS_TAMPERING", (1, 0, 0, 0)),
    ({"telegram_tcp_blocking": True, "telegram_http_blocking": False, "telegram_web_status": "ok"}, None, "telegram", "TCP_RESET", (0, 1, 0, 0)),
    ({"telegram_tcp_blocking": False, "telegram_http_blocking": False, "telegram_web_status": "blocked"}, None, "telegram", "HTTP_BLOCK", (0, 0, 0, 1)),
    ({"whatsapp_endpoints_status": "ok", "whatsapp_endpoints_dns_inconsistent": ["x"], "whatsapp_web_status": "ok"}, None, "whatsapp", "DNS_TAMPERING", (1, 0, 0, 0)),
    ({"whatsapp_endpoints_status": "ok", "whatsapp_endpoints_dns_inconsistent": [], "whatsapp_web_status": "ok"}, None, "whatsapp", "BENIGN", (0, 0, 0, 0)),
    ({"signal_backend_status": "blocked"}, None, "signal", "TLS_DROP", (0, 0, 1, 0)),
    ({"anything": 1}, "stun://stun.example.com:3478", "stunreachability", "BENIGN", (0, 0, 0, 0)),
])
def test_tampering_classification(spark, tk, inp, test, vector, flags):
    valid, _ = _run(spark, [_row(inp=inp, test=test, tk=tk)])
    r = valid.collect()[0]
    assert r["tampering_vector"] == vector
    assert (r["dns_anomaly_flag"], r["tcp_anomaly_flag"], r["tls_anomaly_flag"], r["http_anomaly_flag"]) == flags
    assert r["is_anomaly"] == (vector != "BENIGN")


def test_union_typed_test_keys_cannot_break_parsing(spark):
    # 'blocking' as bool, string, number, object; flags as strings - none may raise or null the struct
    rows = [_row(report_id=f"U{i}", tk=tk) for i, tk in enumerate([
        {"blocking": "dns"}, {"blocking": 7}, {"blocking": {"a": 1}}, {"blocking": "dns", "accessible": "yes"}])]
    valid, _ = _run(spark, rows)
    g = _by_report(valid)
    assert g["U0"]["tampering_vector"] == "DNS_TAMPERING" and g["U3"]["tampering_vector"] == "DNS_TAMPERING"
    assert g["U1"]["tampering_vector"] == "BENIGN"


def test_malformed_test_keys_is_benign_not_fatal(spark):
    row = list(_row()); row[11] = "{not json"
    valid, _ = _run(spark, [tuple(row)])
    assert valid.collect()[0]["tampering_vector"] == "BENIGN"


def test_quarantine_rules(spark):
    bad = [_row(report_id=None), _row(report_id="BT", start="27/09/2026 10:00"), _row(report_id="BA", asn="ASxyz"),
           _row(report_id="B0", asn="AS0"), _row(report_id="BC", cc="Pakistan"), _row(report_id="OK")]
    valid, invalid = _run(spark, bad)
    assert [r["report_id"] for r in valid.collect()] == ["OK"]
    reasons = sorted(r["_dq_reason"] for r in invalid.collect())
    assert reasons == sorted(["MISSING_REPORT_ID", "UNPARSEABLE_MEASUREMENT_START_TIME", "INVALID_PROBE_ASN",
                              "INVALID_PROBE_ASN", "INVALID_PROBE_CC"])
    assert all("203.0.113.77" not in (r["raw_record"] or "") for r in invalid.collect())   # no IPs in quarantine


def test_citizenlab_enrichment_and_risk_tiers(spark):
    rows = [_row(report_id="N", inp="https://www.Example.org/some/path"),          # host match despite www/path/case
            _row(report_id="G", inp="http://blocked.net/"),                          # PK entry beats GLOBAL entry
            _row(report_id="R", inp="https://faith.org/"),
            _row(report_id="C", inp="https://shop.pk/"),
            _row(report_id="U", inp="https://unknown.example/"),
            _row(report_id="NULLIN", inp=None, test="telegram", tk={})]
    g = _by_report(_run(spark, rows)[0])
    assert (g["N"]["content_category"], g["N"]["human_rights_risk_tier"]) == ("NEWS", "HIGH")
    assert (g["G"]["content_category"], g["G"]["human_rights_risk_tier"]) == ("CULTR", "LOW")
    assert (g["R"]["content_category"], g["R"]["human_rights_risk_tier"]) == ("REL", "MEDIUM")
    assert (g["C"]["content_category"], g["C"]["human_rights_risk_tier"]) == ("COMM", "LOW")
    assert (g["U"]["content_category"], g["U"]["human_rights_risk_tier"]) == ("UNCATEGORIZED", "LOW")
    assert g["NULLIN"]["target_url"] is None and g["NULLIN"]["content_category"] == "UNCATEGORIZED"


def test_taxonomy_join_never_fans_out(spark):
    schema = StructType([StructField(n, StringType()) for n in ("url", "category_code", "category_description", "country_code")])
    ref = spark.createDataFrame([("https://example.org/a", "NEWS", "n", "PK"), ("https://example.org/b", "POLR", "p", "PK"),
                                 ("https://example.org/c", "REL", "r", "GLOBAL")], schema)
    df = spark.createDataFrame([_row()], BRONZE_SCHEMA)
    valid, _ = silver.transform_bronze_to_silver(df, ref, SALT, "B1", LOAD_TS)
    assert valid.count() == 1


def test_transform_is_deterministic_across_reruns(spark):
    rows = [_row(report_id=f"D{i}", inp=f"https://s{i}.org/") for i in range(20)]
    a = sorted(tuple(map(str, r)) for r in _run(spark, rows)[0].collect())
    b = sorted(tuple(map(str, r)) for r in _run(spark, rows)[0].collect())
    assert a == b                                                   # identical -> MERGE finds nothing to change


def test_real_sample_files_end_to_end(spark):
    recs = []
    for fn in ("sample_full_load_ooni.json", "sample_incremental_ooni.json"):
        for x in json.load(open(os.path.join(ROOT, "data", "samples", fn))):
            recs.append(_row(report_id=x["report_id"], inp=x.get("input"), test=x["test_name"],
                             start=x["measurement_start_time"], asn=x["probe_asn"], cc=x["probe_cc"],
                             pip=x["probe_ip"], resolver_ip=x["resolver_ip"], rasn=x["resolver_asn"],
                             runtime=x["test_runtime"], tk=x["test_keys"] if isinstance(x["test_keys"], dict) else json.loads(x["test_keys"])))
    valid, invalid = _run(spark, recs)
    assert invalid.count() == 0 and valid.count() == len(recs)
    assert valid.select("measurement_id").distinct().count() == len(recs)
    vectors = {r["tampering_vector"] for r in valid.select("tampering_vector").distinct().collect()}
    assert {"BENIGN", "DNS_TAMPERING"} <= vectors


def test_parameter_resolution_windows():
    p = silver.resolve_silver_parameters(batch_date="2026-09-28", load_type="incremental", lookback_days=3)
    assert (p["window_start"], p["window_end"]) == ("2026-09-25", "2026-09-28")
    p = silver.resolve_silver_parameters(batch_date="2026-09-22", load_type="backfill", lookback_days=0)
    assert (p["window_start"], p["window_end"]) == ("2026-09-22", "2026-09-22")
    p = silver.resolve_silver_parameters(load_type="backfill", backfill_start="2026-09-10", backfill_end="2026-09-14")
    assert (p["window_start"], p["window_end"]) == ("2026-09-10", "2026-09-14")
    p = silver.resolve_silver_parameters(load_type="full")
    assert p["window_start"] is None and p["batch_id"] == "SILVER_FULL_ALL"
    with pytest.raises(ValueError):
        silver.resolve_silver_parameters(load_type="bogus")
    with pytest.raises(ValueError):
        silver.resolve_silver_parameters(load_type="backfill", backfill_start="2026-09-14", backfill_end="2026-09-10")
    assert silver.resolve_silver_parameters(batch_date="2026-09-28")["batch_id"] == \
           silver.resolve_silver_parameters(batch_date="2026-09-28")["batch_id"]      # deterministic


# ---------------------------- schema drift guard --------------------------------
def test_drift_extra_column_is_ignored_and_reported(spark):
    df = spark.createDataFrame([_row()], BRONZE_SCHEMA).withColumn("brand_new_ooni_field", __import__("pyspark.sql.functions", fromlist=["lit"]).lit("x"))
    out, rep = silver.detect_and_absorb_schema_drift(df)
    assert rep["unexpected_columns"] == ["brand_new_ooni_field"]
    assert out.columns == list(silver.BRONZE_INPUT_CONTRACT)


def test_drift_type_change_is_coerced_not_fatal(spark):
    # test_runtime arrives as a non-numeric string, probe_asn arrives as an integer
    F = __import__("pyspark.sql.functions", fromlist=["col"])
    df = (spark.createDataFrame([_row(report_id="A"), _row(report_id="B")], BRONZE_SCHEMA)
          .withColumn("test_runtime", F.when(F.col("report_id") == "A", F.lit("1.25")).otherwise(F.lit("oops")))
          .withColumn("probe_asn", F.lit(9541)))
    out, rep = silver.detect_and_absorb_schema_drift(df)
    assert set(rep["type_changes"]) == {"test_runtime", "probe_asn"}
    got = {r["report_id"]: r for r in out.collect()}
    assert got["A"]["test_runtime"] == 1.25 and got["B"]["test_runtime"] is None and got["A"]["probe_asn"] == "9541"
    valid, invalid = silver.transform_bronze_to_silver(out, _taxonomy(spark), SALT, "B1", LOAD_TS)
    assert valid.count() == 2 and invalid.count() == 0              # batch survives


def test_drift_missing_required_column_fails_fast(spark):
    df = spark.createDataFrame([_row()], BRONZE_SCHEMA).drop("test_keys")
    with pytest.raises(silver.SchemaDriftError):
        silver.detect_and_absorb_schema_drift(df)
