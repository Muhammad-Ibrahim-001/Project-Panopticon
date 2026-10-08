# ==============================================================================
# PIPELINE DEPENDENCY & IMPACT TRACKING
# ==============================================================================
# Upstream Dependencies:
#   - [Module]: notebooks/00_config.py (Imports STORAGE_ROOT, PATHS, TABLE_NAMES)
# Downstream Impact:
#   - [Affects]: notebooks/01_bronze_ingestion.py
#   - [Affects]: notebooks/02_silver_transformation.py
#   - [Affects]: notebooks/run_pipeline.py
#   - [Schema/State Changes]:
#       * Manages lifecycle of 'pipeline_execution_logs' Delta table
#       * Records execution metrics (UUID, timestamps, row counts, failure traces)
# Risk & Side Effects:
#   - Schema alteration to audit logs requires schema evolution on existing Delta logs.
#   - Non-blocking design: logging failures emit warnings and never crash core business pipelines.
# ==============================================================================

"""
Operational Lakehouse Audit Framework for Project Panopticon.
Tracks pipeline execution metadata, duration, row counts, and exceptions in Delta Lake.
"""

import os
import sys
import uuid
import traceback
import importlib
from datetime import datetime
from typing import Optional, Dict, Any

from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, desc
from pyspark.sql.types import (
    StructType, StructField, StringType, 
    TimestampType, DoubleType, LongType
)
from delta.tables import DeltaTable

# Dynamic config import for both Databricks and standalone PySpark
config = None
for _mod_name in ("notebooks.00_config", "00_config"):
    try:
        config = importlib.import_module(_mod_name)
        break
    except (ImportError, ModuleNotFoundError):
        continue

# 13-column operational audit schema
AUDIT_LOG_SCHEMA = StructType([
    StructField("log_id",           StringType(),    False),
    StructField("pipeline_layer",   StringType(),    False),
    StructField("batch_parameter",  StringType(),    False),
    StructField("load_type",        StringType(),    False),
    StructField("start_time",       TimestampType(), False),
    StructField("end_time",         TimestampType(), True),
    StructField("duration_seconds", DoubleType(),    True),
    StructField("status",           StringType(),    False),
    StructField("rows_read",        LongType(),      True),
    StructField("rows_inserted",    LongType(),      True),
    StructField("rows_updated",     LongType(),      True),
    StructField("rows_quarantined", LongType(),      True),
    StructField("error_message",    StringType(),    True),
])


def init_audit_log_table(spark: Optional[SparkSession] = None, log_path: Optional[str] = None) -> str:
    """Initializes the Delta audit log table if it does not already exist."""
    resolved_spark = _resolve_spark_session(spark)
    target_path = log_path or (
        config.PATHS["audit_logs"] if config and hasattr(config, "PATHS")
        else "/FileStore/tables/panopticon/logs/pipeline_execution_logs"
    )

    try:
        if not DeltaTable.isDeltaTable(resolved_spark, target_path):
            empty_df = resolved_spark.createDataFrame([], AUDIT_LOG_SCHEMA)
            empty_df.write.format("delta").mode("overwrite").save(target_path)
            print(f"[AuditLogger] Initialized Delta audit table at: {target_path}")
    except Exception as err:
        print(f"[AuditLogger Warning] Table check failed at {target_path}: {err}")

    return target_path


class PipelineAuditor:
    """Context manager for tracking pipeline stage runs, row counts, and exceptions."""

    def __init__(
        self,
        spark_or_layer: Any = None,
        layer: Optional[str] = None,
        parameter: Optional[str] = None,
        load_type: str = "incremental",
        spark: Optional[SparkSession] = None,
        log_path: Optional[str] = None,
        **extra_metadata: Any
    ):
        # Resolve positional and keyword arguments dynamically
        if isinstance(spark_or_layer, SparkSession):
            self.spark = spark_or_layer
            self.layer = str(layer or "UNSPECIFIED_LAYER")
            self.parameter = str(parameter or "DEFAULT")
        elif isinstance(spark_or_layer, str):
            self.spark = _resolve_spark_session(spark)
            self.layer = spark_or_layer
            self.parameter = str(layer or parameter or "DEFAULT")
        else:
            self.spark = _resolve_spark_session(spark)
            self.layer = str(layer or "UNSPECIFIED_LAYER")
            self.parameter = str(parameter or "DEFAULT")

        self.load_type = str(load_type).capitalize() if load_type else "Incremental"

        # Execution state
        self.log_id = str(uuid.uuid4())
        self.start_time = datetime.utcnow()
        self.end_time: Optional[datetime] = None
        self.duration_seconds: Optional[float] = None
        self.status: str = "Running"
        self.error_message: Optional[str] = None

        # Row counters
        self.rows_read: int = 0
        self.rows_inserted: int = 0
        self.rows_updated: int = 0
        self.rows_quarantined: int = 0

        self.table_path = init_audit_log_table(self.spark, log_path=log_path)

    def set_metrics(
        self,
        rows_read: int = 0,
        rows_inserted: int = 0,
        rows_updated: int = 0,
        rows_quarantined: int = 0
    ) -> "PipelineAuditor":
        """Updates row counts during execution."""
        self.rows_read = int(rows_read or 0)
        self.rows_inserted = int(rows_inserted or 0)
        self.rows_updated = int(rows_updated or 0)
        self.rows_quarantined = int(rows_quarantined or 0)
        return self

    def _commit_to_delta(self) -> None:
        """Upserts current execution state into Delta table via MERGE INTO."""
        try:
            log_record = Row(
                log_id=self.log_id,
                pipeline_layer=self.layer,
                batch_parameter=str(self.parameter),
                load_type=self.load_type,
                start_time=self.start_time,
                end_time=self.end_time,
                duration_seconds=self.duration_seconds,
                status=self.status,
                rows_read=self.rows_read,
                rows_inserted=self.rows_inserted,
                rows_updated=self.rows_updated,
                rows_quarantined=self.rows_quarantined,
                error_message=self.error_message
            )

            log_df = self.spark.createDataFrame([log_record], AUDIT_LOG_SCHEMA)
            delta_table = DeltaTable.forPath(self.spark, self.table_path)

            (delta_table.alias("target")
                .merge(
                    source=log_df.alias("source"),
                    condition="target.log_id = source.log_id"
                )
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute())
        except Exception as log_err:
            print(f"[AuditLogger Warning] Could not persist log ({self.status}): {log_err}")

    def __enter__(self) -> "PipelineAuditor":
        """Logs initial state on entering the block."""
        self._commit_to_delta()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        """Calculates runtime, captures status or failure trace, and commits final log."""
        self.end_time = datetime.utcnow()
        self.duration_seconds = round((self.end_time - self.start_time).total_seconds(), 2)

        if exc_type is not None:
            self.status = "Failure"
            self.error_message = "".join(traceback.format_exception(exc_type, exc_val, exc_tb))
        elif self.rows_quarantined > 0:
            self.status = "Partial_Quarantine"
        else:
            self.status = "Success"

        self._commit_to_delta()
        return False  # Re-raises exceptions to notify caller


def query_recent_logs(
    spark: Optional[SparkSession] = None,
    limit: int = 10,
    log_path: Optional[str] = None
):
    """Returns recent audit logs ordered by start time descending."""
    resolved_spark = _resolve_spark_session(spark)
    target_path = log_path or (
        config.PATHS["audit_logs"] if config and hasattr(config, "PATHS")
        else "/FileStore/tables/panopticon/logs/pipeline_execution_logs"
    )

    return (
        resolved_spark.read.format("delta")
        .load(target_path)
        .orderBy(desc("start_time"))
        .limit(limit)
    )


def _resolve_spark_session(spark: Optional[SparkSession] = None) -> SparkSession:
    """Finds active SparkSession from args, active session, or global notebook scope."""
    if spark is not None and isinstance(spark, SparkSession):
        return spark

    active = SparkSession.getActiveSession()
    if active is not None:
        return active

    try:
        import IPython
        user_ns = IPython.get_ipython().user_ns
        if "spark" in user_ns and isinstance(user_ns["spark"], SparkSession):
            return user_ns["spark"]
    except Exception:
        pass

    return SparkSession.builder.appName("PanopticonAuditLogger").getOrCreate()


if __name__ == "__main__":
    print(f"Audit Logger loaded. Schema contains {len(AUDIT_LOG_SCHEMA.fields)} columns.")
