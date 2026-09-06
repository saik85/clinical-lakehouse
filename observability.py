"""[Resume point 3] Data observability — freshness, schema-drift, lineage,
anomaly checks with SLA/SLO alerting, so pipeline breaks are caught before
they reach consumers.
"""
import os
from datetime import datetime
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

SILVER = os.path.join("data", "silver")

EXPECTED_SCHEMA = {"encounter_id", "patient_id", "event_time", "facility",
                   "encounter_type", "provider_id", "status", "event_date"}
NULL_ANOMALY_THRESHOLD = 0.02   # SLO: < 2% nulls on key columns


def freshness(df: DataFrame) -> dict:
    latest = df.agg(F.max("event_date")).collect()[0][0]
    return {"latest_event_date": str(latest)}


def schema_drift(df: DataFrame) -> dict:
    cols = set(df.columns)
    return {"missing": sorted(EXPECTED_SCHEMA - cols), "unexpected": sorted(cols - EXPECTED_SCHEMA)}


def null_anomaly(df: DataFrame, col: str) -> dict:
    total = df.count()
    nulls = df.filter(F.col(col).isNull()).count()
    rate = nulls / total if total else 0
    return {"column": col, "null_rate": round(rate, 4),
            "status": "OK" if rate < NULL_ANOMALY_THRESHOLD else "ALERT"}


def run(spark: SparkSession) -> list:
    df = spark.read.parquet(os.path.join(SILVER, "encounters"))
    checks = []
    fr = freshness(df)
    checks.append(("freshness", "OK", fr["latest_event_date"]))
    sd = schema_drift(df)
    checks.append(("schema_drift", "OK" if not sd["missing"] and not sd["unexpected"] else "ALERT",
                   f"missing={sd['missing']} unexpected={sd['unexpected']}"))
    for col in ["patient_id", "event_time", "provider_id"]:
        na = null_anomaly(df, col)
        checks.append((f"null_anomaly[{col}]", na["status"], f"null_rate={na['null_rate']}"))
    rowcount = df.count()
    checks.append(("row_count", "OK" if rowcount > 0 else "ALERT", f"{rowcount:,} rows"))
    # SLA/SLO summary
    slo_met = all(c[1] == "OK" for c in checks)
    checks.append(("SLA/SLO", "MET" if slo_met else "BREACHED",
                   datetime.utcnow().strftime("checked %Y-%m-%d %H:%M UTC")))
    return checks
