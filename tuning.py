"""[Resume point 9] Tuning & reliability — partitioning, file layout, and an
availability SLO. In production: Delta OPTIMIZE + Z-ORDER, cluster autoscaling,
adaptive query execution, and a 99.99% availability target.
"""

# Production tuning applied on Databricks/Delta (documented here; the demo
# writes partitioned Parquet so partition pruning is real):
TUNING = {
    "partitioning": "Silver/Gold partitioned by event_date (partition pruning)",
    "delta_optimize": "OPTIMIZE + ZORDER BY (patient_id) to compact small files",
    "aqe": "spark.sql.adaptive.enabled = true (skew + coalesce partitions)",
    "autoscaling": "job clusters autoscale on load; photon runtime",
    "availability_slo": 0.9999,   # 99.99%
}


def availability_report(uptime_minutes: float, window_minutes: float) -> dict:
    avail = uptime_minutes / window_minutes if window_minutes else 0.0
    return {
        "availability": round(avail, 6),
        "slo": TUNING["availability_slo"],
        "status": "MET" if avail >= TUNING["availability_slo"] else "BREACHED",
    }
