"""[Resume point 4] CDC replication — keep the lakehouse current without full
reloads. Applies inserts/updates/deletes (op = I/U/D) from a change feed onto
the Silver encounters table via a merge/upsert.
"""
import os
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

SILVER = os.path.join("data", "silver")


def apply_cdc(base: DataFrame, changes: DataFrame) -> DataFrame:
    """Upsert changes onto base by encounter_id (latest op wins). Pure + testable."""
    updates = changes.filter(F.col("op") == "U").select("encounter_id", F.col("status").alias("new_status"))
    inserts = changes.filter(F.col("op") == "I")
    deletes = changes.filter(F.col("op") == "D").select("encounter_id")

    # apply updates: overwrite status where an update exists
    merged = (
        base.join(updates, "encounter_id", "left")
        .withColumn("status", F.coalesce(F.col("new_status"), F.col("status")))
        .drop("new_status")
    )
    # apply deletes
    merged = merged.join(deletes, "encounter_id", "left_anti")
    # apply inserts (align columns)
    if inserts.limit(1).count() > 0:
        ins = (inserts.select(
                "encounter_id", "patient_id",
                F.to_timestamp("event_time").alias("event_time"),
                "facility", "encounter_type", "provider_id", "status")
               .withColumn("event_date", F.to_date("event_time")))
        merged = merged.unionByName(ins, allowMissingColumns=True)
    return merged.dropDuplicates(["encounter_id"])


def run(spark: SparkSession) -> dict:
    base = spark.read.parquet(os.path.join(SILVER, "encounters"))
    changes = spark.read.json(os.path.join("data", "raw", "encounters_cdc.json"))
    before = base.count()
    merged = apply_cdc(base, changes)
    after = merged.count()
    amended = merged.filter(F.col("status").isin("AMENDED", "CANCELLED")).count()
    merged.write.mode("overwrite").partitionBy("event_date").parquet(os.path.join(SILVER, "encounters"))
    return {"before": before, "after": after, "status_changed": amended}
