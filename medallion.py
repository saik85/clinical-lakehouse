"""[Resume point 1] Consolidate siloed EMR/EHR, claims, billing, lab & provider
feeds into governed Bronze -> Silver -> Gold Delta layers.

Parquet is used here for a zero-setup run; Delta Lake is the production format.
"""
import os
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

RAW = os.path.join("data", "raw")
BRONZE = os.path.join("data", "bronze")
SILVER = os.path.join("data", "silver")
GOLD = os.path.join("data", "gold")


def bronze_ingest(spark: SparkSession, feed: str) -> None:
    """Raw landing — as-is + ingestion metadata (no transforms)."""
    df = (spark.read.option("header", True).option("inferSchema", False)
          .csv(os.path.join(RAW, f"{feed}.csv"))
          .withColumn("_ingested_at", F.current_timestamp())
          .withColumn("_source_file", F.input_file_name()))
    df.write.mode("overwrite").parquet(os.path.join(BRONZE, feed))


def clean_encounters(df: DataFrame) -> DataFrame:
    """Silver transform for the encounters fact (pure, testable)."""
    return (
        df.select(
            F.col("encounter_id"),
            F.col("patient_id"),
            F.to_timestamp("event_time").alias("event_time"),
            F.upper(F.trim("facility")).alias("facility"),
            F.initcap(F.trim("encounter_type")).alias("encounter_type"),
            F.col("provider_id"),
            F.upper(F.trim("status")).alias("status"),
        )
        .filter(F.col("event_time").isNotNull())
        .filter((F.col("patient_id").isNotNull()) & (F.col("patient_id") != ""))
        .withColumn("event_date", F.to_date("event_time"))
        .dropDuplicates(["encounter_id"])
    )


def build_silver(spark: SparkSession) -> None:
    enc = clean_encounters(spark.read.parquet(os.path.join(BRONZE, "encounters")))
    enc.write.mode("overwrite").partitionBy("event_date").parquet(os.path.join(SILVER, "encounters"))
    # pass-through clean for the reference feeds
    for feed in ["demographics", "providers", "claims", "billing", "labs"]:
        spark.read.parquet(os.path.join(BRONZE, feed)).write.mode("overwrite").parquet(os.path.join(SILVER, feed))


def build_gold(spark: SparkSession) -> DataFrame:
    """Gold: daily encounter volume by facility & type."""
    enc = spark.read.parquet(os.path.join(SILVER, "encounters"))
    gold = (enc.groupBy("event_date", "facility", "encounter_type")
            .agg(F.count("*").alias("encounters"),
                 F.countDistinct("patient_id").alias("unique_patients"))
            .orderBy("event_date", "facility"))
    gold.write.mode("overwrite").partitionBy("event_date").parquet(os.path.join(GOLD, "encounter_daily"))
    return gold
