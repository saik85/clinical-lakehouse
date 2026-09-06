"""[Resume point 5] Patient 360 — unify encounters, demographics, claims, and
provider interactions into a single consistent patient view (data product).
"""
import os
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

SILVER = os.path.join("data", "silver")
GOLD = os.path.join("data", "gold")


def build(spark: SparkSession) -> DataFrame:
    enc = spark.read.parquet(os.path.join(SILVER, "encounters"))
    demo = spark.read.parquet(os.path.join(SILVER, "demographics"))
    claims = spark.read.parquet(os.path.join(SILVER, "claims"))
    labs = spark.read.parquet(os.path.join(SILVER, "labs"))

    enc_agg = enc.groupBy("patient_id").agg(
        F.count("*").alias("total_encounters"),
        F.max("event_time").alias("last_encounter"),
        F.countDistinct("facility").alias("facilities_visited"),
        F.countDistinct("provider_id").alias("providers_seen"),
    )
    claim_agg = claims.groupBy("patient_id").agg(
        F.count("*").alias("total_claims"),
        F.round(F.sum(F.col("claim_amount").cast("double")), 2).alias("total_claim_amount"),
    )
    lab_agg = labs.groupBy("patient_id").agg(F.count("*").alias("total_labs"))

    p360 = (
        demo.select("patient_id", "sex", "date_of_birth")
        .join(enc_agg, "patient_id", "left")
        .join(claim_agg, "patient_id", "left")
        .join(lab_agg, "patient_id", "left")
        .na.fill(0)
        .orderBy(F.col("total_encounters").desc())
    )
    p360.write.mode("overwrite").parquet(os.path.join(GOLD, "patient_360"))
    return p360
