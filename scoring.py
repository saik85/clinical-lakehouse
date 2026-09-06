"""Patient deterioration risk scoring — combines recent clinical signals into a
risk score and routes HIGH-risk patients to an alert. Pure, testable transform.

In production this is an ML model served in-stream; here it's an interpretable
rule engine over the Gold Patient-360 features (same interface).
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

HIGH = 60
MEDIUM = 35


def score_patients(p360: DataFrame) -> DataFrame:
    """Score each patient's deterioration risk from 360 features."""
    risk = (
        F.when(F.col("total_encounters") >= 10, 30).when(F.col("total_encounters") >= 5, 15).otherwise(0)
        + F.when(F.col("facilities_visited") >= 8, 20).when(F.col("facilities_visited") >= 4, 10).otherwise(0)
        + F.when(F.col("total_claim_amount") >= 80000, 25).when(F.col("total_claim_amount") >= 30000, 12).otherwise(0)
        + F.when(F.col("total_labs") >= 12, 15).otherwise(0)
    )
    return (
        p360.withColumn("risk_score", risk)
        .withColumn(
            "risk_tier",
            F.when(F.col("risk_score") >= HIGH, F.lit("HIGH"))
            .when(F.col("risk_score") >= MEDIUM, F.lit("MEDIUM"))
            .otherwise(F.lit("LOW")),
        )
        .withColumn("alert", F.when(F.col("risk_score") >= HIGH, F.lit("🚨 ALERT care team")).otherwise(F.lit("")))
    )
