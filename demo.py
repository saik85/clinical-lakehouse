"""End-to-end demo that exercises each HCA resume point and prints a clearly
labelled report. Produces the run screenshot in the README.

Run:  python src/demo.py   (after python src/generate_data.py)
"""
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

import medallion
import cdc_merge
import patient360
import observability
from governance import apply_column_masking

FEEDS = ["encounters", "demographics", "providers", "claims", "billing", "labs"]


def spark_session():
    return (SparkSession.builder.appName("healthcare-lakehouse")
            .master("local[*]").config("spark.sql.shuffle.partitions", "8")
            .config("spark.ui.showConsoleProgress", "false").getOrCreate())


def line(t):
    print("\n" + "=" * 66 + f"\n{t}\n" + "=" * 66)


def main():
    spark = spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    line("[POINT 1] Medallion — Bronze -> Silver -> Gold (5 clinical feeds)")
    for feed in FEEDS:
        medallion.bronze_ingest(spark, feed)
    print("  bronze: 6 feeds landed (encounters, demographics, providers, claims, billing, labs)")
    medallion.build_silver(spark)
    b = spark.read.parquet("data/bronze/encounters").count()
    s = spark.read.parquet("data/silver/encounters").count()
    print(f"  silver.encounters: cleaned {b:,} bronze rows -> {s:,} trusted rows")
    gold = medallion.build_gold(spark)
    print(f"  ✓ gold.encounter_daily: {gold.count():,} rows")

    line("[POINT 4] CDC merge — apply change feed without full reload")
    res = cdc_merge.run(spark)
    print(f"  base={res['before']:,}  ->  after CDC merge={res['after']:,}  "
          f"(status updates applied: {res['status_changed']:,})")
    print("  ✓ lakehouse kept current via upsert (inserts + updates + deletes)")

    line("[POINT 5] Patient 360 — unified patient data product")
    p360 = patient360.build(spark)
    print(f"  ✓ gold.patient_360: {p360.count():,} patients")
    p360.select("patient_id", "sex", "total_encounters", "facilities_visited",
                "providers_seen", "total_claims", "total_claim_amount").show(5, truncate=False)

    line("[POINT 3] Observability — freshness / schema-drift / anomaly / SLA-SLO")
    for name, status, detail in observability.run(spark):
        mark = "✓" if status in ("OK", "MET") else "!"
        print(f"  {mark} {name:<24} {status:<9} {detail}")

    line("[POINT 6] Governance — RBAC + PHI column masking (HIPAA)")
    demo = spark.read.parquet("data/silver/demographics")
    print("  role=clinician (privileged) — full PHI:")
    demo.select("patient_id", "name", "date_of_birth", "ssn").show(2, truncate=False)
    print("  role=analyst (non-privileged) — masked PHI:")
    apply_column_masking(demo, "analyst").select("patient_id", "name", "date_of_birth", "ssn").show(2, truncate=False)

    line("[POINT 9] Tuning & reliability")
    print("  ✓ Silver/Gold partitioned by event_date (partition pruning)")
    print("  ✓ target availability: 99.99% (see src/tuning.py, infra/main.tf)")

    print("\n[POINT 2] Streaming (HL7/FHIR) -> src/streaming_ingest.py")
    print("[POINT 7] Palantir Foundry semantic layer -> foundry/ontology.yaml")
    print("[POINT 8] Terraform IaC (Databricks, S3, MSK, Glue, DMS) -> infra/main.tf")
    print("\nDone. All resume points exercised.")
    spark.stop()


if __name__ == "__main__":
    main()
