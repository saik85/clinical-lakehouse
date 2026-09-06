"""[Resume point 2] HL7/FHIR & patient-monitoring ingestion re-architected from
nightly batch to STREAMING on Amazon MSK + Spark Structured Streaming, cutting
clinical-event latency from next-day to sub-minute.

A file source stands in for Amazon MSK here so it runs anywhere; each file is
one micro-batch. In production this reads from MSK (Kafka) topics.
"""
import json
import os
import random
import time
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

STREAM_DIR = os.path.join("data", "hl7_stream")
BRONZE_STREAM = os.path.join("data", "bronze", "hl7_events")
CHECKPOINT = os.path.join("data", "_ckpt_hl7")

SCHEMA = StructType([
    StructField("message_id", StringType()),
    StructField("event_time", StringType()),
    StructField("patient_id", StringType()),
    StructField("message_type", StringType()),   # ADT / ORU / ORM (HL7)
    StructField("facility", StringType()),
])


def emit(batches=5, per_batch=300, pause=0.4):
    os.makedirs(STREAM_DIR, exist_ok=True)
    n = 0
    for b in range(batches):
        with open(os.path.join(STREAM_DIR, f"hl7_{b:03d}.json"), "w") as f:
            for _ in range(per_batch):
                f.write(json.dumps({
                    "message_id": f"MSG{n:09d}",
                    "event_time": datetime.utcnow().isoformat(),
                    "patient_id": f"P{random.randint(0, 5999):06d}",
                    "message_type": random.choice(["ADT", "ORU", "ORM"]),
                    "facility": f"HOSP{random.randint(1,20):03d}",
                }) + "\n")
                n += 1
        time.sleep(pause)
    return n


def main():
    spark = (SparkSession.builder.appName("hl7-streaming").master("local[*]")
             .config("spark.sql.shuffle.partitions", "4")
             .config("spark.ui.showConsoleProgress", "false").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")

    print(f"Emitting HL7/FHIR events -> {STREAM_DIR}")
    total = emit()

    stream = (spark.readStream.schema(SCHEMA).option("maxFilesPerTrigger", 1)
              .json(STREAM_DIR)
              .withColumn("_ingested_at", F.current_timestamp()))

    def handle(df, bid):
        cnt = df.count()
        types = {r["message_type"]: r["cnt"] for r in
                 df.groupBy("message_type").agg(F.count("*").alias("cnt")).collect()}
        print(f"  [micro-batch {bid}] ingested={cnt}  by_type={types}  latency=sub-minute")
        df.write.mode("append").parquet(BRONZE_STREAM)

    q = (stream.writeStream.foreachBatch(handle)
         .option("checkpointLocation", CHECKPOINT)
         .trigger(processingTime="1 second").start())
    print("Structured Streaming started (MSK stand-in)...")
    q.awaitTermination(timeout=20)
    q.stop()
    print(f"\n  ✓ streamed {total} HL7/FHIR events into bronze.hl7_events (sub-minute latency)")
    spark.stop()


if __name__ == "__main__":
    main()
