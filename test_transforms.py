import os, sys
import pytest
from pyspark.sql import SparkSession
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
from medallion import clean_encounters      # noqa: E402
from cdc_merge import apply_cdc             # noqa: E402
from governance import apply_column_masking  # noqa: E402


@pytest.fixture(scope="session")
def spark():
    s = SparkSession.builder.appName("t").master("local[1]").getOrCreate()
    yield s; s.stop()


def test_clean_encounters(spark):
    cols = ["encounter_id","patient_id","event_time","facility","encounter_type","provider_id","status"]
    rows = [
        ("E1","P1","2024-01-01 10:00:00"," hosp001 "," inpatient ","PR1","final"),
        ("E1","P1","2024-01-01 10:00:00"," hosp001 "," inpatient ","PR1","final"),  # dup
        ("E2","","2024-01-01 11:00:00","HOSP002","Outpatient","PR2","FINAL"),        # no patient
        ("E3","P3","bad-time","HOSP003","Emergency","PR3","FINAL"),                   # bad time
    ]
    out = {r["encounter_id"]: r for r in clean_encounters(spark.createDataFrame(rows, cols)).collect()}
    assert set(out) == {"E1"}                    # only valid+deduped survives
    assert out["E1"]["facility"] == "HOSP001" and out["E1"]["status"] == "FINAL"


def test_cdc_update(spark):
    base = spark.createDataFrame(
        [("E1","P1","2024-01-01 10:00:00","HOSP001","Inpatient","PR1","FINAL","2024-01-01")],
        ["encounter_id","patient_id","event_time","facility","encounter_type","provider_id","status","event_date"])
    changes = spark.createDataFrame([("U","E1","AMENDED")], ["op","encounter_id","status"])
    out = apply_cdc(base, changes).collect()[0]
    assert out["status"] == "AMENDED"


def test_phi_masking(spark):
    df = spark.createDataFrame([("P1","Patient_1","1980-05-10","123-45-6789")],
                               ["patient_id","name","date_of_birth","ssn"])
    analyst = apply_column_masking(df, "analyst").collect()[0]
    assert analyst["name"] == "*** MASKED ***"
    assert analyst["ssn"].endswith("6789") and analyst["ssn"].startswith("***")
    clinician = apply_column_masking(df, "clinician").collect()[0]
    assert clinician["name"] == "Patient_1"      # privileged sees full PHI


def test_risk_scoring(spark):
    import scoring
    df = spark.createDataFrame(
        [("P1","M","1980",12,9,5,120000.0),("P2","F","1990",1,1,0,100.0)],
        ["patient_id","sex","date_of_birth","total_encounters","facilities_visited","total_labs","total_claim_amount"])
    out = {r["patient_id"]: r for r in scoring.score_patients(df).collect()}
    assert out["P1"]["risk_tier"] == "HIGH" and "ALERT" in out["P1"]["alert"]
    assert out["P2"]["risk_tier"] == "LOW"
