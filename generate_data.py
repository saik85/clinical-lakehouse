"""Generate synthetic clinical source feeds that HCA-style ingestion consolidates:
encounters, demographics, claims, billing, labs, providers — plus a CDC change
file (updates/inserts) so the CDC-merge step has real changes to apply.

Synthetic only — NO PHI, no real patients.
"""
import csv
import json
import os
import random
from datetime import datetime, timedelta

RAW = os.path.join("data", "raw")
FACILITIES = [f"HOSP{n:03d}" for n in range(1, 21)]
ENC_TYPES = ["Inpatient", "Outpatient", "Emergency", "Observation"]
SPECIALTIES = ["Cardiology", "Oncology", "Pediatrics", "Radiology", "Primary Care"]
LAB_TESTS = ["CBC", "Lipid Panel", "HbA1c", "Metabolic Panel", "Troponin"]


def _w(name, header, rows):
    os.makedirs(RAW, exist_ok=True)
    with open(os.path.join(RAW, name), "w", newline="") as f:
        wr = csv.writer(f); wr.writerow(header); wr.writerows(rows)


def main(n_patients: int = 6000, n_encounters: int = 25000):
    start = datetime(2024, 1, 1)

    # demographics (contains PHI-like columns to be masked later)
    demo = []
    for i in range(n_patients):
        demo.append((f"P{i:06d}", f"Patient_{i}", f"{random.randint(1940,2015)}-0{random.randint(1,9)}-1{random.randint(0,8)}",
                     f"{random.randint(100,999)}-{random.randint(10,99)}-{random.randint(1000,9999)}",
                     random.choice(["M", "F"])))
    _w("demographics.csv", ["patient_id", "name", "date_of_birth", "ssn", "sex"], demo)

    # providers
    prov = [(f"PR{n:04d}", f"Dr_{n}", random.choice(SPECIALTIES)) for n in range(400)]
    _w("providers.csv", ["provider_id", "provider_name", "specialty"], prov)

    # encounters (main fact) — with ~5% dirty rows
    enc = []
    for i in range(n_encounters):
        ts = start + timedelta(minutes=random.randint(0, 60 * 24 * 150))
        pid = f"P{random.randint(0, n_patients-1):06d}"
        row = [f"E{i:07d}", pid, ts.strftime("%Y-%m-%d %H:%M:%S"),
               random.choice(FACILITIES), random.choice(ENC_TYPES),
               random.choice(prov)[0], "FINAL"]
        r = random.random()
        if r < 0.02:
            row[2] = "bad-timestamp"          # invalid time
        elif r < 0.05:
            row[1] = ""                        # missing patient link
        enc.append(row)
    # add duplicates
    enc += random.sample(enc, k=n_encounters // 200)
    _w("encounters.csv", ["encounter_id", "patient_id", "event_time", "facility",
                           "encounter_type", "provider_id", "status"], enc)

    # claims + billing + labs
    claims = [(f"C{i:07d}", f"P{random.randint(0,n_patients-1):06d}",
               round(random.uniform(50, 40000), 2), random.choice(["PAID", "DENIED", "PENDING"]))
              for i in range(18000)]
    _w("claims.csv", ["claim_id", "patient_id", "claim_amount", "claim_status"], claims)

    billing = [(f"B{i:07d}", f"P{random.randint(0,n_patients-1):06d}", round(random.uniform(20, 15000), 2))
               for i in range(20000)]
    _w("billing.csv", ["bill_id", "patient_id", "billed_amount"], billing)

    labs = [(f"L{i:07d}", f"P{random.randint(0,n_patients-1):06d}", random.choice(LAB_TESTS),
             round(random.uniform(0.1, 300), 1)) for i in range(30000)]
    _w("labs.csv", ["lab_id", "patient_id", "test_name", "result_value"], labs)

    # CDC change file: updates to some encounter statuses + a few new inserts
    changes = []
    sample = random.sample(range(n_encounters), 800)
    for i in sample:
        changes.append({"op": "U", "encounter_id": f"E{i:07d}", "status": random.choice(["AMENDED", "CANCELLED"])})
    for j in range(150):
        changes.append({"op": "I", "encounter_id": f"E{n_encounters + j:07d}",
                        "patient_id": f"P{random.randint(0,n_patients-1):06d}",
                        "event_time": (start + timedelta(days=160)).strftime("%Y-%m-%d %H:%M:%S"),
                        "facility": random.choice(FACILITIES), "encounter_type": "Outpatient",
                        "provider_id": random.choice(prov)[0], "status": "FINAL"})
    with open(os.path.join(RAW, "encounters_cdc.json"), "w") as f:
        for c in changes:
            f.write(json.dumps(c) + "\n")

    print(f"Wrote feeds -> {RAW}")
    print(f"  demographics={len(demo):,}  providers={len(prov):,}  encounters={len(enc):,}")
    print(f"  claims={len(claims):,}  billing={len(billing):,}  labs={len(labs):,}  cdc_changes={len(changes):,}")


if __name__ == "__main__":
    main()
