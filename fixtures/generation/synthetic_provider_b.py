"""Synthetic Delimited/CSV Fixture Generator for Provider B.

Generates reproducible, pipe-delimited tabular feeds representing a regional clinic EHR:
- Patients (PATIENT_ID|BIRTH_DATE|GENDER|POSTAL_CODE|_CHANGE_SEQUENCE|_OPERATION)
- Encounters (ENCOUNTER_ID|PATIENT_ID|CLASS_CODE|PERIOD_START|PERIOD_END|STATUS|_CHANGE_SEQUENCE|_OPERATION)
- Observations (OBS_ID|PATIENT_ID|ENCOUNTER_ID|LOCAL_CODE|DISPLAY_NAME|VALUE|UNIT|EFFECTIVE_TIME|STATUS|_CHANGE_SEQUENCE|_OPERATION)

Includes:
- Standard clinical observations
- Monotonic sequence updates (_change_sequence = 2, _operation = 'update')
- Clinical retractions / deletions (_operation = 'delete')
- Quarantined records (e.g. missing patient ID, non-numeric values)
- Schema drift variants (e.g. additional unrecognized columns)
"""

import csv
import io
import random
from datetime import datetime, timedelta
from typing import Any, Dict, List, Tuple

PROVIDER_B_LOINC_LABS = [
    {"code": "2345-7", "display": "Glucose", "unit": "mg/dL", "min": 70.0, "max": 130.0, "concept_id": "reviewed-catalog/concept-glucose"},
    {"code": "2160-0", "display": "Creatinine", "unit": "mg/dL", "min": 0.5, "max": 1.3, "concept_id": "reviewed-catalog/concept-creatinine"},
    {"code": "3094-0", "display": "BUN", "unit": "mg/dL", "min": 8.0, "max": 22.0, "concept_id": "reviewed-catalog/concept-bun"},
    {"code": "2951-2", "display": "Sodium", "unit": "mmol/L", "min": 136.0, "max": 144.0, "concept_id": "reviewed-catalog/concept-sodium"},
    {"code": "2823-3", "display": "Potassium", "unit": "mmol/L", "min": 3.6, "max": 5.1, "concept_id": "reviewed-catalog/concept-potassium"},
]


def generate_provider_b_tables(
    patient_count: int = 15,
    seed: int = 202,
    fixed_reference_date: str = "2026-11-01",
    include_retraction: bool = True,
    include_update: bool = True,
    include_anomalies: bool = True,
) -> Dict[str, str]:
    """Generate pipe-delimited CSV table strings for Provider B."""
    rng = random.Random(seed)
    ref_dt = datetime.strptime(fixed_reference_date, "%Y-%m-%d")

    # 1. Patients Table
    pat_buf = io.StringIO()
    pat_writer = csv.writer(pat_buf, delimiter="|")
    pat_writer.writerow(["PATIENT_ID", "BIRTH_DATE", "GENDER", "POSTAL_CODE", "_CHANGE_SEQUENCE", "_OPERATION"])

    patient_ids = [f"pb-pat-{i+1:04d}" for i in range(patient_count)]
    for pid in patient_ids:
        age_years = rng.randint(20, 75)
        b_dt = ref_dt - timedelta(days=age_years * 365 + rng.randint(0, 360))
        gender = rng.choice(["male", "female"])
        postal = f"{rng.randint(10000, 99999)}"
        pat_writer.writerow([pid, b_dt.strftime("%Y-%m-%d"), gender, postal, 1, "insert"])

    # 2. Encounters Table
    enc_buf = io.StringIO()
    enc_writer = csv.writer(enc_buf, delimiter="|")
    enc_writer.writerow(["ENCOUNTER_ID", "PATIENT_ID", "CLASS_CODE", "PERIOD_START", "PERIOD_END", "STATUS", "_CHANGE_SEQUENCE", "_OPERATION"])

    enc_ids = []
    enc_counter = 0
    for pid in patient_ids:
        num_enc = rng.randint(1, 2)
        for _ in range(num_enc):
            enc_counter += 1
            eid = f"pb-enc-{enc_counter:05d}"
            enc_ids.append((eid, pid))
            e_dt = ref_dt - timedelta(days=rng.randint(5, 120))
            start_str = e_dt.isoformat() + "Z"
            end_str = (e_dt + timedelta(hours=2)).isoformat() + "Z"
            enc_writer.writerow([eid, pid, "AMB", start_str, end_str, "finished", 1, "insert"])

    # 3. Observations Table
    obs_buf = io.StringIO()
    obs_writer = csv.writer(obs_buf, delimiter="|")
    obs_writer.writerow(["OBS_ID", "PATIENT_ID", "ENCOUNTER_ID", "LOCAL_CODE", "DISPLAY_NAME", "VALUE", "UNIT", "EFFECTIVE_TIME", "STATUS", "_CHANGE_SEQUENCE", "_OPERATION"])

    obs_counter = 0
    created_obs = []

    for eid, pid in enc_ids:
        # Sample 2 labs
        labs = rng.sample(PROVIDER_B_LOINC_LABS, 2)
        for lab in labs:
            obs_counter += 1
            oid = f"pb-obs-{obs_counter:06d}"
            val = round(rng.uniform(lab["min"], lab["max"]), 2)
            e_time = (ref_dt - timedelta(days=rng.randint(2, 60))).isoformat() + "Z"
            obs_writer.writerow([oid, pid, eid, lab["code"], lab["display"], val, lab["unit"], e_time, "final", 1, "insert"])
            created_obs.append({
                "obs_id": oid,
                "patient_id": pid,
                "encounter_id": eid,
                "code": lab["code"],
                "display": lab["display"],
                "val": val,
                "unit": lab["unit"],
                "effective_time": e_time,
            })

    # 4. Clinical Update (Higher Sequence)
    if include_update and created_obs:
        target = created_obs[0]
        obs_writer.writerow([
            target["obs_id"],
            target["patient_id"],
            target["encounter_id"],
            target["code"],
            target["display"],
            round(target["val"] + 8.5, 2),  # corrected value
            target["unit"],
            target["effective_time"],
            "final",
            2,  # sequence 2
            "update",
        ])

    # 5. Clinical Retraction (Delete)
    if include_retraction and len(created_obs) > 1:
        target_retract = created_obs[1]
        obs_writer.writerow([
            target_retract["obs_id"],
            target_retract["patient_id"],
            target_retract["encounter_id"],
            target_retract["code"],
            target_retract["display"],
            target_retract["val"],
            target_retract["unit"],
            target_retract["effective_time"],
            "retracted",
            2,  # sequence 2
            "delete",
        ])

    # 6. Deliberate Injected Anomalies for Quarantine
    if include_anomalies:
        # Non-numeric value
        obs_writer.writerow([
            "pb-obs-anomaly-malformed",
            patient_ids[0],
            enc_ids[0][0],
            "2345-7",
            "Glucose",
            "NOT_A_FLOAT",
            "mg/dL",
            ref_dt.isoformat() + "Z",
            "final",
            1,
            "insert",
        ])
        # Missing patient reference
        obs_writer.writerow([
            "pb-obs-anomaly-missing-pat",
            "",
            enc_ids[0][0],
            "2160-0",
            "Creatinine",
            "1.05",
            "mg/dL",
            ref_dt.isoformat() + "Z",
            "final",
            1,
            "insert",
        ])

    return {
        "patients.csv": pat_buf.getvalue(),
        "encounters.csv": enc_buf.getvalue(),
        "observations.csv": obs_buf.getvalue(),
    }


def generate_drifted_observations_csv(
    base_csv: str,
    add_unexpected_column: bool = True,
) -> str:
    """Generate a drifted CSV variation with unexpected schema evolution."""
    lines = base_csv.strip().split("\n")
    if not lines:
        return base_csv

    reader = csv.reader(lines, delimiter="|")
    rows = list(reader)
    header = rows[0]

    out_buf = io.StringIO()
    writer = csv.writer(out_buf, delimiter="|")

    if add_unexpected_column:
        new_header = header + ["UNEXPECTED_TECHNICIAN_MEMO"]
        writer.writerow(new_header)
        for r in rows[1:]:
            writer.writerow(r + ["Sample technician memo note"])
    else:
        writer.writerows(rows)

    return out_buf.getvalue()
