"""Synthetic FHIR R4 Fixture Generator for Provider A.

Generates deterministic, attributable Synthea-style FHIR R4 resources:
- Patient
- Encounter
- Observation (with standard LOINC lab concepts)

Includes deliberate variations:
- Normal valid records
- Updates with higher change sequence
- Duplicates with identical sequence
- Quarantined records (e.g. malformed numeric values, missing patient reference)
"""

import hashlib
import json
import random
from datetime import datetime, timedelta
from typing import Any, Dict, List, Tuple


REVIEWED_LOINC_LABS = [
    {"code": "2345-7", "display": "Glucose [Mass/volume] in Serum or Plasma", "unit": "mg/dL", "min": 65.0, "max": 140.0, "concept_id": "reviewed-catalog/concept-glucose"},
    {"code": "2160-0", "display": "Creatinine [Mass/volume] in Serum or Plasma", "unit": "mg/dL", "min": 0.6, "max": 1.4, "concept_id": "reviewed-catalog/concept-creatinine"},
    {"code": "3094-0", "display": "Urea nitrogen [Mass/volume] in Serum or Plasma", "unit": "mg/dL", "min": 7.0, "max": 25.0, "concept_id": "reviewed-catalog/concept-bun"},
    {"code": "2951-2", "display": "Sodium [Moles/volume] in Serum or Plasma", "unit": "mmol/L", "min": 135.0, "max": 145.0, "concept_id": "reviewed-catalog/concept-sodium"},
    {"code": "2823-3", "display": "Potassium [Moles/volume] in Serum or Plasma", "unit": "mmol/L", "min": 3.5, "max": 5.2, "concept_id": "reviewed-catalog/concept-potassium"},
    {"code": "2075-0", "display": "Chloride [Moles/volume] in Serum or Plasma", "unit": "mmol/L", "min": 96.0, "max": 106.0, "concept_id": "reviewed-catalog/concept-chloride"},
    {"code": "17861-6", "display": "Calcium [Mass/volume] in Serum or Plasma", "unit": "mg/dL", "min": 8.5, "max": 10.5, "concept_id": "reviewed-catalog/concept-calcium"},
    {"code": "1975-2", "display": "Bilirubin.total [Mass/volume] in Serum or Plasma", "unit": "mg/dL", "min": 0.2, "max": 1.2, "concept_id": "reviewed-catalog/concept-bilirubin"},
    {"code": "1751-7", "display": "Albumin [Mass/volume] in Serum or Plasma", "unit": "g/dL", "min": 3.5, "max": 5.0, "concept_id": "reviewed-catalog/concept-albumin"},
    {"code": "2885-2", "display": "Protein [Mass/volume] in Serum or Plasma", "unit": "g/dL", "min": 6.0, "max": 8.3, "concept_id": "reviewed-catalog/concept-protein"},
    {"code": "1920-8", "display": "Aspartate aminotransferase [Enzymatic activity/volume]", "unit": "U/L", "min": 10.0, "max": 40.0, "concept_id": "reviewed-catalog/concept-ast"},
    {"code": "1742-6", "display": "Alanine aminotransferase [Enzymatic activity/volume]", "unit": "U/L", "min": 7.0, "max": 56.0, "concept_id": "reviewed-catalog/concept-alt"},
    {"code": "4544-3", "display": "Hematocrit [Volume Fraction] of Blood", "unit": "%", "min": 36.0, "max": 50.0, "concept_id": "reviewed-catalog/concept-hematocrit"},
    {"code": "718-7", "display": "Hemoglobin [Mass/volume] in Blood", "unit": "g/dL", "min": 12.0, "max": 17.5, "concept_id": "reviewed-catalog/concept-hemoglobin"},
    {"code": "6690-2", "display": "Leukocytes [#/volume] in Blood", "unit": "10*3/uL", "min": 4.5, "max": 11.0, "concept_id": "reviewed-catalog/concept-wbc"},
    {"code": "777-3", "display": "Platelets [#/volume] in Blood", "unit": "10*3/uL", "min": 150.0, "max": 450.0, "concept_id": "reviewed-catalog/concept-platelets"},
    {"code": "4548-4", "display": "Hemoglobin A1c/Hemoglobin.total in Blood", "unit": "%", "min": 4.5, "max": 11.5, "concept_id": "reviewed-catalog/concept-hba1c"},
    {"code": "2093-3", "display": "Cholesterol [Mass/volume] in Serum or Plasma", "unit": "mg/dL", "min": 125.0, "max": 250.0, "concept_id": "reviewed-catalog/concept-cholesterol"},
    {"code": "2085-9", "display": "Cholesterol in HDL [Mass/volume] in Serum or Plasma", "unit": "mg/dL", "min": 35.0, "max": 75.0, "concept_id": "reviewed-catalog/concept-hdl"},
    {"code": "13457-7", "display": "Cholesterol in LDL [Mass/volume] in Serum or Plasma", "unit": "mg/dL", "min": 70.0, "max": 180.0, "concept_id": "reviewed-catalog/concept-ldl"},
    {"code": "2571-8", "display": "Triglyceride [Mass/volume] in Serum or Plasma", "unit": "mg/dL", "min": 50.0, "max": 250.0, "concept_id": "reviewed-catalog/concept-triglycerides"},
]


def generate_provider_a_bundle(
    patient_count: int = 100,
    seed: int = 42,
    fixed_reference_date: str = "2026-11-01",
    include_anomalies: bool = True,
    include_duplicates: bool = True,
    include_updates: bool = True,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """Generate a reproducible bundle of FHIR R4 resources with metadata provenance."""
    rng = random.Random(seed)
    ref_dt = datetime.strptime(fixed_reference_date, "%Y-%m-%d")

    resources: List[Dict[str, Any]] = []
    metadata = {
        "provider": "provider-a",
        "feed": "fhir-r4",
        "seed": seed,
        "fixed_reference_date": fixed_reference_date,
        "generator_version": "1.0.0",
        "patient_count": patient_count,
    }

    genders = ["male", "female"]
    postal_codes = ["02138", "02139", "02140", "02141", "02142"]

    patient_ids = []

    # 1. Patients
    for i in range(1, patient_count + 1):
        pid = f"pat-{i:04d}"
        patient_ids.append(pid)
        # Age between 18 and 80 as of ref_dt
        age_years = rng.randint(18, 80)
        dob = ref_dt - timedelta(days=age_years * 365 + rng.randint(0, 364))
        gender = rng.choice(genders)
        postal = rng.choice(postal_codes)

        pat_resource = {
            "resourceType": "Patient",
            "id": pid,
            "meta": {"versionId": "1", "lastUpdated": dob.isoformat() + "Z"},
            "birthDate": dob.strftime("%Y-%m-%d"),
            "gender": gender,
            "address": [{"postalCode": postal, "country": "US"}],
            "_change_sequence": 1,
            "_operation": "insert",
        }
        resources.append(pat_resource)

    # 2. Encounters and Observations
    enc_counter = 0
    obs_counter = 0

    for pid in patient_ids:
        # 1 to 3 encounters per patient
        num_encounters = rng.randint(1, 3)
        for _ in range(num_encounters):
            enc_counter += 1
            enc_id = f"enc-{enc_counter:05d}"
            enc_date = ref_dt - timedelta(days=rng.randint(1, 365))

            enc_resource = {
                "resourceType": "Encounter",
                "id": enc_id,
                "meta": {"versionId": "1", "lastUpdated": enc_date.isoformat() + "Z"},
                "status": "finished",
                "class": {"code": "AMB", "display": "ambulatory"},
                "subject": {"reference": f"Patient/{pid}"},
                "period": {
                    "start": enc_date.isoformat() + "Z",
                    "end": (enc_date + timedelta(hours=1)).isoformat() + "Z",
                },
                "_change_sequence": 1,
                "_operation": "insert",
            }
            resources.append(enc_resource)

            # 2 to 4 observations per encounter
            num_obs = rng.randint(2, 4)
            selected_labs = rng.sample(REVIEWED_LOINC_LABS, min(num_obs, len(REVIEWED_LOINC_LABS)))

            for lab in selected_labs:
                obs_counter += 1
                obs_id = f"obs-{obs_counter:06d}"
                val = round(rng.uniform(lab["min"], lab["max"]), 2)

                obs_resource = {
                    "resourceType": "Observation",
                    "id": obs_id,
                    "meta": {"versionId": "1", "lastUpdated": enc_date.isoformat() + "Z"},
                    "status": "final",
                    "code": {
                        "coding": [{
                            "system": "http://loinc.org",
                            "code": lab["code"],
                            "display": lab["display"],
                        }],
                        "text": lab["display"],
                    },
                    "subject": {"reference": f"Patient/{pid}"},
                    "encounter": {"reference": f"Encounter/{enc_id}"},
                    "effectiveDateTime": enc_date.isoformat() + "Z",
                    "valueQuantity": {
                        "value": val,
                        "unit": lab["unit"],
                        "system": "http://unitsofmeasure.org",
                        "code": lab["unit"],
                    },
                    "_change_sequence": 1,
                    "_operation": "insert",
                }
                resources.append(obs_resource)

    # 3. Deliberate Injected Anomalies for Quarantine Testing
    if include_anomalies:
        # Anomaly 1: Observation with malformed string value (non-numeric in valueQuantity)
        resources.append({
            "resourceType": "Observation",
            "id": "obs-anomaly-malformed-num",
            "meta": {"versionId": "1", "lastUpdated": ref_dt.isoformat() + "Z"},
            "status": "final",
            "code": {
                "coding": [{"system": "http://loinc.org", "code": "2345-7", "display": "Glucose"}],
                "text": "Glucose",
            },
            "subject": {"reference": f"Patient/{patient_ids[0]}"},
            "effectiveDateTime": ref_dt.isoformat() + "Z",
            "valueQuantity": {
                "value": "MALFORMED_NON_NUMERIC_TEXT",
                "unit": "mg/dL",
            },
            "_change_sequence": 1,
            "_operation": "insert",
        })

        # Anomaly 2: Observation missing patient reference
        resources.append({
            "resourceType": "Observation",
            "id": "obs-anomaly-missing-patient",
            "meta": {"versionId": "1", "lastUpdated": ref_dt.isoformat() + "Z"},
            "status": "final",
            "code": {
                "coding": [{"system": "http://loinc.org", "code": "2160-0", "display": "Creatinine"}],
                "text": "Creatinine",
            },
            "effectiveDateTime": ref_dt.isoformat() + "Z",
            "valueQuantity": {"value": 1.1, "unit": "mg/dL"},
            "_change_sequence": 1,
            "_operation": "insert",
        })

    # 4. Deliberate Duplicate (Exact same resource and sequence)
    if include_duplicates and obs_counter > 0:
        first_obs = next(r for r in resources if r["resourceType"] == "Observation" and r["id"] == "obs-000001")
        dup_obs = dict(first_obs)
        resources.append(dup_obs)

    # 5. Deliberate Clinical Update (Higher change sequence = 2)
    if include_updates and obs_counter > 1:
        second_obs = next(r for r in resources if r["resourceType"] == "Observation" and r["id"] == "obs-000002")
        update_obs = dict(second_obs)
        update_obs["_change_sequence"] = 2
        update_obs["_operation"] = "update"
        update_obs["valueQuantity"] = dict(second_obs["valueQuantity"])
        update_obs["valueQuantity"]["value"] = round(second_obs["valueQuantity"]["value"] + 5.0, 2)
        resources.append(update_obs)

    # Summary Counts
    type_counts = {}
    for r in resources:
        t = r["resourceType"]
        type_counts[t] = type_counts.get(t, 0) + 1

    metadata["total_resources"] = len(resources)
    metadata["resource_type_counts"] = type_counts
    return resources, metadata


def write_ndjson_batch(resources: List[Dict[str, Any]], output_path: str) -> Tuple[str, int]:
    """Write resources to an NDJSON file, returning (sha256_hash, byte_count)."""
    hasher = hashlib.sha256()
    bytes_count = 0
    with open(output_path, "w", encoding="utf-8") as f:
        for r in resources:
            line = json.dumps(r) + "\n"
            line_bytes = line.encode("utf-8")
            hasher.update(line_bytes)
            bytes_count += len(line_bytes)
            f.write(line)
    return hasher.hexdigest(), bytes_count


if __name__ == "__main__":
    res, meta = generate_provider_a_bundle(patient_count=10)
    print(f"Generated {meta['total_resources']} resources for Provider A: {meta['resource_type_counts']}")
