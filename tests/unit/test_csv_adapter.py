"""Unit tests for Provider B Delimited CSV Adapter and Retractions."""

import pytest
from fixtures.generation.synthetic_provider_b import generate_provider_b_tables
from pipelines.adapters.csv.csv_adapter import CSVAdapter


class TestCSVAdapter:
    def setup_method(self):
        self.adapter = CSVAdapter(provider="provider-b", delimiter="|")

    def test_parse_valid_patients(self):
        csv_text = "PATIENT_ID|BIRTH_DATE|GENDER|POSTAL_CODE|_CHANGE_SEQUENCE|_OPERATION\npb-pat-0001|1985-04-12|female|02138|1|insert"
        envelopes = self.adapter.parse_csv_content(csv_text, "Patient", "man-001")
        assert len(envelopes) == 1
        env = envelopes[0]
        assert env.disposition == "accepted"
        assert env.source_resource_id == "pb-pat-0001"
        assert env.operation == "insert"

        canonical = self.adapter.to_canonical_patient(env, "run-001", "man-001")
        assert canonical is not None
        assert canonical.patient_key == "pb-pat-001" or canonical.patient_key == "pb-pat-0001"
        assert canonical.birth_date == "1985-04-12"
        assert canonical.gender == "female"
        assert canonical.supersession_state == "current"

    def test_parse_valid_observation_and_normalization(self):
        csv_text = "OBS_ID|PATIENT_ID|ENCOUNTER_ID|LOCAL_CODE|DISPLAY_NAME|VALUE|UNIT|EFFECTIVE_TIME|STATUS|_CHANGE_SEQUENCE|_OPERATION\npb-obs-0001|pb-pat-0001|pb-enc-0001|2345-7|Glucose|110.5|mg/dL|2026-10-15T08:00:00Z|final|1|insert"
        envelopes = self.adapter.parse_csv_content(csv_text, "Observation", "man-001")
        assert len(envelopes) == 1
        env = envelopes[0]
        assert env.disposition == "accepted"

        canonical = self.adapter.to_canonical_observation(env, "run-001", "man-001")
        assert canonical is not None
        assert canonical.source_key == "pb-obs-0001"
        assert canonical.normalized.normalized_concept == "reviewed-catalog/concept-glucose"
        assert canonical.normalized.normalized_value == 110.5
        assert canonical.supersession_state == "current"

    def test_clinical_retraction_sets_retracted_state(self):
        """Section 13 Deletion & Retraction: operation delete must mark supersession_state as deleted."""
        csv_text = "OBS_ID|PATIENT_ID|ENCOUNTER_ID|LOCAL_CODE|DISPLAY_NAME|VALUE|UNIT|EFFECTIVE_TIME|STATUS|_CHANGE_SEQUENCE|_OPERATION\npb-obs-retracted|pb-pat-0001|pb-enc-0001|2345-7|Glucose|95.0|mg/dL|2026-10-15T08:00:00Z|retracted|2|delete"
        envelopes = self.adapter.parse_csv_content(csv_text, "Observation", "man-001")
        assert len(envelopes) == 1
        env = envelopes[0]
        assert env.disposition == "accepted"
        assert env.operation == "delete"
        assert env.change_sequence == 2

        canonical = self.adapter.to_canonical_observation(env, "run-001", "man-001")
        assert canonical is not None
        assert canonical.supersession_state == "deleted"

    def test_quarantine_malformed_numeric_value(self):
        csv_text = "OBS_ID|PATIENT_ID|ENCOUNTER_ID|LOCAL_CODE|DISPLAY_NAME|VALUE|UNIT|EFFECTIVE_TIME|STATUS|_CHANGE_SEQUENCE|_OPERATION\npb-obs-bad|pb-pat-0001|pb-enc-0001|2345-7|Glucose|CORRUPT_NOT_NUMERIC|mg/dL|2026-10-15T08:00:00Z|final|1|insert"
        envelopes = self.adapter.parse_csv_content(csv_text, "Observation", "man-001")
        assert len(envelopes) == 1
        env = envelopes[0]
        assert env.disposition == "quarantined"
        assert any("Malformed numeric" in r for r in env.quarantine_reasons)
        assert self.adapter.to_canonical_observation(env, "r1", "m1") is None

    def test_quarantine_missing_patient_id(self):
        csv_text = "OBS_ID|PATIENT_ID|ENCOUNTER_ID|LOCAL_CODE|DISPLAY_NAME|VALUE|UNIT|EFFECTIVE_TIME|STATUS|_CHANGE_SEQUENCE|_OPERATION\npb-obs-no-pat||pb-enc-0001|2345-7|Glucose|95.0|mg/dL|2026-10-15T08:00:00Z|final|1|insert"
        envelopes = self.adapter.parse_csv_content(csv_text, "Observation", "man-001")
        assert len(envelopes) == 1
        env = envelopes[0]
        assert env.disposition == "quarantined"
        assert any("Missing required PATIENT_ID" in r for r in env.quarantine_reasons)

    def test_synthetic_bundle_generator_and_parsing(self):
        tables = generate_provider_b_tables(patient_count=10, seed=42)
        pat_envs = self.adapter.parse_csv_content(tables["patients.csv"], "Patient", "m1")
        enc_envs = self.adapter.parse_csv_content(tables["encounters.csv"], "Encounter", "m1")
        obs_envs = self.adapter.parse_csv_content(tables["observations.csv"], "Observation", "m1")

        assert len(pat_envs) == 10
        assert len(enc_envs) >= 10
        assert len(obs_envs) >= 20

        # Verify the 2 injected anomalies are quarantined
        quarantined_obs = [e for e in obs_envs if e.disposition == "quarantined"]
        assert len(quarantined_obs) == 2
