"""Unit tests for FHIR R4 Adapter."""

import pytest
from pipelines.adapters.fhir.fhir_adapter import FHIRAdapter
from fixtures.generation.synthetic_provider_a import generate_provider_a_bundle


class TestFHIRAdapter:
    def setup_method(self):
        self.adapter = FHIRAdapter(provider="provider-a")

    def test_parse_valid_patient(self):
        raw = {
            "resourceType": "Patient",
            "id": "pat-0001",
            "birthDate": "1980-05-15",
            "gender": "female",
            "address": [{"postalCode": "02138"}],
            "_change_sequence": 1,
            "_operation": "insert",
        }
        envelope = self.adapter.parse_resource_dict(raw, manifest_id="man-001")
        assert envelope.disposition == "accepted"
        assert envelope.logical_key == ("provider-a", "fhir-r4-emr", "Patient", "pat-0001")

        canonical = self.adapter.to_canonical_patient(envelope, run_id="run-001", manifest_id="man-001")
        assert canonical is not None
        assert canonical.patient_key == "pat-0001"
        assert canonical.birth_date == "1980-05-15"
        assert canonical.gender == "female"
        assert canonical.postal_code == "02138"

    def test_parse_valid_observation_and_normalization(self):
        raw = {
            "resourceType": "Observation",
            "id": "obs-0001",
            "status": "final",
            "code": {
                "coding": [{"system": "http://loinc.org", "code": "2345-7", "display": "Glucose"}],
                "text": "Glucose",
            },
            "subject": {"reference": "Patient/pat-0001"},
            "encounter": {"reference": "Encounter/enc-0001"},
            "effectiveDateTime": "2026-10-15T10:30:00Z",
            "valueQuantity": {"value": 95.5, "unit": "mg/dL"},
            "_change_sequence": 1,
            "_operation": "insert",
        }
        envelope = self.adapter.parse_resource_dict(raw, manifest_id="man-001")
        assert envelope.disposition == "accepted"

        canonical = self.adapter.to_canonical_observation(envelope, run_id="run-001", manifest_id="man-001")
        assert canonical is not None
        assert canonical.patient_key == "pat-0001"
        assert canonical.encounter_key == "enc-0001"
        assert canonical.source_key == "obs-0001"
        assert canonical.normalized.normalized_concept == "reviewed-catalog/concept-glucose"
        assert canonical.normalized.normalized_value == 95.5
        assert canonical.normalized.normalized_unit == "mg/dL"
        assert canonical.original.original_code == "2345-7"

    def test_quarantine_malformed_numeric_observation(self):
        raw = {
            "resourceType": "Observation",
            "id": "obs-bad-num",
            "status": "final",
            "code": {"coding": [{"system": "http://loinc.org", "code": "2345-7"}]},
            "subject": {"reference": "Patient/pat-0001"},
            "effectiveDateTime": "2026-10-15T10:30:00Z",
            "valueQuantity": {"value": "NOT_A_NUMBER", "unit": "mg/dL"},
        }
        envelope = self.adapter.parse_resource_dict(raw, manifest_id="man-001")
        assert envelope.disposition == "quarantined"
        assert any("Malformed numeric" in r for r in envelope.quarantine_reasons)
        assert self.adapter.to_canonical_observation(envelope, run_id="r1", manifest_id="m1") is None

    def test_quarantine_missing_patient_reference(self):
        raw = {
            "resourceType": "Observation",
            "id": "obs-no-pat",
            "status": "final",
            "code": {"coding": [{"system": "http://loinc.org", "code": "2345-7"}]},
            "effectiveDateTime": "2026-10-15T10:30:00Z",
            "valueQuantity": {"value": 100.0, "unit": "mg/dL"},
        }
        envelope = self.adapter.parse_resource_dict(raw, manifest_id="man-001")
        assert envelope.disposition == "quarantined"
        assert any("Missing or invalid subject" in r for r in envelope.quarantine_reasons)

    def test_bundle_adapter_parsing_and_conservation(self):
        resources, meta = generate_provider_a_bundle(
            patient_count=10,
            seed=42,
            include_anomalies=True,
            include_duplicates=True,
            include_updates=True,
        )
        envelopes = [self.adapter.parse_resource_dict(r, manifest_id="test-batch") for r in resources]

        assert len(envelopes) == len(resources)
        accepted = [e for e in envelopes if e.disposition == "accepted"]
        quarantined = [e for e in envelopes if e.disposition == "quarantined"]

        # 2 deliberate anomalies should be quarantined
        assert len(quarantined) == 2
        assert len(accepted) == len(envelopes) - 2
