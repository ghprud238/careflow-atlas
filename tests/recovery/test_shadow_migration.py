"""Shadow Migration and Backward Compatibility Replay Tests."""

import json
from contracts import MappingDSL
from fixtures.generation.synthetic_provider_a import generate_provider_a_bundle
from packages.mapping_dsl.interpreter import DSLInterpreter
from pipelines.adapters.fhir.fhir_adapter import FHIRAdapter


class TestShadowMigration:
    def test_shadow_replay_evaluates_candidate_mapping_rules(self):
        """Historical data can be re-evaluated under new mapping rules in shadow mode."""
        # 1. Historical raw records with local test code LOCAL_LAB_017
        historical_records = [
            {
                "resourceType": "Observation",
                "id": f"obs-shadow-{i}",
                "status": "final",
                "code": {"coding": [{"system": "http://local.org", "code": "LOCAL_LAB_017"}]},
                "subject": {"reference": "Patient/pat-001"},
                "effectiveDateTime": "2026-10-01T08:00:00Z",
                "valueQuantity": {"value": f"1{i},5", "unit": "mg/dL"},
                "_change_sequence": 1,
            }
            for i in range(5)
        ]

        # 2. Legacy evaluation: under v1.0.0 without rule -> all quarantined for unfamiliar code or comma decimal
        adapter = FHIRAdapter()
        legacy_envelopes = [adapter.parse_resource_dict(r, "man-legacy") for r in historical_records]
        legacy_quarantined = [e for e in legacy_envelopes if e.disposition == "quarantined"]
        assert len(legacy_quarantined) == 5

        # 3. Modern rule: v1.1.0 parse_decimal mapping rule
        candidate_dsl = MappingDSL(
            provider_id="provider-a",
            source_contract_version="v1.1.0",
            match={"local_test_code": "LOCAL_LAB_017"},
            target_concept_ref="2345-7",
            value_operation="parse_decimal",
            source_unit="mg/dL",
            target_unit="mg/dL",
            terminology_snapshot="loinc_2026_01",
        )

        interpreter = DSLInterpreter()
        shadow_resolved = []

        for r in historical_records:
            val_str = r["valueQuantity"]["value"]
            unit = r["valueQuantity"]["unit"]
            code = r["code"]["coding"][0]["code"]

            matched, norm_concept, norm_val, norm_unit, err = interpreter.apply_observation_rule(
                raw_code=code,
                raw_val_str=val_str,
                raw_unit=unit,
                dsl=candidate_dsl,
            )
            if matched and err is None:
                shadow_resolved.append({
                    "id": r["id"],
                    "norm_concept": norm_concept,
                    "norm_val": norm_val,
                })

        # 4. Shadow concordance evaluation
        assert len(shadow_resolved) == 5
        assert all(res["norm_concept"] == "2345-7" for res in shadow_resolved)
        assert shadow_resolved[0]["norm_val"] == 10.5
        assert shadow_resolved[1]["norm_val"] == 11.5
