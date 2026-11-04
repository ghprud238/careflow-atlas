"""Unit tests for Scoped MCP Tools Server and Scope Authorization."""

import pytest
from contracts import CrossProviderAccessError, MappingDSL
from services.mcp_tools.tool_server import ScopedMCPTools


class TestScopedMCPTools:
    def setup_method(self):
        self.tools = ScopedMCPTools()
        self.provider = "provider_a"

    def test_cross_provider_access_denial(self):
        """Cross-provider access must fail closed with CrossProviderAccessError."""
        with pytest.raises(CrossProviderAccessError):
            self.tools.get_schema_diff(
                caller_scope="provider_b",  # Mismatch!
                provider_id="provider_a",
                old_version="v1.0.0",
                new_version="v1.1.0",
            )

        with pytest.raises(CrossProviderAccessError):
            self.tools.get_safe_profile(
                caller_scope="provider_b",
                provider_id="provider_a",
                target_code="LOCAL_LAB_017",
            )

        with pytest.raises(CrossProviderAccessError):
            self.tools.propose_replay(
                caller_scope="provider_b",
                provider_id="provider_a",
                target_code="LOCAL_LAB_017",
            )

    def test_schema_diff(self):
        diff = self.tools.get_schema_diff(
            caller_scope="provider_a",
            provider_id="provider_a",
            old_version="v1.0.0",
            new_version="v1.1.0",
        )
        assert diff["provider_id"] == "provider_a"
        assert "LOCAL_LAB_017" in diff["added_local_codes"]
        assert diff["format_delta"]["representation_change_detected"] is True

    def test_term_lookup_and_aliases(self):
        candidates = self.tools.lookup_term(
            caller_scope="provider_a",
            query="glucose",
            vocabulary="LOINC",
        )
        assert len(candidates) > 0
        top = candidates[0]
        assert "2345-7" in top["code"] or "glucose" in top["display"].lower()

        aliases = self.tools.get_approved_aliases(
            caller_scope="provider_a",
            concept_code="2345-7",
        )
        assert "LOCAL_LAB_017" in aliases
        assert "GLUCOSE" in aliases

    def test_safe_profile_phi_free_invariant(self):
        """Verify get_safe_profile returns statistical aggregates with zero raw PHI."""
        sample_records = [
            {"value": "12,5", "unit": "mg/dL"},
            {"value": "14,0", "unit": "mg/dL"},
            {"value": "9,8", "unit": "mg/dL"},
        ]
        profile = self.tools.get_safe_profile(
            caller_scope="provider_a",
            provider_id="provider_a",
            target_code="LOCAL_LAB_017",
            quarantined_samples=sample_records,
        )
        assert profile["record_count"] == 3
        assert profile["contains_phi"] is False
        assert profile["patient_identifiers_suppressed"] is True
        assert profile["sample_patient_identifiers"] == []
        assert profile["raw_payloads_included"] is False
        assert profile["value_distribution"]["approx_mean"] == pytest.approx(12.1, 0.1)

    def test_dry_run_mapping(self):
        sample_records = [
            {"code": "LOCAL_LAB_017", "value": "12,5", "unit": "mg/dL"},
            {"code": "LOCAL_LAB_017", "value": "15,2", "unit": "mg/dL"},
        ]

        dsl = MappingDSL(
            provider_id="provider_a",
            source_contract_version="v1.1.0",
            match={"local_test_code": "LOCAL_LAB_017"},
            target_concept_ref="2345-7",
            value_operation="parse_decimal",
            source_unit="mg/dL",
            target_unit="mg/dL",
            terminology_snapshot="loinc_2026_01",
        )

        res = self.tools.dry_run_mapping(
            caller_scope="provider_a",
            provider_id="provider_a",
            sample_records=sample_records,
            dsl=dsl,
        )
        assert res["total_tested"] == 2
        assert res["success_count"] == 2
        assert res["failure_count"] == 0
        assert res["dry_run_passed"] is True
        assert len(res["sample_converted_values"]) == 2
        assert res["sample_converted_values"][0]["normalized_value"] == 12.5

    def test_propose_replay(self):
        replay_plan = self.tools.propose_replay(
            caller_scope="provider_a",
            provider_id="provider_a",
            target_code="LOCAL_LAB_017",
        )
        assert replay_plan["provider_id"] == "provider_a"
        assert replay_plan["requires_full_lake_rescan"] is False
        assert replay_plan["execution_strategy"] == "scoped_quarantine_drain"
