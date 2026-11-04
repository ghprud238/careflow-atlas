"""Unit tests for Governed Investigation Agent State Machine."""

import pytest
from contracts import MappingProposal
from services.agent.investigation_agent import InvestigationAgent
from services.mcp_tools.tool_server import ScopedMCPTools


class TestInvestigationAgent:
    def setup_method(self):
        self.tools = ScopedMCPTools()
        self.agent = InvestigationAgent(tools=self.tools)

    def test_investigation_happy_path_produces_valid_proposal(self):
        quarantined_samples = [
            {"code": "LOCAL_LAB_017", "value": "12,5", "unit": "mg/dL"},
            {"code": "LOCAL_LAB_017", "value": "14,8", "unit": "mg/dL"},
        ]

        proposal = self.agent.investigate(
            provider_id="provider_a",
            target_code="LOCAL_LAB_017",
            source_contract_version="v1.1.0",
            quarantined_samples=quarantined_samples,
        )

        assert isinstance(proposal, MappingProposal)
        assert proposal.disposition == "requires_review"
        assert proposal.provider_id == "provider_a"
        assert proposal.typed_dsl.match["local_test_code"] == "LOCAL_LAB_017"
        assert proposal.typed_dsl.value_operation == "parse_decimal"
        assert proposal.typed_dsl.target_concept_ref == "2345-7"
        assert proposal.proposal_hash != ""
        assert len(proposal.proposal_hash) == 64  # SHA-256
        assert proposal.safe_test_results.get("dry_run_passed") is True
        assert self.agent.tool_call_count <= 8

    def test_budget_ceiling_enforcement(self):
        """Hard reliability budget of max tool calls must be enforced."""
        proposal = self.agent.investigate(
            provider_id="provider_a",
            target_code="LOCAL_LAB_017",
            source_contract_version="v1.1.0",
            override_max_budget=2,  # Force budget failure early
        )
        assert proposal.disposition == "budget_exceeded"
        assert proposal.typed_dsl.target_concept_ref == "budget_exceeded"

    def test_abstention_on_unresolvable_code(self):
        """If evidence is insufficient to resolve term, agent MUST abstain."""
        proposal = self.agent.investigate(
            provider_id="provider_a",
            target_code="UNRESOLVABLE_LAB_999",
            source_contract_version="v1.1.0",
        )
        assert proposal.disposition == "abstain"
        assert proposal.typed_dsl.target_concept_ref == "unresolved"
