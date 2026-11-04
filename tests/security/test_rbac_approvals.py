"""Security tests for RBAC Approval Authorization."""

from datetime import datetime, timedelta
import pytest
from contracts import (
    Approval,
    AuthorizationDeniedError,
    CrossProviderAccessError,
    MappingDSL,
    MappingProposal,
)
from services.authorization.rbac_service import RBACService


class TestRBACApprovals:
    def setup_method(self):
        self.rbac = RBACService()
        self.dsl = MappingDSL(
            provider_id="provider-a",
            source_contract_version="v1.0.0",
            match={"local_test_code": "LOCAL_LAB_017"},
            target_concept_ref="2345-7",
            value_operation="parse_decimal",
            terminology_snapshot="loinc_2026_01",
        )
        self.proposal = MappingProposal(
            proposal_id="prop-rbac-001",
            provider_id="provider-a",
            source_contract_version="v1.0.0",
            typed_dsl=self.dsl,
            terminology_snapshot="loinc_2026_01",
            prompt_version="v1.0",
            model_version="m1.0",
        )

    def test_authorized_clinical_roles_succeed(self):
        valid_roles = ["terminology_reviewer", "lead_pathologist", "chief_medical_officer"]
        for role in valid_roles:
            approval = Approval(
                proposal_hash=self.proposal.proposal_hash,
                authorized_reviewer="reviewer@careflow.org",
                reviewer_role=role,
                provider_scope="provider-a",
                evidence_version="v1.0",
                expected_registry_version="v1.0.0",
                expiry=datetime.utcnow() + timedelta(hours=24),
                decision="approved",
            )
            # Should not raise
            self.rbac.authorize_approval(approval, self.proposal)

    def test_unauthorized_roles_denied(self):
        unauthorized = ["data_engineer", "intern", "guest_reviewer", "anonymous"]
        for role in unauthorized:
            approval = Approval(
                proposal_hash=self.proposal.proposal_hash,
                authorized_reviewer="unauthorized@careflow.org",
                reviewer_role=role,
                provider_scope="provider-a",
                evidence_version="v1.0",
                expected_registry_version="v1.0.0",
                expiry=datetime.utcnow() + timedelta(hours=24),
                decision="approved",
            )
            with pytest.raises(AuthorizationDeniedError):
                self.rbac.authorize_approval(approval, self.proposal)

    def test_cross_provider_approval_scope_denied(self):
        """Reviewer authorized only for Provider B cannot approve Provider A proposal."""
        approval = Approval(
            proposal_hash=self.proposal.proposal_hash,
            authorized_reviewer="reviewer_b@careflow.org",
            reviewer_role="lead_pathologist",
            provider_scope="provider-b",  # Scope mismatch!
            evidence_version="v1.0",
            expected_registry_version="v1.0.0",
            expiry=datetime.utcnow() + timedelta(hours=24),
            decision="approved",
        )
        with pytest.raises(CrossProviderAccessError):
            self.rbac.authorize_approval(approval, self.proposal)
