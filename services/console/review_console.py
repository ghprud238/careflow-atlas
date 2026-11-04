"""Human-in-the-Loop Review Console Service and CLI.

Provides terminology reviewers and clinical leads with an ergonomic interface to:
1. List pending mapping proposals.
2. Render comprehensive proposal dossiers (evidence refs, statistical profiles, dry run metrics).
3. Validate reviewer credentials against RBAC policies.
4. Sign immutable Approval artifacts bound to exact proposal hashes.
"""

from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
from contracts import Approval, MappingProposal
from services.authorization.rbac_service import RBACService


class ReviewConsole:
    """Console service for human clinical review of AI-generated mapping proposals."""

    def __init__(self, rbac_service: Optional[RBACService] = None):
        self.rbac = rbac_service or RBACService()

    def render_proposal_dossier(self, proposal: MappingProposal) -> str:
        """Format an inspection dossier for terminal review."""
        dsl = proposal.typed_dsl
        lines = [
            "=" * 70,
            f"CLINICAL MAPPING PROPOSAL DOSSIER: {proposal.proposal_id}",
            "=" * 70,
            f"Provider ID:        {proposal.provider_id}",
            f"Contract Version:   {proposal.source_contract_version}",
            f"Proposal SHA-256:   {proposal.proposal_hash}",
            f"Disposition:        {proposal.disposition}",
            f"Created At:         {proposal.created_at.isoformat()}",
            "-" * 70,
            "MAPPING SPECIFICATION (ALLOWLIST DSL):",
            f"  Match Predicate:  {dsl.match}",
            f"  Target Concept:   {dsl.target_concept_ref}",
            f"  Value Operation:  {dsl.value_operation}",
            f"  Unit Conversion:  {dsl.source_unit} -> {dsl.target_unit}",
            f"  Terminology Ref:  {dsl.terminology_snapshot}",
            "-" * 70,
            "INVESTIGATION EVIDENCE & SAFE PROFILE:",
        ]

        for ref in proposal.evidence_refs:
            lines.append(f"  - Evidence Ref: {ref}")

        lines.append("-" * 70)
        lines.append("SAFE TEST (DRY RUN) RESULTS:")
        for k, v in proposal.safe_test_results.items():
            lines.append(f"  {k}: {v}")

        lines.append("=" * 70)
        return "\n".join(lines)

    def sign_approval(
        self,
        proposal: MappingProposal,
        reviewer_id: str,
        reviewer_role: str,
        provider_scope: str,
        expected_registry_version: str,
        decision: str = "approved",
        validity_hours: int = 24,
        notes: str = "",
    ) -> Approval:
        """Sign and authorize an Approval artifact for a proposal."""
        expiry = datetime.utcnow() + timedelta(hours=validity_hours)

        approval = Approval(
            proposal_hash=proposal.proposal_hash,
            authorized_reviewer=reviewer_id,
            reviewer_role=reviewer_role,
            provider_scope=provider_scope,
            evidence_version=proposal.source_contract_version,
            expected_registry_version=expected_registry_version,
            expiry=expiry,
            decision=decision,
            notes=notes,
        )

        # Enforce RBAC validation
        self.rbac.authorize_approval(approval, proposal)
        return approval
