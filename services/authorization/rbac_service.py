"""Role-Based Access Control (RBAC) and Approval Authorization Service.

Section 8 & 11 Governance Invariants:
- Human authorization gate requires designated clinical roles:
  terminology_reviewer, lead_pathologist, chief_medical_officer.
- Prevents unauthorized engineers, analysts, or automated accounts from approving semantic mappings.
- Enforces provider scope alignment: reviewer must have authority over the specific provider or consortium.
"""

from datetime import datetime
from typing import Set
from contracts import (
    Approval,
    AuthorizationDeniedError,
    CrossProviderAccessError,
    MappingProposal,
)

AUTHORIZED_CLINICAL_ROLES: Set[str] = {
    "terminology_reviewer",
    "lead_pathologist",
    "chief_medical_officer",
    "consortium_medical_director",
}


class RBACService:
    """Validates human reviewer roles and tenant scope authorization."""

    def __init__(self, allowed_roles: Set[str] = None):
        self.allowed_roles = allowed_roles or AUTHORIZED_CLINICAL_ROLES

    def authorize_approval(
        self,
        approval: Approval,
        proposal: MappingProposal,
        current_time: datetime = None,
    ) -> None:
        """Enforce strict RBAC and tenant authorization before promotion."""
        now = current_time or datetime.utcnow()

        # 1. Role validation
        if approval.reviewer_role not in self.allowed_roles:
            raise AuthorizationDeniedError(
                f"Unauthorized reviewer role: '{approval.reviewer_role}'. "
                f"Requires one of: {sorted(self.allowed_roles)}"
            )

        # 2. Provider scope validation
        if approval.provider_scope != "consortium_wide":
            if approval.provider_scope != proposal.provider_id:
                raise CrossProviderAccessError(
                    f"Reviewer provider scope '{approval.provider_scope}' does not authorize "
                    f"proposal for provider '{proposal.provider_id}'."
                )

        # 3. Decision validation
        if approval.decision != "approved":
            raise AuthorizationDeniedError(
                f"Cannot authorize mapping promotion with decision '{approval.decision}'."
            )

        # 4. Expiration validation
        if now > approval.expiry:
            raise AuthorizationDeniedError(
                f"Approval expired at {approval.expiry}, current time is {now}."
            )
