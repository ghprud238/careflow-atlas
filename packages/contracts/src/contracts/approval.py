"""Human review decision contract bound to exact proposal hash."""

from datetime import datetime
from typing import Literal
from pydantic import BaseModel, Field
from contracts.errors import ApprovalHashMismatchError, StaleApprovalError

DecisionType = Literal["approved", "rejected"]


class Approval(BaseModel):
    """Immutable human review authorization artifact.
    
    Section 8 contract:
    approval: proposal hash, authorized reviewer, provider/scope, evidence version,
    expected registry version, expiry, decision, timestamp.
    """
    proposal_hash: str = Field(description="SHA-256 hash of the exact proposal reviewed")
    authorized_reviewer: str = Field(description="Reviewer identifier, e.g. email or username")
    reviewer_role: str = Field(description="RBAC role of the reviewer, e.g. terminology_reviewer")
    provider_scope: str = Field(description="Provider scope for which approval is valid")
    evidence_version: str = Field(description="Version of evidence/profile reviewed")
    expected_registry_version: str = Field(description="Mapping registry version expected at review time")
    expiry: datetime = Field(description="Expiration timestamp for this authorization")
    decision: DecisionType = Field(description="approved or rejected")
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    notes: str = Field(default="", description="Optional reviewer remarks or rationale")

    def validate_for_execution(
        self,
        target_proposal_hash: str,
        current_registry_version: str,
        current_time: datetime,
    ) -> None:
        """Enforce strict authorization invariants before promotion.
        
        Section 11 Approval invariant:
        'Bind full artifact hash, provider/source scope, evidence/terminology versions,
        expected registry version, authorized reviewer/role, expiry, decision. Modified content,
        stale registry/evidence, or expiry requires revalidation.'
        """
        if self.decision != "approved":
            raise ValueError(f"Cannot execute proposal with decision: {self.decision}")
        
        if self.proposal_hash != target_proposal_hash:
            raise ApprovalHashMismatchError(
                f"Approved hash {self.proposal_hash} does not match target proposal hash {target_proposal_hash}"
            )
        
        if current_time > self.expiry:
            raise StaleApprovalError(f"Approval expired at {self.expiry}, current time is {current_time}")
        
        if self.expected_registry_version != current_registry_version:
            raise StaleApprovalError(
                f"Registry version advanced from expected {self.expected_registry_version} to {current_registry_version}. Revalidation required."
            )
