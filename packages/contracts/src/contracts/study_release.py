"""Study Release contract for reproducible research cohorts."""

from datetime import datetime
from typing import Dict, Literal
from pydantic import BaseModel, Field

ReleaseState = Literal["active", "revoked"]


class StudyRelease(BaseModel):
    """Immutable research snapshot manifest binding multiple tables and provider snapshots.
    
    Section 8 contract:
    study_release: release/study IDs, owner, cohort digest, fixed reference date,
    provider/table snapshot bundle, quality/privacy versions, code digest,
    output checksums, active/revoked state.
    """
    release_id: str = Field(description="Unique release identifier")
    study_id: str = Field(description="Associated research study identifier")
    owner: str = Field(description="Researcher or system owner")
    cohort_digest: str = Field(description="SHA-256 digest of cohort query and cohort member list")
    fixed_reference_date: str = Field(description="Fixed ISO date (YYYY-MM-DD) used for cohort lookback")
    provider_table_snapshot_bundle: Dict[str, Dict[str, int]] = Field(
        description="Snapshot bundle: provider -> {table_name: snapshot_id}"
    )
    quality_privacy_versions: Dict[str, str] = Field(
        description="Versions of quality gates and suppression rules applied"
    )
    code_digest: str = Field(description="Digest of code repository generating this release")
    output_checksums: Dict[str, str] = Field(
        description="Map of exported materialized artifacts to SHA-256 checksums"
    )
    active_state: ReleaseState = Field(default="active", description="active or revoked")
    created_at: datetime = Field(default_factory=datetime.utcnow)
    revoked_at: datetime | None = None
    revocation_reason: str | None = None

    def revoke(self, reason: str) -> None:
        """Revoke future gateway access while maintaining immutable manifest history."""
        self.active_state = "revoked"
        self.revoked_at = datetime.utcnow()
        self.revocation_reason = reason
