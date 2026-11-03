"""Pipeline Run contract for tracking deterministic batch executions."""

from datetime import datetime
from typing import Dict, Literal, Optional
from pydantic import BaseModel, Field

RunState = Literal["running", "completed", "completed_with_exceptions", "failed"]


class PipelineRun(BaseModel):
    """Execution record tracking pinned versions and committed Iceberg table snapshots.
    
    Section 8 contract:
    pipeline_run: run ID, manifest digest, pinned schema/mapping/config/code versions,
    expected output scope, attempt, state, error class, committed snapshots.
    """
    run_id: str = Field(description="Unique run identifier")
    manifest_digest: str = Field(description="Digest of the source manifest being processed")
    pinned_schema_version: str = Field(description="Pinned schema version for this run")
    pinned_mapping_version: str = Field(description="Pinned mapping version for this run")
    pinned_config_version: str = Field(description="Pinned configuration digest")
    pinned_code_version: str = Field(description="Git commit hash or digest of the pipeline code")
    expected_output_scope: str = Field(description="Target provider and dataset scope, e.g. provider-a:silver")
    attempt: int = Field(default=1, ge=1, description="Execution attempt count")
    state: RunState = Field(default="running", description="Platform run status")
    error_class: Optional[str] = Field(default=None, description="Class name of error if failed")
    error_message: Optional[str] = Field(default=None, description="Safe error description")
    committed_snapshots: Dict[str, int] = Field(
        default_factory=dict,
        description="Map of table name to committed Iceberg snapshot ID"
    )
    started_at: datetime = Field(default_factory=datetime.utcnow)
    completed_at: Optional[datetime] = None

    def mark_completed(self, has_exceptions: bool = False, committed_snapshots: Optional[Dict[str, int]] = None) -> None:
        """Complete the run with appropriate product state."""
        self.state = "completed_with_exceptions" if has_exceptions else "completed"
        if committed_snapshots:
            self.committed_snapshots.update(committed_snapshots)
        self.completed_at = datetime.utcnow()

    def mark_failed(self, error_cls: str, message: str) -> None:
        """Mark run as failed with explicit error categorization."""
        self.state = "failed"
        self.error_class = error_cls
        self.error_message = message
        self.completed_at = datetime.utcnow()
