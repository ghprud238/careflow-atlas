"""Resource Envelope contract for parsed input items."""

from datetime import datetime
from typing import Any, Dict, Literal, Optional, Tuple
from pydantic import BaseModel, Field

OperationType = Literal["insert", "update", "delete"]
DispositonType = Literal["accepted", "duplicate", "quarantined", "intentionally_excluded"]


class ResourceEnvelope(BaseModel):
    """Normalized carrier for parsed resources prior to canonical persistence.
    
    Section 8 contract:
    resource_envelope: provider, source system, resource type, source resource ID,
    opaque source version, adapter-established change sequence, operation, event time,
    ingestion time, payload URI/hash.
    """
    provider: str = Field(description="Originating provider ID")
    source_system: str = Field(description="Source system within the provider")
    resource_type: str = Field(description="Healthcare entity type, e.g. Patient, Encounter, Observation")
    source_resource_id: str = Field(description="Unique identifier within source system")
    opaque_source_version: str = Field(description="Opaque version from source, non-sortable")
    change_sequence: int = Field(ge=0, description="Adapter-established monotonic sequence per resource")
    operation: OperationType = Field(description="insert, update, or delete")
    event_time: datetime = Field(description="Clinical event timestamp from payload")
    ingestion_time: datetime = Field(default_factory=datetime.utcnow, description="Time landed on platform")
    payload_uri: str = Field(description="URI of raw payload or chunk in bronze store")
    payload_hash: str = Field(description="SHA-256 digest of individual resource payload")
    payload: Optional[Dict[str, Any]] = Field(default=None, description="Optional parsed payload map")
    disposition: DispositonType = Field(default="accepted", description="Evaluation disposition")
    quarantine_reasons: list[str] = Field(default_factory=list, description="Findings if quarantined")

    @property
    def logical_key(self) -> Tuple[str, str, str, str]:
        """Logical resource key: (provider, source_system, resource_type, source_resource_id)."""
        return (self.provider, self.source_system, self.resource_type, self.source_resource_id)
