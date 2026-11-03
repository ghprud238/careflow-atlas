"""Source Manifest contract for CareFlow Atlas landing protocol."""

from datetime import datetime
from typing import Literal, Optional
from pydantic import BaseModel, Field, model_validator
from contracts.errors import ConflictingDeliveryError, DuplicateDeliveryError

ManifestReadiness = Literal["pending", "ready"]
DeliveryDisposition = Literal["accepted", "duplicate", "conflict"]


class SourceManifest(BaseModel):
    """Immutable record tracking feed delivery and landing readiness.
    
    Section 8 contract:
    source_manifest: provider, feed, batch ID, source interval, object URI/hash/bytes,
    adapter version, readiness, delivery conflict/disposition.
    """
    manifest_id: str = Field(description="Unique identifier for this manifest, e.g. provider-batch_id")
    provider: str = Field(description="Provider identifier, e.g. provider-a")
    feed: str = Field(description="Feed category or type, e.g. fhir-r4, csv-labs")
    batch_id: str = Field(description="Batch identifier provided by source feed")
    source_interval_start: datetime = Field(description="Start time of the source data window")
    source_interval_end: datetime = Field(description="End time of the source data window")
    object_uri: str = Field(description="URI pointing to the raw staged object in bronze storage")
    object_hash: str = Field(description="SHA-256 hex digest of the object payload")
    object_bytes: int = Field(ge=0, description="Size of the raw payload in bytes")
    adapter_version: str = Field(description="Version of the adapter designated to parse this feed")
    readiness: ManifestReadiness = Field(default="pending", description="Readiness state: pending or ready")
    disposition: DeliveryDisposition = Field(default="accepted", description="accepted, duplicate, or conflict")
    created_at: datetime = Field(default_factory=datetime.utcnow)
    ready_at: Optional[datetime] = None

    @model_validator(mode="after")
    def validate_interval(self) -> "SourceManifest":
        if self.source_interval_start > self.source_interval_end:
            raise ValueError("source_interval_start must precede or equal source_interval_end")
        return self

    def check_delivery_conflict(self, existing: "SourceManifest") -> DeliveryDisposition:
        """Evaluate idempotency and conflict against an existing manifest with same batch ID.
        
        Section 9 Landing invariant:
        'Same batch ID/hash is duplicate; same ID/different hash is conflict.'
        """
        if self.provider != existing.provider or self.batch_id != existing.batch_id:
            return "accepted"
        
        if self.object_hash == existing.object_hash:
            return "duplicate"
        else:
            return "conflict"
