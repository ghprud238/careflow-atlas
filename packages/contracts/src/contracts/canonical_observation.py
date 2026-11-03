"""Canonical Observation contract for Silver layer."""

from datetime import datetime
from typing import Literal, Optional
from pydantic import BaseModel, Field

SupersessionState = Literal["current", "superseded", "deleted"]


class OriginalObservation(BaseModel):
    """Raw, unaltered source representation preserved for audit and lineage."""
    original_code: str = Field(description="Raw source code, e.g. LOCAL_LAB_017 or LOINC code")
    original_coding_system: str = Field(description="Coding system identifier or local namespace")
    original_text: str = Field(description="Raw display text or concept description")
    original_value: str = Field(description="Original value as delivered by source")
    original_unit: Optional[str] = Field(default=None, description="Original unit string")
    original_status: str = Field(description="Original status, e.g. final, amended, preliminary")


class NormalizedObservation(BaseModel):
    """Standardized representation produced by deterministic mapping rules."""
    normalized_concept: Optional[str] = Field(default=None, description="Standard concept ID from reviewed catalog")
    normalized_value: Optional[float] = Field(default=None, description="Parsed decimal numeric value")
    normalized_unit: Optional[str] = Field(default=None, description="Standardized canonical unit")


class Lineage(BaseModel):
    """Lineage references linking this canonical record to its source manifest and pipeline run."""
    manifest_id: str = Field(description="Manifest where this observation was landed")
    run_id: str = Field(description="Pipeline run that processed and normalized this record")
    resource_envelope_hash: str = Field(description="Hash of the originating resource envelope")


class CanonicalObservation(BaseModel):
    """Versioned canonical clinical observation stored in Silver Iceberg tables.
    
    Section 8 contract:
    canonical_observation: provider-scoped patient/encounter/source keys, sequence,
    effective time, original coding/text/value/unit/status, normalized concept/value/unit,
    mapping reference, normalization version, lineage, supersession/deletion state.
    """
    provider: str = Field(description="Provider identifier")
    patient_key: str = Field(description="Provider-scoped patient reference key")
    encounter_key: Optional[str] = Field(default=None, description="Provider-scoped encounter reference key")
    source_key: str = Field(description="Source resource identifier")
    sequence: int = Field(ge=0, description="Monotonic sequence number established by adapter")
    effective_time: datetime = Field(description="Effective date/time of observation")
    original: OriginalObservation = Field(description="Unaltered raw clinical data")
    normalized: NormalizedObservation = Field(description="Deterministic normalized data")
    mapping_reference: Optional[str] = Field(default=None, description="Approved mapping rule/version ID")
    normalization_version: str = Field(description="Version of normalization algorithm applied")
    lineage: Lineage = Field(description="Audit and provenance lineage")
    supersession_state: SupersessionState = Field(
        default="current",
        description="Whether this record is current, superseded by newer sequence, or deleted"
    )
    created_at: datetime = Field(default_factory=datetime.utcnow)
