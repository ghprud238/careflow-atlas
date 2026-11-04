"""Canonical Encounter contract for Silver layer."""

from datetime import datetime
from typing import Optional
from pydantic import BaseModel, Field
from contracts.canonical_observation import Lineage, SupersessionState


class CanonicalEncounter(BaseModel):
    """Versioned canonical encounter record stored in Silver Iceberg tables."""
    provider: str = Field(description="Originating provider ID")
    encounter_key: str = Field(description="Provider-scoped encounter reference key")
    patient_key: str = Field(description="Associated provider-scoped patient key")
    source_resource_id: str = Field(description="Original source resource ID")
    sequence: int = Field(ge=0, description="Adapter-established monotonic sequence number")
    class_code: Optional[str] = Field(default=None, description="Encounter class, e.g. AMB, IMP, EMER")
    period_start: Optional[datetime] = Field(default=None, description="Encounter start timestamp")
    period_end: Optional[datetime] = Field(default=None, description="Encounter end timestamp")
    status: str = Field(default="finished", description="Encounter status, e.g. finished, cancelled")
    type_coding: Optional[str] = Field(default=None, description="Type coding code or descriptor")
    lineage: Lineage = Field(description="Audit and provenance lineage")
    supersession_state: SupersessionState = Field(
        default="current",
        description="Whether this record is current, superseded by newer sequence, or deleted"
    )
    created_at: datetime = Field(default_factory=datetime.utcnow)
