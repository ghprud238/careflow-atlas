"""Canonical Patient contract for Silver layer."""

from datetime import datetime
from typing import Optional
from pydantic import BaseModel, Field
from contracts.canonical_observation import Lineage, SupersessionState


class CanonicalPatient(BaseModel):
    """Versioned canonical patient record stored in Silver Iceberg tables."""
    provider: str = Field(description="Originating provider ID")
    patient_key: str = Field(description="Provider-scoped patient reference key")
    source_resource_id: str = Field(description="Original source resource ID")
    sequence: int = Field(ge=0, description="Adapter-established monotonic sequence number")
    birth_date: Optional[str] = Field(default=None, description="Birth date in YYYY-MM-DD format")
    gender: Optional[str] = Field(default=None, description="Administrative gender, e.g. male, female, other")
    postal_code: Optional[str] = Field(default=None, description="Generalized postal code")
    deceased_boolean: bool = Field(default=False, description="Whether patient is deceased")
    lineage: Lineage = Field(description="Audit and provenance lineage")
    supersession_state: SupersessionState = Field(
        default="current",
        description="Whether this record is current, superseded by newer sequence, or deleted"
    )
    created_at: datetime = Field(default_factory=datetime.utcnow)
