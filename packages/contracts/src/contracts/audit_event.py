"""Audit Event contract enforcing payload-free operational telemetry."""

from datetime import datetime
from typing import Any, Dict, Literal, Optional
from pydantic import BaseModel, Field, model_validator
from contracts.errors import ContractViolationError

AuditOutcome = Literal["success", "failure", "denied"]

# Forbidden keys that might leak PHI/PII into audit events
DISALLOWED_PAYLOAD_KEYS = {
    "patient_name", "first_name", "last_name", "ssn", "mrn", "birth_date",
    "dob", "phone", "email", "address", "raw_payload", "payload", "note_text"
}


class AuditEvent(BaseModel):
    """Immutable audit record for platform events and security operations.
    
    Section 8 contract:
    audit_event: actor/service, action/resource/scope, outcome, correlation ID,
    old/new version, timestamp. No raw patient payloads.
    """
    actor_or_service: str = Field(description="Service name or user identifier performing the action")
    action: str = Field(description="Operation name, e.g. ingest_batch, approve_mapping, publish_release")
    resource: str = Field(description="Target resource URI or identifier")
    scope: str = Field(description="Security scope or provider domain, e.g. provider-a")
    outcome: AuditOutcome = Field(description="Result of action: success, failure, denied")
    correlation_id: str = Field(description="End-to-end trace correlation ID")
    old_version: Optional[str] = Field(default=None, description="Previous version identifier if applicable")
    new_version: Optional[str] = Field(default=None, description="New version identifier if applicable")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Safe operational attributes only")
    timestamp: datetime = Field(default_factory=datetime.utcnow)

    @model_validator(mode="after")
    def enforce_no_patient_payloads(self) -> "AuditEvent":
        """Strictly prohibit patient identifiers and clinical payloads in audit telemetry."""
        for key in self.metadata:
            if key.lower() in DISALLOWED_PAYLOAD_KEYS:
                raise ContractViolationError(
                    f"Prohibited telemetry key '{key}' found in AuditEvent metadata. Raw patient payloads forbidden."
                )
        return self
