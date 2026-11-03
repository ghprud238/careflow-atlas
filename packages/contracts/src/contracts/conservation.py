"""Conservation invariant verification at parsed-resource-envelope granularity."""

from pydantic import BaseModel, Field, model_validator
from contracts.errors import ConservationViolationError


class ConservationAccounting(BaseModel):
    """Enforces mathematical conservation across all parsed resources.
    
    Section 8 Conservation:
    N_input = N_accepted + N_duplicate + N_quarantined + N_intentionally_excluded.
    Each resource has one disposition and potentially multiple findings.
    Account for unreadable files at manifest level; never invent parsed-resource counts.
    """
    n_input: int = Field(ge=0, description="Total parsed resource envelopes in batch")
    n_accepted: int = Field(ge=0, description="Envelopes accepted and processed to canonical records")
    n_duplicate: int = Field(ge=0, description="Envelopes classified as duplicates")
    n_quarantined: int = Field(ge=0, description="Envelopes quarantined due to schema/clinical violations")
    n_intentionally_excluded: int = Field(ge=0, description="Envelopes intentionally filtered per contract")

    @model_validator(mode="after")
    def verify_conservation(self) -> "ConservationAccounting":
        total_accounted = (
            self.n_accepted
            + self.n_duplicate
            + self.n_quarantined
            + self.n_intentionally_excluded
        )
        if self.n_input != total_accounted:
            raise ConservationViolationError(
                f"Conservation invariant violated: n_input ({self.n_input}) != "
                f"n_accepted ({self.n_accepted}) + n_duplicate ({self.n_duplicate}) + "
                f"n_quarantined ({self.n_quarantined}) + n_excluded ({self.n_intentionally_excluded}) "
                f"(total accounted: {total_accounted}, diff: {self.n_input - total_accounted})"
            )
        return self
