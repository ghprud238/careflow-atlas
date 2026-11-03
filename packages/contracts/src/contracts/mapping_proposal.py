"""Mapping Proposal contract for governed agent investigation."""

import hashlib
import json
from datetime import datetime
from typing import Any, Dict, List, Literal, Optional
from pydantic import BaseModel, Field, model_validator
from contracts.errors import UnsupportedDSLOperationError

ProposalDisposition = Literal["requires_review", "abstain", "budget_exceeded"]
ALLOWED_OPERATIONS = {"rename_field", "parse_decimal", "map_approved_code", "convert_approved_unit"}


class MappingDSL(BaseModel):
    """Declarative, non-executable mapping specification.
    
    Section 11 invariant:
    'Allowlist operations such as rename_field, parse_decimal, map_approved_code,
    convert_approved_unit. No generated Python or free-form SQL.'
    """
    proposal_type: Literal["observation_mapping", "schema_mapping"] = "observation_mapping"
    provider_id: str
    source_contract_version: str
    match: Dict[str, str] = Field(description="Exact match predicates, e.g. {'local_test_code': 'LOCAL_LAB_017'}")
    target_concept_ref: str = Field(description="Standardized concept ID from reviewed catalog")
    value_operation: Literal["parse_decimal", "as_string", "none"] = "parse_decimal"
    source_unit: Optional[str] = None
    target_unit: Optional[str] = None
    unit_conversion: Optional[str] = None
    terminology_snapshot: str
    disposition: ProposalDisposition = "requires_review"

    @model_validator(mode="after")
    def validate_operations(self) -> "MappingDSL":
        # Ensure no arbitrary executable syntax exists
        for key, val in self.match.items():
            if any(forbidden in str(val).lower() for forbidden in ["select", "drop", "delete", "exec", "eval", "import"]):
                raise UnsupportedDSLOperationError(f"Prohibited tokens detected in match predicate: {val}")
        return self


class MappingProposal(BaseModel):
    """Investigation outcome produced by the bounded agent control plane.
    
    Section 8 contract:
    mapping_proposal: ID/hash, source scope, typed DSL, candidates, terminology snapshot,
    evidence refs, safe test results, prompt/model versions, disposition.
    """
    proposal_id: str = Field(description="Unique proposal identifier")
    proposal_hash: str = Field(default="", description="Deterministic SHA-256 hash of proposal artifact")
    provider_id: str = Field(description="Provider scope")
    source_contract_version: str = Field(description="Version of provider contract investigated")
    typed_dsl: MappingDSL = Field(description="Typed allowlist mapping DSL")
    candidates: List[Dict[str, Any]] = Field(default_factory=list, description="Ranked concept candidates evaluated")
    terminology_snapshot: str = Field(description="Pinned terminology snapshot used for lookup")
    evidence_refs: List[str] = Field(default_factory=list, description="References to safe profiles and schema diffs")
    safe_test_results: Dict[str, Any] = Field(default_factory=dict, description="Dry run execution metrics and summary")
    prompt_version: str = Field(description="Pinned version of the agent prompt template")
    model_version: str = Field(description="Exact model identifier used during investigation")
    disposition: ProposalDisposition = Field(default="requires_review")
    created_at: datetime = Field(default_factory=datetime.utcnow)

    @model_validator(mode="after")
    def compute_deterministic_hash(self) -> "MappingProposal":
        """Compute stable SHA-256 hash across canonical proposal fields."""
        hash_payload = {
            "provider_id": self.provider_id,
            "source_contract_version": self.source_contract_version,
            "typed_dsl": self.typed_dsl.model_dump(),
            "terminology_snapshot": self.terminology_snapshot,
            "prompt_version": self.prompt_version,
            "model_version": self.model_version,
            "disposition": self.disposition,
        }
        computed = hashlib.sha256(json.dumps(hash_payload, sort_keys=True).encode("utf-8")).hexdigest()
        if not self.proposal_hash:
            self.proposal_hash = computed
        elif self.proposal_hash != computed:
            raise ValueError(f"Proposal hash mismatch: expected {computed}, got {self.proposal_hash}")
        return self
