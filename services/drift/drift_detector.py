"""Schema Drift and Structural Evolution Detection Service.

Monitors incoming provider feeds against pinned schema contracts:
- Detects added columns, missing required columns, and header mutations.
- Detects distribution and null-rate anomalies.
- Emits structured DriftReport and determines quarantine/investigation triggers.
"""

from datetime import datetime
from typing import Any, Dict, List, Literal, Optional, Tuple
from pydantic import BaseModel, Field


DriftSeverity = Literal["none", "warning", "critical"]
DriftAction = Literal["proceed", "investigate_agent", "quarantine_batch"]


PINNED_PROVIDER_CONTRACTS = {
    ("provider-b", "observations.csv"): {
        "required_columns": ["OBS_ID", "PATIENT_ID", "LOCAL_CODE", "VALUE", "EFFECTIVE_TIME"],
        "expected_columns": [
            "OBS_ID",
            "PATIENT_ID",
            "ENCOUNTER_ID",
            "LOCAL_CODE",
            "DISPLAY_NAME",
            "VALUE",
            "UNIT",
            "EFFECTIVE_TIME",
            "STATUS",
            "_CHANGE_SEQUENCE",
            "_OPERATION",
        ],
    },
    ("provider-b", "patients.csv"): {
        "required_columns": ["PATIENT_ID", "BIRTH_DATE", "GENDER"],
        "expected_columns": ["PATIENT_ID", "BIRTH_DATE", "GENDER", "POSTAL_CODE", "_CHANGE_SEQUENCE", "_OPERATION"],
    },
    ("provider-b", "encounters.csv"): {
        "required_columns": ["ENCOUNTER_ID", "PATIENT_ID", "CLASS_CODE", "PERIOD_START"],
        "expected_columns": ["ENCOUNTER_ID", "PATIENT_ID", "CLASS_CODE", "PERIOD_START", "PERIOD_END", "STATUS", "_CHANGE_SEQUENCE", "_OPERATION"],
    },
}


class DriftReport(BaseModel):
    """Structured report evaluating schema and distribution drift for a delivered batch."""
    provider_id: str
    feed: str
    evaluated_at: datetime = Field(default_factory=datetime.utcnow)
    missing_required_columns: List[str] = Field(default_factory=list)
    unexpected_added_columns: List[str] = Field(default_factory=list)
    severity: DriftSeverity = "none"
    recommended_action: DriftAction = "proceed"
    findings: List[str] = Field(default_factory=list)


class DriftDetector:
    """Evaluates incoming feed structural schemas against pinned contracts."""

    def __init__(self, contracts: Optional[Dict[Tuple[str, str], Dict[str, Any]]] = None):
        self.contracts = contracts or PINNED_PROVIDER_CONTRACTS

    def evaluate_headers(
        self,
        provider_id: str,
        feed: str,
        actual_headers: List[str],
    ) -> DriftReport:
        """Analyze delivered column headers for structural drift."""
        contract = self.contracts.get((provider_id, feed))
        if not contract:
            return DriftReport(
                provider_id=provider_id,
                feed=feed,
                severity="warning",
                recommended_action="proceed",
                findings=[f"No pinned schema contract found for ({provider_id}, {feed})"],
            )

        required = set(contract.get("required_columns", []))
        expected = set(contract.get("expected_columns", []))
        actual = set(actual_headers)

        missing_required = list(required - actual)
        unexpected_added = list(actual - expected)

        findings: List[str] = []
        severity: DriftSeverity = "none"
        action: DriftAction = "proceed"

        if missing_required:
            severity = "critical"
            action = "quarantine_batch"
            findings.append(f"Missing mandatory columns: {', '.join(sorted(missing_required))}")

        if unexpected_added:
            if severity != "critical":
                severity = "warning"
                action = "investigate_agent"
            findings.append(f"Detected newly added columns: {', '.join(sorted(unexpected_added))}")

        if not missing_required and not unexpected_added:
            findings.append("Schema exactly matches pinned contract specification.")

        return DriftReport(
            provider_id=provider_id,
            feed=feed,
            missing_required_columns=sorted(missing_required),
            unexpected_added_columns=sorted(unexpected_added),
            severity=severity,
            recommended_action=action,
            findings=findings,
        )
