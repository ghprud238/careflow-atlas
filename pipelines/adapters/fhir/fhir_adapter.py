"""FHIR R4 Adapter for Provider A.

Parses FHIR R4 JSON payloads into typed ResourceEnvelopes and Canonical records:
- Patient
- Encounter
- Observation

Enforces validation, quarantine classification, and attributable findings.
"""

import hashlib
import json
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from contracts import (
    CanonicalEncounter,
    CanonicalObservation,
    CanonicalPatient,
    Lineage,
    NormalizedObservation,
    OperationType,
    OriginalObservation,
    ResourceEnvelope,
)
from fixtures.generation.synthetic_provider_a import REVIEWED_LOINC_LABS

REVIEWED_CATALOG = {item["code"]: item for item in REVIEWED_LOINC_LABS}


class FHIRAdapter:
    """Adapter translating Provider A FHIR R4 resources to platform contracts."""

    def __init__(self, provider: str = "provider-a", source_system: str = "fhir-r4-emr"):
        self.provider = provider
        self.source_system = source_system

    def parse_resource_dict(
        self,
        raw_res: Dict[str, Any],
        manifest_id: str,
        payload_uri: str = "",
    ) -> ResourceEnvelope:
        """Parse raw FHIR resource dictionary into a validated ResourceEnvelope."""
        res_type = raw_res.get("resourceType", "Unknown")
        source_id = str(raw_res.get("id", ""))
        version_id = str(raw_res.get("meta", {}).get("versionId", "1"))
        change_seq = int(raw_res.get("_change_sequence", 1))
        op: OperationType = raw_res.get("_operation", "insert")

        # Compute payload hash
        payload_bytes = json.dumps(raw_res, sort_keys=True).encode("utf-8")
        payload_hash = hashlib.sha256(payload_bytes).hexdigest()

        # Determine event time
        event_time = self._extract_event_time(raw_res)

        envelope = ResourceEnvelope(
            provider=self.provider,
            source_system=self.source_system,
            resource_type=res_type,
            source_resource_id=source_id,
            opaque_source_version=version_id,
            change_sequence=change_seq,
            operation=op,
            event_time=event_time,
            payload_uri=payload_uri,
            payload_hash=payload_hash,
            payload=raw_res,
            disposition="accepted",
            quarantine_reasons=[],
        )

        # Validate resource
        self._validate_envelope(envelope, raw_res)
        return envelope

    def _extract_event_time(self, raw_res: Dict[str, Any]) -> datetime:
        """Extract event timestamp from FHIR resource fields."""
        res_type = raw_res.get("resourceType")
        dt_str = None
        if res_type == "Observation":
            dt_str = raw_res.get("effectiveDateTime") or raw_res.get("issued")
        elif res_type == "Encounter":
            dt_str = raw_res.get("period", {}).get("start")
        elif res_type == "Patient":
            dt_str = raw_res.get("meta", {}).get("lastUpdated") or raw_res.get("birthDate")

        if dt_str:
            try:
                # Handle ISO timestamps with Z or offsets
                clean_str = dt_str.replace("Z", "+00:00")
                return datetime.fromisoformat(clean_str)
            except Exception:
                pass
        return datetime.utcnow()

    def _validate_envelope(self, envelope: ResourceEnvelope, raw: Dict[str, Any]) -> None:
        """Validate clinical rules and populate quarantine findings if violated."""
        if not envelope.source_resource_id:
            envelope.disposition = "quarantined"
            envelope.quarantine_reasons.append("Missing required resource ID")

        if envelope.resource_type == "Observation":
            # 1. Subject reference
            subject_ref = raw.get("subject", {}).get("reference", "")
            if not subject_ref or not subject_ref.startswith("Patient/"):
                envelope.disposition = "quarantined"
                envelope.quarantine_reasons.append("Missing or invalid subject Patient reference")

            # 2. Status check
            status = raw.get("status", "")
            if status in ["entered-in-error", "cancelled"]:
                envelope.disposition = "quarantined"
                envelope.quarantine_reasons.append(f"Invalid observation status: {status}")

            # 3. Numeric representation check
            val_qty = raw.get("valueQuantity", {})
            if "value" in val_qty:
                val = val_qty["value"]
                try:
                    float(val)
                except (ValueError, TypeError):
                    envelope.disposition = "quarantined"
                    envelope.quarantine_reasons.append(f"Malformed numeric valueQuantity value: {val}")

        elif envelope.resource_type == "Encounter":
            subject_ref = raw.get("subject", {}).get("reference", "")
            if not subject_ref or not subject_ref.startswith("Patient/"):
                envelope.disposition = "quarantined"
                envelope.quarantine_reasons.append("Missing or invalid encounter subject Patient reference")

        elif envelope.resource_type == "Patient":
            # Verify birthDate
            birth_date = raw.get("birthDate")
            if not birth_date:
                envelope.disposition = "quarantined"
                envelope.quarantine_reasons.append("Missing birthDate")

    def to_canonical_patient(self, envelope: ResourceEnvelope, run_id: str, manifest_id: str) -> Optional[CanonicalPatient]:
        """Convert accepted Patient envelope to CanonicalPatient."""
        if envelope.disposition != "accepted" or envelope.resource_type != "Patient":
            return None
        raw = envelope.payload or {}
        lineage = Lineage(
            manifest_id=manifest_id,
            run_id=run_id,
            resource_envelope_hash=envelope.payload_hash,
        )
        postal = None
        addresses = raw.get("address", [])
        if addresses and isinstance(addresses, list):
            postal = addresses[0].get("postalCode")

        return CanonicalPatient(
            provider=self.provider,
            patient_key=f"pat-{envelope.source_resource_id.replace('pat-', '')}",
            source_resource_id=envelope.source_resource_id,
            sequence=envelope.change_sequence,
            birth_date=raw.get("birthDate"),
            gender=raw.get("gender"),
            postal_code=postal,
            deceased_boolean=raw.get("deceasedBoolean", False),
            lineage=lineage,
            supersession_state="current",
        )

    def to_canonical_encounter(self, envelope: ResourceEnvelope, run_id: str, manifest_id: str) -> Optional[CanonicalEncounter]:
        """Convert accepted Encounter envelope to CanonicalEncounter."""
        if envelope.disposition != "accepted" or envelope.resource_type != "Encounter":
            return None
        raw = envelope.payload or {}
        lineage = Lineage(
            manifest_id=manifest_id,
            run_id=run_id,
            resource_envelope_hash=envelope.payload_hash,
        )
        subject_ref = raw.get("subject", {}).get("reference", "")
        patient_id = subject_ref.replace("Patient/", "")
        patient_key = f"pat-{patient_id.replace('pat-', '')}"

        period = raw.get("period", {})
        start_time = None
        end_time = None
        if period.get("start"):
            try:
                start_time = datetime.fromisoformat(period["start"].replace("Z", "+00:00"))
            except Exception:
                pass
        if period.get("end"):
            try:
                end_time = datetime.fromisoformat(period["end"].replace("Z", "+00:00"))
            except Exception:
                pass

        class_code = raw.get("class", {}).get("code")
        return CanonicalEncounter(
            provider=self.provider,
            encounter_key=f"enc-{envelope.source_resource_id.replace('enc-', '')}",
            patient_key=patient_key,
            source_resource_id=envelope.source_resource_id,
            sequence=envelope.change_sequence,
            class_code=class_code,
            period_start=start_time,
            period_end=end_time,
            status=raw.get("status", "finished"),
            lineage=lineage,
            supersession_state="current",
        )

    def to_canonical_observation(
        self,
        envelope: ResourceEnvelope,
        run_id: str,
        manifest_id: str,
        normalization_version: str = "1.0.0",
    ) -> Optional[CanonicalObservation]:
        """Convert accepted Observation envelope to CanonicalObservation with deterministic normalization."""
        if envelope.disposition != "accepted" or envelope.resource_type != "Observation":
            return None
        raw = envelope.payload or {}
        lineage = Lineage(
            manifest_id=manifest_id,
            run_id=run_id,
            resource_envelope_hash=envelope.payload_hash,
        )

        subject_ref = raw.get("subject", {}).get("reference", "")
        patient_id = subject_ref.replace("Patient/", "")
        patient_key = f"pat-{patient_id.replace('pat-', '')}"

        encounter_ref = raw.get("encounter", {}).get("reference")
        encounter_key = None
        if encounter_ref:
            enc_id = encounter_ref.replace("Encounter/", "")
            encounter_key = f"enc-{enc_id.replace('enc-', '')}"

        # Extract coding
        codings = raw.get("code", {}).get("coding", [])
        orig_code = codings[0].get("code", "UNKNOWN") if codings else "UNKNOWN"
        orig_system = codings[0].get("system", "") if codings else ""
        orig_text = raw.get("code", {}).get("text", "") or (codings[0].get("display", "") if codings else "")

        val_qty = raw.get("valueQuantity", {})
        raw_val = str(val_qty.get("value", ""))
        orig_unit = val_qty.get("unit")
        status = raw.get("status", "final")

        orig_obs = OriginalObservation(
            original_code=orig_code,
            original_coding_system=orig_system,
            original_text=orig_text,
            original_value=raw_val,
            original_unit=orig_unit,
            original_status=status,
        )

        # Deterministic normalization against reviewed catalog
        norm_concept = None
        norm_val = None
        norm_unit = orig_unit
        mapping_ref = None

        if orig_code in REVIEWED_CATALOG:
            cat_entry = REVIEWED_CATALOG[orig_code]
            norm_concept = cat_entry["concept_id"]
            mapping_ref = f"reviewed-catalog/loinc-{orig_code}"
            try:
                norm_val = float(raw_val)
            except Exception:
                norm_val = None

        norm_obs = NormalizedObservation(
            normalized_concept=norm_concept,
            normalized_value=norm_val,
            normalized_unit=norm_unit,
        )

        return CanonicalObservation(
            provider=self.provider,
            patient_key=patient_key,
            encounter_key=encounter_key,
            source_key=envelope.source_resource_id,
            sequence=envelope.change_sequence,
            effective_time=envelope.event_time,
            original=orig_obs,
            normalized=norm_obs,
            mapping_reference=mapping_ref,
            normalization_version=normalization_version,
            lineage=lineage,
            supersession_state="current",
        )
