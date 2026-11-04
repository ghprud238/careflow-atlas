"""Delimited CSV/Pipe Adapter for Provider B.

Parses tabular clinical records (Patients, Encounters, Observations) into typed
ResourceEnvelopes and Canonical records:
- Maps pipe/comma delimited rows into standard carriers.
- Supports lifecycle operations: insert, update, and delete (clinical retraction).
- Enforces validation and quarantines malformed numbers, missing patient references.
- Deterministic normalization against reviewed catalog.
"""

import csv
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


class CSVAdapter:
    """Adapter translating Provider B Delimited CSV records into platform contracts."""

    def __init__(
        self,
        provider: str = "provider-b",
        source_system: str = "csv-clinic-lis",
        delimiter: str = "|",
    ):
        self.provider = provider
        self.source_system = source_system
        self.delimiter = delimiter

    def parse_csv_content(
        self,
        csv_text: str,
        resource_type: str,
        manifest_id: str,
        payload_uri: str = "",
    ) -> List[ResourceEnvelope]:
        """Parse raw delimited text into a list of validated ResourceEnvelopes."""
        lines = [line.strip() for line in csv_text.strip().split("\n") if line.strip()]
        if not lines:
            return []

        reader = csv.DictReader(lines, delimiter=self.delimiter)
        envelopes: List[ResourceEnvelope] = []

        for row in reader:
            env = self.parse_row_dict(
                row=row,
                resource_type=resource_type,
                manifest_id=manifest_id,
                payload_uri=payload_uri,
            )
            envelopes.append(env)

        return envelopes

    def parse_row_dict(
        self,
        row: Dict[str, Any],
        resource_type: str,
        manifest_id: str,
        payload_uri: str = "",
    ) -> ResourceEnvelope:
        """Parse a single CSV row dictionary into a ResourceEnvelope."""
        # Extract row identifier based on resource type
        if resource_type == "Observation":
            source_id = str(row.get("OBS_ID") or row.get("observation_id") or row.get("id") or "")
        elif resource_type == "Encounter":
            source_id = str(row.get("ENCOUNTER_ID") or row.get("encounter_id") or row.get("id") or "")
        elif resource_type == "Patient":
            source_id = str(row.get("PATIENT_ID") or row.get("patient_id") or row.get("id") or "")
        else:
            source_id = str(row.get("id") or "")

        change_seq = int(row.get("_CHANGE_SEQUENCE") or row.get("_change_sequence") or 1)
        raw_op = str(row.get("_OPERATION") or row.get("_operation") or "insert").lower()
        op: OperationType = "delete" if raw_op in ["delete", "retract", "retracted"] else ("update" if raw_op == "update" else "insert")

        # Payload hash
        payload_bytes = json.dumps(row, sort_keys=True).encode("utf-8")
        payload_hash = hashlib.sha256(payload_bytes).hexdigest()

        # Extract event time
        event_time = self._extract_event_time(row, resource_type)

        envelope = ResourceEnvelope(
            provider=self.provider,
            source_system=self.source_system,
            resource_type=resource_type,
            source_resource_id=source_id,
            opaque_source_version=str(change_seq),
            change_sequence=change_seq,
            operation=op,
            event_time=event_time,
            payload_uri=payload_uri,
            payload_hash=payload_hash,
            payload=row,
            disposition="accepted",
            quarantine_reasons=[],
        )

        self._validate_row(envelope, row, resource_type)
        return envelope

    def _extract_event_time(self, row: Dict[str, Any], resource_type: str) -> datetime:
        """Extract event timestamp from CSV row columns."""
        dt_str = (
            row.get("EFFECTIVE_TIME")
            or row.get("PERIOD_START")
            or row.get("BIRTH_DATE")
            or row.get("effective_time")
        )
        if dt_str:
            try:
                clean_str = dt_str.replace("Z", "+00:00")
                return datetime.fromisoformat(clean_str)
            except Exception:
                pass
        return datetime.utcnow()

    def _validate_row(
        self,
        envelope: ResourceEnvelope,
        row: Dict[str, Any],
        resource_type: str,
    ) -> None:
        """Validate clinical consistency and flag quarantine findings."""
        if not envelope.source_resource_id:
            envelope.disposition = "quarantined"
            envelope.quarantine_reasons.append("Missing required source resource ID")

        if resource_type == "Observation":
            pat_id = row.get("PATIENT_ID") or row.get("patient_id")
            if not pat_id:
                envelope.disposition = "quarantined"
                envelope.quarantine_reasons.append("Missing required PATIENT_ID reference")

            # Numeric value check (unless operation is delete)
            if envelope.operation != "delete":
                val = row.get("VALUE") or row.get("value")
                try:
                    float(str(val).replace(",", "."))
                except (ValueError, TypeError):
                    envelope.disposition = "quarantined"
                    envelope.quarantine_reasons.append(f"Malformed numeric VALUE: {val}")

            status = (row.get("STATUS") or row.get("status") or "").lower()
            if status in ["entered-in-error", "cancelled"]:
                envelope.disposition = "quarantined"
                envelope.quarantine_reasons.append(f"Invalid observation status: {status}")

        elif resource_type == "Encounter":
            pat_id = row.get("PATIENT_ID") or row.get("patient_id")
            if not pat_id:
                envelope.disposition = "quarantined"
                envelope.quarantine_reasons.append("Missing required encounter PATIENT_ID")

        elif resource_type == "Patient":
            b_date = row.get("BIRTH_DATE") or row.get("birth_date")
            if not b_date:
                envelope.disposition = "quarantined"
                envelope.quarantine_reasons.append("Missing required BIRTH_DATE")

    def to_canonical_patient(
        self,
        envelope: ResourceEnvelope,
        run_id: str,
        manifest_id: str,
    ) -> Optional[CanonicalPatient]:
        """Convert accepted Patient envelope to CanonicalPatient."""
        if envelope.disposition != "accepted" or envelope.resource_type != "Patient":
            return None
        row = envelope.payload or {}
        lineage = Lineage(
            manifest_id=manifest_id,
            run_id=run_id,
            resource_envelope_hash=envelope.payload_hash,
        )

        patient_key = envelope.source_resource_id
        b_date = row.get("BIRTH_DATE") or row.get("birth_date") or ""
        gender = (row.get("GENDER") or row.get("gender") or "unknown").lower()
        postal = row.get("POSTAL_CODE") or row.get("postal_code")

        supersession = "deleted" if envelope.operation == "delete" else "current"

        return CanonicalPatient(
            provider=self.provider,
            patient_key=patient_key,
            source_resource_id=envelope.source_resource_id,
            sequence=envelope.change_sequence,
            birth_date=b_date,
            gender=gender,
            postal_code=postal,
            deceased_boolean=False,
            lineage=lineage,
            supersession_state=supersession,
        )

    def to_canonical_encounter(
        self,
        envelope: ResourceEnvelope,
        run_id: str,
        manifest_id: str,
    ) -> Optional[CanonicalEncounter]:
        """Convert accepted Encounter envelope to CanonicalEncounter."""
        if envelope.disposition != "accepted" or envelope.resource_type != "Encounter":
            return None
        row = envelope.payload or {}
        lineage = Lineage(
            manifest_id=manifest_id,
            run_id=run_id,
            resource_envelope_hash=envelope.payload_hash,
        )

        pat_id = row.get("PATIENT_ID") or row.get("patient_id") or ""
        class_code = row.get("CLASS_CODE") or row.get("class_code") or "AMB"
        start_str = row.get("PERIOD_START") or row.get("period_start")
        end_str = row.get("PERIOD_END") or row.get("period_end")

        try:
            start_time = datetime.fromisoformat(start_str.replace("Z", "+00:00")) if start_str else envelope.event_time
        except Exception:
            start_time = envelope.event_time

        try:
            end_time = datetime.fromisoformat(end_str.replace("Z", "+00:00")) if end_str else None
        except Exception:
            end_time = None

        supersession = "deleted" if envelope.operation == "delete" else "current"

        return CanonicalEncounter(
            provider=self.provider,
            encounter_key=envelope.source_resource_id,
            patient_key=pat_id,
            source_resource_id=envelope.source_resource_id,
            sequence=envelope.change_sequence,
            class_code=class_code,
            period_start=start_time,
            period_end=end_time,
            status=row.get("STATUS") or "finished",
            lineage=lineage,
            supersession_state=supersession,
        )

    def to_canonical_observation(
        self,
        envelope: ResourceEnvelope,
        run_id: str,
        manifest_id: str,
        normalization_version: str = "1.0.0",
    ) -> Optional[CanonicalObservation]:
        """Convert accepted Observation envelope to CanonicalObservation with retraction handling."""
        if envelope.disposition != "accepted" or envelope.resource_type != "Observation":
            return None
        row = envelope.payload or {}
        lineage = Lineage(
            manifest_id=manifest_id,
            run_id=run_id,
            resource_envelope_hash=envelope.payload_hash,
        )

        pat_id = row.get("PATIENT_ID") or row.get("patient_id") or ""
        enc_id = row.get("ENCOUNTER_ID") or row.get("encounter_id")
        orig_code = row.get("LOCAL_CODE") or row.get("code") or "UNKNOWN"
        orig_text = row.get("DISPLAY_NAME") or row.get("display") or ""
        raw_val = str(row.get("VALUE") or row.get("value") or "")
        orig_unit = row.get("UNIT") or row.get("unit")
        status = row.get("STATUS") or row.get("status") or "final"

        orig_obs = OriginalObservation(
            original_code=orig_code,
            original_coding_system="http://clinic-b.org/codes",
            original_text=orig_text,
            original_value=raw_val,
            original_unit=orig_unit,
            original_status=status,
        )

        # Normalization
        norm_concept = None
        norm_val = None
        norm_unit = orig_unit
        mapping_ref = None

        if orig_code in REVIEWED_CATALOG:
            cat_entry = REVIEWED_CATALOG[orig_code]
            norm_concept = cat_entry["concept_id"]
            mapping_ref = f"reviewed-catalog/loinc-{orig_code}"
            try:
                sanitized_val = raw_val.replace(",", ".")
                norm_val = float(sanitized_val)
            except Exception:
                norm_val = None

        norm_obs = NormalizedObservation(
            normalized_concept=norm_concept,
            normalized_value=norm_val,
            normalized_unit=norm_unit,
        )

        # Section 13 Retraction Handling:
        # If operation is delete, supersession_state must be 'retracted'
        supersession = "deleted" if envelope.operation == "delete" else "current"

        return CanonicalObservation(
            provider=self.provider,
            patient_key=pat_id,
            encounter_key=enc_id,
            source_key=envelope.source_resource_id,
            sequence=envelope.change_sequence,
            effective_time=envelope.event_time,
            original=orig_obs,
            normalized=norm_obs,
            mapping_reference=mapping_ref,
            normalization_version=normalization_version,
            lineage=lineage,
            supersession_state=supersession,
        )
