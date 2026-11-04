"""Security tests for Cryptographic Audit Ledger and Tamper Detection."""

import uuid
import psycopg2
import pytest
from contracts import AuditEvent, ContractViolationError
from services.audit.audit_service import AuditService


class TestAuditLedger:
    def setup_method(self):
        self.audit = AuditService()

    def test_chained_audit_logging_and_integrity_verification(self):
        corr_id = f"corr-{uuid.uuid4().hex[:8]}"

        event_1 = AuditEvent(
            actor_or_service="manifest_manager",
            action="manifest_landed",
            resource="careflow-bronze/provider-a/batch-001.ndjson",
            scope="provider-a",
            outcome="success",
            correlation_id=corr_id,
            metadata={"byte_count": 54321, "sha256": "abcdef1234567890"},
        )
        entry_1 = self.audit.log_event(event_1)
        assert entry_1["entry_hash"] != ""

        event_2 = AuditEvent(
            actor_or_service="pipeline_executor",
            action="pipeline_started",
            resource="careflow_catalog.careflow_silver",
            scope="provider-a",
            outcome="success",
            correlation_id=corr_id,
            metadata={"run_id": "run-001"},
        )
        entry_2 = self.audit.log_event(event_2)
        assert entry_2["previous_hash"] == entry_1["entry_hash"]

        # Verify chain integrity
        is_valid, violations = self.audit.verify_chain_integrity()
        assert is_valid is True
        assert len(violations) == 0

    def test_payload_free_privacy_invariant(self):
        """AuditEvent contract must strictly reject raw patient payloads or PHI."""
        with pytest.raises(ContractViolationError):
            AuditEvent(
                actor_or_service="test_actor",
                action="leak_test",
                resource="test_resource",
                scope="provider-a",
                outcome="success",
                correlation_id="corr-123",
                metadata={"patient_name": "John Doe", "ssn": "000-00-0000"},
            )

    def test_tamper_detection(self):
        """Direct database tampering must be detected by verify_chain_integrity."""
        corr_id = f"corr-tamper-{uuid.uuid4().hex[:8]}"
        event = AuditEvent(
            actor_or_service="test_actor",
            action="critical_action",
            resource="test_resource",
            scope="provider-a",
            outcome="success",
            correlation_id=corr_id,
            metadata={"original_value": "authentic"},
        )
        entry = self.audit.log_event(event)
        seq_num = entry["sequence_num"]

        # Directly tamper with details in PostgreSQL
        with self.audit._get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE careflow_audit.chained_audit_ledger
                    SET details = '{"original_value": "TAMPERED_CONTENT"}'::jsonb
                    WHERE sequence_num = %s
                    """,
                    (seq_num,),
                )
                conn.commit()

        # Integrity verification must detect the tamper
        is_valid, violations = self.audit.verify_chain_integrity()
        assert is_valid is False
        assert any(f"Tamper detected at sequence {seq_num}" in v for v in violations)

        # Restore entry to keep test environment clean
        with self.audit._get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE careflow_audit.chained_audit_ledger
                    SET details = '{"original_value": "authentic"}'::jsonb
                    WHERE sequence_num = %s
                    """,
                    (seq_num,),
                )
                conn.commit()
