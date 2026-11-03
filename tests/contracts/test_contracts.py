"""Contract and invariant unit tests for CareFlow Atlas."""

from datetime import datetime, timedelta
import pytest
from pydantic import ValidationError

from contracts import (
    Approval,
    ApprovalHashMismatchError,
    AuditEvent,
    CanonicalObservation,
    ConservationAccounting,
    ConservationViolationError,
    ContractViolationError,
    Lineage,
    MappingDSL,
    MappingProposal,
    NormalizedObservation,
    OriginalObservation,
    PipelineRun,
    ResourceEnvelope,
    SourceManifest,
    StaleApprovalError,
    StudyRelease,
    UnsupportedDSLOperationError,
)


class TestSourceManifestContract:
    def test_valid_manifest(self):
        start = datetime(2026, 11, 1, 0, 0, 0)
        end = datetime(2026, 11, 1, 23, 59, 59)
        manifest = SourceManifest(
            manifest_id="provider-a-batch-001",
            provider="provider-a",
            feed="fhir-r4",
            batch_id="batch-001",
            source_interval_start=start,
            source_interval_end=end,
            object_uri="s3://careflow-bronze/provider-a/batch-001.ndjson",
            object_hash="e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            object_bytes=1024,
            adapter_version="1.0.0",
            readiness="ready",
            disposition="accepted",
        )
        assert manifest.manifest_id == "provider-a-batch-001"
        assert manifest.readiness == "ready"

    def test_interval_start_after_end_fails(self):
        start = datetime(2026, 11, 2, 0, 0, 0)
        end = datetime(2026, 11, 1, 0, 0, 0)
        with pytest.raises(ValidationError):
            SourceManifest(
                manifest_id="bad-interval",
                provider="provider-a",
                feed="fhir-r4",
                batch_id="batch-bad",
                source_interval_start=start,
                source_interval_end=end,
                object_uri="s3://careflow-bronze/bad.ndjson",
                object_hash="abc",
                object_bytes=100,
                adapter_version="1.0.0",
            )

    def test_delivery_duplicate_and_conflict_check(self):
        t1 = datetime(2026, 11, 1, 0, 0, 0)
        t2 = datetime(2026, 11, 1, 1, 0, 0)
        m1 = SourceManifest(
            manifest_id="prov-a-b1",
            provider="provider-a",
            feed="fhir-r4",
            batch_id="b1",
            source_interval_start=t1,
            source_interval_end=t2,
            object_uri="s3://b1.ndjson",
            object_hash="hash-123",
            object_bytes=500,
            adapter_version="1.0.0",
        )
        m2_duplicate = SourceManifest(
            manifest_id="prov-a-b1-dup",
            provider="provider-a",
            feed="fhir-r4",
            batch_id="b1",
            source_interval_start=t1,
            source_interval_end=t2,
            object_uri="s3://b1-dup.ndjson",
            object_hash="hash-123",  # same hash
            object_bytes=500,
            adapter_version="1.0.0",
        )
        m3_conflict = SourceManifest(
            manifest_id="prov-a-b1-conflict",
            provider="provider-a",
            feed="fhir-r4",
            batch_id="b1",
            source_interval_start=t1,
            source_interval_end=t2,
            object_uri="s3://b1-conflict.ndjson",
            object_hash="hash-DIFFERENT",  # differing hash
            object_bytes=600,
            adapter_version="1.0.0",
        )
        assert m2_duplicate.check_delivery_conflict(m1) == "duplicate"
        assert m3_conflict.check_delivery_conflict(m1) == "conflict"


class TestConservationInvariant:
    def test_conservation_balanced(self):
        c = ConservationAccounting(
            n_input=100,
            n_accepted=80,
            n_duplicate=10,
            n_quarantined=8,
            n_intentionally_excluded=2,
        )
        assert c.n_input == 100

    def test_conservation_unbalanced_raises_error(self):
        with pytest.raises(ConservationViolationError):
            ConservationAccounting(
                n_input=100,
                n_accepted=70,
                n_duplicate=10,
                n_quarantined=5,
                n_intentionally_excluded=5,  # sums to 90, not 100
            )


class TestMappingProposalAndApprovalContracts:
    def test_proposal_deterministic_hash(self):
        dsl = MappingDSL(
            proposal_type="observation_mapping",
            provider_id="provider-b",
            source_contract_version="lab-feed-v2",
            match={"local_test_code": "LOCAL_LAB_017"},
            target_concept_ref="reviewed-catalog/concept-017",
            value_operation="parse_decimal",
            source_unit="mmol/L",
            target_unit="mmol/L",
            terminology_snapshot="catalog-v1",
            disposition="requires_review",
        )
        p1 = MappingProposal(
            proposal_id="prop-001",
            provider_id="provider-b",
            source_contract_version="lab-feed-v2",
            typed_dsl=dsl,
            terminology_snapshot="catalog-v1",
            prompt_version="v1.2",
            model_version="gemini-1.5-pro",
        )
        assert len(p1.proposal_hash) == 64

        # Same parameters produce identical hash
        p2 = MappingProposal(
            proposal_id="prop-002",
            provider_id="provider-b",
            source_contract_version="lab-feed-v2",
            typed_dsl=dsl,
            terminology_snapshot="catalog-v1",
            prompt_version="v1.2",
            model_version="gemini-1.5-pro",
        )
        assert p1.proposal_hash == p2.proposal_hash

    def test_disallowed_sql_in_mapping_dsl_rejected(self):
        with pytest.raises(UnsupportedDSLOperationError):
            MappingDSL(
                provider_id="provider-b",
                source_contract_version="v1",
                match={"code": "1; DROP TABLE users; --"},
                target_concept_ref="concept-x",
                terminology_snapshot="snap1",
            )

    def test_approval_validation_success_and_tamper_checks(self):
        now = datetime.utcnow()
        valid_expiry = now + timedelta(hours=24)
        expired_time = now - timedelta(hours=1)
        prop_hash = "a" * 64

        approval = Approval(
            proposal_hash=prop_hash,
            authorized_reviewer="reviewer@careflow.org",
            reviewer_role="terminology_reviewer",
            provider_scope="provider-b",
            evidence_version="ev-v1",
            expected_registry_version="reg-1.0.0",
            expiry=valid_expiry,
            decision="approved",
        )

        # 1. Valid execution passes
        approval.validate_for_execution(
            target_proposal_hash=prop_hash,
            current_registry_version="reg-1.0.0",
            current_time=now,
        )

        # 2. Hash mismatch rejected
        with pytest.raises(ApprovalHashMismatchError):
            approval.validate_for_execution(
                target_proposal_hash="b" * 64,
                current_registry_version="reg-1.0.0",
                current_time=now,
            )

        # 3. Stale registry version rejected
        with pytest.raises(StaleApprovalError):
            approval.validate_for_execution(
                target_proposal_hash=prop_hash,
                current_registry_version="reg-1.0.1",  # advanced
                current_time=now,
            )

        # 4. Expired approval rejected
        approval_expired = approval.model_copy(update={"expiry": expired_time})
        with pytest.raises(StaleApprovalError):
            approval_expired.validate_for_execution(
                target_proposal_hash=prop_hash,
                current_registry_version="reg-1.0.0",
                current_time=now,
            )


class TestAuditEventContract:
    def test_valid_payload_free_audit_event(self):
        evt = AuditEvent(
            actor_or_service="ingestion_service",
            action="ingest_batch",
            resource="s3://careflow-bronze/provider-a/batch-001.ndjson",
            scope="provider-a",
            outcome="success",
            correlation_id="trace-12345",
            metadata={"batch_bytes": 1024, "resource_count": 50},
        )
        assert evt.outcome == "success"

    def test_patient_payload_leak_raises_error(self):
        with pytest.raises(ContractViolationError):
            AuditEvent(
                actor_or_service="ingestion_service",
                action="ingest_batch",
                resource="s3://careflow-bronze/leak.ndjson",
                scope="provider-a",
                outcome="success",
                correlation_id="trace-12345",
                metadata={"mrn": "12345678", "patient_name": "John Doe"},
            )
