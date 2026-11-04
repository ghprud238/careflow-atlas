"""Contract tests for landing protocol and manifest state transitions."""

import uuid
from datetime import datetime
import pytest
from contracts import ConflictingDeliveryError
from services.ingest.manifest_manager import ManifestManager


class TestManifestLandingProtocol:
    def setup_method(self):
        self.mgr = ManifestManager()

    def test_successful_landing_and_readiness(self):
        batch_id = f"test-{uuid.uuid4().hex[:8]}"
        payload = b'{"resourceType": "Patient", "id": "pat-test-01"}\n'
        start = datetime(2026, 11, 1, 0, 0, 0)
        end = datetime(2026, 11, 1, 1, 0, 0)

        manifest = self.mgr.stage_and_register_batch(
            provider="provider-a",
            feed="fhir-r4",
            batch_id=batch_id,
            payload_bytes=payload,
            source_interval_start=start,
            source_interval_end=end,
        )

        assert manifest.readiness == "ready"
        assert manifest.disposition == "accepted"
        assert manifest.object_bytes == len(payload)

        # Retrieve and verify download from MinIO
        downloaded = self.mgr.fetch_staged_payload(manifest.object_uri)
        assert downloaded == payload

    def test_duplicate_delivery_is_idempotent(self):
        batch_id = f"test-{uuid.uuid4().hex[:8]}"
        payload = b'{"resourceType": "Patient", "id": "pat-dup-01"}\n'
        start = datetime(2026, 11, 1, 0, 0, 0)
        end = datetime(2026, 11, 1, 1, 0, 0)

        # First delivery
        m1 = self.mgr.stage_and_register_batch(
            provider="provider-a",
            feed="fhir-r4",
            batch_id=batch_id,
            payload_bytes=payload,
            source_interval_start=start,
            source_interval_end=end,
        )
        assert m1.disposition == "accepted"

        # Re-delivery of identical payload
        m2 = self.mgr.stage_and_register_batch(
            provider="provider-a",
            feed="fhir-r4",
            batch_id=batch_id,
            payload_bytes=payload,
            source_interval_start=start,
            source_interval_end=end,
        )
        assert m2.disposition == "duplicate"
        assert m2.object_hash == m1.object_hash

    def test_conflicting_delivery_raises_error(self):
        batch_id = f"test-{uuid.uuid4().hex[:8]}"
        payload1 = b'{"resourceType": "Patient", "id": "pat-orig"}\n'
        payload2 = b'{"resourceType": "Patient", "id": "pat-MODIFIED"}\n'
        start = datetime(2026, 11, 1, 0, 0, 0)
        end = datetime(2026, 11, 1, 1, 0, 0)

        self.mgr.stage_and_register_batch(
            provider="provider-a",
            feed="fhir-r4",
            batch_id=batch_id,
            payload_bytes=payload1,
            source_interval_start=start,
            source_interval_end=end,
        )

        with pytest.raises(ConflictingDeliveryError):
            self.mgr.stage_and_register_batch(
                provider="provider-a",
                feed="fhir-r4",
                batch_id=batch_id,
                payload_bytes=payload2,  # different content
                source_interval_start=start,
                source_interval_end=end,
            )
