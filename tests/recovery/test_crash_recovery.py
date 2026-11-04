"""Recovery and Resilience tests for CareFlow Atlas.

Verifies:
1. Crash recovery: interrupted or restarted batch execution converges without duplicate rows.
2. Replay idempotency: re-running identical ready manifests produces identical table counts.
3. Conflict denial: conflicting payloads delivered with duplicate batch IDs fail closed.
"""

import json
import uuid
from datetime import datetime
import pytest
from contracts import ConflictingDeliveryError
from fixtures.generation.synthetic_provider_a import generate_provider_a_bundle
from pipelines.spark.canonical_transform import CanonicalPipeline, get_spark_session
from services.ingest.manifest_manager import ManifestManager


@pytest.fixture(scope="module")
def spark():
    return get_spark_session()


@pytest.fixture(scope="module")
def pipeline(spark):
    return CanonicalPipeline(spark)


@pytest.fixture(scope="module")
def manifest_mgr():
    return ManifestManager()


class TestCrashRecoveryAndResilience:
    def test_idempotent_recovery_after_restarted_batch(self, spark, pipeline, manifest_mgr):
        provider = "provider-a"
        batch_id = f"batch-recov-{uuid.uuid4().hex[:8]}"
        run_id_initial = f"run-init-{uuid.uuid4().hex[:8]}"
        run_id_recovery = f"run-recov-{uuid.uuid4().hex[:8]}"

        # 1. Generate Batch
        resources, meta = generate_provider_a_bundle(
            patient_count=12,
            seed=505,
            fixed_reference_date="2026-11-01",
            include_anomalies=False,
            include_duplicates=False,
            include_updates=False,
        )
        # Prefix resources with unique test run identifier to isolate from previous test artifacts
        prefix = f"recov-{uuid.uuid4().hex[:6]}"
        for r in resources:
            if r.get("id"):
                r["id"] = f"{prefix}-{r['id']}"
            if r.get("subject", {}).get("reference", "").startswith("Patient/"):
                r["subject"]["reference"] = f"Patient/{prefix}-{r['subject']['reference'][8:]}"

        ndjson_bytes = ("\n".join(json.dumps(r) for r in resources) + "\n").encode("utf-8")

        # 2. Stage to Bronze
        manifest = manifest_mgr.stage_and_register_batch(
            provider=provider,
            feed="fhir-r4",
            batch_id=batch_id,
            payload_bytes=ndjson_bytes,
            source_interval_start=datetime(2026, 10, 1, 0, 0, 0),
            source_interval_end=datetime(2026, 11, 1, 0, 0, 0),
            adapter_version="1.0.0",
        )

        # 3. Initial Run
        run_1, acc_1 = pipeline.execute_manifest_ingest(manifest.manifest_id, run_id_initial)
        assert run_1.state in ["completed", "completed_with_exceptions"]

        pat_count_1 = spark.sql(f"SELECT COUNT(*) as c FROM careflow_catalog.careflow_silver.patients WHERE provider = '{provider}' AND manifest_id = '{manifest.manifest_id}'").collect()[0]["c"]
        obs_count_1 = spark.sql(f"SELECT COUNT(*) as c FROM careflow_catalog.careflow_silver.observations WHERE provider = '{provider}' AND manifest_id = '{manifest.manifest_id}'").collect()[0]["c"]
        assert pat_count_1 == 12
        assert obs_count_1 > 0

        # 4. Simulated Recovery Run (Replaying exact same manifest)
        run_2, acc_2 = pipeline.execute_manifest_ingest(manifest.manifest_id, run_id_recovery)
        assert run_2.state in ["completed", "completed_with_exceptions"]

        # 5. Verify Zero Duplicate Logical Rows
        pat_count_2 = spark.sql(f"SELECT COUNT(*) as c FROM careflow_catalog.careflow_silver.patients WHERE provider = '{provider}' AND manifest_id = '{manifest.manifest_id}'").collect()[0]["c"]
        obs_count_2 = spark.sql(f"SELECT COUNT(*) as c FROM careflow_catalog.careflow_silver.observations WHERE provider = '{provider}' AND manifest_id = '{manifest.manifest_id}'").collect()[0]["c"]

        assert pat_count_2 == pat_count_1, f"Patients duplicated: {pat_count_2} != {pat_count_1}"
        assert obs_count_2 == obs_count_1, f"Observations duplicated: {obs_count_2} != {obs_count_1}"

    def test_conflicting_content_delivery_rejected(self, manifest_mgr):
        """A batch delivered with an existing batch ID but differing checksum must fail closed."""
        provider = "provider-a"
        batch_id = f"batch-conflict-{uuid.uuid4().hex[:8]}"

        payload_1 = b'{"resourceType": "Patient", "id": "p1"}\n'
        payload_2 = b'{"resourceType": "Patient", "id": "p1-MODIFIED"}\n'

        # Initial delivery
        m1 = manifest_mgr.stage_and_register_batch(
            provider=provider,
            feed="fhir-r4",
            batch_id=batch_id,
            payload_bytes=payload_1,
            source_interval_start=datetime(2026, 10, 1, 0, 0, 0),
            source_interval_end=datetime(2026, 11, 1, 0, 0, 0),
            adapter_version="1.0.0",
        )
        assert m1.disposition == "accepted"

        # Conflicting delivery with same batch ID but different bytes
        with pytest.raises(ConflictingDeliveryError):
            manifest_mgr.stage_and_register_batch(
                provider=provider,
                feed="fhir-r4",
                batch_id=batch_id,
                payload_bytes=payload_2,
                source_interval_start=datetime(2026, 10, 1, 0, 0, 0),
                source_interval_end=datetime(2026, 11, 1, 0, 0, 0),
                adapter_version="1.0.0",
            )
