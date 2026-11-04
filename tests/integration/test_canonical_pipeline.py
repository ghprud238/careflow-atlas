"""Integration test for Phase 1: Single Provider Vertical Slice & Duplicate-Safe Replay."""

import json
import uuid
from datetime import datetime, timedelta
import pytest
from pyspark.sql import functions as F

from fixtures.generation.synthetic_provider_a import generate_provider_a_bundle
from pipelines.spark.canonical_transform import CanonicalPipeline, get_spark_session
from pipelines.spark.cohort_job import CohortJob
from services.ingest.manifest_manager import ManifestManager
from services.release_service.release_manager import ReleaseManager


@pytest.fixture(scope="module")
def spark():
    s = get_spark_session()
    yield s


@pytest.fixture(scope="module")
def pipeline(spark):
    spark.sql("DROP TABLE IF EXISTS careflow_catalog.careflow_silver.patients")
    spark.sql("DROP TABLE IF EXISTS careflow_catalog.careflow_silver.encounters")
    spark.sql("DROP TABLE IF EXISTS careflow_catalog.careflow_silver.observations")
    spark.sql("DROP TABLE IF EXISTS careflow_catalog.careflow_quarantine.quarantined_records")
    return CanonicalPipeline(spark)


@pytest.fixture(scope="module")
def manifest_mgr():
    return ManifestManager()


class TestPhase1SingleProviderPipeline:
    def test_full_slice_ingest_and_duplicate_safe_replay(self, spark, pipeline, manifest_mgr):
        batch_id = f"batch-slice-{uuid.uuid4().hex[:8]}"
        run_id_1 = f"run-initial-{uuid.uuid4().hex[:8]}"
        run_id_2 = f"run-replay-{uuid.uuid4().hex[:8]}"

        # 1. Generate Synthetic Bundle for Provider A (with deliberate duplicates and anomalies)
        resources, meta = generate_provider_a_bundle(
            patient_count=15,
            seed=101,
            fixed_reference_date="2026-11-01",
            include_anomalies=True,
            include_duplicates=True,
            include_updates=True,
        )
        ndjson_bytes = ("\n".join(json.dumps(r) for r in resources) + "\n").encode("utf-8")

        # 2. Stage to Bronze & Register Ready Manifest
        manifest = manifest_mgr.stage_and_register_batch(
            provider="provider-a",
            feed="fhir-r4",
            batch_id=batch_id,
            payload_bytes=ndjson_bytes,
            source_interval_start=datetime(2026, 10, 1, 0, 0, 0),
            source_interval_end=datetime(2026, 11, 1, 0, 0, 0),
            adapter_version="1.0.0",
        )
        assert manifest.readiness == "ready"

        # 3. First Batch Execution (Initial Ingest)
        run_1, accounting_1 = pipeline.execute_manifest_ingest(manifest.manifest_id, run_id_1)
        assert run_1.state in ["completed", "completed_with_exceptions"]
        assert accounting_1.n_input == meta["total_resources"]
        assert accounting_1.n_quarantined == 2  # exactly the 2 injected anomalies

        # Count Logical Current Rows after initial ingest
        pat_count_1 = spark.sql("SELECT COUNT(*) as c FROM careflow_catalog.careflow_silver.patients WHERE provider = 'provider-a'").collect()[0]["c"]
        enc_count_1 = spark.sql("SELECT COUNT(*) as c FROM careflow_catalog.careflow_silver.encounters WHERE provider = 'provider-a'").collect()[0]["c"]
        obs_count_1 = spark.sql("SELECT COUNT(*) as c FROM careflow_catalog.careflow_silver.observations WHERE provider = 'provider-a'").collect()[0]["c"]

        assert pat_count_1 == 15, f"Expected 15 patients, found {pat_count_1}"
        assert enc_count_1 > 0
        assert obs_count_1 > 0
        print(f"\n[Run 1] Patients: {pat_count_1}, Encounters: {enc_count_1}, Observations: {obs_count_1}")

        # Check committed snapshot IDs
        snaps_run_1 = run_1.committed_snapshots
        assert "patients" in snaps_run_1 and "observations" in snaps_run_1 and "encounters" in snaps_run_1

        # 4. Duplicate-Safe Replay (FR-09)
        # Re-running ingestion over the exact same manifest must NOT create duplicate logical rows
        run_2, accounting_2 = pipeline.execute_manifest_ingest(manifest.manifest_id, run_id_2)

        pat_count_2 = spark.sql("SELECT COUNT(*) as c FROM careflow_catalog.careflow_silver.patients WHERE provider = 'provider-a'").collect()[0]["c"]
        enc_count_2 = spark.sql("SELECT COUNT(*) as c FROM careflow_catalog.careflow_silver.encounters WHERE provider = 'provider-a'").collect()[0]["c"]
        obs_count_2 = spark.sql("SELECT COUNT(*) as c FROM careflow_catalog.careflow_silver.observations WHERE provider = 'provider-a'").collect()[0]["c"]

        print(f"[Run 2 - Replay] Patients: {pat_count_2}, Encounters: {enc_count_2}, Observations: {obs_count_2}")
        assert pat_count_2 == pat_count_1, "Replay created extra logical patient rows!"
        assert enc_count_2 == enc_count_1, "Replay created extra logical encounter rows!"
        assert obs_count_2 == obs_count_1, "Replay created extra logical observation rows!"

        # 5. Verify In-Place Updates (FR-06)
        # obs-000002 had sequence=2 injected: verify current sequence is 2
        updated_obs = spark.sql("SELECT sequence, normalized_value FROM careflow_catalog.careflow_silver.observations WHERE source_key = 'obs-000002'").collect()
        assert len(updated_obs) == 1, "Expected single current row for obs-000002"
        assert updated_obs[0]["sequence"] == 2, "obs-000002 sequence was not updated to 2"

        # 6. Research Cohort Release Job (FR-10, FR-12)
        snapshot_bundle = {"provider-a": snaps_run_1}
        cohort_job = CohortJob(spark)
        release = cohort_job.run_cohort_study(
            study_id="study-diabetes-screening",
            owner="lead_clinical_researcher",
            provider="provider-a",
            snapshot_bundle=snapshot_bundle,
            target_concept="reviewed-catalog/concept-glucose",
            fixed_reference_date="2026-11-01",
            lookback_days=365,
            min_age=18,
        )

        assert release.active_state == "active"
        assert len(release.cohort_digest) == 64
        assert int(release.output_checksums["cohort_member_count"]) > 0
        print(f"\n[Cohort Release] Release ID: {release.release_id}, Members: {release.output_checksums['cohort_member_count']}")

        # 7. Release Revocation Test (Section 13)
        release_mgr = ReleaseManager()
        fetched = release_mgr.get_release(release.release_id)
        assert fetched is not None
        assert fetched.active_state == "active"

        revoked = release_mgr.revoke_release(release.release_id, reason="Study protocol amendment")
        assert revoked is True
        fetched_after = release_mgr.get_release(release.release_id)
        assert fetched_after.active_state == "revoked"
        assert fetched_after.revocation_reason == "Study protocol amendment"
