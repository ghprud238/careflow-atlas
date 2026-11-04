"""Integration test for Phase 3: Multiprovider Federation, Schema Evolution, and Retractions.

Scenario:
1. Ingests heterogeneous data feeds: Provider A (FHIR R4 JSON) and Provider B (Delimited CSV/Pipe).
2. Verifies partition isolation across provider namespaces in Iceberg Silver layer.
3. Tests monotonic sequence updates: higher sequence update overwrites earlier sequence.
4. Tests clinical retractions/deletions: _operation = 'delete' sets supersession_state = 'deleted'.
5. Materializes federated research cohort across both providers; verifies retracted records are excluded.
6. Evaluates schema drift detection on evolved provider feed.
"""

import json
import uuid
from datetime import datetime, timedelta
import pytest
from pyspark.sql import functions as F

from fixtures.generation.synthetic_provider_a import generate_provider_a_bundle
from fixtures.generation.synthetic_provider_b import generate_provider_b_tables
from pipelines.spark.canonical_transform import CanonicalPipeline, get_spark_session
from pipelines.spark.cohort_job import CohortJob
from services.drift.drift_detector import DriftDetector
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


class TestPhase3MultiproviderEvolution:
    def test_multiprovider_federation_updates_retractions_and_drift(
        self, spark, pipeline, manifest_mgr
    ):
        # ---------------------------------------------------------
        # Step 1: Ingest Provider A (FHIR R4 JSON)
        # ---------------------------------------------------------
        prov_a = "provider-a"
        batch_id_a = f"batch-p3-a-{uuid.uuid4().hex[:8]}"
        run_id_a = f"run-p3-a-{uuid.uuid4().hex[:8]}"

        res_a, meta_a = generate_provider_a_bundle(
            patient_count=10,
            seed=101,
            fixed_reference_date="2026-11-01",
            include_anomalies=False,
            include_duplicates=False,
            include_updates=False,
        )
        ndjson_bytes_a = ("\n".join(json.dumps(r) for r in res_a) + "\n").encode("utf-8")

        manifest_a = manifest_mgr.stage_and_register_batch(
            provider=prov_a,
            feed="fhir-r4",
            batch_id=batch_id_a,
            payload_bytes=ndjson_bytes_a,
            source_interval_start=datetime(2026, 10, 1, 0, 0, 0),
            source_interval_end=datetime(2026, 11, 1, 0, 0, 0),
            adapter_version="1.0.0",
        )
        run_res_a, accounting_a = pipeline.execute_manifest_ingest(manifest_a.manifest_id, run_id_a)
        assert run_res_a.state == "completed"
        assert accounting_a.n_accepted == len(res_a)

        # ---------------------------------------------------------
        # Step 2: Ingest Provider B (Delimited CSV/Pipe) with Updates & Retractions
        # ---------------------------------------------------------
        prov_b = "provider-b"
        batch_id_b = f"batch-p3-b-{uuid.uuid4().hex[:8]}"
        run_id_b = f"run-p3-b-{uuid.uuid4().hex[:8]}"

        tables_b = generate_provider_b_tables(
            patient_count=10,
            seed=202,
            fixed_reference_date="2026-11-01",
            include_retraction=True,  # Retracts pb-obs-000002 with seq=2
            include_update=True,      # Updates pb-obs-000001 with seq=2
            include_anomalies=True,   # 2 quarantined anomalies
        )
        payload_bytes_b = json.dumps(tables_b).encode("utf-8")

        manifest_b = manifest_mgr.stage_and_register_batch(
            provider=prov_b,
            feed="csv",
            batch_id=batch_id_b,
            payload_bytes=payload_bytes_b,
            source_interval_start=datetime(2026, 10, 1, 0, 0, 0),
            source_interval_end=datetime(2026, 11, 1, 0, 0, 0),
            adapter_version="1.0.0",
        )
        run_res_b, accounting_b = pipeline.execute_manifest_ingest(manifest_b.manifest_id, run_id_b)
        assert run_res_b.state == "completed_with_exceptions"
        assert accounting_b.n_quarantined == 2  # exactly the 2 injected anomalies

        # ---------------------------------------------------------
        # Step 3: Partition Isolation Verification
        # ---------------------------------------------------------
        pat_a_count = spark.sql(f"SELECT COUNT(*) as c FROM careflow_catalog.careflow_silver.patients WHERE provider = '{prov_a}'").collect()[0]["c"]
        pat_b_count = spark.sql(f"SELECT COUNT(*) as c FROM careflow_catalog.careflow_silver.patients WHERE provider = '{prov_b}'").collect()[0]["c"]
        assert pat_a_count == 10
        assert pat_b_count == 10

        obs_a_count = spark.sql(f"SELECT COUNT(*) as c FROM careflow_catalog.careflow_silver.observations WHERE provider = '{prov_a}'").collect()[0]["c"]
        obs_b_count = spark.sql(f"SELECT COUNT(*) as c FROM careflow_catalog.careflow_silver.observations WHERE provider = '{prov_b}'").collect()[0]["c"]
        assert obs_a_count > 0
        assert obs_b_count > 0

        # Verify partitions are isolated
        total_pats = spark.sql("SELECT COUNT(*) as c FROM careflow_catalog.careflow_silver.patients").collect()[0]["c"]
        assert total_pats == pat_a_count + pat_b_count

        # ---------------------------------------------------------
        # Step 4: Monotonic Clinical Updates & Retractions Verification
        # ---------------------------------------------------------
        # 4a. Verify Clinical Update: pb-obs-000001 converged to sequence 2
        updated_obs = spark.sql(
            f"SELECT sequence, supersession_state FROM careflow_catalog.careflow_silver.observations WHERE provider = '{prov_b}' AND source_key = 'pb-obs-000001'"
        ).collect()
        assert len(updated_obs) == 1
        assert updated_obs[0]["sequence"] == 2
        assert updated_obs[0]["supersession_state"] == "current"

        # 4b. Verify Clinical Retraction (Section 13): pb-obs-000002 has supersession_state = 'deleted'
        retracted_obs = spark.sql(
            f"SELECT sequence, supersession_state FROM careflow_catalog.careflow_silver.observations WHERE provider = '{prov_b}' AND source_key = 'pb-obs-000002'"
        ).collect()
        assert len(retracted_obs) == 1
        assert retracted_obs[0]["sequence"] == 2
        assert retracted_obs[0]["supersession_state"] == "deleted"

        # ---------------------------------------------------------
        # Step 5: Multi-Provider Federated Cohort Materialization
        # ---------------------------------------------------------
        snaps_a_pat = int(spark.sql(f"SELECT snapshot_id FROM careflow_catalog.careflow_silver.patients.snapshots ORDER BY committed_at DESC LIMIT 1").collect()[0]["snapshot_id"])
        snaps_a_obs = int(spark.sql(f"SELECT snapshot_id FROM careflow_catalog.careflow_silver.observations.snapshots ORDER BY committed_at DESC LIMIT 1").collect()[0]["snapshot_id"])

        fed_snapshot_bundle = {
            prov_a: {"patients": snaps_a_pat, "observations": snaps_a_obs},
            prov_b: {"patients": snaps_a_pat, "observations": snaps_a_obs},
        }

        cohort_job = CohortJob(spark=spark)
        release = cohort_job.run_cohort_study(
            study_id="federated_glucose_study",
            owner="consortium_lead@careflow.org",
            snapshot_bundle=fed_snapshot_bundle,
            target_concept="reviewed-catalog/concept-glucose",
            fixed_reference_date="2026-11-01",
        )

        assert release.active_state == "active"
        assert int(release.output_checksums.get("cohort_member_count", "0")) > 0

        # Read back the materialized parquet dataset via pyarrow from MinIO Gold lake
        import io
        import pyarrow.parquet as pq

        obj = cohort_job.s3_client.get_object(
            Bucket=cohort_job.gold_bucket,
            Key=f"releases/{release.release_id}/cohort_dataset.parquet",
        )
        gold_table = pq.read_table(io.BytesIO(obj["Body"].read()))
        found_providers = set(gold_table.column("provider").to_pylist())
        assert prov_a in found_providers
        assert prov_b in found_providers

        # Verify that the retracted observation is NOT present in the research cohort
        obs_ids = gold_table.column("observation_id").to_pylist()
        assert "pb-obs-000002" not in obs_ids

        # ---------------------------------------------------------
        # Step 6: Schema Drift Detection
        # ---------------------------------------------------------
        drift_detector = DriftDetector()

        # Normal header
        normal_headers = ["OBS_ID", "PATIENT_ID", "ENCOUNTER_ID", "LOCAL_CODE", "DISPLAY_NAME", "VALUE", "UNIT", "EFFECTIVE_TIME", "STATUS", "_CHANGE_SEQUENCE", "_OPERATION"]
        report_normal = drift_detector.evaluate_headers(prov_b, "observations.csv", normal_headers)
        assert report_normal.severity == "none"
        assert report_normal.recommended_action == "proceed"

        # Drifted header with unexpected column
        drifted_headers = normal_headers + ["UNEXPECTED_TECHNICIAN_MEMO"]
        report_drift = drift_detector.evaluate_headers(prov_b, "observations.csv", drifted_headers)
        assert report_drift.severity == "warning"
        assert report_drift.recommended_action == "investigate_agent"
        assert "UNEXPECTED_TECHNICIAN_MEMO" in report_drift.unexpected_added_columns
