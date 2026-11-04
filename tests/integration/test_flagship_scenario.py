"""End-to-End Flagship Scenario Integration Test for Phase 2.

Scenario:
1. Incoming FHIR batch contains an unfamiliar lab observation LOCAL_LAB_017
   with comma representation "12,5" -> safely quarantined.
2. Initial cohort release (Release 1) generated without the quarantined observation.
3. Bounded Investigation Agent investigates via scoped MCP tools and generates typed MappingProposal.
4. Human reviewer verifies proposal artifact and authorizes exact proposal hash.
5. Replay Executor promotes mapping to v1.1.0 and executes targeted scoped replay.
6. Quarantined record is transformed, merged into Silver observations, and drained from quarantine.
7. Updated cohort release (Release 2) materialized; release comparison shows material update.
8. Replay executed again proves idempotency and duplicate safety.
"""

import json
import uuid
from datetime import datetime, timedelta
import pytest

from contracts import Approval, StaleApprovalError
from fixtures.generation.synthetic_provider_a import generate_provider_a_bundle
from packages.mapping_dsl.registry import MappingRegistry
from pipelines.spark.canonical_transform import CanonicalPipeline, get_spark_session
from pipelines.spark.cohort_job import CohortJob
from services.agent.investigation_agent import InvestigationAgent
from services.ingest.manifest_manager import ManifestManager
from services.mapping_executor.executor import ReplayExecutor
from services.mcp_tools.tool_server import ScopedMCPTools
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


class TestPhase2FlagshipScenario:
    def test_complete_flagship_investigation_and_scoped_replay(
        self, spark, pipeline, manifest_mgr
    ):
        provider_id = "provider-a"
        batch_id = f"batch-flagship-{uuid.uuid4().hex[:8]}"
        run_id_initial = f"run-init-{uuid.uuid4().hex[:8]}"

        # ---------------------------------------------------------
        # Step 1: Generate Bundle with Unfamiliar Lab Observation
        # ---------------------------------------------------------
        resources, meta = generate_provider_a_bundle(
            patient_count=10,
            seed=42,
            fixed_reference_date="2026-11-01",
            include_anomalies=False,
            include_duplicates=False,
            include_updates=False,
        )

        # Dedicated adult patient and encounter for flagship verification
        flagship_pat_id = "pat-flagship-001"
        flagship_enc_id = "enc-flagship-001"
        flagship_obs_id = "obs-flagship-LOCAL_LAB_017"

        resources.append({
            "resourceType": "Patient",
            "id": flagship_pat_id,
            "meta": {"versionId": "1", "lastUpdated": "2026-10-01T00:00:00Z"},
            "birthDate": "1980-05-15",
            "gender": "female",
            "address": [{"postalCode": "02138", "country": "US"}],
            "_change_sequence": 1,
            "_operation": "insert",
        })

        resources.append({
            "resourceType": "Encounter",
            "id": flagship_enc_id,
            "meta": {"versionId": "1", "lastUpdated": "2026-10-25T09:00:00Z"},
            "status": "finished",
            "class": {"code": "AMB", "display": "ambulatory"},
            "subject": {"reference": f"Patient/{flagship_pat_id}"},
            "period": {
                "start": "2026-10-25T09:00:00Z",
                "end": "2026-10-25T10:00:00Z",
            },
            "_change_sequence": 1,
            "_operation": "insert",
        })

        # Inject unfamiliar lab observation with comma decimal representation
        resources.append({
            "resourceType": "Observation",
            "id": flagship_obs_id,
            "meta": {"versionId": "1", "lastUpdated": "2026-10-25T10:00:00Z"},
            "status": "final",
            "code": {
                "coding": [
                    {
                        "system": "http://local-hospital.org/codes",
                        "code": "LOCAL_LAB_017",
                        "display": "Blood Glucose Fasting",
                    }
                ],
                "text": "Blood Glucose Fasting",
            },
            "subject": {"reference": f"Patient/{flagship_pat_id}"},
            "encounter": {"reference": f"Encounter/{flagship_enc_id}"},
            "effectiveDateTime": "2026-10-25T10:00:00Z",
            "valueQuantity": {
                "value": "12,5",
                "unit": "mg/dL",
            },
            "_change_sequence": 1,
            "_operation": "insert",
        })

        ndjson_bytes = ("\n".join(json.dumps(r) for r in resources) + "\n").encode("utf-8")

        # Stage and register Bronze manifest
        manifest = manifest_mgr.stage_and_register_batch(
            provider=provider_id,
            feed="fhir-r4",
            batch_id=batch_id,
            payload_bytes=ndjson_bytes,
            source_interval_start=datetime(2026, 10, 1, 0, 0, 0),
            source_interval_end=datetime(2026, 11, 1, 0, 0, 0),
            adapter_version="1.1.0",
        )
        assert manifest.readiness == "ready"

        # ---------------------------------------------------------
        # Step 2: Initial Pipeline Execution & Quarantine Verification
        # ---------------------------------------------------------
        run_res, accounting = pipeline.execute_manifest_ingest(manifest.manifest_id, run_id_initial)
        assert run_res.state == "completed_with_exceptions"
        assert accounting.n_quarantined == 1

        # Verify record is quarantined with exact findings
        q_records = spark.sql(
            f"SELECT * FROM careflow_catalog.careflow_quarantine.quarantined_records WHERE provider = '{provider_id}'"
        ).collect()
        assert len(q_records) == 1
        assert q_records[0]["source_resource_id"] == flagship_obs_id
        assert "LOCAL_LAB_017" in q_records[0]["findings"]
        assert "12,5" in q_records[0]["findings"]

        # Verify Silver observations does NOT yet have the quarantined record
        silv_matches = spark.sql(
            f"SELECT * FROM careflow_catalog.careflow_silver.observations WHERE source_key = '{flagship_obs_id}'"
        ).collect()
        assert len(silv_matches) == 0

        # Snapshot bundle for Release 1
        snap_bundle_1 = {
            provider_id: {
                "patients": int(spark.sql("SELECT snapshot_id FROM careflow_catalog.careflow_silver.patients.snapshots ORDER BY committed_at DESC LIMIT 1").collect()[0]["snapshot_id"]),
                "observations": int(spark.sql("SELECT snapshot_id FROM careflow_catalog.careflow_silver.observations.snapshots ORDER BY committed_at DESC LIMIT 1").collect()[0]["snapshot_id"]),
            }
        }

        # Materialize Release 1 (Baseline)
        cohort_job = CohortJob(spark=spark)
        release_1 = cohort_job.run_cohort_study(
            study_id="flagship_glucose_cohort",
            owner="research_lead@careflow.org",
            provider=provider_id,
            snapshot_bundle=snap_bundle_1,
            target_concept="reviewed-catalog/concept-glucose",
            fixed_reference_date="2026-11-01",
        )
        assert release_1.active_state == "active"

        # ---------------------------------------------------------
        # Step 3: Governed Agent Investigation via Scoped Tools
        # ---------------------------------------------------------
        scoped_tools = ScopedMCPTools()
        agent = InvestigationAgent(tools=scoped_tools, max_tool_budget=8)

        # Agent investigates target code LOCAL_LAB_017
        proposal = agent.investigate(
            provider_id=provider_id,
            target_code="LOCAL_LAB_017",
            source_contract_version="v1.1.0",
        )

        assert proposal.disposition == "requires_review"
        assert proposal.provider_id == provider_id
        assert proposal.typed_dsl.match["local_test_code"] == "LOCAL_LAB_017"
        assert proposal.typed_dsl.value_operation == "parse_decimal"
        assert proposal.typed_dsl.target_concept_ref == "2345-7"
        assert proposal.proposal_hash != ""
        assert agent.tool_call_count <= 8
        print(f"\n[Agent] Generated Proposal ID: {proposal.proposal_id}, Hash: {proposal.proposal_hash}")

        # ---------------------------------------------------------
        # Step 4: Human Review & Authorization Gate
        # ---------------------------------------------------------
        registry = MappingRegistry()
        current_reg_version = registry.get_latest_version()

        approval = Approval(
            proposal_hash=proposal.proposal_hash,
            authorized_reviewer="dr_chen_lead_pathologist@careflow.org",
            reviewer_role="terminology_reviewer",
            provider_scope=provider_id,
            evidence_version="v1.1.0",
            expected_registry_version=current_reg_version,
            expiry=datetime.utcnow() + timedelta(hours=24),
            decision="approved",
            notes="Reviewed and confirmed: LOCAL_LAB_017 corresponds to Fasting Blood Glucose (2345-7). Comma decimal format verified.",
        )

        # ---------------------------------------------------------
        # Step 5: Governed Promotion & Targeted Scoped Replay
        # ---------------------------------------------------------
        executor = ReplayExecutor(spark=spark)
        replay_summary = executor.execute_governed_promotion_and_replay(
            proposal=proposal,
            approval=approval,
            promoted_by=approval.authorized_reviewer,
        )

        assert replay_summary["promoted_version"] != current_reg_version
        assert replay_summary["replayed_count"] == 1
        assert flagship_obs_id in replay_summary["source_ids_replayed"]
        print(f"\n[Executor] Promoted version: {replay_summary['promoted_version']}, Replayed count: {replay_summary['replayed_count']}")

        # Verify record is now in Silver observations with canonical mapping and value
        replayed_silver = spark.sql(
            f"SELECT * FROM careflow_catalog.careflow_silver.observations WHERE source_key = '{flagship_obs_id}'"
        ).collect()
        assert len(replayed_silver) == 1
        obs_row = replayed_silver[0]
        assert obs_row["normalized_concept"] == "reviewed-catalog/concept-glucose"
        assert obs_row["normalized_value"] == 12.5
        assert obs_row["normalized_unit"] == "mg/dL"
        assert "mapping-registry" in obs_row["mapping_reference"]
        assert obs_row["normalization_version"] == replay_summary["promoted_version"]

        # Verify record was drained from quarantine
        remaining_q = spark.sql(
            f"SELECT * FROM careflow_catalog.careflow_quarantine.quarantined_records WHERE source_resource_id = '{flagship_obs_id}'"
        ).collect()
        assert len(remaining_q) == 0

        # ---------------------------------------------------------
        # Step 6: Materialize Release 2 & Manifest Comparison
        # ---------------------------------------------------------
        snap_bundle_2 = {
            provider_id: {
                "patients": snap_bundle_1[provider_id]["patients"],
                "observations": replay_summary["silver_observations_snapshot_id"],
            }
        }

        release_2 = cohort_job.run_cohort_study(
            study_id="flagship_glucose_cohort",
            owner="research_lead@careflow.org",
            provider=provider_id,
            snapshot_bundle=snap_bundle_2,
            target_concept="reviewed-catalog/concept-glucose",
            fixed_reference_date="2026-11-01",
        )

        release_mgr = ReleaseManager()
        comparison = release_mgr.compare_releases(release_1, release_2)

        assert comparison["comparison_state"] == "materially_updated"
        assert comparison["checksum_differences"]["digest_changed"] is True
        assert "observations" in comparison["snapshot_differences"]
        print(f"\n[Release Comparison] Comparison State: {comparison['comparison_state']}")
        print(f"[Release 1 Digest]: {release_1.cohort_digest}")
        print(f"[Release 2 Digest]: {release_2.cohort_digest}")

        # ---------------------------------------------------------
        # Step 7: Replay Idempotency & Security Invariant Verification
        # ---------------------------------------------------------
        # 1. Attempting to promote the same approval a second time must fail closed (stale expected version)
        with pytest.raises(StaleApprovalError):
            executor.execute_governed_promotion_and_replay(
                proposal=proposal,
                approval=approval,
                promoted_by=approval.authorized_reviewer,
            )

        # 2. Running scoped replay directly for the promoted rule finds 0 remaining records
        second_replay = executor.execute_scoped_replay(
            provider=provider_id,
            target_code="LOCAL_LAB_017",
            dsl=proposal.typed_dsl,
            version=replay_summary["promoted_version"],
        )
        assert second_replay["replayed_count"] == 0

        # 3. Silver count remains unchanged (exactly 1 row, zero duplicates)
        final_silv_count = spark.sql(
            f"SELECT COUNT(*) as c FROM careflow_catalog.careflow_silver.observations WHERE source_key = '{flagship_obs_id}'"
        ).collect()[0]["c"]
        assert final_silv_count == 1
