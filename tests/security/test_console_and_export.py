"""Security tests for Review Console and Controlled Export Service."""

import hashlib
from datetime import datetime, timedelta
import pytest
from contracts import MappingDSL, MappingProposal, StudyRelease
from services.console.review_console import ReviewConsole
from services.export.export_service import ControlledExportService


class TestConsoleAndExport:
    def test_review_console_dossier_and_signing(self):
        console = ReviewConsole()
        dsl = MappingDSL(
            provider_id="provider-a",
            source_contract_version="v1.1.0",
            match={"local_test_code": "LOCAL_LAB_017"},
            target_concept_ref="2345-7",
            value_operation="parse_decimal",
            terminology_snapshot="loinc_2026_01",
        )
        proposal = MappingProposal(
            proposal_id="prop-console-001",
            provider_id="provider-a",
            source_contract_version="v1.1.0",
            typed_dsl=dsl,
            terminology_snapshot="loinc_2026_01",
            prompt_version="v1.0",
            model_version="m1.0",
            evidence_refs=["schema_diff:v1.0->v1.1", "safe_profile:LOCAL_LAB_017"],
            safe_test_results={"dry_run_passed": True, "converted_count": 5},
        )

        dossier = console.render_proposal_dossier(proposal)
        assert "CLINICAL MAPPING PROPOSAL DOSSIER" in dossier
        assert proposal.proposal_hash in dossier
        assert "LOCAL_LAB_017" in dossier
        assert "parse_decimal" in dossier

        # Sign approval via console
        approval = console.sign_approval(
            proposal=proposal,
            reviewer_id="lead_pathologist@careflow.org",
            reviewer_role="lead_pathologist",
            provider_scope="provider-a",
            expected_registry_version="v1.0.0",
            notes="Clinically verified",
        )
        assert approval.proposal_hash == proposal.proposal_hash
        assert approval.decision == "approved"

    def test_controlled_export_service_packaging(self):
        export_service = ControlledExportService()

        # Mock an active study release
        file_content = b"Mock Parquet Cohort Bytes"
        file_hash = hashlib.sha256(file_content).hexdigest()
        file_key = "releases/test-export/cohort.parquet"

        # Upload mock file to MinIO
        export_service.s3_client.put_object(
            Bucket=export_service.gold_bucket,
            Key=file_key,
            Body=file_content,
        )

        release = StudyRelease(
            release_id="rel-export-001",
            study_id="study-cardiovascular",
            owner="cardio_lead",
            cohort_digest="cohort_digest_abc123",
            fixed_reference_date="2026-11-01",
            provider_table_snapshot_bundle={"provider-a": {"patients": 1, "observations": 2}},
            quality_privacy_versions={"privacy": "v1"},
            code_digest="code_digest_123",
            output_checksums={file_key: file_hash, "cohort_member_count": "100"},
            active_state="active",
        )

        pkg = export_service.prepare_export_package(
            release=release,
            recipient_id="harvard_medical_school",
            export_purpose="clinical_trial_reproducibility",
        )

        assert pkg["release_id"] == "rel-export-001"
        assert pkg["recipient_id"] == "harvard_medical_school"
        assert pkg["recipient_hmac_attestation"] != ""
        assert len(pkg["verified_files"]) == 1
        assert pkg["verified_files"][0]["sha256"] == file_hash
