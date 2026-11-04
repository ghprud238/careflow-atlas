"""Unit tests for Mapping DSL Interpreter and Versioned Registry."""

from datetime import datetime, timedelta
import pytest
from contracts import (
    Approval,
    ApprovalHashMismatchError,
    MappingDSL,
    MappingProposal,
    StaleApprovalError,
    UnsupportedDSLOperationError,
)
from packages.mapping_dsl.interpreter import DSLInterpreter
from packages.mapping_dsl.registry import MappingRegistry


def test_dsl_interpreter_parse_decimal_and_code():
    interpreter = DSLInterpreter()
    dsl = MappingDSL(
        provider_id="provider_a",
        source_contract_version="v1.0.0",
        match={"local_test_code": "LOCAL_LAB_017"},
        target_concept_ref="2339-0",  # Glucose [Mass/volume] in Blood
        value_operation="parse_decimal",
        source_unit="mg/dL",
        target_unit="mg/dL",
        terminology_snapshot="loinc_2026_01",
    )

    matched, norm_code, norm_val, norm_unit, err = interpreter.apply_observation_rule(
        raw_code="LOCAL_LAB_017",
        raw_val_str=" 12,5 ",
        raw_unit="mg/dL",
        dsl=dsl,
    )
    assert matched is True
    assert norm_code == "2339-0"
    assert norm_val == 12.5
    assert norm_unit == "mg/dL"
    assert err is None


def test_dsl_interpreter_unit_conversion():
    interpreter = DSLInterpreter()
    dsl = MappingDSL(
        provider_id="provider_a",
        source_contract_version="v1.0.0",
        match={"local_test_code": "LOCAL_LAB_017"},
        target_concept_ref="2339-0",
        value_operation="parse_decimal",
        source_unit="mmol/L",
        target_unit="mg/dL",
        terminology_snapshot="loinc_2026_01",
    )

    matched, norm_code, norm_val, norm_unit, err = interpreter.apply_observation_rule(
        raw_code="LOCAL_LAB_017",
        raw_val_str="5.0",
        raw_unit="mmol/L",
        dsl=dsl,
    )
    assert matched is True
    assert norm_code == "2339-0"
    assert norm_val == pytest.approx(5.0 * 18.0182, 0.01)
    assert norm_unit == "mg/dL"
    assert err is None


def test_dsl_injection_denial():
    with pytest.raises(UnsupportedDSLOperationError):
        MappingDSL(
            provider_id="provider_a",
            source_contract_version="v1.0.0",
            match={"local_test_code": "SELECT * FROM users"},
            target_concept_ref="2339-0",
            value_operation="parse_decimal",
            terminology_snapshot="loinc_2026_01",
        )


def test_mapping_registry_promotion_and_versioning():
    registry = MappingRegistry()
    initial_ver = registry.get_latest_version()
    assert initial_ver.startswith("v1.")

    dsl = MappingDSL(
        provider_id="provider_a",
        source_contract_version="v1.0.0",
        match={"local_test_code": "LOCAL_LAB_017"},
        target_concept_ref="2339-0",
        value_operation="parse_decimal",
        source_unit="mg/dL",
        target_unit="mg/dL",
        terminology_snapshot="loinc_2026_01",
    )

    proposal = MappingProposal(
        proposal_id="prop-test-001",
        provider_id="provider_a",
        source_contract_version="v1.0.0",
        typed_dsl=dsl,
        terminology_snapshot="loinc_2026_01",
        prompt_version="investigation_v1.0",
        model_version="model-v1.0",
    )

    # Valid approval
    approval = Approval(
        proposal_hash=proposal.proposal_hash,
        authorized_reviewer="reviewer@example.com",
        reviewer_role="terminology_reviewer",
        provider_scope="provider_a",
        evidence_version="v1.0",
        expected_registry_version=initial_ver,
        expiry=datetime.utcnow() + timedelta(hours=24),
        decision="approved",
    )

    new_version = registry.promote_proposal(proposal, approval, promoted_by="reviewer@example.com")
    assert new_version != initial_ver

    # Verify retrieval
    rule = registry.get_rule("provider_a", "LOCAL_LAB_017")
    assert rule is not None
    assert rule.target_concept_ref == "2339-0"


def test_mapping_registry_rejects_hash_mismatch():
    registry = MappingRegistry()
    current_ver = registry.get_latest_version()

    dsl = MappingDSL(
        provider_id="provider_a",
        source_contract_version="v1.0.0",
        match={"local_test_code": "LOCAL_LAB_099"},
        target_concept_ref="2339-0",
        value_operation="parse_decimal",
        terminology_snapshot="loinc_2026_01",
    )

    proposal = MappingProposal(
        proposal_id="prop-test-099",
        provider_id="provider_a",
        source_contract_version="v1.0.0",
        typed_dsl=dsl,
        terminology_snapshot="loinc_2026_01",
        prompt_version="investigation_v1.0",
        model_version="model-v1.0",
    )

    tampered_approval = Approval(
        proposal_hash="tampered_hash_1234567890",
        authorized_reviewer="reviewer@example.com",
        reviewer_role="terminology_reviewer",
        provider_scope="provider_a",
        evidence_version="v1.0",
        expected_registry_version=current_ver,
        expiry=datetime.utcnow() + timedelta(hours=24),
        decision="approved",
    )

    with pytest.raises(ApprovalHashMismatchError):
        registry.promote_proposal(proposal, tampered_approval)


def test_mapping_registry_rejects_expired_approval():
    registry = MappingRegistry()
    current_ver = registry.get_latest_version()

    dsl = MappingDSL(
        provider_id="provider_a",
        source_contract_version="v1.0.0",
        match={"local_test_code": "LOCAL_LAB_088"},
        target_concept_ref="2339-0",
        value_operation="parse_decimal",
        terminology_snapshot="loinc_2026_01",
    )

    proposal = MappingProposal(
        proposal_id="prop-test-088",
        provider_id="provider_a",
        source_contract_version="v1.0.0",
        typed_dsl=dsl,
        terminology_snapshot="loinc_2026_01",
        prompt_version="investigation_v1.0",
        model_version="model-v1.0",
    )

    expired_approval = Approval(
        proposal_hash=proposal.proposal_hash,
        authorized_reviewer="reviewer@example.com",
        reviewer_role="terminology_reviewer",
        provider_scope="provider_a",
        evidence_version="v1.0",
        expected_registry_version=current_ver,
        expiry=datetime.utcnow() - timedelta(minutes=10),  # expired
        decision="approved",
    )

    with pytest.raises(StaleApprovalError):
        registry.promote_proposal(proposal, expired_approval)
