-- CareFlow Atlas Database Initialization
-- Sets up schemas, provider-isolated roles, and audit ledger

-- Extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Schemas
CREATE SCHEMA IF NOT EXISTS careflow_catalog;
CREATE SCHEMA IF NOT EXISTS careflow_control;
CREATE SCHEMA IF NOT EXISTS careflow_audit;
CREATE SCHEMA IF NOT EXISTS provider_a_isolated;
CREATE SCHEMA IF NOT EXISTS provider_b_isolated;
CREATE SCHEMA IF NOT EXISTS provider_c_isolated;

-- Provider Isolated Roles (Section 7 & FR-11: Provider Isolation)
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'provider_a_role') THEN
        CREATE ROLE provider_a_role WITH LOGIN PASSWORD 'provider_a_secret';
    END IF;
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'provider_b_role') THEN
        CREATE ROLE provider_b_role WITH LOGIN PASSWORD 'provider_b_secret';
    END IF;
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'provider_c_role') THEN
        CREATE ROLE provider_c_role WITH LOGIN PASSWORD 'provider_c_secret';
    END IF;
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'researcher_role') THEN
        CREATE ROLE researcher_role WITH LOGIN PASSWORD 'researcher_secret';
    END IF;
END $$;

-- Role Schema Permissions: Least-privilege isolation
REVOKE ALL ON SCHEMA provider_a_isolated FROM PUBLIC, provider_b_role, provider_c_role;
REVOKE ALL ON SCHEMA provider_b_isolated FROM PUBLIC, provider_a_role, provider_c_role;
REVOKE ALL ON SCHEMA provider_c_isolated FROM PUBLIC, provider_a_role, provider_b_role;

GRANT USAGE, CREATE ON SCHEMA provider_a_isolated TO provider_a_role;
GRANT USAGE, CREATE ON SCHEMA provider_b_isolated TO provider_b_role;
GRANT USAGE, CREATE ON SCHEMA provider_c_isolated TO provider_c_role;

GRANT USAGE, CREATE ON SCHEMA careflow_catalog TO careflow_admin;
GRANT USAGE, CREATE ON SCHEMA careflow_control TO careflow_admin;
GRANT USAGE, CREATE ON SCHEMA careflow_audit TO careflow_admin;

-- Control Plane Audit Table (Section 8: audit_event - no raw patient payloads)
CREATE TABLE IF NOT EXISTS careflow_audit.audit_events (
    event_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    actor_or_service VARCHAR(128) NOT NULL,
    action VARCHAR(128) NOT NULL,
    resource VARCHAR(256) NOT NULL,
    scope VARCHAR(128) NOT NULL,
    outcome VARCHAR(32) NOT NULL CHECK (outcome IN ('success', 'failure', 'denied')),
    correlation_id VARCHAR(128) NOT NULL,
    old_version VARCHAR(64),
    new_version VARCHAR(64),
    details JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS careflow_audit.chained_audit_ledger (
    sequence_num BIGSERIAL PRIMARY KEY,
    event_id UUID NOT NULL,
    actor_or_service VARCHAR(128) NOT NULL,
    action VARCHAR(128) NOT NULL,
    resource VARCHAR(256) NOT NULL,
    scope VARCHAR(128) NOT NULL,
    outcome VARCHAR(32) NOT NULL,
    correlation_id VARCHAR(128) NOT NULL,
    previous_hash VARCHAR(64) NOT NULL,
    entry_hash VARCHAR(64) NOT NULL UNIQUE,
    details JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_audit_correlation ON careflow_audit.audit_events(correlation_id);
CREATE INDEX IF NOT EXISTS idx_audit_scope ON careflow_audit.audit_events(scope);
CREATE INDEX IF NOT EXISTS idx_audit_actor ON careflow_audit.audit_events(actor_or_service);

-- Manifests Ledger
CREATE TABLE IF NOT EXISTS careflow_control.source_manifests (
    manifest_id VARCHAR(128) PRIMARY KEY,
    provider VARCHAR(64) NOT NULL,
    feed VARCHAR(64) NOT NULL,
    batch_id VARCHAR(128) NOT NULL,
    source_interval_start TIMESTAMPTZ NOT NULL,
    source_interval_end TIMESTAMPTZ NOT NULL,
    object_uri TEXT NOT NULL,
    object_hash VARCHAR(128) NOT NULL,
    object_bytes BIGINT NOT NULL,
    adapter_version VARCHAR(64) NOT NULL,
    readiness VARCHAR(32) NOT NULL CHECK (readiness IN ('pending', 'ready')),
    delivery_disposition VARCHAR(32) NOT NULL CHECK (delivery_disposition IN ('accepted', 'duplicate', 'conflict')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_provider_batch UNIQUE (provider, batch_id)
);

-- Pipeline Runs Ledger
CREATE TABLE IF NOT EXISTS careflow_control.pipeline_runs (
    run_id VARCHAR(128) PRIMARY KEY,
    manifest_digest VARCHAR(128) NOT NULL,
    pinned_schema_version VARCHAR(64) NOT NULL,
    pinned_mapping_version VARCHAR(64) NOT NULL,
    pinned_config_version VARCHAR(64) NOT NULL,
    pinned_code_version VARCHAR(64) NOT NULL,
    expected_output_scope VARCHAR(128) NOT NULL,
    attempt INT NOT NULL DEFAULT 1,
    state VARCHAR(32) NOT NULL CHECK (state IN ('running', 'completed', 'completed_with_exceptions', 'failed')),
    error_class VARCHAR(128),
    committed_snapshots JSONB DEFAULT '{}'::jsonb,
    started_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMPTZ
);

-- Study Releases Ledger (ADR-0005 & Section 8)
CREATE TABLE IF NOT EXISTS careflow_control.study_releases (
    release_id VARCHAR(128) PRIMARY KEY,
    study_id VARCHAR(128) NOT NULL,
    owner VARCHAR(128) NOT NULL,
    cohort_digest VARCHAR(128) NOT NULL,
    fixed_reference_date VARCHAR(32) NOT NULL,
    provider_table_snapshot_bundle JSONB NOT NULL,
    quality_privacy_versions JSONB NOT NULL,
    code_digest VARCHAR(128) NOT NULL,
    output_checksums JSONB NOT NULL,
    active_state VARCHAR(32) NOT NULL CHECK (active_state IN ('active', 'revoked')),
    revoked_at TIMESTAMPTZ,
    revocation_reason TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Mapping Proposals & Approvals (Section 8, 11)
CREATE TABLE IF NOT EXISTS careflow_control.mapping_proposals (
    proposal_id VARCHAR(128) PRIMARY KEY,
    proposal_hash VARCHAR(128) NOT NULL UNIQUE,
    provider_id VARCHAR(64) NOT NULL,
    source_contract_version VARCHAR(64) NOT NULL,
    typed_dsl JSONB NOT NULL,
    candidates JSONB DEFAULT '[]'::jsonb,
    terminology_snapshot VARCHAR(64) NOT NULL,
    evidence_refs JSONB DEFAULT '[]'::jsonb,
    safe_test_results JSONB DEFAULT '{}'::jsonb,
    prompt_version VARCHAR(64) NOT NULL,
    model_version VARCHAR(64) NOT NULL,
    disposition VARCHAR(32) NOT NULL,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS careflow_control.approvals (
    approval_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    proposal_hash VARCHAR(128) NOT NULL,
    authorized_reviewer VARCHAR(128) NOT NULL,
    reviewer_role VARCHAR(64) NOT NULL,
    provider_scope VARCHAR(64) NOT NULL,
    evidence_version VARCHAR(64) NOT NULL,
    expected_registry_version VARCHAR(64) NOT NULL,
    expiry TIMESTAMPTZ NOT NULL,
    decision VARCHAR(32) NOT NULL CHECK (decision IN ('approved', 'rejected')),
    notes TEXT,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS careflow_control.mapping_registry (
    registry_version VARCHAR(64) PRIMARY KEY,
    previous_version VARCHAR(64),
    rules JSONB NOT NULL,
    approval_hash VARCHAR(128),
    promoted_by VARCHAR(128) NOT NULL,
    promoted_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);
