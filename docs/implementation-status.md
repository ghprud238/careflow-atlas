# CareFlow Atlas — Implementation Status

**Current Status:** All Phases (Phase 0 through Phase 6) COMPLETED  
**Last Updated:** 2026-11-04  
**Audience:** Project owner, engineering reviewers, coding agents, hiring committees  

---

## 1. Roadmap & Phase Progress

| Phase | Description | Deliverable & Gate | Status |
| :--- | :--- | :--- | :--- |
| **0** | Compatibility spike & foundations | Compatible locked stack; contracts; ingest/write/merge/snapshot smoke test | **COMPLETED** |
| **1** | Single provider vertical slice | One provider to versioned cohort; duplicate-safe replay | **COMPLETED** |
| **2** | Governed agent investigation | Agent tools $\to$ proposal $\to$ review $\to$ replay $\to$ new release | **COMPLETED** |
| **3** | Multiprovider & evolution | CSV feed, corrections/deletes, drift, second adapter | **COMPLETED** |
| **4** | Security & console | Isolation, artifact approval, controlled export, console UI | **COMPLETED** |
| **5** | Evaluation & benchmarks | Recovery, evaluation, benchmarks, shadow migration | **COMPLETED** |
| **6** | Handoff & portfolio | ADRs, runbooks, reproducibility package, demo recording | **COMPLETED** |

---

## 2. Completed Requirement IDs & Feature Matrix

- **FR-01 (Extensible Adapter):** Implemented [`FHIRAdapter`](../pipelines/adapters/fhir/fhir_adapter.py) and [`CSVAdapter`](../pipelines/adapters/csv/csv_adapter.py) parsing both FHIR R4 JSON and Delimited CSV/pipe feeds into standard `ResourceEnvelope` carriers.
- **FR-02 (Verified Immutable Input):** Bronze landing protocol verifies byte count and SHA-256 before marking manifest ready; duplicate delivery flagged, conflict rejected.
- **FR-03 (Supported Resource Parsing):** Heterogeneous resources parsed to typed envelopes with preserved clinical status, units, and original/normalized fields.
- **FR-04 (Provenance):** Every canonical patient, encounter, and observation retains `Lineage(manifest_id, run_id, resource_envelope_hash)`.
- **FR-05 (Safe Quarantine):** Malformed numeric representations, missing patient references, and unreviewed codes safely quarantined to `careflow_quarantine.quarantined_records` with attributable findings.
- **FR-06 (Corrections & Retractions):** Monotonic sequence update verified in Iceberg Silver tables (`sequence = 2` supersedes `sequence = 1`); clinical retractions (`_operation = 'delete'`) update `supersession_state = 'deleted'`.
- **FR-07 (Agent Investigation Control Plane):** [`InvestigationAgent`](../services/agent/investigation_agent.py) operates state machine with hard reliability budget ($\le 8$ tool calls) and fail-closed abstention.
- **FR-08 (Scoped MCP Tools):** [`ScopedMCPTools`](../services/mcp_tools/tool_server.py) enforces caller scope checks (denying cross-provider requests with `CrossProviderAccessError`) and privacy-preserving aggregate profiles (`contains_phi=False`).
- **FR-09 (Safe Replay & Duplicate Protection):** Scoped replay drains quarantined records into Silver without duplicating rows; second execution finds 0 records to replay and maintains identical logical counts.
- **FR-10 (Complete Releases):** [`StudyRelease`](../packages/contracts/src/contracts/study_release.py) binds immutable snapshot bundle across single and federated providers `{provider: {table: snapshot_id}}`.
- **FR-11 (Exact Proposal Hash Authorization):** [`Approval`](../packages/contracts/src/contracts/approval.py) artifact binds to exact SHA-256 hash of [`MappingProposal`](../packages/contracts/src/contracts/mapping_proposal.py); enforces expected registry version and expiration.
- **FR-12 (Reproducibility & Snapshot Isolation):** Federated cohort research job queries pinned snapshot bundles across providers, evaluates adult criteria, lookback window, and deterministic tie-breaking. Retracted records are strictly excluded from the cohort.
- **FR-13 (Targeted Scoped Replay):** [`ReplayExecutor`](../services/mapping_executor/executor.py) extracts raw records from Bronze, applies newly promoted DSL rule, merges into Silver, and drains from quarantine.
- **FR-14 (Pinned Configuration & Conservation):** Conservation invariant strictly verified: $N_{input} == N_{accepted} + N_{duplicate} + N_{quarantined} + N_{excluded}$.
- **FR-15 (Schema Drift Detection):** [`DriftDetector`](../services/drift/drift_detector.py) evaluates column headers against pinned schema contracts, classifying added columns (`investigate_agent`) and missing required columns (`quarantine_batch`).
- **FR-16 (Tenant Isolation):** Multi-tenant PostgreSQL database schemas and user permissions (`provider_a_role` cannot access `provider_b_isolated`).
- **FR-17 (Cryptographic Audit Ledger):** Hash-chained append-only audit ledger ($H_i = \text{SHA256}(H_{i-1} \parallel \text{payload})$) with tamper detection via [`AuditService`](../services/audit/audit_service.py).
- **FR-18 (RBAC Approval Authorization):** [`RBACService`](../services/authorization/rbac_service.py) enforces verified clinical reviewer roles (`lead_pathologist`, `terminology_reviewer`) and provider authority.
- **FR-19 (Controlled Export Service):** [`ControlledExportService`](../services/export/export_service.py) verifies dataset checksums and signs recipient export packages with HMAC-SHA256 attestations.
- **FR-20 (Resilience, Recovery & Benchmarks):** Verified crash recovery, shadow candidate replay evaluation, and high-volume throughput benchmarks ($> 112,000$ recs/sec parsing).

---

## 3. Verified Artifacts & Test Execution Matrix

| Test Suite | Command | Verification Evidence | Status |
| :--- | :--- | :--- | :--- |
| **Unit Tests** | `make test-unit` | 24 tests passed across adapters, agent, DSL, drift detector | **PASSED** |
| **Contract Invariants** | `make test-contracts` | 13 tests passed on conservation laws, WORM manifest, approval hashing | **PASSED** |
| **Security & Isolation** | `make test-security` | 10 tests passed on tenant isolation, RBAC, chained audit ledger, export signing | **PASSED** |
| **Crash Recovery** | `make test-recovery` | 3 tests passed on restarted batch convergence, conflict denial, shadow replay | **PASSED** |
| **Throughput & Scale** | `make test-benchmarks` | High-volume batch (370 resources) verified; balanced conservation audit | **PASSED** |
| **Integration Scenarios** | `make test-integration` | 3 comprehensive end-to-end multi-provider and flagship tests passed | **PASSED** |
| **End-to-End Walkthrough** | `make demo` | 10-step automated architectural walkthrough verified end-to-end | **PASSED** |

---

## 4. Key Architectural Deliverables

1. **Architecture & Threat Model:**
   - [`docs/architecture.md`](../docs/architecture.md): Complete end-to-end data/control plane diagrams and storage models.
   - [`docs/threat-model.md`](../docs/threat-model.md): Comprehensive STRIDE analysis and sandboxed DSL security.
2. **Operational Readiness:**
   - [`docs/runbooks/operations_runbook.md`](../docs/runbooks/operations_runbook.md): Manifest ingestion, quarantine resolution, reviewer workflows.
   - [`docs/runbooks/disaster_recovery.md`](../docs/runbooks/disaster_recovery.md): Iceberg snapshot rollback, PITR recovery, MinIO re-drive.
3. **Scale & Performance:**
   - [`docs/benchmark-methodology.md`](../docs/benchmark-methodology.md): Rigorous benchmarking methodology and measured results.
4. **Architectural Decision Records (ADRs):**
   - ADR-0001 through ADR-0012 documented and approved in [`docs/adr/`](../docs/adr/).
