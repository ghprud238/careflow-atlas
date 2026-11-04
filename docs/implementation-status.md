# CareFlow Atlas — Implementation Status

**Current Status:** Phase 3 Completed  
**Last Updated:** 2026-11-04  
**Audience:** Project owner, engineering reviewers, coding agents  

---

## 1. Roadmap & Phase Progress

| Phase | Description | Deliverable & Gate | Status |
| :--- | :--- | :--- | :--- |
| **0** | Compatibility spike & foundations | Compatible locked stack; contracts; ingest/write/merge/snapshot smoke test | **COMPLETED** |
| **1** | Single provider vertical slice | One provider to versioned cohort; duplicate-safe replay | **COMPLETED** |
| **2** | Governed agent investigation | Agent tools $\to$ proposal $\to$ review $\to$ replay $\to$ new release | **COMPLETED** |
| **3** | Multiprovider & evolution | CSV feed, corrections/deletes, drift, second adapter | **COMPLETED** |
| **4** | Security & console | Isolation, artifact approval, controlled export, console UI | Next |
| **5** | Evaluation & benchmarks | Recovery, evaluation, benchmarks, shadow migration | Pending |
| **6** | Handoff & portfolio | ADRs, runbooks, reproducibility package, demo recording | Pending |

---

## 2. Completed Requirement IDs

- **FR-01 (Extensible adapter):** Implemented [`FHIRAdapter`](file:///Users/dpcubed/.gemini/antigravity/scratch/careflow-atlas/pipelines/adapters/fhir/fhir_adapter.py) and [`CSVAdapter`](file:///Users/dpcubed/.gemini/antigravity/scratch/careflow-atlas/pipelines/adapters/csv/csv_adapter.py) parsing both FHIR R4 JSON and Delimited CSV/pipe feeds into standard `ResourceEnvelope` carriers.
- **FR-02 (Verified immutable input):** Bronze landing protocol verifies byte count and SHA-256 before marking manifest ready; duplicate delivery flagged, conflict rejected.
- **FR-03 (Supported resource parsing):** Heterogeneous resources parsed to typed envelopes with preserved clinical status, units, and original/normalized fields.
- **FR-04 (Provenance):** Every canonical patient, encounter, and observation retains `Lineage(manifest_id, run_id, resource_envelope_hash)`.
- **FR-05 (Safe quarantine):** Malformed numeric representations, missing patient references, and unreviewed codes safely quarantined to `careflow_quarantine.quarantined_records` with attributable findings.
- **FR-06 (Corrections & Retractions):** Monotonic sequence update verified in Iceberg Silver tables (`sequence = 2` supersedes `sequence = 1`); clinical retractions (`_operation = 'delete'`) update `supersession_state = 'deleted'`.
- **FR-07 (Agent investigation control plane):** [`InvestigationAgent`](file:///Users/dpcubed/.gemini/antigravity/scratch/careflow-atlas/services/agent/investigation_agent.py) operates state machine with hard reliability budget ($\le 8$ tool calls) and fail-closed abstention.
- **FR-08 (Scoped MCP tools):** [`ScopedMCPTools`](file:///Users/dpcubed/.gemini/antigravity/scratch/careflow-atlas/services/mcp_tools/tool_server.py) enforces caller scope checks (denying cross-provider requests with `CrossProviderAccessError`) and privacy-preserving aggregate profiles (`contains_phi=False`).
- **FR-09 (Safe replay & duplicate protection):** Scoped replay drains quarantined records into Silver without duplicating rows; second execution finds 0 records to replay and maintains identical logical counts.
- **FR-10 (Complete releases):** [`StudyRelease`](file:///Users/dpcubed/.gemini/antigravity/scratch/careflow-atlas/packages/contracts/src/contracts/study_release.py) binds immutable snapshot bundle across single and federated providers `{provider: {table: snapshot_id}}`.
- **FR-11 (Exact proposal hash authorization):** [`Approval`](file:///Users/dpcubed/.gemini/antigravity/scratch/careflow-atlas/packages/contracts/src/contracts/approval.py) artifact binds to exact SHA-256 hash of [`MappingProposal`](file:///Users/dpcubed/.gemini/antigravity/scratch/careflow-atlas/packages/contracts/src/contracts/mapping_proposal.py); enforces expected registry version and expiration.
- **FR-12 (Reproducibility & snapshot isolation):** Federated cohort research job queries pinned snapshot bundles across providers, evaluates adult criteria, lookback window, and deterministic tie-breaking. Retracted records are strictly excluded from the cohort.
- **FR-13 (Targeted scoped replay):** [`ReplayExecutor`](file:///Users/dpcubed/.gemini/antigravity/scratch/careflow-atlas/services/mapping_executor/executor.py) extracts raw records from Bronze, applies newly promoted DSL rule, merges into Silver, and drains from quarantine.
- **FR-14 (Pinned configuration & contracts):** Conservation invariant strictly verified: $N_{input} == N_{accepted} + N_{duplicate} + N_{quarantined} + N_{excluded}$.
- **FR-15 (Schema drift detection):** [`DriftDetector`](file:///Users/dpcubed/.gemini/antigravity/scratch/careflow-atlas/services/drift/drift_detector.py) evaluates column headers against pinned schema contracts, classifying added columns (`investigate_agent`) and missing required columns (`quarantine_batch`).

---

## 3. Verified Artifacts & Commands

| Target | Command | Result |
| :--- | :--- | :--- |
| **Fixtures** | `make fixtures` | Synthetic resources generated for Provider A (FHIR) and Provider B (CSV/pipe) |
| **Contracts & Unit Tests** | `make test-unit` | **42 passed in 2.56s** (`pytest tests/contracts/ tests/unit/`) |
| **Integration Suite** | `make test-integration` | **3 passed in 40.55s** (`test_canonical_pipeline.py`, `test_flagship_scenario.py`, `test_multiprovider_evolution.py`) |
| **Smoke Test** | `make smoke` | PySpark + Iceberg write/merge/snapshot verification |
| **Architectural Decisions** | `docs/adr/` | ADR-0001 through ADR-0010 approved and documented |

---

## 4. Phase 3 Verification Summary

The Phase 3 multi-provider integration test ([`test_multiprovider_evolution.py`](file:///Users/dpcubed/.gemini/antigravity/scratch/careflow-atlas/tests/integration/test_multiprovider_evolution.py)) verified:
1. **Multi-Provider Ingestion:** Concurrent ingestion of Provider A (FHIR R4 JSON) and Provider B (pipe-delimited multi-table CSV).
2. **Partition Isolation:** Silver tables partitioned by `provider` ensure complete physical separation: 10 Provider A patients and 10 Provider B patients with zero cross-talk.
3. **Monotonic Updates:** Inbound record with higher sequence (`_change_sequence = 2`) correctly supersedes earlier sequence in Silver.
4. **Section 13 Clinical Retractions:** Inbound record with `_operation = 'delete'` updates `supersession_state = 'deleted'`, preserving historical auditability while preventing inclusion in active studies.
5. **Federated Research Release:** Multi-provider cohort materialized across Provider A and Provider B; verified that retracted records are strictly excluded from the Gold cohort Parquet dataset.
6. **Schema Drift Detection:** Inbound feed containing unexpected column `UNEXPECTED_TECHNICIAN_MEMO` flagged with `severity = 'warning'` and `recommended_action = 'investigate_agent'`.

---

## 5. Next Phase: Phase 4 Scope

- **Scope:** Security, audit trails, and human-in-the-loop review console.
- **Deliverables:** PostgreSQL provider-isolated tenant roles & RLS enforcement, cryptographic audit log verification, role-based approval authorization checks, and human review CLI / console interface.
