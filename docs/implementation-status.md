# CareFlow Atlas — Implementation Status

**Current Status:** Phase 2 Completed  
**Last Updated:** 2026-11-04  
**Audience:** Project owner, engineering reviewers, coding agents  

---

## 1. Roadmap & Phase Progress

| Phase | Description | Deliverable & Gate | Status |
| :--- | :--- | :--- | :--- |
| **0** | Compatibility spike & foundations | Compatible locked stack; contracts; ingest/write/merge/snapshot smoke test | **COMPLETED** |
| **1** | Single provider vertical slice | One provider to versioned cohort; duplicate-safe replay | **COMPLETED** |
| **2** | Governed agent investigation | Agent tools $\to$ proposal $\to$ review $\to$ replay $\to$ new release | **COMPLETED** |
| **3** | Multiprovider & evolution | CSV feed, corrections/deletes, drift, second adapter | Next |
| **4** | Security & console | Isolation, artifact approval, controlled export, console UI | Pending |
| **5** | Evaluation & benchmarks | Recovery, evaluation, benchmarks, shadow migration | Pending |
| **6** | Handoff & portfolio | ADRs, runbooks, reproducibility package, demo recording | Pending |

---

## 2. Completed Requirement IDs

- **FR-01 (Extensible adapter):** Implemented [`FHIRAdapter`](file:///Users/dpcubed/.gemini/antigravity/scratch/careflow-atlas/pipelines/adapters/fhir/fhir_adapter.py) parsing `Patient`, `Encounter`, `Observation` into standard `ResourceEnvelope` carriers.
- **FR-02 (Verified immutable input):** Bronze landing protocol verifies byte count and SHA-256 before marking manifest ready; duplicate delivery flagged, conflict rejected.
- **FR-03 (Supported resource parsing):** Synthetic FHIR R4 resources parsed to typed envelopes with preserved clinical status, units, and original/normalized fields.
- **FR-04 (Provenance):** Every canonical patient, encounter, and observation retains `Lineage(manifest_id, run_id, resource_envelope_hash)`.
- **FR-05 (Safe quarantine):** Malformed numeric representations, missing patient references, and unreviewed codes safely quarantined to `careflow_quarantine.quarantined_records` with attributable findings.
- **FR-06 (Corrections & Updates):** Monotonic sequence update verified in Iceberg Silver tables (`obs-000002` sequence 2 overwrites sequence 1).
- **FR-07 (Agent investigation control plane):** [`InvestigationAgent`](file:///Users/dpcubed/.gemini/antigravity/scratch/careflow-atlas/services/agent/investigation_agent.py) operates state machine with hard reliability budget ($\le 8$ tool calls) and fail-closed abstention.
- **FR-08 (Scoped MCP tools):** [`ScopedMCPTools`](file:///Users/dpcubed/.gemini/antigravity/scratch/careflow-atlas/services/mcp_tools/tool_server.py) enforces caller scope checks (denying cross-provider requests with `CrossProviderAccessError`) and privacy-preserving aggregate profiles (`contains_phi=False`).
- **FR-09 (Safe replay & duplicate protection):** Scoped replay drains quarantined records into Silver without duplicating rows; second execution finds 0 records to replay and maintains identical logical counts.
- **FR-10 (Complete releases):** [`StudyRelease`](file:///Users/dpcubed/.gemini/antigravity/scratch/careflow-atlas/packages/contracts/src/contracts/study_release.py) binds immutable snapshot bundle `{provider: {table: snapshot_id}}`.
- **FR-11 (Exact proposal hash authorization):** [`Approval`](file:///Users/dpcubed/.gemini/antigravity/scratch/careflow-atlas/packages/contracts/src/contracts/approval.py) artifact binds to exact SHA-256 hash of [`MappingProposal`](file:///Users/dpcubed/.gemini/antigravity/scratch/careflow-atlas/packages/contracts/src/contracts/mapping_proposal.py); enforces expected registry version and expiration.
- **FR-12 (Reproducibility & snapshot isolation):** Asynchronous cohort research job queries pinned snapshot bundle, evaluates adult criteria, lookback window, and deterministic tie-breaking.
- **FR-13 (Targeted scoped replay):** [`ReplayExecutor`](file:///Users/dpcubed/.gemini/antigravity/scratch/careflow-atlas/services/mapping_executor/executor.py) extracts raw records from Bronze, applies newly promoted DSL rule, merges into Silver, and drains from quarantine.
- **FR-14 (Pinned configuration & contracts):** Conservation invariant strictly verified: $N_{input} == N_{accepted} + N_{duplicate} + N_{quarantined} + N_{excluded}$.

---

## 3. Verified Artifacts & Commands

| Target | Command | Result |
| :--- | :--- | :--- |
| **Fixtures** | `make fixtures` | Synthetic resources generated with LOINC concepts and deliberate anomalies |
| **Contracts & Unit Tests** | `make test-unit` | **33 passed in 1.10s** (`pytest tests/contracts/ tests/unit/`) |
| **Integration Suite** | `make test-integration` | **2 passed in 27.07s** (`test_canonical_pipeline.py` + `test_flagship_scenario.py`) |
| **Smoke Test** | `make smoke` | PySpark + Iceberg write/merge/snapshot verification |
| **Architectural Decisions** | `docs/adr/` | ADR-0001 through ADR-0008 approved and documented |

---

## 4. Flagship Scenario Verification Summary

The flagship scenario test ([`test_flagship_scenario.py`](file:///Users/dpcubed/.gemini/antigravity/scratch/careflow-atlas/tests/integration/test_flagship_scenario.py)) completed all 7 verification steps end-to-end:
1. **Quarantine:** Unfamiliar lab `LOCAL_LAB_017` with representation anomaly `"12,5"` was quarantined on arrival.
2. **Baseline Release:** Release 1 materialized with `obs-flagship-LOCAL_LAB_017` excluded.
3. **Agent Investigation:** `InvestigationAgent` executed bounded tool calls ($\le 8$), verified schema delta and safe statistical profile, and emitted deterministic `MappingProposal` (`hash: abe5363d...`).
4. **Human Gate:** Reviewer signed `Approval` artifact binding exact hash and `v1.0.0` expected registry version.
5. **Promotion & Scoped Replay:** `ReplayExecutor` promoted registry to `v1.1.0`, transformed quarantined observation to `reviewed-catalog/concept-glucose` with numeric value `12.5`, merged into Silver observations, and deleted from quarantine table.
6. **Release 2 Comparison:** Materialized Release 2; `ReleaseManager.compare_releases` verified `comparison_state == "materially_updated"` and digest change (`6756f46d...` $\to$ `04fc1026...`).
7. **Idempotency & Security:** Replaying a second time found 0 records to replay (exact 1 row preserved in Silver); attempting to re-promote the stale approval failed closed with `StaleApprovalError`.

---

## 5. Next Phase: Phase 3 Scope

- **Scope:** Multiprovider federation, schema evolution, and drift detection.
- **Deliverables:** Provider B (delimited CSV / pipe feed adapter), clinical update and retraction handling, schema drift monitoring, and cross-provider isolation checks.
