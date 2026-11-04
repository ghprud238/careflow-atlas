# CareFlow Atlas — Implementation Status

**Current Status:** Phase 1 Completed  
**Last Updated:** 2026-11-04  
**Audience:** Project owner, engineering reviewers, coding agents  

---

## 1. Roadmap & Phase Progress

| Phase | Description | Deliverable & Gate | Status |
| :--- | :--- | :--- | :--- |
| **0** | Compatibility spike & foundations | Compatible locked stack; contracts; ingest/write/merge/snapshot smoke test | **COMPLETED** |
| **1** | Single provider vertical slice | One provider to versioned cohort; duplicate-safe replay | **COMPLETED** |
| **2** | Governed agent investigation | Agent tools $\to$ proposal $\to$ review $\to$ replay $\to$ new release | Next |
| **3** | Multiprovider & evolution | CSV feed, corrections/deletes, drift, second adapter | Pending |
| **4** | Security & console | Isolation, artifact approval, controlled export, console UI | Pending |
| **5** | Evaluation & benchmarks | Recovery, evaluation, benchmarks, shadow migration | Pending |
| **6** | Handoff & portfolio | ADRs, runbooks, reproducibility package, demo recording | Pending |

---

## 2. Completed Requirement IDs

- **FR-01 (Extensible adapter):** Implemented [`FHIRAdapter`](file:///Users/dpcubed/.gemini/antigravity/scratch/careflow-atlas/pipelines/adapters/fhir/fhir_adapter.py) parsing `Patient`, `Encounter`, `Observation` into standard `ResourceEnvelope` carriers.
- **FR-02 (Verified immutable input):** Bronze landing protocol verifies byte count and SHA-256 before marking manifest ready; duplicate delivery flagged, conflict rejected.
- **FR-03 (Supported resource parsing):** Synthetic FHIR R4 resources parsed to typed envelopes with preserved clinical status, units, and original/normalized fields.
- **FR-04 (Provenance):** Every canonical patient, encounter, and observation retains `Lineage(manifest_id, run_id, resource_envelope_hash)`.
- **FR-05 (Safe quarantine):** Malformed numeric representations and missing patient references safely quarantined to `careflow_quarantine.quarantined_records` with attributable findings.
- **FR-06 (Corrections & Updates):** Monotonic sequence update verified in Iceberg Silver tables (`obs-000002` sequence 2 overwrites sequence 1).
- **FR-09 (Safe replay & duplicate protection):** Replaying the exact same batch resulted in identical table counts: 15 patients, 32 encounters, 98 observations (zero extra logical rows).
- **FR-10 (Complete releases):** [`StudyRelease`](file:///Users/dpcubed/.gemini/antigravity/scratch/careflow-atlas/packages/contracts/src/contracts/study_release.py) binds immutable snapshot bundle `{provider: {table: snapshot_id}}`.
- **FR-12 (Reproducibility & snapshot isolation):** Asynchronous cohort research job queries pinned snapshot bundle, evaluates adult criteria, lookback window, and deterministic tie-breaking.
- **FR-14 (Pinned configuration & contracts):** Conservation invariant strictly verified: $N_{input} == N_{accepted} + N_{duplicate} + N_{quarantined} + N_{excluded}$.

---

## 3. Verified Artifacts & Commands

| Target | Command | Result |
| :--- | :--- | :--- |
| **Fixtures** | `make fixtures` | 104 synthetic resources generated with LOINC concepts and deliberate anomalies |
| **Contracts & Unit Tests** | `make test-unit` | 18 passed in 1.66s (`pytest`) |
| **Integration Suite** | `make test-integration` | 1 passed in 44.15s: full slice ingest, duplicate-safe replay, updates, and cohort release |
| **Smoke Test** | `make smoke` | PySpark + Iceberg write/merge/snapshot verification |

---

## 4. Known Limitations & Unresolved Risks

1. **Agent Integration Deferred:** LangGraph investigation state machine and scoped MCP tools are scheduled for Phase 2.
2. **Provider Scope:** Currently Provider A (FHIR R4); Provider B (CSV) and Provider C (unstructured/adversarial) scheduled for Phase 3.
3. **Airflow Orchestration:** Pipeline executed via direct PySpark batch API; Airflow DAG orchestration deferred per ADR-0006.

---

## 5. Next Phase: Phase 2 Scope

- **Scope:** Governed agent control plane for unfamiliar lab investigation.
- **Deliverables:** Scoped MCP server (`get_schema_diff`, `lookup_term`, `get_approved_aliases`, `get_safe_profile`, `dry_run_mapping`), LangGraph state machine, typed DSL proposal, exact-hash human approval, and scoped replay.
