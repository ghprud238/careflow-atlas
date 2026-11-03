# CareFlow Atlas — Implementation Status

**Current Status:** Phase 0 Completed  
**Last Updated:** 2026-11-03  
**Audience:** Project owner, engineering reviewers, coding agents  

---

## 1. Roadmap & Phase Progress

| Phase | Description | Deliverable & Gate | Status |
| :--- | :--- | :--- | :--- |
| **0** | Compatibility spike & foundations | Compatible locked stack; contracts; ingest/write/merge/snapshot smoke test | **COMPLETED** |
| **1** | Single provider vertical slice | One provider to versioned cohort; duplicate-safe replay | Next |
| **2** | Governed agent investigation | Agent tools $\to$ proposal $\to$ review $\to$ replay $\to$ new release | Pending |
| **3** | Multiprovider & evolution | CSV feed, corrections/deletes, drift, second adapter | Pending |
| **4** | Security & console | Isolation, artifact approval, controlled export, console UI | Pending |
| **5** | Evaluation & benchmarks | Recovery, evaluation, benchmarks, shadow migration | Pending |
| **6** | Handoff & portfolio | ADRs, runbooks, reproducibility package, demo recording | Pending |

---

## 2. Completed Requirement IDs

- **FR-02 (Verified immutable input):** Validated in contracts and landing protocol. Hash-identical re-deliveries marked as duplicate, differing hashes marked as conflict.
- **FR-04 (Provenance):** Linked lineage and manifest hashes embedded in `CanonicalObservation` and `PipelineRun`.
- **FR-09 (Safe replay & duplicate protection):** Proved in PySpark + Iceberg MERGE test: duplicate sequence delivery ignored, monotonic sequence updates properly, zero duplicate logical rows.
- **FR-12 (Reproducibility & Snapshot isolation):** Proved via Iceberg snapshot time-travel: Snapshot 1 preserved while Snapshot 2 reflects updated state.
- **FR-14 (Pinned configuration & Contracts):** Pydantic contracts enforce strict schema, immutable intervals, and non-executable DSL allowlists.

---

## 3. Verified Artifacts & Commands

| Target | Command | Result |
| :--- | :--- | :--- |
| **Bootstrap** | `make bootstrap` | Virtual environment synced (Python 3.11.14 via `uv`) |
| **Infrastructure** | `make up` | PostgreSQL 16 & MinIO containers healthy, buckets initialized |
| **Contract Invariant Tests** | `make test-contracts` | 10 passed in 4.12s (`pytest`) |
| **Storage & Spark Smoke Test** | `make smoke` | Table creation, append, MERGE, snapshots, time-travel verified |

---

## 4. Known Limitations & Unresolved Risks

1. **Airflow Orchestration Deferred:** Airflow 3.x orchestration is intentionally deferred per ADR-0006 until Phase 1.
2. **Synthetic Fixture Ingestion:** Synthea generator setup is scheduled for Phase 1. Current smoke test uses controlled synthetic records.
3. **Single Host Deployment:** Local Docker Compose provides logical schema/role and bucket isolation; multi-region production cloud IAM isolation is simulated.
4. **Agent Integration:** LangGraph control plane and MCP server are scheduled for Phase 2.

---

## 5. Next Phase: Phase 1 Scope

- **Scope:** One synthetic FHIR provider (Provider A), Patient, Encounter, and Observation resources.
- **Contracts affected:** `SourceManifest`, `ResourceEnvelope`, `PipelineRun`, `CanonicalObservation`.
- **Expected tests:** Manifest ingestion, deduplication, quarantine classification, canonical persistence, duplicate-safe replay.
