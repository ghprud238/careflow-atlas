# ADR-0007: Scoped Quarantine Drain Replay versus Blanket Lake Rescan

## Status
Accepted

## Context
When an unfamiliar lab code is resolved and approved, previously quarantined records must be integrated into canonical Silver tables. Typical data platform designs resort to either:
1. Re-ingesting or reprocessing the entire historical data lake (extremely expensive, wasteful, and introduces write contention).
2. Manually writing one-off update scripts (error-prone, breaks provenance and lineage).

## Decision
We implement **Targeted Scoped Replay**:
1. **Targeted Selection:** The `ReplayExecutor` identifies only the quarantined records matching the approved rule's provider and match predicates (e.g. `findings LIKE '%LOCAL_LAB_017%'`).
2. **Immutable Bronze Retrieval:** The executor fetches the exact raw resource payloads from the immutable Bronze manifests using stored `manifest_id` and `source_resource_id`.
3. **Canonical Normalization & Silver Merge:** The executor applies the newly promoted mapping rule via `DSLInterpreter`, assigns a new `normalization_version` and `mapping_reference`, and performs an Iceberg `MERGE INTO careflow_silver.observations`.
4. **Quarantine Drain:** Upon successful commit to Silver, the replayed records are deleted from `careflow_quarantine.quarantined_records`.
5. **Idempotency:** Replaying a rule when records have already been reprocessed finds 0 records to drain, maintaining exact logical row counts without duplicate creation.

## Consequences
- **Positive:** Low-latency, minimal compute cost; zero impact on un-affected clinical records; full lineage preservation; duplicate-safe.
- **Negative:** Requires maintaining relational links between quarantine records and Bronze manifest payloads.
