# ADR-0009: Monotonic Sequence Supersession and Soft Deletion Semantics

## Status
Accepted

## Context
Healthcare feeds frequently deliver out-of-order records, late-arriving updates, amendments, and clinical retractions (e.g. lab results entered in error, corrected vitals, or patient record merges). In research and clinical operations, historical reproducibility requires an immutable audit trail while downstream consumer queries require current, un-retracted data.

Hard deleting rows in data lakes destroys provenance, breaks reproducibility for past study releases, and introduces severe write lock contention.

## Decision
1. **Monotonic Sequence Supersession:** Every record carries an adapter-established `_change_sequence` (or monotonic version timestamp). In Iceberg Silver tables, `MERGE INTO` updates records only when `source.sequence > target.sequence`.
2. **Clinical Retractions / Soft Deletes:** When an inbound record indicates a retraction or deletion (`_operation == 'delete'`), the canonical record is updated with `supersession_state = 'deleted'`.
3. **Audit Trail Preservation:** The record remains physically present in Iceberg table files with its original lineage (`manifest_id`, `run_id`, `resource_envelope_hash`), enabling historical point-in-time time-travel queries.
4. **Research Release Invariant:** Downstream cohort materialization queries filter strictly on `supersession_state == 'current'`, mathematically preventing deleted or retracted clinical records from entering research cohorts.

## Consequences
- **Positive:** Full audit compliance; immutable lineage; out-of-order tolerance; zero risk of deleted records entering study cohorts.
- **Negative:** Storage size grows monotonically unless compaction/retention jobs vacuum historical snapshots.
