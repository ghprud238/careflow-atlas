# ADR 0005: Research Release Bundle Manifests for Multi-Table Consistency

## Status
Accepted

## Context
A clinical research study requires relational consistency across multiple clinical entities: `Patient`, `Encounter`, `Observation`, `Condition`, `MedicationRequest`.
While Apache Iceberg guarantees ACID transactions on individual tables, a table commit does not span multiple independent Iceberg tables.
If an ETL batch or study materialization commits `Patient` and `Encounter` but encounters an infrastructure failure before committing `Observation`, an uncoordinated query could observe an inconsistent or partial state.

## Decision
Implement explicit **Multi-Table Release Manifests** (`StudyRelease`):
1. Ingest runs record the exact snapshot ID committed for each respective table.
2. A research release bundle is validated against mandatory release gates (conservation, privacy suppression, checksum validation) before exposure.
3. The release is published as an immutable `StudyRelease` record containing a pinned dictionary: `provider_table_snapshot_bundle = {provider: {table_name: snapshot_id}}`.
4. Downstream research queries and exports are required to bind to the pinned release snapshot bundle. Direct queries against table `latest` pointers are strictly prohibited in research workflows.

## Consequences
### Positive
- Cross-table relational consistency is mathematically guaranteed.
- Zero risk of "dirty reads" during long-running batch ingestion jobs.
- Clean crash recovery: if a commit fails mid-batch, previous complete releases remain active.

### Negative / Trade-offs
- Readers must resolve table references via the release manifest rather than querying bare table names.

## Reconsideration Trigger
If Iceberg or a catalog specification introduces standardized multi-table ACID transactions, evaluate native multi-table catalog commits.
