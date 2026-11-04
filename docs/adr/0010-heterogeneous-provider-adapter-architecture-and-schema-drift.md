# ADR-0010: Heterogeneous Provider Adapters and Schema Drift Detection

## Status
Accepted

## Context
A health system ETL platform must ingest data from diverse partner feeds: modern EHRs transmitting FHIR R4 JSON bundles, and regional clinics transmitting delimited CSV/pipe text files. Hardcoding ingest pipelines to a single protocol leads to architectural fragmentation, while unmonitored format shifts cause silent ingest failures.

## Decision
1. **Extensible Adapter Pattern:** Individual provider feeds are translated by dedicated adapters ([`FHIRAdapter`](../../pipelines/adapters/fhir/fhir_adapter.py), [`CSVAdapter`](../../pipelines/adapters/csv/csv_adapter.py)) into standard `ResourceEnvelope` carriers before reaching the PySpark transformation layer.
2. **Provider Partitioning:** Canonical Iceberg Silver tables (`patients`, `encounters`, `observations`, `quarantined_records`) are partitioned by `provider`. This guarantees tenant partition pruning and physical data isolation.
3. **Schema Drift Detection:** Inbound feeds are evaluated against pinned schema contracts by [`DriftDetector`](../../services/drift/drift_detector.py).
   - Missing required columns trigger critical severity and quarantine the batch (`quarantine_batch`).
   - Newly added or modified columns trigger a warning and invoke agent investigation (`investigate_agent`) without stalling un-affected tables.

## Consequences
- **Positive:** Multi-provider federation without changing downstream transformation logic; automated detection of partner schema evolution; complete physical partition isolation.
- **Negative:** Requires maintaining separate parsing adapters per format family.
