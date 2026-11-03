# ADR 0002: Deterministic Mapping Reuse vs Per-Record Model Inference

## Status
Accepted

## Context
When unfamiliar lab terminology or novel provider schemas land in the ingestion pipeline, the system must establish canonical clinical mappings.
Two architectural extremes exist:
1. **Per-record LLM inference:** Passing each unmapped record through an LLM during streaming or batch execution.
2. **Deterministic rules-only:** Rejecting all unknown terms until an engineer manually edits code.

Per-record model calls present severe flaws in regulated healthcare ETL:
- High latency and non-deterministic variations for identical inputs.
- Prohibitive API spend at scale (e.g. 1,000,000 observations).
- Inability to prove provenance or clinical correctness without inspecting millions of model calls.

## Decision
Separate the platform into a **probabilistic agent control plane** and a **deterministic transformation engine**:
1. When novel terms or schemas are encountered, the pipeline quarantines affected resources with safe attribution.
2. An investigation agent collects evidence via scoped read-only tools and proposes a typed, declarative mapping artifact (DSL).
3. A human clinical/terminology reviewer approves the exact proposal artifact hash.
4. The approved mapping version is registered in the mapping registry.
5. PySpark applies the approved mapping deterministically across all historical and future records. Zero model calls are made during per-record ETL processing.

## Consequences
### Positive
- Predictable performance, throughput, and zero per-record model latency or inference costs.
- Complete regulatory audit trail: every normalized row points directly to an approved mapping version and human approval hash.
- Full tolerance of AI service outages: once mappings are registered, batch processing continues uninterrupted.

### Negative / Trade-offs
- Novel terms require an asynchronous human approval step before quarantine release.
- Edge cases with ambiguous local semantics remain quarantined until investigated.

## Reconsideration Trigger
If human review latency creates an unacceptable operational bottleneck for low-risk non-clinical administrative fields, evaluate bounded automatic promotion for strictly allowlisted, low-risk syntactic transformations.
