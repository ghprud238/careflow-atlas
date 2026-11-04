# ADR-0001: Probabilistic Control Plane versus Deterministic Execution

## Status
Accepted

## Context
Healthcare data systems ingest messy, heterogeneous feeds with localized test codes, varying numeric representations (e.g. comma decimals), and evolving schemas. Using AI models to infer clinical mappings per-record at runtime introduces non-deterministic outputs, latency bottlenecks, high token costs, and unacceptable clinical hallucination risks.

Conversely, manual mapping processes are slow, backlogged, and frequently stall pipelines when unexpected source variations arrive.

## Decision
We enforce a strict architectural boundary between probabilistic investigation and deterministic execution:
1. **Bounded Agent Control Plane:** An isolated AI agent state machine is deployed exclusively to investigate unfamiliar codes, schema differences, and quarantined representation anomalies.
2. **Read-Only Scoped Tools:** The agent operates only through scoped MCP tools (`get_schema_diff`, `lookup_term`, `get_approved_aliases`, `get_safe_profile`, `dry_run_mapping`). It has no access to raw lake data, patient identifiers, credentials, or execution permissions.
3. **Declarative Output:** The agent cannot execute or promote code. It produces a typed, declarative `MappingProposal` artifact specifying allowlisted operations (`rename_field`, `parse_decimal`, `map_approved_code`, `convert_approved_unit`).
4. **Human Review Gate:** Promotion to the mapping registry requires explicit, cryptographically bound human authorization against the exact proposal hash.
5. **Deterministic Data Plane:** The data transformation engine (PySpark + Iceberg) only executes approved declarative rules from the versioned mapping registry.

## Consequences
- **Positive:** Zero non-deterministic mutations in production tables; auditability across all schema transitions; predictable execution latency and zero per-record model inference costs.
- **Negative:** Requires human-in-the-loop for newly encountered concepts; requires maintaining a sandboxed DSL interpreter and tool ecosystem.
