# ADR-0004: Payload-Free Control Plane and Scoped MCP Tools

## Status
Accepted

## Context
AI agents investigating schema evolution and clinical anomalies could accidentally expose Protected Health Information (PHI) or patient identifiers in prompt logs, model inference context, or checkpoints. Furthermore, in multi-tenant or multi-provider healthcare environments, an agent investigating one provider must never be able to inspect or leak data belonging to another provider.

## Decision
1. **Scope Authorization Guard:** Every tool request authenticates the caller's provider scope independently of model arguments. If `caller_scope != requested_provider_id`, the tool server fails closed and raises `CrossProviderAccessError`.
2. **Payload-Free Aggregate Profiles:** The `get_safe_profile` tool generates statistical aggregates only: record count, null rate, detected format patterns, approximate value distributions, and distinct units. It strictly suppresses all patient IDs, names, dates of birth, free text, and raw JSON payloads (`contains_phi=False`).
3. **Isolated Memory Dry Runs:** The `dry_run_mapping` tool executes candidate DSL rules against in-memory synthetic or anonymized test samples, returning only validation metrics and format conformance without mutating persistent state.

## Consequences
- **Positive:** HIPAA compliance; mathematical prevention of cross-provider tenant data leaks; complete privacy protection during AI model reasoning.
- **Negative:** Agent must reason from statistical patterns rather than seeing individual patient medical records.
