# ADR 0008: Single-Writer Limits and Failover Protocol

## Status
Accepted

## Context
When multiple workers attempt concurrent writes to the same provider and entity scope in Apache Iceberg, optimistic concurrency control (OCC) can cause commit conflicts and retry storms, especially under heavy batch volume.
Furthermore, distributed locks with expiring leases (e.g. Redis timeouts) do not constitute a safe fenced active-active protocol and risk split-brain commits if a stalled worker awakens.

## Decision
Serialize writers per provider/resource scope in the MVP architecture:
1. Writers are partitioned by `(provider, feed, partition)`.
2. A single PySpark batch process owns the commit sequence for a designated provider scope during a scheduled window.
3. If a writer crashes or hangs, the orchestration engine must confirm that the failed worker is terminated and its PID/container killed before starting a replacement.
4. Active-active concurrent writers are explicitly deferred until fenced epoch fencing is implemented.

## Consequences
### Positive
- Zero commit conflicts or OCC retry overhead.
- Predictable monotonic sequence assignment and deterministic deduplication.
- Minimal operational complexity for MVP.

### Negative / Trade-offs
- Throughput is bounded by the capacity of the single scoped Spark batch executor.

## Reconsideration Trigger
When horizontal write throughput per individual provider exceeds single-executor scaling limits, implement fenced epoch locks or table partitioning across independent sub-namespaces.
