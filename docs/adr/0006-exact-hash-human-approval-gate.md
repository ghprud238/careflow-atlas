# ADR-0006: Exact-Hash Human Approval Gate and Versioned Promotion

## Status
Accepted

## Context
In regulated clinical and research domains, automating schema or semantic mappings without clinical oversight can lead to silent data corruption, misdiagnoses, and biased study cohorts. Standard "accept" buttons that approve dynamic or mutating rules risk approving modified or stale logic.

## Decision
1. **Deterministic Proposal Hashing:** Every `MappingProposal` computes an immutable SHA-256 hash over its canonical attributes: provider ID, contract version, typed DSL model, terminology snapshot, prompt version, model version, and disposition.
2. **Exact Hash Binding:** The human `Approval` artifact binds to the exact `proposal_hash`. Any modification to the proposal alters the hash and invalidates the approval.
3. **Registry Version Preconditions:** The approval specifies the `expected_registry_version`. If another promotion occurs concurrently, advancing the registry version, the approval is rejected with `StaleApprovalError`, requiring revalidation.
4. **Expiry Enforcement:** Approvals carry a hard expiration timestamp (`expiry`). Stale approvals cannot be executed.

## Consequences
- **Positive:** Uncompromised clinical governance; cryptographic guarantee that what was reviewed is exactly what is executed; prevents race conditions during rule promotion.
- **Negative:** Requires strict review ceremony; concurrent promotions require sequential review.
