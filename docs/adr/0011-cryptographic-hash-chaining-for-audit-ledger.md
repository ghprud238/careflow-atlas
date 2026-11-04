# ADR-0011: Cryptographic Hash Chaining for Audit Ledger

## Status
Accepted

## Context
Healthcare data platforms operating under regulatory oversight (HIPAA, 21 CFR Part 11) must prove that system event logs and audit trails have not been retroactively altered, truncated, or forged. Conventional database logging without integrity proofs is vulnerable to database administrator tampering or silent data corruption.

Simultaneously, audit logs must never become a side-channel for Protected Health Information (PHI) leaks.

## Decision
1. **Cryptographic Hash Chaining:** Every audit event entry calculates its hash by binding the previous block's hash with its canonical event attributes:
   $$H_i = \text{SHA-256}(H_{i-1} \parallel \text{canonical\_payload}_i)$$
   with genesis block $H_0 = \text{"0"} \times 64$.
2. **Payload-Free Privacy Invariant:** The `AuditEvent` contract strictly prohibits patient identifiers, names, dates of birth, social security numbers, or raw payloads in metadata. Violations raise `ContractViolationError` and fail closed.
3. **Automated Verification:** The `AuditService.verify_chain_integrity()` function audits the chain from genesis, detecting any modified records, omitted sequences, or inserted blocks.

## Consequences
- **Positive:** Mathematical guarantee of audit log immutability and tamper-detection; zero risk of PHI leakage into operational logs.
- **Negative:** Requires sequential append operations for entry hash generation.
