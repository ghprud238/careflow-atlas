# CareFlow Atlas — Threat Model & Security Architecture

## 1. Executive Summary & Security Objectives
CareFlow Atlas manages protected healthcare data across federated provider boundaries while providing an automated, agentic terminology investigation control plane.

Primary Security Invariants:
1. **Zero Raw Clinical Data / PHI in Agent Control Plane:**
   The LLM agent only ever observes aggregate distributions, distinct sample values, and metadata profiles (`contains_phi=False`). No patient names, MRNs, dates of birth, or individual identifiers are visible to the agent.
2. **Exact-Hash Human Approval Gate:**
   No probabilistic output from an LLM can execute against clinical data without human authorization bound to the exact cryptographic hash of the proposal.
3. **Tenant & Provider Isolation:**
   Multi-tenant execution strictly isolates data physically (partitioned Iceberg tables and PostgreSQL role-based schema isolation) and logically (denying cross-provider access via `CrossProviderAccessError`).
4. **Append-Only Tamper-Evident Audit Ledger:**
   Every system action is recorded in a hash-chained cryptographic ledger where $H_i = \text{SHA256}(H_{i-1} \parallel \text{payload}_i)$.

---

## 2. STRIDE Threat Analysis

| Threat (STRIDE) | Attack Vector | CareFlow Atlas Mitigation | Verification Test |
| :--- | :--- | :--- | :--- |
| **Spoofing** | Attacker impersonates clinical reviewer to approve unverified mappings | RBAC authorization checks require verified clinical role (`lead_pathologist`, `terminology_reviewer`) and user ID. | [`test_rbac_approvals.py`](../tests/security/test_rbac_approvals.py) |
| **Tampering** | Rogue actor modifies approved mapping DSL rule after sign-off | Mapping registry enforces exact SHA-256 hash match: $H(\text{proposal}) \equiv \text{approval.proposal\_hash}$. Tampering raises `ProposalHashMismatchError`. | [`test_mapping_dsl.py`](../tests/unit/test_mapping_dsl.py) |
| **Repudiation** | Reviewer or service denies performing a data export or approval | Chained audit ledger cryptographically binds actor, role, correlation ID, timestamp, and payload hash. Any modification breaks chain verification. | [`test_audit_ledger.py`](../tests/security/test_audit_ledger.py) |
| **Information Disclosure** | Agent or MCP tool leaks patient PHI during terminology investigation | MCP tools return synthetic/aggregate profiles only; Pydantic audit contracts fail closed if `contains_phi=True` or if patient identifiers appear. | [`test_mcp_tools.py`](../tests/unit/test_mcp_tools.py) |
| **Denial of Service** | Agent enters infinite loop or exhausts tool call quota | Hard budget ceiling ($\le 8$ tool calls) halts execution with fail-closed abstention if threshold is reached. | [`test_agent_state_machine.py`](../tests/unit/test_agent_state_machine.py) |
| **Elevation of Privilege** | Tenant A attempts to access or approve Tenant B mappings | `ScopedMCPTools` and `RBACService` enforce strict provider scoping (`caller_scope == resource_scope`), throwing `CrossProviderAccessError`. Database schemas enforce PostgreSQL user role isolation. | [`test_tenant_isolation.py`](../tests/security/test_tenant_isolation.py) |

---

## 3. Sandboxed DSL Security
CareFlow Atlas explicitly avoids arbitrary Python or SQL code generation.
The mapping DSL is restricted to an allowlist of pure mathematical and lookup operations:
- `parse_decimal()`
- `map_approved_code(system, code)`
- `convert_approved_unit(from_unit, to_unit, multiplier)`

Any attempt to inject SQL, arbitrary code, filesystem calls, or network requests is rejected during AST validation and runtime execution.
