# CareFlow Atlas — Operations Runbook

## 1. System Overview & Architecture Summary
CareFlow Atlas operates a decoupled lakehouse architecture:
- **Bronze Layer (`careflow-bronze` bucket):** Immutable, checksummed raw payloads (`SourceManifest` metadata registered in Postgres `careflow_metadata.source_manifests`).
- **Silver Layer (`careflow-silver` bucket):** Canonical Apache Iceberg v2 tables (`patients`, `encounters`, `observations`) registered via JDBC Catalog in Postgres (`careflow_catalog`). Partitioned physically by `provider`.
- **Quarantine Layer (`careflow-quarantine` bucket):** Iceberg v2 table `quarantined_records` holding unmapped or schema-violating records with zero PHI loss.
- **Gold Layer (`careflow-gold` bucket):** Cohort datasets and release bundles (`StudyRelease`).
- **Control Plane:** Governed LLM Agent (`InvestigationAgent`) operating with scoped MCP tools, privacy-preserving profiles, and human approval gates.

---

## 2. Ingestion Monitoring & Manifest Reconciliation

### 2.1 Manifest Lifecycle
1. **Pending Landing:** Partner feeds land in MinIO/S3.
2. **Byte & Checksum Verification:** `ManifestManager.stage_and_register_batch` verifies SHA-256 and byte counts.
3. **Execution:** Canonical Spark transform performs Iceberg MERGE.
4. **Conservation Audit:**
   $$\text{Accounting: } N_{\text{input}} = N_{\text{accepted}} + N_{\text{duplicate}} + N_{\text{quarantined}} + N_{\text{excluded}}$$
   If conservation is unbalanced, the job raises `ConservationViolationError` and fails closed before committing snapshots.

### 2.2 Alert Triggers & Runbook Responses
| Alert | Condition | Runbook Action |
| :--- | :--- | :--- |
| `UnbalancedConservation` | $N_{input} \ne \sum \text{outputs}$ | Critical: Batch aborted. Inspect driver logs for malformed envelopes or unhandled exceptions. Manifest remains `pending`. |
| `HighQuarantineRate` | Quarantined records $> 5\%$ of batch | Warning: Partner feed introduced new unmapped terminology or schema drift. Trigger `InvestigationAgent` to review safe profiles. |
| `DuplicateManifestDelivery` | Identical SHA256 delivered again | Info: Idempotently skipped; marked `duplicate`. |
| `ConflictingManifestDelivery` | Same batch ID with differing SHA256 | Critical: Delivery rejected with `ConflictingDeliveryError`. Alert security and partner point of contact. |

---

## 3. Governed Terminology Investigation & Human Approval

### 3.1 Unmapped Code Alert Flow
When unmapped clinical codes (e.g. local EHR lab codes like `GLUC-RANDOM-POC` or `PB-GLUC`) arrive:
1. Records are safely diverted to `careflow_quarantine.quarantined_records`.
2. Schema drift detector or scheduled triage launches [`InvestigationAgent`](file:///Users/dpcubed/.gemini/antigravity/scratch/careflow-atlas/services/agent/investigation_agent.py).
3. The agent is hard-budgeted to $\le 8$ tool calls against [`ScopedMCPTools`](file:///Users/dpcubed/.gemini/antigravity/scratch/careflow-atlas/services/mcp_tools/tool_server.py).
4. All tool interactions receive **payload-free aggregate summaries** (`contains_phi=False`), ensuring zero PHI enters LLM prompts.
5. The agent either produces a deterministic [`MappingProposal`](file:///Users/dpcubed/.gemini/antigravity/scratch/careflow-atlas/packages/contracts/src/contracts/mapping_proposal.py) or safely abstains (`abstain_investigation`).

### 3.2 Clinical Reviewer Workflow
1. Reviewers log in via [`ReviewConsole`](file:///Users/dpcubed/.gemini/antigravity/scratch/careflow-atlas/services/console/review_console.py) CLI:
   ```bash
   python -m services.console.review_console inspect <PROPOSAL_ID>
   ```
2. The console displays:
   - Target Concept and Mapping Rule.
   - Aggregate statistics (distribution, quantiles, sample counts).
   - Validation test results (100% dry run conversion).
   - Exact Deterministic SHA-256 Hash.
3. Reviewer signs approval with RBAC role (`lead_pathologist` or `terminology_reviewer`):
   ```bash
   python -m services.console.review_console approve <PROPOSAL_ID> --user dr_vance --role lead_pathologist
   ```
4. Signed [`Approval`](file:///Users/dpcubed/.gemini/antigravity/scratch/careflow-atlas/packages/contracts/src/contracts/approval.py) binds cryptographically to the proposal hash and registers into [`MappingRegistry`](file:///Users/dpcubed/.gemini/antigravity/scratch/careflow-atlas/packages/mapping_dsl/registry.py).

---

## 4. Scoped Replay Execution

To drain quarantined records without lake-wide reprocessing:
1. Trigger [`ReplayExecutor`](file:///Users/dpcubed/.gemini/antigravity/scratch/careflow-atlas/services/mapping_executor/executor.py):
   ```python
   executor = ReplayExecutor(spark=spark, registry=registry)
   executor.replay_quarantined_scope(
       provider=provider,
       manifest_id=manifest_id,
       target_code=target_code,
       approved_rule=proposal.transformation_dsl,
   )
   ```
2. Replay queries only records where `source_resource_id` matches the quarantined scope.
3. Transformed records are merged into Iceberg Silver with updated snapshot commit.
4. Quarantined records are deleted from `careflow_quarantine.quarantined_records`.
5. Idempotency guarantee: subsequent replay calls find 0 pending records and no-op.
