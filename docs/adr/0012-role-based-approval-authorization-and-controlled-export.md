# ADR-0012: Role-Based Approval Authorization and Controlled Research Export

## Status
Accepted

## Context
Semantic clinical mappings mutate patient lab interpretations. Allowing uncredentialed automated services, data engineers, or external callers to sign approval artifacts risks catastrophic diagnostic or study errors.

Furthermore, releasing multi-table research datasets to consortium partners requires cryptographic proof that the delivered files match the exact approved study release.

## Decision
1. **Clinical Role Allowlist:** Only credentialed roles (`terminology_reviewer`, `lead_pathologist`, `chief_medical_officer`, `consortium_medical_director`) are authorized to sign `Approval` artifacts. Engineering and guest roles are denied.
2. **Provider Scope Alignment:** Reviewers must hold explicit authority over the target provider or hold consortium-wide credentials. Cross-provider approval attempts are denied.
3. **Controlled Research Export:** The `ControlledExportService` verifies Parquet dataset checksums against `release.output_checksums`, attaches recipient-bound HMAC-SHA256 signatures, and emits attributable export manifests.

## Consequences
- **Positive:** Cryptographically enforced human oversight; end-to-end dataset integrity from Bronze ingest through partner export.
- **Negative:** Requires role assignment administration.
