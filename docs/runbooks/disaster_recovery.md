# CareFlow Atlas — Disaster Recovery Runbook

## 1. Disaster Recovery Scenarios & RTO/RPO Objectives

| Component | RPO (Recovery Point Objective) | RTO (Recovery Time Objective) | Strategy |
| :--- | :--- | :--- | :--- |
| **Bronze Raw Objects (`careflow-bronze`)** | 0 seconds (WORM storage) | $< 15$ minutes | Versioned S3/MinIO bucket; read-only access. |
| **Silver Apache Iceberg Tables** | 0 seconds (Atomic commits) | $< 5$ minutes | Snapshot rollbacks via `ALTER TABLE ... ROLLBACK TO SNAPSHOT`. |
| **Catalog & Audit Database (PostgreSQL)** | $< 1$ hour | $< 30$ minutes | WAL archiving, PITR (Point-in-Time Recovery), read replicas. |
| **Chained Audit Ledger** | 0 seconds | $< 10$ minutes | Cryptographic hash chain verification; tamper detection. |

---

## 2. Iceberg Snapshot Rollback Protocol

If a corrupted or unintended write occurs (e.g. faulty adapter code or failed deployment), Iceberg v2 enables instant O(1) rollback without touching data files:

### 2.1 Identifying Prior Clean Snapshot
```sql
-- Query snapshot history for affected table
SELECT snapshot_id, committed_at, summary['operation'] as op
FROM careflow_catalog.careflow_silver.observations.snapshots
ORDER BY committed_at DESC;
```

### 2.2 Executing Atomic Snapshot Rollback
In PySpark:
```python
from pipelines.spark.canonical_transform import get_spark_session

spark = get_spark_session()
clean_snapshot_id = 1234567890123456789  # Replace with known good snapshot ID
spark.sql(f"CALL careflow_catalog.system.rollback_to_snapshot('careflow_silver.observations', {clean_snapshot_id})")
```

### 2.3 Verification
Verify table count and verify that active snapshot matches `clean_snapshot_id`:
```sql
SELECT current_snapshot_id() FROM careflow_catalog.careflow_silver.observations;
```

---

## 3. PostgreSQL Metadata & Audit Ledger PITR

If the PostgreSQL catalog is damaged or compromised:
1. Stop inbound ingestion workers to prevent inconsistent writes.
2. Restore Postgres data directory from base backup.
3. Replay WAL up to target timestamp:
   ```conf
   restore_command = 'cp /var/lib/postgresql/wal_archive/%f %p'
   recovery_target_time = '2026-11-04 10:00:00 UTC'
   recovery_target_action = 'promote'
   ```
4. Run cryptographic ledger integrity verification:
   ```python
   from services.audit.audit_service import AuditService
   audit = AuditService()
   assert audit.verify_chain_integrity(), "Tampering or ledger gap detected during recovery!"
   ```

---

## 4. MinIO / S3 Object Store Recovery

If MinIO service crashes or storage volume fails:
1. Re-launch MinIO container or node:
   ```bash
   docker compose up -d minio
   ```
2. Re-initialize storage buckets if fresh volume:
   ```bash
   python infrastructure/compose/init_buckets.py
   ```
3. Re-drive pending or uncommitted manifests from raw source archive:
   ```bash
   python -m services.ingest.manifest_manager reprocess-pending
   ```
4. Verify conservation invariant:
   $$N_{\text{input}} = N_{\text{accepted}} + N_{\text{duplicate}} + N_{\text{quarantined}} + N_{\text{excluded}}$$
