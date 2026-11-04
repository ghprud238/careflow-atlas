"""Cryptographic Audit Ledger Service.

Section 8 & 14 Invariants:
- Payload-free audit trail: zero Protected Health Information (PHI), patient IDs, or raw payloads.
- Cryptographic hash chaining: every event hashes the previous block's hash and its canonical payload.
- Tamper-detection: verify_chain_integrity recomputes the chain from genesis and detects insertions,
  modifications, or deletions immediately.
"""

import hashlib
import json
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
import psycopg2
from psycopg2.extras import RealDictCursor

from contracts import AuditEvent, ContractViolationError

GENESIS_HASH = "0" * 64


class AuditService:
    """Manages immutable, cryptographically chained audit events."""

    def __init__(
        self,
        db_conn_str: str = "postgresql://careflow_admin:careflow_dev_password@localhost:5432/careflow_metadata",
    ):
        self.db_conn_str = db_conn_str

    def _get_db(self):
        return psycopg2.connect(self.db_conn_str, cursor_factory=RealDictCursor)

    def _compute_entry_hash(self, previous_hash: str, event_dict: Dict[str, Any]) -> str:
        """Compute stable SHA-256 hash chaining previous hash and canonical event payload."""
        payload_to_hash = {
            "previous_hash": previous_hash,
            "actor_or_service": event_dict["actor_or_service"],
            "action": event_dict["action"],
            "resource": event_dict["resource"],
            "scope": event_dict["scope"],
            "outcome": event_dict["outcome"],
            "correlation_id": event_dict["correlation_id"],
            "details": event_dict.get("metadata", event_dict.get("details", {})),
        }
        canonical_bytes = json.dumps(payload_to_hash, sort_keys=True).encode("utf-8")
        return hashlib.sha256(canonical_bytes).hexdigest()

    def log_event(self, event: AuditEvent) -> Dict[str, Any]:
        """Validate and append a cryptographically chained audit event."""
        event_dict = event.model_dump()
        event_id = uuid.uuid4()

        with self._get_db() as conn:
            with conn.cursor() as cur:
                # 1. Fetch latest entry hash
                cur.execute(
                    """
                    SELECT entry_hash FROM careflow_audit.chained_audit_ledger
                    ORDER BY sequence_num DESC LIMIT 1
                    """
                )
                last_row = cur.fetchone()
                prev_hash = last_row["entry_hash"] if last_row else GENESIS_HASH

                # 2. Compute new entry hash
                new_entry_hash = self._compute_entry_hash(prev_hash, event_dict)

                # 3. Insert into chained audit ledger
                cur.execute(
                    """
                    INSERT INTO careflow_audit.chained_audit_ledger (
                        event_id, actor_or_service, action, resource, scope,
                        outcome, correlation_id, previous_hash, entry_hash, details, created_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING sequence_num, entry_hash, created_at
                    """,
                    (
                        str(event_id),
                        event.actor_or_service,
                        event.action,
                        event.resource,
                        event.scope,
                        event.outcome,
                        event.correlation_id,
                        prev_hash,
                        new_entry_hash,
                        json.dumps(event.metadata),
                        event.timestamp,
                    ),
                )
                res = cur.fetchone()
                conn.commit()

                return {
                    "sequence_num": res["sequence_num"],
                    "event_id": str(event_id),
                    "entry_hash": res["entry_hash"],
                    "previous_hash": prev_hash,
                }

    def verify_chain_integrity(self) -> Tuple[bool, List[str]]:
        """Verify the complete cryptographic hash chain from genesis.
        
        Returns:
            (is_valid, list_of_violations)
        """
        violations: List[str] = []

        with self._get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT sequence_num, event_id, actor_or_service, action, resource,
                           scope, outcome, correlation_id, previous_hash, entry_hash, details
                    FROM careflow_audit.chained_audit_ledger
                    ORDER BY sequence_num ASC
                    """
                )
                rows = cur.fetchall()

        if not rows:
            return True, []

        expected_prev_hash = GENESIS_HASH

        for i, row in enumerate(rows):
            seq = row["sequence_num"]
            actual_prev = row["previous_hash"]
            actual_entry = row["entry_hash"]

            # 1. Verify previous hash link
            if actual_prev != expected_prev_hash:
                violations.append(
                    f"Broken link at sequence {seq}: previous_hash '{actual_prev}' != expected '{expected_prev_hash}'"
                )

            # 2. Recompute entry hash
            recomputed = self._compute_entry_hash(expected_prev_hash, row)
            if actual_entry != recomputed:
                violations.append(
                    f"Tamper detected at sequence {seq}: entry_hash '{actual_entry}' != recomputed '{recomputed}'"
                )

            expected_prev_hash = actual_entry

        is_valid = len(violations) == 0
        return is_valid, violations
