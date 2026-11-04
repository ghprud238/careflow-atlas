"""Versioned Mapping Registry for promoted clinical mapping rules."""

import json
from datetime import datetime
from typing import Dict, List, Optional
import psycopg2
from psycopg2.extras import RealDictCursor

from contracts import Approval, MappingDSL, MappingProposal, StaleApprovalError


class MappingRegistry:
    """Manages versioned, approved clinical mapping rules."""

    def __init__(
        self,
        db_conn_str: str = "postgresql://careflow_admin:careflow_dev_password@localhost:5432/careflow_metadata",
    ):
        self.db_conn_str = db_conn_str
        self._ensure_initial_version()

    def _get_db(self):
        return psycopg2.connect(self.db_conn_str, cursor_factory=RealDictCursor)

    def _ensure_initial_version(self) -> None:
        """Seed initial base version v1.0.0 if registry is empty."""
        with self._get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT registry_version FROM careflow_control.mapping_registry LIMIT 1")
                if not cur.fetchone():
                    cur.execute(
                        """
                        INSERT INTO careflow_control.mapping_registry (
                            registry_version, previous_version, rules, approval_hash, promoted_by
                        ) VALUES ('v1.0.0', NULL, '{}'::jsonb, 'initial-system-baseline', 'system')
                        """
                    )
                    conn.commit()

    def get_latest_version(self) -> str:
        """Retrieve the current active mapping registry version."""
        with self._get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT registry_version FROM careflow_control.mapping_registry
                    ORDER BY promoted_at DESC LIMIT 1
                    """
                )
                row = cur.fetchone()
                return row["registry_version"] if row else "v1.0.0"

    def promote_proposal(
        self,
        proposal: MappingProposal,
        approval: Approval,
        promoted_by: str = "mapping_executor",
    ) -> str:
        """Promote an approved proposal to a new versioned mapping release.
        
        Section 11 Governed promotion:
        'Bind full artifact hash, provider/source scope, evidence/terminology versions,
        expected registry version, authorized reviewer/role, expiry, decision.'
        """
        current_version = self.get_latest_version()
        now = datetime.utcnow()

        # Validate authorization invariants
        approval.validate_for_execution(
            target_proposal_hash=proposal.proposal_hash,
            current_registry_version=current_version,
            current_time=now,
        )

        with self._get_db() as conn:
            with conn.cursor() as cur:
                # 1. Fetch current rules dict
                cur.execute(
                    "SELECT rules FROM careflow_control.mapping_registry WHERE registry_version = %s",
                    (current_version,),
                )
                current_rules = cur.fetchone()["rules"]

                # 2. Add new approved rule
                rule_key = f"{proposal.provider_id}:{proposal.typed_dsl.match.get('local_test_code')}"
                current_rules[rule_key] = proposal.typed_dsl.model_dump()

                # 3. Increment version (e.g. v1.0.0 -> v1.1.0)
                ver_num = int(current_version.replace("v1.", "").replace(".0", "")) + 1
                next_version = f"v1.{ver_num}.0"

                # 4. Insert new registry version
                cur.execute(
                    """
                    INSERT INTO careflow_control.mapping_registry (
                        registry_version, previous_version, rules, approval_hash, promoted_by, promoted_at
                    ) VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                    """,
                    (
                        next_version,
                        current_version,
                        json.dumps(current_rules),
                        approval.proposal_hash,
                        promoted_by,
                    ),
                )
                conn.commit()
                return next_version

    def get_rule(self, provider_id: str, code: str) -> Optional[MappingDSL]:
        """Look up active mapping rule for provider and source code."""
        with self._get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT rules FROM careflow_control.mapping_registry
                    ORDER BY promoted_at DESC LIMIT 1
                    """
                )
                row = cur.fetchone()
                if not row or not row["rules"]:
                    return None
                rules = row["rules"]
                key = f"{provider_id}:{code}"
                if key in rules:
                    return MappingDSL(**rules[key])
                return None
