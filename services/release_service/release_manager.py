"""Release Manager for multi-table research releases."""

import json
from datetime import datetime
from typing import Any, Dict, Optional
import psycopg2
from psycopg2.extras import RealDictCursor

from contracts import StudyRelease


class ReleaseManager:
    """Publishes and resolves immutable multi-table research snapshot bundles (ADR-0005)."""

    def __init__(
        self,
        db_conn_str: str = "postgresql://careflow_admin:careflow_dev_password@localhost:5432/careflow_metadata",
    ):
        self.db_conn_str = db_conn_str

    def _get_db(self):
        return psycopg2.connect(self.db_conn_str, cursor_factory=RealDictCursor)

    def publish_release(self, release: StudyRelease) -> StudyRelease:
        """Publish a validated, immutable study release manifest.
        
        Section 9 Multi-table release:
        'Record each required snapshot ID, validate the bundle, write a release manifest,
        then conditionally update its active pointer.'
        """
        with self._get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO careflow_control.study_releases (
                        release_id, study_id, owner, cohort_digest, fixed_reference_date,
                        provider_table_snapshot_bundle, quality_privacy_versions, code_digest,
                        output_checksums, active_state, created_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        release.release_id,
                        release.study_id,
                        release.owner,
                        release.cohort_digest,
                        release.fixed_reference_date,
                        json.dumps(release.provider_table_snapshot_bundle),
                        json.dumps(release.quality_privacy_versions),
                        release.code_digest,
                        json.dumps(release.output_checksums),
                        release.active_state,
                        release.created_at,
                    ),
                )
                conn.commit()
        return release

    def get_release(self, release_id: str) -> Optional[StudyRelease]:
        """Fetch immutable study release by ID."""
        with self._get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT * FROM careflow_control.study_releases
                    WHERE release_id = %s
                    """,
                    (release_id,),
                )
                row = cur.fetchone()
                if not row:
                    return None
                return StudyRelease(
                    release_id=row["release_id"],
                    study_id=row["study_id"],
                    owner=row["owner"],
                    cohort_digest=row["cohort_digest"],
                    fixed_reference_date=row["fixed_reference_date"],
                    provider_table_snapshot_bundle=row["provider_table_snapshot_bundle"],
                    quality_privacy_versions=row["quality_privacy_versions"],
                    code_digest=row["code_digest"],
                    output_checksums=row["output_checksums"],
                    active_state=row["active_state"],
                    created_at=row["created_at"],
                    revoked_at=row.get("revoked_at"),
                    revocation_reason=row.get("revocation_reason"),
                )

    def revoke_release(self, release_id: str, reason: str) -> bool:
        """Revoke active status of release (Section 13 Deletion & Revocation)."""
        with self._get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE careflow_control.study_releases
                    SET active_state = 'revoked', revoked_at = CURRENT_TIMESTAMP, revocation_reason = %s
                    WHERE release_id = %s AND active_state = 'active'
                    """,
                    (reason, release_id),
                )
                affected = cur.rowcount
                conn.commit()
                return affected > 0
