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

    def compare_releases(self, release_a: StudyRelease, release_b: StudyRelease) -> Dict[str, Any]:
        """Compare two study releases and report delta across snapshots and checksums."""
        snapshot_diff = {}
        all_providers = set(release_a.provider_table_snapshot_bundle.keys()) | set(release_b.provider_table_snapshot_bundle.keys())
        for p in all_providers:
            bundle_a = release_a.provider_table_snapshot_bundle.get(p, {})
            bundle_b = release_b.provider_table_snapshot_bundle.get(p, {})
            if isinstance(bundle_a, dict) and isinstance(bundle_b, dict):
                all_tables = set(bundle_a.keys()) | set(bundle_b.keys())
                for tbl in all_tables:
                    s_a = bundle_a.get(tbl)
                    s_b = bundle_b.get(tbl)
                    if s_a != s_b:
                        snapshot_diff[f"{p}.{tbl}"] = {"previous_snapshot": s_a, "current_snapshot": s_b}
                        snapshot_diff[tbl] = {"previous_snapshot": s_a, "current_snapshot": s_b}
            else:
                if bundle_a != bundle_b:
                    snapshot_diff[p] = {"previous_snapshot": bundle_a, "current_snapshot": bundle_b}

        checksum_diff = {
            "release_a_digest": release_a.cohort_digest,
            "release_b_digest": release_b.cohort_digest,
            "digest_changed": release_a.cohort_digest != release_b.cohort_digest,
            "files_a": list(release_a.output_checksums.keys()),
            "files_b": list(release_b.output_checksums.keys()),
        }

        return {
            "base_release_id": release_a.release_id,
            "target_release_id": release_b.release_id,
            "snapshot_differences": snapshot_diff,
            "checksum_differences": checksum_diff,
            "comparison_state": "materially_updated" if checksum_diff["digest_changed"] else "identical",
        }

