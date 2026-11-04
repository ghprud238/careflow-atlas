"""Controlled Research Export Service.

Section 9 & 14 Invariants:
- Controlled export of immutable Gold research releases.
- Verifies dataset integrity against release.output_checksums.
- Attaches cryptographic HMAC recipient signature and manifest attribution.
"""

import hashlib
import hmac
import json
from datetime import datetime
from typing import Any, Dict, Optional
import boto3
from botocore.client import Config

from contracts import StudyRelease


class ControlledExportService:
    """Prepares and cryptographically attests governed study export packages."""

    def __init__(
        self,
        s3_endpoint: str = "http://localhost:9000",
        s3_access_key: str = "minioadmin",
        s3_secret_key: str = "miniopassword",
        gold_bucket: str = "careflow-gold",
        hmac_key: str = "careflow-attributable-export-secret",
    ):
        self.gold_bucket = gold_bucket
        self.hmac_key = hmac_key.encode("utf-8")
        self.s3_client = boto3.client(
            "s3",
            endpoint_url=s3_endpoint,
            aws_access_key_id=s3_access_key,
            aws_secret_access_key=s3_secret_key,
            config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
            region_name="us-east-1",
        )

    def prepare_export_package(
        self,
        release: StudyRelease,
        recipient_id: str,
        export_purpose: str = "research_analysis",
    ) -> Dict[str, Any]:
        """Validate dataset checksums and generate a cryptographic export package."""
        if release.active_state != "active":
            raise ValueError(f"Cannot export inactive or revoked release (state: {release.active_state})")

        # 1. Verify Parquet files exist in Gold Lake and match recorded checksums
        verified_files = []
        for file_key, expected_hash in release.output_checksums.items():
            if file_key.endswith(".parquet"):
                obj = self.s3_client.get_object(Bucket=self.gold_bucket, Key=file_key)
                file_bytes = obj["Body"].read()
                actual_hash = hashlib.sha256(file_bytes).hexdigest()
                if actual_hash != expected_hash:
                    raise ValueError(
                        f"Dataset corruption detected for '{file_key}': "
                        f"expected {expected_hash}, got {actual_hash}"
                    )
                verified_files.append({
                    "file_key": file_key,
                    "byte_count": len(file_bytes),
                    "sha256": actual_hash,
                })

        # 2. Compute HMAC Attestation for Recipient
        signature_payload = {
            "release_id": release.release_id,
            "study_id": release.study_id,
            "cohort_digest": release.cohort_digest,
            "recipient_id": recipient_id,
            "export_purpose": export_purpose,
        }
        sig_bytes = json.dumps(signature_payload, sort_keys=True).encode("utf-8")
        recipient_hmac = hmac.new(self.hmac_key, sig_bytes, hashlib.sha256).hexdigest()

        # 3. Formulate Export Manifest
        export_manifest = {
            "export_id": f"exp-{release.release_id[:8]}-{recipient_id[:8]}",
            "release_id": release.release_id,
            "study_id": release.study_id,
            "recipient_id": recipient_id,
            "export_purpose": export_purpose,
            "cohort_digest": release.cohort_digest,
            "verified_files": verified_files,
            "recipient_hmac_attestation": recipient_hmac,
            "exported_at": datetime.utcnow().isoformat(),
        }

        return export_manifest
