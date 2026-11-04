"""Manifest landing protocol and Bronze object storage manager."""

import hashlib
from datetime import datetime
from typing import List, Optional, Tuple
import boto3
from botocore.client import Config
import psycopg2
from psycopg2.extras import RealDictCursor

from contracts import (
    ConflictingDeliveryError,
    DeliveryDisposition,
    DuplicateDeliveryError,
    SourceManifest,
)


class ManifestManager:
    """Manages Bronze landing protocol, checksum verification, and manifest readiness."""

    def __init__(
        self,
        db_conn_str: str = "postgresql://careflow_admin:careflow_dev_password@localhost:5432/careflow_metadata",
        s3_endpoint: str = "http://localhost:9000",
        s3_access_key: str = "minioadmin",
        s3_secret_key: str = "miniopassword",
        bronze_bucket: str = "careflow-bronze",
    ):
        self.db_conn_str = db_conn_str
        self.bronze_bucket = bronze_bucket
        self.s3_client = boto3.client(
            "s3",
            endpoint_url=s3_endpoint,
            aws_access_key_id=s3_access_key,
            aws_secret_access_key=s3_secret_key,
            config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
            region_name="us-east-1",
        )

    def _get_db(self):
        return psycopg2.connect(self.db_conn_str, cursor_factory=RealDictCursor)

    def stage_and_register_batch(
        self,
        provider: str,
        feed: str,
        batch_id: str,
        payload_bytes: bytes,
        source_interval_start: datetime,
        source_interval_end: datetime,
        adapter_version: str = "1.0.0",
    ) -> SourceManifest:
        """Execute Section 9 Landing Protocol:
        1. Create pending manifest.
        2. Upload to staging in bronze.
        3. Verify bytes and checksum.
        4. Detect duplicate / conflict.
        5. Mark ready.
        """
        calc_hash = hashlib.sha256(payload_bytes).hexdigest()
        calc_bytes = len(payload_bytes)
        manifest_id = f"{provider}-{batch_id}"
        object_key = f"{provider}/{batch_id}.ndjson"
        object_uri = f"s3://{self.bronze_bucket}/{object_key}"

        with self._get_db() as conn:
            with conn.cursor() as cur:
                # Check for existing delivery with same provider and batch_id
                cur.execute(
                    """
                    SELECT manifest_id, provider, batch_id, object_hash, object_bytes, readiness, delivery_disposition
                    FROM careflow_control.source_manifests
                    WHERE provider = %s AND batch_id = %s
                    """,
                    (provider, batch_id),
                )
                existing = cur.fetchone()

                if existing:
                    existing_hash = existing["object_hash"]
                    if existing_hash == calc_hash:
                        # Idempotent duplicate delivery
                        return SourceManifest(
                            manifest_id=existing["manifest_id"],
                            provider=provider,
                            feed=feed,
                            batch_id=batch_id,
                            source_interval_start=source_interval_start,
                            source_interval_end=source_interval_end,
                            object_uri=object_uri,
                            object_hash=existing_hash,
                            object_bytes=existing["object_bytes"],
                            adapter_version=adapter_version,
                            readiness="ready",
                            disposition="duplicate",
                        )
                    else:
                        # Conflicting delivery: same batch ID but differing hash
                        cur.execute(
                            """
                            UPDATE careflow_control.source_manifests
                            SET delivery_disposition = 'conflict', updated_at = CURRENT_TIMESTAMP
                            WHERE provider = %s AND batch_id = %s
                            """,
                            (provider, batch_id),
                        )
                        conn.commit()
                        raise ConflictingDeliveryError(
                            f"Delivery conflict for provider '{provider}', batch '{batch_id}': "
                            f"existing hash {existing_hash} != incoming hash {calc_hash}"
                        )

                # 1. Register pending manifest
                cur.execute(
                    """
                    INSERT INTO careflow_control.source_manifests (
                        manifest_id, provider, feed, batch_id, source_interval_start, source_interval_end,
                        object_uri, object_hash, object_bytes, adapter_version, readiness, delivery_disposition
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'pending', 'accepted')
                    """,
                    (
                        manifest_id,
                        provider,
                        feed,
                        batch_id,
                        source_interval_start,
                        source_interval_end,
                        object_uri,
                        calc_hash,
                        calc_bytes,
                        adapter_version,
                    ),
                )
                conn.commit()

        # 2. Upload to Bronze storage
        self.s3_client.put_object(
            Bucket=self.bronze_bucket,
            Key=object_key,
            Body=payload_bytes,
        )

        # 3. Verify staged bytes & checksum
        s3_obj = self.s3_client.get_object(Bucket=self.bronze_bucket, Key=object_key)
        uploaded_bytes = s3_obj["Body"].read()
        uploaded_hash = hashlib.sha256(uploaded_bytes).hexdigest()

        if uploaded_hash != calc_hash or len(uploaded_bytes) != calc_bytes:
            raise ValueError(f"Staged object checksum verification failed for {object_uri}")

        # 4. Mark Ready
        with self._get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE careflow_control.source_manifests
                    SET readiness = 'ready', updated_at = CURRENT_TIMESTAMP
                    WHERE manifest_id = %s
                    """,
                    (manifest_id,),
                )
                conn.commit()

        return SourceManifest(
            manifest_id=manifest_id,
            provider=provider,
            feed=feed,
            batch_id=batch_id,
            source_interval_start=source_interval_start,
            source_interval_end=source_interval_end,
            object_uri=object_uri,
            object_hash=calc_hash,
            object_bytes=calc_bytes,
            adapter_version=adapter_version,
            readiness="ready",
            disposition="accepted",
        )

    def get_ready_manifest(self, manifest_id: str) -> Optional[SourceManifest]:
        """Fetch a single verified ready manifest."""
        with self._get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT * FROM careflow_control.source_manifests
                    WHERE manifest_id = %s AND readiness = 'ready'
                    """,
                    (manifest_id,),
                )
                row = cur.fetchone()
                if not row:
                    return None
                return SourceManifest(
                    manifest_id=row["manifest_id"],
                    provider=row["provider"],
                    feed=row["feed"],
                    batch_id=row["batch_id"],
                    source_interval_start=row["source_interval_start"],
                    source_interval_end=row["source_interval_end"],
                    object_uri=row["object_uri"],
                    object_hash=row["object_hash"],
                    object_bytes=row["object_bytes"],
                    adapter_version=row["adapter_version"],
                    readiness=row["readiness"],
                    disposition=row["delivery_disposition"],
                )

    def fetch_staged_payload(self, object_uri: str) -> bytes:
        """Download staged object bytes from bronze storage."""
        # e.g. s3://careflow-bronze/provider-a/batch.ndjson
        parts = object_uri.replace("s3://", "").split("/", 1)
        bucket = parts[0]
        key = parts[1]
        obj = self.s3_client.get_object(Bucket=bucket, Key=key)
        return obj["Body"].read()
