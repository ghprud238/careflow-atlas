"""Research Cohort DSL Evaluation & Materialization Job (Section 12).

Evaluates declarative cohort criteria over pinned Iceberg snapshots:
- Adults (>= 18) as of fixed reference date.
- Latest eligible observation within explicit lookback window.
- Deterministic tie-breaking (effective_time DESC, source_key ASC).
- Materializes gold research release with output checksums and release manifest.
"""

import hashlib
import io
import json
import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, Optional, Tuple
import boto3
from botocore.client import Config
import pyarrow as pa
import pyarrow.parquet as pq
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from contracts import StudyRelease
from pipelines.spark.canonical_transform import get_spark_session
from services.release_service.release_manager import ReleaseManager


class CohortJob:
    """Executes asynchronous research cohort selection against pinned table snapshot bundles."""

    def __init__(
        self,
        spark: Optional[SparkSession] = None,
        s3_endpoint: str = "http://localhost:9000",
        s3_access_key: str = "minioadmin",
        s3_secret_key: str = "miniopassword",
        gold_bucket: str = "careflow-gold",
    ):
        self.spark = spark or get_spark_session()
        self.release_mgr = ReleaseManager()
        self.gold_bucket = gold_bucket
        self.s3_client = boto3.client(
            "s3",
            endpoint_url=s3_endpoint,
            aws_access_key_id=s3_access_key,
            aws_secret_access_key=s3_secret_key,
            config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
            region_name="us-east-1",
        )

    def run_cohort_study(
        self,
        study_id: str,
        owner: str,
        provider: str,
        snapshot_bundle: Dict[str, Dict[str, int]],
        target_concept: str = "reviewed-catalog/concept-glucose",
        fixed_reference_date: str = "2026-11-01",
        lookback_days: int = 365,
        min_age: int = 18,
    ) -> StudyRelease:
        """Execute Section 12 Research Cohort Selection."""
        release_id = f"rel-{uuid.uuid4().hex[:12]}"

        # 1. Resolve Pinned Snapshot IDs
        prov_snaps = snapshot_bundle.get(provider, {})
        pat_snap = prov_snaps.get("patients")
        obs_snap = prov_snaps.get("observations")

        if not pat_snap or not obs_snap:
            raise ValueError(f"Snapshot bundle missing required tables for {provider}: {prov_snaps}")

        # 2. Read exact pinned snapshots
        pat_df = (
            self.spark.read.option("snapshot-id", pat_snap)
            .table("careflow_catalog.careflow_silver.patients")
            .filter(F.col("provider") == provider)
        )
        obs_df = (
            self.spark.read.option("snapshot-id", obs_snap)
            .table("careflow_catalog.careflow_silver.observations")
            .filter(F.col("provider") == provider)
        )

        # 3. Apply Adult Age Filter as of fixed reference date
        ref_dt = datetime.strptime(fixed_reference_date, "%Y-%m-%d")
        cutoff_birth_date = (ref_dt - timedelta(days=min_age * 365)).strftime("%Y-%m-%d")

        adults_df = pat_df.filter(
            (F.col("birth_date").isNotNull()) & (F.col("birth_date") <= cutoff_birth_date)
        ).select(
            F.col("patient_key"),
            F.col("birth_date"),
            F.col("gender"),
            F.col("postal_code"),
        )

        # 4. Filter Observations & Resolve Latest per Patient (Deterministic Tie-breaking)
        lookback_start = (ref_dt - timedelta(days=lookback_days))

        target_obs_df = obs_df.filter(
            (F.col("normalized_concept") == target_concept)
            & (F.col("effective_time") >= lookback_start)
            & (F.col("effective_time") <= ref_dt)
            & (F.col("supersession_state") == "current")
            & (F.col("normalized_value").isNotNull())
        )

        # Window: latest effective_time, tie-break by source_key ASC
        w = Window.partitionBy("patient_key").orderBy(F.col("effective_time").desc(), F.col("source_key").asc())
        latest_obs_df = (
            target_obs_df.withColumn("rn", F.row_number().over(w))
            .filter(F.col("rn") == 1)
            .select(
                F.col("patient_key"),
                F.col("source_key").alias("observation_id"),
                F.col("effective_time").alias("observation_time"),
                F.col("normalized_concept"),
                F.col("normalized_value"),
                F.col("normalized_unit"),
            )
        )

        # 5. Join Cohort
        cohort_df = adults_df.join(latest_obs_df, on="patient_key", how="inner")
        rows = [r.asDict() for r in cohort_df.collect()]
        cohort_count = len(rows)

        # 6. Materialize Parquet Dataset via PyArrow and upload to MinIO Gold Lake
        output_key = f"releases/{release_id}/cohort_dataset.parquet"
        if rows:
            pa_table = pa.Table.from_pylist(rows)
            buf = io.BytesIO()
            pq.write_table(pa_table, buf)
            buf.seek(0)
            parquet_bytes = buf.read()
        else:
            parquet_bytes = b""

        self.s3_client.put_object(
            Bucket=self.gold_bucket,
            Key=output_key,
            Body=parquet_bytes,
        )

        # 7. Compute Attributable Output Checksum and Cohort Digest
        file_checksum = hashlib.sha256(parquet_bytes).hexdigest()
        digest_bytes = json.dumps(rows, default=str, sort_keys=True).encode("utf-8")
        cohort_digest = hashlib.sha256(digest_bytes).hexdigest()

        output_checksums = {
            output_key: file_checksum,
            "cohort_member_count": str(cohort_count),
        }

        # 8. Construct and Publish StudyRelease Manifest
        release = StudyRelease(
            release_id=release_id,
            study_id=study_id,
            owner=owner,
            cohort_digest=cohort_digest,
            fixed_reference_date=fixed_reference_date,
            provider_table_snapshot_bundle=snapshot_bundle,
            quality_privacy_versions={
                "privacy_rules": "hipaa_toy_suppression_v1",
                "quality_policy": "phase1_strict_v1",
            },
            code_digest="git-phase-1-cohort-job",
            output_checksums=output_checksums,
            active_state="active",
        )

        self.release_mgr.publish_release(release)
        return release
