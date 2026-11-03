"""CareFlow Atlas Phase 0 Smoke Test.

Verifies end-to-end:
1. PySpark 3.5 + Apache Iceberg Runtime (Scala 2.12, v1.6.1).
2. PostgreSQL JDBC Catalog (careflow_catalog on PostgreSQL 16).
3. MinIO S3 Object Store (S3FileIO warehouse in careflow-silver).
4. Iceberg format v2 table creation.
5. Ingest (append), MERGE (upsert + duplicate protection), and snapshot metadata reads.
6. Time travel / snapshot reproducibility.
"""

import os
import sys
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def build_spark_session() -> SparkSession:
    """Build PySpark session wired to Iceberg JDBC catalog and MinIO S3 storage."""
    # Ensure Java 17 options and Spark local settings
    os.environ["JAVA_HOME"] = os.environ.get(
        "JAVA_HOME",
        "/Library/Java/JavaVirtualMachines/jdk-17.jdk/Contents/Home"
    )
    os.environ["AWS_REGION"] = "us-east-1"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
    os.environ["AWS_ACCESS_KEY_ID"] = "minioadmin"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "miniopassword"

    builder = (
        SparkSession.builder
        .appName("CareFlow-Atlas-Phase0-SmokeTest")
        .master("local[2]")
        .config(
            "spark.jars.packages",
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,"
            "org.postgresql:postgresql:42.7.3,"
            "org.apache.iceberg:iceberg-aws-bundle:1.6.1"
        )
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.defaultCatalog", "careflow_catalog")
        .config("spark.sql.catalog.careflow_catalog", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.careflow_catalog.type", "jdbc")
        .config("spark.sql.catalog.careflow_catalog.uri", "jdbc:postgresql://localhost:5432/careflow_metadata")
        .config("spark.sql.catalog.careflow_catalog.jdbc.user", "careflow_admin")
        .config("spark.sql.catalog.careflow_catalog.jdbc.password", "careflow_dev_password")
        .config("spark.sql.catalog.careflow_catalog.jdbc.schema-version", "V1")
        .config("spark.sql.catalog.careflow_catalog.warehouse", "s3://careflow-silver/iceberg_warehouse")
        .config("spark.sql.catalog.careflow_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.careflow_catalog.s3.endpoint", "http://localhost:9000")
        .config("spark.sql.catalog.careflow_catalog.s3.path-style-access", "true")
        .config("spark.sql.catalog.careflow_catalog.s3.access-key-id", "minioadmin")
        .config("spark.sql.catalog.careflow_catalog.s3.secret-access-key", "miniopassword")
        .config("spark.sql.catalog.careflow_catalog.client.region", "us-east-1")
        .config("spark.sql.catalog.careflow_catalog.s3.region", "us-east-1")

        # JVM reflection permissions for Java 17
        .config("spark.driver.extraJavaOptions", "-Dderby.system.home=/tmp/derby --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.nio.cs=ALL-UNNAMED --add-opens=java.base/sun.security.action=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED")
    )
    return builder.getOrCreate()


def run_smoke_test():
    print("=" * 70)
    print("CareFlow Atlas Phase 0 Storage & Processing Smoke Test")
    print("=" * 70)

    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        # 1. Namespace Creation
        print("\n[Step 1] Creating Iceberg namespace: careflow_silver...")
        spark.sql("CREATE NAMESPACE IF NOT EXISTS careflow_catalog.careflow_silver")

        # 2. Table Creation (Iceberg format v2)
        table_name = "careflow_catalog.careflow_silver.smoke_observations"
        print(f"\n[Step 2] Creating Iceberg v2 table: {table_name}...")
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")
        spark.sql(f"""
            CREATE TABLE {table_name} (
                provider STRING,
                patient_key STRING,
                encounter_key STRING,
                source_key STRING,
                sequence BIGINT,
                effective_time TIMESTAMP,
                original_code STRING,
                original_value STRING,
                original_unit STRING,
                normalized_concept STRING,
                normalized_value DOUBLE,
                normalized_unit STRING,
                normalization_version STRING,
                supersession_state STRING
            )
            USING iceberg
            PARTITIONED BY (provider)
            TBLPROPERTIES (
                'format-version' = '2',
                'write.delete.mode' = 'merge-on-read',
                'write.update.mode' = 'merge-on-read'
            )
        """)
        print(f"Table {table_name} created successfully.")

        # 3. Ingest Batch 1 (Initial Append)
        print("\n[Step 3] Ingesting initial batch of observations (Sequence 1)...")
        now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        spark.sql(f"""
            INSERT INTO {table_name} VALUES
            ('provider-a', 'pat-1', 'enc-1', 'obs-001', 1, TIMESTAMP '{now}', 'LOCAL_LAB_017', '12.5', 'mmol/L', 'reviewed-catalog/concept-017', 12.5, 'mmol/L', '1.0.0', 'current'),
            ('provider-a', 'pat-2', 'enc-2', 'obs-002', 1, TIMESTAMP '{now}', 'LOCAL_LAB_017', '8.0', 'mmol/L', 'reviewed-catalog/concept-017', 8.0, 'mmol/L', '1.0.0', 'current')
        """)

        count_after_batch1 = spark.sql(f"SELECT COUNT(*) as cnt FROM {table_name}").collect()[0]["cnt"]
        print(f"Count after Batch 1: {count_after_batch1}")
        assert count_after_batch1 == 2, f"Expected 2 rows, found {count_after_batch1}"

        # Inspect Snapshot 1
        snapshots_df = spark.sql(f"SELECT snapshot_id, parent_id, operation, summary FROM {table_name}.snapshots").collect()
        assert len(snapshots_df) >= 1, "Expected at least 1 snapshot"
        snap1_id = snapshots_df[0]["snapshot_id"]
        print(f"Committed Snapshot 1 ID: {snap1_id} (Operation: {snapshots_df[0]['operation']})")

        # 4. Merge Batch 2 (Upsert + Duplicate Protection)
        print("\n[Step 4] Applying MERGE operation with updated sequence and duplicate...")
        # Staging DataFrame with:
        # - obs-001 updated to sequence 2, normalized_value 13.0 (Update)
        # - obs-002 with sequence 1 (Duplicate -> must NOT update or duplicate)
        # - obs-003 with sequence 1 (Insert)
        staging_data = [
            ("provider-a", "pat-1", "enc-1", "obs-001", 2, datetime.utcnow(), "LOCAL_LAB_017", "13.0", "mmol/L", "reviewed-catalog/concept-017", 13.0, "mmol/L", "1.0.0", "current"),
            ("provider-a", "pat-2", "enc-2", "obs-002", 1, datetime.utcnow(), "LOCAL_LAB_017", "8.0", "mmol/L", "reviewed-catalog/concept-017", 8.0, "mmol/L", "1.0.0", "current"),
            ("provider-a", "pat-3", "enc-3", "obs-003", 1, datetime.utcnow(), "LOCAL_LAB_017", "15.2", "mmol/L", "reviewed-catalog/concept-017", 15.2, "mmol/L", "1.0.0", "current"),
        ]
        staging_df = spark.createDataFrame(
            staging_data,
            schema="provider STRING, patient_key STRING, encounter_key STRING, source_key STRING, sequence BIGINT, effective_time TIMESTAMP, original_code STRING, original_value STRING, original_unit STRING, normalized_concept STRING, normalized_value DOUBLE, normalized_unit STRING, normalization_version STRING, supersession_state STRING"
        )
        staging_df.createOrReplaceTempView("staging_updates")

        merge_sql = f"""
            MERGE INTO {table_name} t
            USING staging_updates s
            ON t.provider = s.provider AND t.source_key = s.source_key
            WHEN MATCHED AND s.sequence > t.sequence THEN
                UPDATE SET
                    t.sequence = s.sequence,
                    t.normalized_value = s.normalized_value,
                    t.effective_time = s.effective_time,
                    t.supersession_state = s.supersession_state
            WHEN NOT MATCHED THEN
                INSERT (
                    provider, patient_key, encounter_key, source_key, sequence, effective_time,
                    original_code, original_value, original_unit, normalized_concept, normalized_value,
                    normalized_unit, normalization_version, supersession_state
                )
                VALUES (
                    s.provider, s.patient_key, s.encounter_key, s.source_key, s.sequence, s.effective_time,
                    s.original_code, s.original_value, s.original_unit, s.normalized_concept, s.normalized_value,
                    s.normalized_unit, s.normalization_version, s.supersession_state
                )
        """
        spark.sql(merge_sql)

        # Invariant Verification after MERGE
        print("\n[Step 5] Verifying deduplication and monotonic sequence invariants...")
        current_rows = spark.sql(f"SELECT source_key, sequence, normalized_value FROM {table_name} ORDER BY source_key").collect()
        print("Logical current rows in Iceberg table:")
        for r in current_rows:
            print(f"  {r['source_key']}: sequence={r['sequence']}, normalized_value={r['normalized_value']}")

        assert len(current_rows) == 3, f"Expected exactly 3 logical rows, got {len(current_rows)}"
        row_map = {r["source_key"]: r for r in current_rows}
        assert row_map["obs-001"]["sequence"] == 2 and row_map["obs-001"]["normalized_value"] == 13.0, "obs-001 was not updated to sequence 2"
        assert row_map["obs-002"]["sequence"] == 1 and row_map["obs-002"]["normalized_value"] == 8.0, "obs-002 was altered unexpectedly"
        assert row_map["obs-003"]["sequence"] == 1 and row_map["obs-003"]["normalized_value"] == 15.2, "obs-003 was not inserted"

        # Inspect Snapshot 2
        snapshots_df = spark.sql(f"SELECT snapshot_id, parent_id, operation, summary FROM {table_name}.snapshots ORDER BY committed_at DESC").collect()
        snap2_id = snapshots_df[0]["snapshot_id"]
        print(f"\nCommitted Snapshot 2 ID: {snap2_id} (Parent: {snapshots_df[0]['parent_id']}, Operation: {snapshots_df[0]['operation']})")
        assert snap2_id != snap1_id, "Snapshot ID must change after MERGE"

        # 5. Time-Travel Verification (Reproducibility)
        print("\n[Step 6] Verifying Snapshot Time Travel & Release Reproducibility...")
        snap1_view = spark.read.option("snapshot-id", snap1_id).table(table_name)
        snap1_rows = {r["source_key"]: r for r in snap1_view.collect()}
        assert len(snap1_rows) == 2, f"Snapshot 1 must contain exactly 2 rows, got {len(snap1_rows)}"
        assert snap1_rows["obs-001"]["sequence"] == 1 and snap1_rows["obs-001"]["normalized_value"] == 12.5, "Snapshot 1 historical state was not preserved"
        print(f"Verified Snapshot 1 ({snap1_id}): obs-001 sequence=1, normalized_value=12.5 (Preserved!)")

        snap2_view = spark.read.option("snapshot-id", snap2_id).table(table_name)
        snap2_rows = {r["source_key"]: r for r in snap2_view.collect()}
        assert len(snap2_rows) == 3, f"Snapshot 2 must contain exactly 3 rows, got {len(snap2_rows)}"
        assert snap2_rows["obs-001"]["sequence"] == 2 and snap2_rows["obs-001"]["normalized_value"] == 13.0, "Snapshot 2 state mismatch"
        print(f"Verified Snapshot 2 ({snap2_id}): obs-001 sequence=2, normalized_value=13.0 (Updated!)")

        print("\n" + "=" * 70)
        print("ALL PHASE 0 STORAGE & PROCESSING GATES PASSED SUCCESSFULLY!")
        print("=" * 70)

    finally:
        spark.stop()


if __name__ == "__main__":
    run_smoke_test()
