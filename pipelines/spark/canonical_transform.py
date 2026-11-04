"""Canonical Spark batch transformation job for Silver Iceberg layer.

Implements Section 9 ETL and publication semantics:
- Reads verified ready manifest from Bronze store.
- Parses and validates FHIR R4 resources via FHIRAdapter.
- Enforces parsed-resource Conservation invariant.
- Persists quarantined findings to restricted quarantine store.
- Deduplicates incoming changes before deterministic MERGE INTO Silver tables.
- Emits committed snapshot IDs across clinical tables.
"""

import json
import os
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from contracts import (
    CanonicalEncounter,
    CanonicalObservation,
    CanonicalPatient,
    ConservationAccounting,
    PipelineRun,
    SourceManifest,
)
from pipelines.adapters.csv.csv_adapter import CSVAdapter
from pipelines.adapters.fhir.fhir_adapter import FHIRAdapter
from services.ingest.manifest_manager import ManifestManager


def get_spark_session() -> SparkSession:
    """Initialize or retrieve PySpark session configured for Iceberg and MinIO."""
    os.environ["JAVA_HOME"] = os.environ.get(
        "JAVA_HOME",
        "/Library/Java/JavaVirtualMachines/jdk-17.jdk/Contents/Home"
    )
    os.environ["AWS_REGION"] = "us-east-1"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
    os.environ["AWS_ACCESS_KEY_ID"] = "minioadmin"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "miniopassword"

    return (
        SparkSession.builder
        .appName("CareFlow-Atlas-Canonical-ETL")
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
        .config("spark.driver.extraJavaOptions", "-Dderby.system.home=/tmp/derby --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.nio.cs=ALL-UNNAMED --add-opens=java.base/sun.security.action=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED")
        .getOrCreate()
    )


class CanonicalPipeline:
    """Executes deterministic validation, normalization, and merge into Iceberg Silver tables."""

    def __init__(self, spark: Optional[SparkSession] = None):
        self.spark = spark or get_spark_session()
        self.manifest_mgr = ManifestManager()
        self._ensure_silver_tables()

    def _ensure_silver_tables(self) -> None:
        """Create canonical Iceberg v2 tables if they do not exist."""
        self.spark.sql("CREATE NAMESPACE IF NOT EXISTS careflow_catalog.careflow_silver")
        self.spark.sql("CREATE NAMESPACE IF NOT EXISTS careflow_catalog.careflow_quarantine")

        # Patients Table
        self.spark.sql("""
            CREATE TABLE IF NOT EXISTS careflow_catalog.careflow_silver.patients (
                provider STRING,
                patient_key STRING,
                source_resource_id STRING,
                sequence BIGINT,
                birth_date STRING,
                gender STRING,
                postal_code STRING,
                deceased_boolean BOOLEAN,
                manifest_id STRING,
                run_id STRING,
                supersession_state STRING
            )
            USING iceberg
            PARTITIONED BY (provider)
            TBLPROPERTIES ('format-version'='2', 'write.delete.mode'='merge-on-read', 'write.update.mode'='merge-on-read')
        """)

        # Encounters Table
        self.spark.sql("""
            CREATE TABLE IF NOT EXISTS careflow_catalog.careflow_silver.encounters (
                provider STRING,
                encounter_key STRING,
                patient_key STRING,
                source_resource_id STRING,
                sequence BIGINT,
                class_code STRING,
                period_start TIMESTAMP,
                period_end TIMESTAMP,
                status STRING,
                manifest_id STRING,
                run_id STRING,
                supersession_state STRING
            )
            USING iceberg
            PARTITIONED BY (provider)
            TBLPROPERTIES ('format-version'='2', 'write.delete.mode'='merge-on-read', 'write.update.mode'='merge-on-read')
        """)

        # Observations Table
        self.spark.sql("""
            CREATE TABLE IF NOT EXISTS careflow_catalog.careflow_silver.observations (
                provider STRING,
                patient_key STRING,
                encounter_key STRING,
                source_key STRING,
                sequence BIGINT,
                effective_time TIMESTAMP,
                original_code STRING,
                original_coding_system STRING,
                original_text STRING,
                original_value STRING,
                original_unit STRING,
                original_status STRING,
                normalized_concept STRING,
                normalized_value DOUBLE,
                normalized_unit STRING,
                mapping_reference STRING,
                normalization_version STRING,
                manifest_id STRING,
                run_id STRING,
                supersession_state STRING
            )
            USING iceberg
            PARTITIONED BY (provider)
            TBLPROPERTIES ('format-version'='2', 'write.delete.mode'='merge-on-read', 'write.update.mode'='merge-on-read')
        """)

        # Quarantine Table
        self.spark.sql("""
            CREATE TABLE IF NOT EXISTS careflow_catalog.careflow_quarantine.quarantined_records (
                provider STRING,
                manifest_id STRING,
                run_id STRING,
                resource_type STRING,
                source_resource_id STRING,
                findings STRING,
                payload_hash STRING,
                quarantined_at TIMESTAMP
            )
            USING iceberg
            PARTITIONED BY (provider)
            TBLPROPERTIES ('format-version'='2')
        """)

    def execute_manifest_ingest(self, manifest_id: str, run_id: str) -> Tuple[PipelineRun, ConservationAccounting]:
        """Process exact ready manifest into canonical Silver tables."""
        manifest = self.manifest_mgr.get_ready_manifest(manifest_id)
        if not manifest:
            raise ValueError(f"Manifest '{manifest_id}' not found or not in ready state")

        payload_bytes = self.manifest_mgr.fetch_staged_payload(manifest.object_uri)
        payload_str = payload_bytes.decode("utf-8").strip()

        envelopes = []

        # Route to appropriate adapter based on feed and provider
        if manifest.feed in ["csv", "pipe", "delimited"] or manifest.provider == "provider-b":
            csv_adapter = CSVAdapter(provider=manifest.provider)
            try:
                parsed_json = json.loads(payload_str)
                if isinstance(parsed_json, dict) and any(k.endswith(".csv") for k in parsed_json.keys()):
                    if "patients.csv" in parsed_json:
                        envelopes.extend(csv_adapter.parse_csv_content(parsed_json["patients.csv"], "Patient", manifest_id, manifest.object_uri))
                    if "encounters.csv" in parsed_json:
                        envelopes.extend(csv_adapter.parse_csv_content(parsed_json["encounters.csv"], "Encounter", manifest_id, manifest.object_uri))
                    if "observations.csv" in parsed_json:
                        envelopes.extend(csv_adapter.parse_csv_content(parsed_json["observations.csv"], "Observation", manifest_id, manifest.object_uri))
                else:
                    envelopes.extend(csv_adapter.parse_csv_content(payload_str, "Observation", manifest_id, manifest.object_uri))
            except json.JSONDecodeError:
                envelopes.extend(csv_adapter.parse_csv_content(payload_str, "Observation", manifest_id, manifest.object_uri))
            adapter = csv_adapter
        else:
            adapter = FHIRAdapter(provider=manifest.provider)
            lines = [line.strip() for line in payload_str.split("\n") if line.strip()]
            for line in lines:
                raw_res = json.loads(line)
                env = adapter.parse_resource_dict(raw_res, manifest_id=manifest_id, payload_uri=manifest.object_uri)
                envelopes.append(env)

        n_input = len(envelopes)
        quarantined = [e for e in envelopes if e.disposition == "quarantined"]
        accepted_candidates = [e for e in envelopes if e.disposition == "accepted"]

        # Deduplicate within batch by (provider, resource_type, source_resource_id, change_sequence)
        seen_keys = set()
        accepted = []
        duplicates = []

        for e in accepted_candidates:
            key = (e.provider, e.resource_type, e.source_resource_id, e.change_sequence)
            if key in seen_keys:
                duplicates.append(e)
            else:
                seen_keys.add(key)
                accepted.append(e)

        n_accepted = len(accepted)
        n_duplicate = len(duplicates)
        n_quarantined = len(quarantined)
        n_excluded = 0

        # Verify Conservation Invariant
        accounting = ConservationAccounting(
            n_input=n_input,
            n_accepted=n_accepted,
            n_duplicate=n_duplicate,
            n_quarantined=n_quarantined,
            n_intentionally_excluded=n_excluded,
        )

        # 1. Persist Quarantined Records
        if quarantined:
            quarantine_rows = [
                (
                    e.provider,
                    manifest_id,
                    run_id,
                    e.resource_type,
                    e.source_resource_id,
                    "; ".join(e.quarantine_reasons),
                    e.payload_hash,
                    datetime.utcnow(),
                )
                for e in quarantined
            ]
            q_schema = StructType([
                StructField("provider", StringType(), False),
                StructField("manifest_id", StringType(), False),
                StructField("run_id", StringType(), False),
                StructField("resource_type", StringType(), False),
                StructField("source_resource_id", StringType(), False),
                StructField("findings", StringType(), False),
                StructField("payload_hash", StringType(), False),
                StructField("quarantined_at", TimestampType(), False),
            ])
            q_df = self.spark.createDataFrame(quarantine_rows, schema=q_schema)
            q_df.write.format("iceberg").mode("append").save("careflow_catalog.careflow_quarantine.quarantined_records")

        # 2. Canonical Transformations & MERGE INTO
        # Patients
        pat_records = [
            adapter.to_canonical_patient(e, run_id=run_id, manifest_id=manifest_id)
            for e in accepted if e.resource_type == "Patient"
        ]
        pat_records = [p for p in pat_records if p is not None]
        if pat_records:
            self._merge_patients(pat_records)

        # Encounters
        enc_records = [
            adapter.to_canonical_encounter(e, run_id=run_id, manifest_id=manifest_id)
            for e in accepted if e.resource_type == "Encounter"
        ]
        enc_records = [enc for enc in enc_records if enc is not None]
        if enc_records:
            self._merge_encounters(enc_records)

        # Observations
        obs_records = [
            adapter.to_canonical_observation(e, run_id=run_id, manifest_id=manifest_id)
            for e in accepted if e.resource_type == "Observation"
        ]
        obs_records = [o for o in obs_records if o is not None]
        if obs_records:
            self._merge_observations(obs_records)

        # 3. Retrieve Latest Committed Snapshot IDs
        committed_snapshots = {}
        for tbl in ["patients", "encounters", "observations"]:
            snaps = self.spark.sql(f"SELECT snapshot_id FROM careflow_catalog.careflow_silver.{tbl}.snapshots ORDER BY committed_at DESC LIMIT 1").collect()
            if snaps:
                committed_snapshots[tbl] = int(snaps[0]["snapshot_id"])

        pipeline_run = PipelineRun(
            run_id=run_id,
            manifest_digest=manifest.object_hash,
            pinned_schema_version="fhir-r4-1.0.0",
            pinned_mapping_version="reviewed-catalog-1.0.0",
            pinned_config_version="config-1.0.0",
            pinned_code_version="git-phase-1",
            expected_output_scope=f"{manifest.provider}:silver",
            state="completed_with_exceptions" if n_quarantined > 0 else "completed",
            committed_snapshots=committed_snapshots,
        )
        return pipeline_run, accounting

    def _merge_patients(self, patients: List[CanonicalPatient]) -> None:
        rows = [
            (
                p.provider, p.patient_key, p.source_resource_id, p.sequence,
                p.birth_date, p.gender, p.postal_code, p.deceased_boolean,
                p.lineage.manifest_id, p.lineage.run_id, p.supersession_state
            )
            for p in patients
        ]
        schema = StructType([
            StructField("provider", StringType(), False),
            StructField("patient_key", StringType(), False),
            StructField("source_resource_id", StringType(), False),
            StructField("sequence", LongType(), False),
            StructField("birth_date", StringType(), True),
            StructField("gender", StringType(), True),
            StructField("postal_code", StringType(), True),
            StructField("deceased_boolean", BooleanType(), False),
            StructField("manifest_id", StringType(), False),
            StructField("run_id", StringType(), False),
            StructField("supersession_state", StringType(), False),
        ])
        df = self.spark.createDataFrame(rows, schema=schema)
        df.createOrReplaceTempView("stg_patients_raw")
        self.spark.sql("""
            CREATE OR REPLACE TEMP VIEW stg_patients AS
            SELECT provider, patient_key, source_resource_id, sequence, birth_date, gender, postal_code, deceased_boolean, manifest_id, run_id, supersession_state
            FROM (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY provider, patient_key ORDER BY sequence DESC) as rn
                FROM stg_patients_raw
            ) WHERE rn = 1
        """)

        self.spark.sql("""
            MERGE INTO careflow_catalog.careflow_silver.patients t
            USING stg_patients s
            ON t.provider = s.provider AND t.patient_key = s.patient_key
            WHEN MATCHED AND s.sequence > t.sequence THEN
                UPDATE SET
                    t.sequence = s.sequence,
                    t.birth_date = s.birth_date,
                    t.gender = s.gender,
                    t.postal_code = s.postal_code,
                    t.deceased_boolean = s.deceased_boolean,
                    t.manifest_id = s.manifest_id,
                    t.run_id = s.run_id,
                    t.supersession_state = s.supersession_state
            WHEN NOT MATCHED THEN
                INSERT (
                    provider, patient_key, source_resource_id, sequence, birth_date,
                    gender, postal_code, deceased_boolean, manifest_id, run_id, supersession_state
                )
                VALUES (
                    s.provider, s.patient_key, s.source_resource_id, s.sequence, s.birth_date,
                    s.gender, s.postal_code, s.deceased_boolean, s.manifest_id, s.run_id, s.supersession_state
                )
        """)

    def _merge_encounters(self, encounters: List[CanonicalEncounter]) -> None:
        rows = [
            (
                e.provider, e.encounter_key, e.patient_key, e.source_resource_id, e.sequence,
                e.class_code, e.period_start, e.period_end, e.status,
                e.lineage.manifest_id, e.lineage.run_id, e.supersession_state
            )
            for e in encounters
        ]
        schema = StructType([
            StructField("provider", StringType(), False),
            StructField("encounter_key", StringType(), False),
            StructField("patient_key", StringType(), False),
            StructField("source_resource_id", StringType(), False),
            StructField("sequence", LongType(), False),
            StructField("class_code", StringType(), True),
            StructField("period_start", TimestampType(), True),
            StructField("period_end", TimestampType(), True),
            StructField("status", StringType(), False),
            StructField("manifest_id", StringType(), False),
            StructField("run_id", StringType(), False),
            StructField("supersession_state", StringType(), False),
        ])
        df = self.spark.createDataFrame(rows, schema=schema)
        df.createOrReplaceTempView("stg_encounters_raw")
        self.spark.sql("""
            CREATE OR REPLACE TEMP VIEW stg_encounters AS
            SELECT provider, encounter_key, patient_key, source_resource_id, sequence, class_code, period_start, period_end, status, manifest_id, run_id, supersession_state
            FROM (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY provider, encounter_key ORDER BY sequence DESC) as rn
                FROM stg_encounters_raw
            ) WHERE rn = 1
        """)

        self.spark.sql("""
            MERGE INTO careflow_catalog.careflow_silver.encounters t
            USING stg_encounters s
            ON t.provider = s.provider AND t.encounter_key = s.encounter_key
            WHEN MATCHED AND s.sequence > t.sequence THEN
                UPDATE SET
                    t.sequence = s.sequence,
                    t.class_code = s.class_code,
                    t.period_start = s.period_start,
                    t.period_end = s.period_end,
                    t.status = s.status,
                    t.manifest_id = s.manifest_id,
                    t.run_id = s.run_id,
                    t.supersession_state = s.supersession_state
            WHEN NOT MATCHED THEN
                INSERT (
                    provider, encounter_key, patient_key, source_resource_id, sequence,
                    class_code, period_start, period_end, status, manifest_id, run_id, supersession_state
                )
                VALUES (
                    s.provider, s.encounter_key, s.patient_key, s.source_resource_id, s.sequence,
                    s.class_code, s.period_start, s.period_end, s.status, s.manifest_id, s.run_id, s.supersession_state
                )
        """)

    def _merge_observations(self, observations: List[CanonicalObservation]) -> None:
        rows = [
            (
                o.provider, o.patient_key, o.encounter_key, o.source_key, o.sequence,
                o.effective_time, o.original.original_code, o.original.original_coding_system,
                o.original.original_text, o.original.original_value, o.original.original_unit,
                o.original.original_status, o.normalized.normalized_concept, o.normalized.normalized_value,
                o.normalized.normalized_unit, o.mapping_reference, o.normalization_version,
                o.lineage.manifest_id, o.lineage.run_id, o.supersession_state
            )
            for o in observations
        ]
        schema = StructType([
            StructField("provider", StringType(), False),
            StructField("patient_key", StringType(), False),
            StructField("encounter_key", StringType(), True),
            StructField("source_key", StringType(), False),
            StructField("sequence", LongType(), False),
            StructField("effective_time", TimestampType(), False),
            StructField("original_code", StringType(), True),
            StructField("original_coding_system", StringType(), True),
            StructField("original_text", StringType(), True),
            StructField("original_value", StringType(), True),
            StructField("original_unit", StringType(), True),
            StructField("original_status", StringType(), True),
            StructField("normalized_concept", StringType(), True),
            StructField("normalized_value", DoubleType(), True),
            StructField("normalized_unit", StringType(), True),
            StructField("mapping_reference", StringType(), True),
            StructField("normalization_version", StringType(), False),
            StructField("manifest_id", StringType(), False),
            StructField("run_id", StringType(), False),
            StructField("supersession_state", StringType(), False),
        ])
        df = self.spark.createDataFrame(rows, schema=schema)
        df.createOrReplaceTempView("stg_observations_raw")
        self.spark.sql("""
            CREATE OR REPLACE TEMP VIEW stg_observations AS
            SELECT provider, patient_key, encounter_key, source_key, sequence, effective_time,
                   original_code, original_coding_system, original_text, original_value, original_unit,
                   original_status, normalized_concept, normalized_value, normalized_unit,
                   mapping_reference, normalization_version, manifest_id, run_id, supersession_state
            FROM (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY provider, source_key ORDER BY sequence DESC) as rn
                FROM stg_observations_raw
            ) WHERE rn = 1
        """)

        self.spark.sql("""
            MERGE INTO careflow_catalog.careflow_silver.observations t
            USING stg_observations s
            ON t.provider = s.provider AND t.source_key = s.source_key
            WHEN MATCHED AND s.sequence > t.sequence THEN
                UPDATE SET
                    t.sequence = s.sequence,
                    t.effective_time = s.effective_time,
                    t.original_value = s.original_value,
                    t.normalized_value = s.normalized_value,
                    t.manifest_id = s.manifest_id,
                    t.run_id = s.run_id,
                    t.supersession_state = s.supersession_state
            WHEN NOT MATCHED THEN
                INSERT (
                    provider, patient_key, encounter_key, source_key, sequence,
                    effective_time, original_code, original_coding_system, original_text,
                    original_value, original_unit, original_status, normalized_concept,
                    normalized_value, normalized_unit, mapping_reference, normalization_version,
                    manifest_id, run_id, supersession_state
                )
                VALUES (
                    s.provider, s.patient_key, s.encounter_key, s.source_key, s.sequence,
                    s.effective_time, s.original_code, s.original_coding_system, s.original_text,
                    s.original_value, s.original_unit, s.original_status, s.normalized_concept,
                    s.normalized_value, s.normalized_unit, s.mapping_reference, s.normalization_version,
                    s.manifest_id, s.run_id, s.supersession_state
                )
        """)
