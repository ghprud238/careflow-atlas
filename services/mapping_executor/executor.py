"""Deterministic Replay Executor for Governed Clinical Mapping Promotion.

Section 11 Invariants:
- Human authorization gate: validates Approval artifact exact proposal hash before promotion.
- Promotes mapping to PostgreSQL mapping registry (v1.0.0 -> v1.1.0).
- Scoped replay: only targeted quarantined records matching approved rule are reprocessed.
- Deterministic MERGE INTO Iceberg Silver observations with updated lineage and mapping_reference.
- Deletes replayed records from quarantine upon successful commit.
- Duplicate-safe and idempotent.
"""

import json
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from contracts import (
    Approval,
    CanonicalObservation,
    Lineage,
    MappingProposal,
    NormalizedObservation,
    OriginalObservation,
)
from packages.mapping_dsl.interpreter import DSLInterpreter
from packages.mapping_dsl.registry import MappingRegistry
from pipelines.adapters.fhir.fhir_adapter import REVIEWED_CATALOG
from pipelines.spark.canonical_transform import get_spark_session
from services.ingest.manifest_manager import ManifestManager
from services.release_service.release_manager import ReleaseManager


class ReplayExecutor:
    """Executes governed promotion and targeted scoped replay for approved mappings."""

    def __init__(
        self,
        spark: Optional[SparkSession] = None,
        db_conn_str: str = "postgresql://careflow_admin:careflow_dev_password@localhost:5432/careflow_metadata",
    ):
        self.spark = spark or get_spark_session()
        self.db_conn_str = db_conn_str
        self.registry = MappingRegistry(db_conn_str=db_conn_str)
        self.manifest_mgr = ManifestManager(db_conn_str=db_conn_str)
        self.interpreter = DSLInterpreter()
        self.release_mgr = ReleaseManager(db_conn_str=db_conn_str)

    def execute_governed_promotion_and_replay(
        self,
        proposal: MappingProposal,
        approval: Approval,
        promoted_by: str = "terminology_reviewer",
    ) -> Dict[str, Any]:
        """Execute Section 11 Governed Promotion & Scoped Replay.
        
        1. Validate approval hash and promote mapping rule in registry.
        2. Identify and fetch matching quarantined records.
        3. Apply approved DSL rule to transform records to canonical observations.
        4. MERGE INTO Iceberg careflow_silver.observations.
        5. Remove replayed records from careflow_quarantine.quarantined_records.
        6. Return execution summary and committed snapshot ID.
        """
        # 1. Governed Promotion
        new_version = self.registry.promote_proposal(
            proposal=proposal,
            approval=approval,
            promoted_by=promoted_by,
        )

        target_code = (
            proposal.typed_dsl.match.get("local_test_code")
            or proposal.typed_dsl.match.get("code")
            or ""
        )
        return self.execute_scoped_replay(
            provider=proposal.provider_id,
            target_code=target_code,
            dsl=proposal.typed_dsl,
            version=new_version,
        )

    def execute_scoped_replay(
        self,
        provider: str,
        target_code: str,
        dsl: Any,
        version: str,
    ) -> Dict[str, Any]:
        """Execute targeted scoped replay for a given provider, code, and active DSL rule."""
        # 2. Identify Targeted Quarantined Records
        # Query careflow_quarantine for matching records
        q_tbl = self.spark.table("careflow_catalog.careflow_quarantine.quarantined_records")
        q_records = (
            q_tbl.filter(
                (q_tbl.provider == provider)
                & (
                    q_tbl.findings.contains(target_code)
                    | q_tbl.source_resource_id.contains(target_code)
                )
            )
            .collect()
        )

        if not q_records:
            snaps = self.spark.sql(
                "SELECT snapshot_id FROM careflow_catalog.careflow_silver.observations.snapshots ORDER BY committed_at DESC LIMIT 1"
            ).collect()
            obs_snapshot_id = int(snaps[0]["snapshot_id"]) if snaps else None
            return {
                "promoted_version": version,
                "replayed_count": 0,
                "source_ids_replayed": [],
                "silver_observations_snapshot_id": obs_snapshot_id,
                "message": f"No quarantined records found for provider '{provider}' and code '{target_code}'",
            }

        # 3. Fetch Raw Bronze Payloads & Apply Approved DSL
        canonical_obs_list: List[CanonicalObservation] = []
        source_ids_to_drain: List[str] = []

        # Cache fetched manifests to avoid redundant S3 reads
        manifest_cache: Dict[str, List[Dict[str, Any]]] = {}

        for row in q_records:
            man_id = row["manifest_id"]
            source_id = row["source_resource_id"]
            run_id = row["run_id"]

            if man_id not in manifest_cache:
                manifest = self.manifest_mgr.get_ready_manifest(man_id)
                if manifest:
                    payload_bytes = self.manifest_mgr.fetch_staged_payload(manifest.object_uri)
                    lines = [json.loads(line) for line in payload_bytes.decode("utf-8").split("\n") if line.strip()]
                    manifest_cache[man_id] = lines
                else:
                    manifest_cache[man_id] = []

            # Find matching raw FHIR resource
            raw_res = next((r for r in manifest_cache[man_id] if r.get("id") == source_id), None)
            if not raw_res:
                continue

            # Extract fields
            codings = raw_res.get("code", {}).get("coding", [])
            raw_code = codings[0].get("code", "") if codings else ""
            raw_val_str = str(raw_res.get("valueQuantity", {}).get("value", ""))
            raw_unit = raw_res.get("valueQuantity", {}).get("unit")
            orig_text = raw_res.get("code", {}).get("text", "")

            matched, norm_concept, norm_val, norm_unit, err = self.interpreter.apply_observation_rule(
                raw_code=raw_code,
                raw_val_str=raw_val_str,
                raw_unit=raw_unit,
                dsl=dsl,
            )

            if matched and err is None and norm_val is not None:
                subject_ref = raw_res.get("subject", {}).get("reference", "")
                patient_id = subject_ref.replace("Patient/", "")
                patient_key = f"pat-{patient_id.replace('pat-', '')}"

                encounter_ref = raw_res.get("encounter", {}).get("reference")
                encounter_key = None
                if encounter_ref:
                    enc_id = encounter_ref.replace("Encounter/", "")
                    encounter_key = f"enc-{enc_id.replace('enc-', '')}"

                eff_str = raw_res.get("effectiveDateTime") or raw_res.get("issued")
                try:
                    eff_time = datetime.fromisoformat(eff_str.replace("Z", "+00:00")) if eff_str else datetime.utcnow()
                except Exception:
                    eff_time = datetime.utcnow()

                seq = int(raw_res.get("_change_sequence", 1))

                # Normalize concept to reviewed catalog if matched
                concept_id = norm_concept
                if norm_concept in REVIEWED_CATALOG:
                    concept_id = REVIEWED_CATALOG[norm_concept]["concept_id"]

                orig_obs = OriginalObservation(
                    original_code=raw_code,
                    original_coding_system=codings[0].get("system", "") if codings else "",
                    original_text=orig_text,
                    original_value=raw_val_str,
                    original_unit=raw_unit,
                    original_status=raw_res.get("status", "final"),
                )

                norm_obs = NormalizedObservation(
                    normalized_concept=concept_id,
                    normalized_value=norm_val,
                    normalized_unit=norm_unit,
                )

                lineage = Lineage(
                    manifest_id=man_id,
                    run_id=run_id,
                    resource_envelope_hash=row["payload_hash"],
                )

                canonical_obs = CanonicalObservation(
                    provider=provider,
                    patient_key=patient_key,
                    encounter_key=encounter_key,
                    source_key=source_id,
                    sequence=seq,
                    effective_time=eff_time,
                    original=orig_obs,
                    normalized=norm_obs,
                    mapping_reference=f"mapping-registry/{version}/{target_code}",
                    normalization_version=version,
                    lineage=lineage,
                    supersession_state="current",
                )
                canonical_obs_list.append(canonical_obs)
                source_ids_to_drain.append(source_id)

        # 4. Merge Canonical Observations into Iceberg Silver
        if canonical_obs_list:
            self._merge_observations(canonical_obs_list)

        # 5. Drain Quarantined Records from Iceberg Quarantine Table
        if source_ids_to_drain:
            ids_clause = ", ".join(f"'{sid}'" for sid in source_ids_to_drain)
            self.spark.sql(
                f"""
                DELETE FROM careflow_catalog.careflow_quarantine.quarantined_records
                WHERE provider = '{provider}' AND source_resource_id IN ({ids_clause})
                """
            )

        # 6. Retrieve Latest Committed Snapshot ID
        snaps = self.spark.sql(
            "SELECT snapshot_id FROM careflow_catalog.careflow_silver.observations.snapshots ORDER BY committed_at DESC LIMIT 1"
        ).collect()
        obs_snapshot_id = int(snaps[0]["snapshot_id"]) if snaps else None

        return {
            "promoted_version": version,
            "replayed_count": len(canonical_obs_list),
            "source_ids_replayed": source_ids_to_drain,
            "silver_observations_snapshot_id": obs_snapshot_id,
        }

    def _merge_observations(self, observations: List[CanonicalObservation]) -> None:
        """Merge canonical observations into careflow_silver.observations using deduplicated staging."""
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
        df.createOrReplaceTempView("stg_replayed_obs_raw")
        self.spark.sql("""
            CREATE OR REPLACE TEMP VIEW stg_replayed_obs AS
            SELECT provider, patient_key, encounter_key, source_key, sequence, effective_time,
                   original_code, original_coding_system, original_text, original_value, original_unit,
                   original_status, normalized_concept, normalized_value, normalized_unit,
                   mapping_reference, normalization_version, manifest_id, run_id, supersession_state
            FROM (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY provider, source_key ORDER BY sequence DESC) as rn
                FROM stg_replayed_obs_raw
            ) WHERE rn = 1
        """)

        self.spark.sql("""
            MERGE INTO careflow_catalog.careflow_silver.observations t
            USING stg_replayed_obs s
            ON t.provider = s.provider AND t.source_key = s.source_key
            WHEN MATCHED AND s.sequence > t.sequence THEN
                UPDATE SET
                    t.sequence = s.sequence,
                    t.effective_time = s.effective_time,
                    t.original_value = s.original_value,
                    t.normalized_concept = s.normalized_concept,
                    t.normalized_value = s.normalized_value,
                    t.normalized_unit = s.normalized_unit,
                    t.mapping_reference = s.mapping_reference,
                    t.normalization_version = s.normalization_version,
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
