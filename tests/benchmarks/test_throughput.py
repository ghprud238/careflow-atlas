"""High-Throughput Performance and Scale Benchmark Suite."""

import json
import time
import uuid
from datetime import datetime
import pytest
from fixtures.generation.synthetic_provider_a import generate_provider_a_bundle
from pipelines.adapters.fhir.fhir_adapter import FHIRAdapter
from pipelines.spark.canonical_transform import CanonicalPipeline, get_spark_session
from services.ingest.manifest_manager import ManifestManager


@pytest.fixture(scope="module")
def spark():
    return get_spark_session()


@pytest.fixture(scope="module")
def pipeline(spark):
    return CanonicalPipeline(spark)


@pytest.fixture(scope="module")
def manifest_mgr():
    return ManifestManager()


class TestThroughputBenchmarks:
    def test_high_volume_batch_throughput_and_conservation(
        self, spark, pipeline, manifest_mgr
    ):
        provider = "provider-a"
        batch_id = f"batch-bench-{uuid.uuid4().hex[:8]}"
        run_id = f"run-bench-{uuid.uuid4().hex[:8]}"

        # 1. Generate High-Volume Batch (40 patients, ~200-300 resources)
        gen_start = time.time()
        resources, meta = generate_provider_a_bundle(
            patient_count=40,
            seed=777,
            fixed_reference_date="2026-11-01",
            include_anomalies=True,
            include_duplicates=True,
            include_updates=True,
        )
        gen_time = time.time() - gen_start
        total_resources = len(resources)
        ndjson_bytes = ("\n".join(json.dumps(r) for r in resources) + "\n").encode("utf-8")
        payload_kb = len(ndjson_bytes) / 1024.0

        # 2. Benchmark Bronze Landing & Hashing
        landing_start = time.time()
        manifest = manifest_mgr.stage_and_register_batch(
            provider=provider,
            feed="fhir-r4",
            batch_id=batch_id,
            payload_bytes=ndjson_bytes,
            source_interval_start=datetime(2026, 10, 1, 0, 0, 0),
            source_interval_end=datetime(2026, 11, 1, 0, 0, 0),
            adapter_version="1.0.0",
        )
        landing_time = time.time() - landing_start
        landing_rate = total_resources / (landing_time or 0.001)

        # 3. Benchmark Adapter Parsing Throughput
        adapter = FHIRAdapter(provider=provider)
        parse_start = time.time()
        envelopes = [adapter.parse_resource_dict(r, manifest.manifest_id) for r in resources]
        parse_time = time.time() - parse_start
        parse_rate = total_resources / (parse_time or 0.001)

        # 4. Benchmark PySpark Iceberg MERGE Pipeline
        spark_start = time.time()
        run_res, accounting = pipeline.execute_manifest_ingest(manifest.manifest_id, run_id)
        spark_time = time.time() - spark_start
        spark_rate = total_resources / (spark_time or 0.001)

        # 5. Verify Conservation Invariant
        assert accounting.n_input == total_resources
        assert accounting.n_input == (
            accounting.n_accepted
            + accounting.n_duplicate
            + accounting.n_quarantined
            + accounting.n_intentionally_excluded
        )

        # Print Benchmark Report
        print("\n" + "=" * 65)
        print("CAREFLOW ATLAS HIGH-VOLUME THROUGHPUT BENCHMARK")
        print("=" * 65)
        print(f"Total Resources Processed:  {total_resources} ({payload_kb:.1f} KB)")
        print(f"Bronze Landing Latency:     {landing_time * 1000:.1f} ms ({landing_rate:.0f} recs/sec)")
        print(f"Adapter Parse Latency:      {parse_time * 1000:.1f} ms ({parse_rate:.0f} recs/sec)")
        print(f"PySpark Iceberg MERGE Time: {spark_time:.2f} s ({spark_rate:.0f} recs/sec)")
        print(f"Conservation Audit:         BALANCED (Accepted: {accounting.n_accepted}, Quarantined: {accounting.n_quarantined}, Duplicates: {accounting.n_duplicate})")
        print("=" * 65)

        assert parse_rate > 500  # Expected > 500 records/sec
        assert run_res.state in ["completed", "completed_with_exceptions"]
