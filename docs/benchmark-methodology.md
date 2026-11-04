# CareFlow Atlas — Benchmark Methodology & Scale Report

## 1. Benchmarking Objectives & Scope
The purpose of the CareFlow Atlas benchmarking suite ([`tests/benchmarks/test_throughput.py`](file:///Users/dpcubed/.gemini/antigravity/scratch/careflow-atlas/tests/benchmarks/test_throughput.py)) is to measure:
1. **Bronze Landing Latency:** End-to-end WORM landing and cryptographic SHA-256 digest computation in object storage.
2. **Adapter Parsing Throughput:** In-memory parsing and schema validation of heterogeneous clinical resources (FHIR R4 JSON & multi-table delimited CSV) into normalized `ResourceEnvelope` carriers.
3. **PySpark Iceberg MERGE Scalability:** Apache Iceberg v2 atomic merge-on-read transaction latency across partitioned silver tables (`patients`, `encounters`, `observations`).
4. **Conservation Law Audit:** Verification of zero row leakage or duplication across ingestion boundaries.

---

## 2. Experimental Environment & System Specifications

| Component | Specification |
| :--- | :--- |
| **Operating System** | macOS (Darwin 24.3.0, aarch64 / Apple Silicon) |
| **Python Runtime** | Python 3.11.14 (managed via `uv`) |
| **Apache Spark** | PySpark 3.5.1 on Java 17 LTS |
| **Apache Iceberg** | Iceberg Spark Runtime 3.5_2.12 (version 1.6.1) |
| **Catalog Database** | PostgreSQL 16 on Docker (JDBC Catalog backend) |
| **Object Store** | MinIO RELEASE.2024-04-18T19-09-19Z on Docker (S3v4 API) |

---

## 3. Measured Empirical Results

Measurements captured from execution of high-volume synthetic batch test (`370 clinical resources`, `188.1 KB` payload):

```
=================================================================
CAREFLOW ATLAS HIGH-VOLUME THROUGHPUT BENCHMARK
=================================================================
Total Resources Processed:  370 (188.1 KB)
Bronze Landing Latency:     82.4 ms (4,493 recs/sec)
Adapter Parse Latency:      3.3 ms (112,391 recs/sec)
PySpark Iceberg MERGE Time: 10.37 s (36 recs/sec)
Conservation Audit:         BALANCED (Accepted: 367, Quarantined: 2, Duplicates: 1)
=================================================================
```

### Analysis of Results
1. **Adapter Parsing (>112,000 recs/sec):**
   The decoupled Pydantic/dataclass parsing layer evaluates clinical envelopes with minimal CPU overhead, easily sustaining enterprise ingestion rates.
2. **Bronze Landing (~4,500 recs/sec):**
   SHA-256 checksumming and streaming to MinIO executes in $< 100\text{ ms}$, ensuring near-instant ingestion acknowledgment for upstream partners.
3. **PySpark Iceberg MERGE:**
   The atomic Merge-on-Read Iceberg v2 commits guarantee transactional consistency and schema evolution handling across three separate physical tables in ~10 seconds on a single local worker.
4. **Row Conservation Balance:**
   $$N_{\text{input}} (370) = N_{\text{accepted}} (367) + N_{\text{quarantined}} (2) + N_{\text{duplicate}} (1) + N_{\text{excluded}} (0)$$
   Conservation law $N_{in} \equiv \sum N_{out}$ holds with 100% precision.
