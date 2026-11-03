# ADR 0003: Iceberg Table Format with JDBC Catalog vs Plain Parquet Files

## Status
Accepted

## Context
CareFlow Atlas requires a reproducible data plane that supports:
1. Versioned canonical clinical records (Silver layer) with snapshot isolation.
2. In-place MERGE (upserts, corrections, deletions) without rewriting entire table partitions.
3. Time-travel queries so researchers can query historic snapshots and verify cohort reproducibility.
4. Schema evolution without corrupting legacy data files.

Plain Parquet files on object storage (MinIO/S3) lack ACID transaction guarantees, fine-grained snapshot isolation, and atomic merge capabilities without bespoke file-level manifest locking.

## Decision
Adopt **Apache Iceberg (format-version 2)** with an **S3FileIO** storage layer backed by MinIO, coordinated via a **PostgreSQL JDBC catalog**.

- **Format Version 2:** Enables row-level equality deletes and position deletes, necessary for clinical corrections and deletions without full partition rewrites.
- **JDBC Catalog on PostgreSQL:** Provides transactional metadata commits, provider-isolated schemas, and zero external dependency bloat compared to Hive Metastore.
- **S3FileIO:** Direct S3 multipart upload and metadata persistence without Hadoop S3A file status bottlenecks.

## Consequences
### Positive
- True snapshot isolation: readers query pinned snapshot IDs, completely decoupled from concurrent ETL writes.
- Deterministic MERGE: updates advance monotonic sequence while duplicates are skipped at storage layer.
- Time travel: historic research releases point to immutable snapshot IDs.

### Negative / Trade-offs
- Requires JVM/Scala jar dependencies in PySpark (`iceberg-spark-runtime-3.5_2.12`, `postgresql-42.7.3`, `iceberg-aws-bundle`).
- Catalog requires relational database availability for table lock/commit coordination.

## Reconsideration Trigger
If catalog load exceeds PostgreSQL capacity (>5,000 commits/sec) or multi-engine federation (e.g. Trino + Spark) requires an enterprise REST catalog, migrate to Apache Polaris or Nessie REST Catalog.
