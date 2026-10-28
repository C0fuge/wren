# Arrow Flight SQL Spark connector

Spark DataSource V2 connector (`format("flight")`) for an Arrow Flight SQL server that runs DuckDB over Parquet
(HDFS, S3, lakeFS; Iceberg and Hive catalogs). Reads are columnar Arrow batches, one Spark partition per Flight
endpoint, with filter / projection / aggregate / limit pushdown; writes go through Flight SQL statements.

```scala
spark.read.format("flight")
  .option("host", "flight-host").option("port", "32010")
  .option("user", "...").option("password", "...")
  .option("table", "tpch.lineitem")
  .load()
```

Status: Spark 3.5 (Scala 2.12), JDK 21. Backlog and decisions: `docs/STATUS.md`.

## Build

The connector depends on the server repository's client core and (for tests) the server itself; the commit used
is pinned in `kestrel.ref`.

```sh
scripts/install-kestrel.sh          # builds and installs the server artifacts at kestrel.ref
./mvnw verify                       # unit + Spark local-mode tests (no perf / real-services)
```

The jar to put on Spark's class path is `target/arrow-flight-spark-connector-<version>-shaded.jar` (Jackson
relocated; Spark itself is `provided`).

## License

Apache License 2.0 (see `LICENSE`).
