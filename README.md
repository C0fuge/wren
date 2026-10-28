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

## Kerberos on YARN

With `kerberos.servicePrincipal` (TLS required) every process authenticates with SPNEGO, so executors would need
their own TGT. Instead, the driver can obtain a server session and ship it in the job credentials: the connector
registers a Spark delegation token provider (service `flight`) that, when configured, authenticates to each listed
server with the driver's Kerberos login and stores the signed session the server returns. Executors use that
session instead of SPNEGO; without one they fall back to SPNEGO with their own login (and fail with an error
naming what is missing).

```sh
spark-submit --principal user@REALM --keytab user.keytab \
  --conf spark.flight.kerberos.servers=flight-host:32010 \
  --conf spark.flight.kerberos.servicePrincipal=flight/flight-host@REALM \
  --conf spark.flight.kerberos.tls.truststore.jksFile=/path/truststore.jks \
  --conf spark.flight.kerberos.tls.truststore.pass=... \
  ...
```

- `spark.flight.kerberos.servers`: comma-separated `host:port`, the host exactly as in the read/write `host`
  option (sessions are matched by host and port).
- `spark.flight.kerberos.servicePrincipal`, `spark.flight.kerberos.tls.verifyServer` (default `true`),
  `spark.flight.kerberos.tls.truststore.jksFile` / `.pass`: as the read/write options of the same name.
- Server sessions last ten minutes; Spark re-obtains them before expiry only when it can log in again
  (`--principal/--keytab`, or `spark.kerberos.renewal.credentials=ccache`). `spark.security.credentials.flight.enabled=false`
  turns the provider off.

## License

Apache License 2.0 (see `LICENSE`).
