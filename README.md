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

Status: Spark 3.5 (Scala 2.12, default) and Spark 4.0 (Scala 2.13), JDK 21. Backlog and decisions:
`docs/STATUS.md`.

## Build

The connector depends on the server repository's client core and (for tests) the server itself; the commit used
is pinned in `kestrel.ref`.

```sh
scripts/install-kestrel.sh          # builds and installs the server artifacts at kestrel.ref
./mvnw verify                       # unit + Spark local-mode tests (no perf / real-services)
./mvnw -Pspark-4.0 verify           # the same against Spark 4.0 / Scala 2.13
```

Spark build lines (profiles): `spark-3.5` (default), `spark-4.0`; `spark-3.3` and `spark-3.4` compile but cannot run:
those Spark versions do not start on JDK 21, which the server's client core requires. Build the jar for the Spark
line the cluster runs (Scala 2.12 and 2.13 jars are not interchangeable).

The jar to put on Spark's class path is `target/arrow-flight-spark-connector-<version>-shaded.jar` (Spark and
slf4j are `provided`). It relocates everything it bundles that Spark or Hadoop also ship (Arrow, FlatBuffers,
Netty with its native libraries, gRPC, protobuf, Guava, Gson, Jackson, Apache Commons, under
`pro.surpin.data.arrowflight.shaded`), so it works with `--jars` whatever comes first on the class path.
`./mvnw verify` checks that (`ShadedJarHadoop313IT`): Spark of the build line plus a Hadoop 3.1.3 client
(`src/test/hadoop-3.1.3/pom.xml`) with only the shaded jar added reads a table from the in-process server.

## Spark catalog

`FlightCatalog` exposes the server's schemas as Spark namespaces and its tables as Spark tables, read through the
connector (same pushdown and columnar reads as `format("flight")`):

```sh
spark-sql \
  --conf spark.sql.catalog.flight=pro.surpin.data.arrowflight.client.spark.FlightCatalog \
  --conf spark.sql.catalog.flight.host=flight-host \
  --conf spark.sql.catalog.flight.port=32010 \
  --conf spark.sql.catalog.flight.user=... --conf spark.sql.catalog.flight.password=...
```

```sql
SHOW NAMESPACES IN flight;
SHOW TABLES IN flight.tpch;
DESCRIBE TABLE flight.tpch.lineitem;
SELECT l_returnflag, count(*) FROM flight.tpch.lineitem GROUP BY l_returnflag;
```

- Options under `spark.sql.catalog.<name>.` are the read options without `table`: connection, credentials, TLS,
  Kerberos (`kerberos.servicePrincipal`, works with shipped sessions as below), `client.*`, `column.quote`, and scan
  options such as `partition.size` or `statistics.filterSelectivity`, which become defaults for every scan of the
  catalog's tables. `default.schema` is also the catalog's default namespace (after `USE flight`,
  `SELECT * FROM lineitem` resolves in it).
- Listings come from the Flight SQL `GetDbSchemas` / `GetTables` commands, so the server's authorization decides
  what is visible. Names are matched exactly (case-sensitive); one namespace level only.
- Read-only: `CREATE`/`ALTER`/`DROP` of namespaces and tables fail. Writes go through `format("flight")`.

## Kerberos on YARN

With `kerberos.servicePrincipal` (TLS required) every process authenticates with SPNEGO, so executors would need
their own TGT. Instead, the driver can obtain a server session and ship it in the job credentials: the connector
registers a Spark delegation token provider (service `flight`) that, when configured, authenticates to each listed
server with the driver's Kerberos login and stores the signed session the server returns. Executors (and the
driver's own schema and endpoint planning) use that session instead of SPNEGO; without one they fall back to
SPNEGO with their own login (and fail with an error naming what is missing).

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
- Spark creates its delegation token manager on YARN and Kubernetes only; on a standalone cluster nothing is
  shipped and executors need their own Kerberos login.

## License

Apache License 2.0 (see `LICENSE`).
