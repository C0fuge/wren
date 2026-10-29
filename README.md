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

Aggregate pushdown is all or nothing per query block, and Spark translates arithmetic (`+ - * /`) into connector
expressions only with `spark.sql.ansi.enabled=true` (Spark 4's default; Spark 3.5 defaults to `false`). Without
ANSI mode an aggregate such as `SUM(l_extendedprice * (1 - l_discount))` therefore keeps the whole aggregation in
Spark (results are the same; TPC-H Q1 at SF1 is ~6x slower than with ANSI mode). Aggregates of plain columns are
pushed either way.

Pushed partial counts: Spark merges the partial `COUNT`s of a pushed aggregation (also those behind an `AVG`) with
`sum`, which is null on no rows. On Spark 3.5 with ANSI mode that made a correlated scalar subquery with a pushed
decimal `AVG` fail while Spark planned it (`[ARITHMETIC_OVERFLOW] Overflow in sum of decimals`, TPC-H Q17; Spark
evaluates the subquery's aggregates on zero rows and divided null by null). The connector therefore adds an optimizer
rule (`FlightPushedCounts`) to Spark's `experimental.extraOptimizations` of the session that resolves a Flight table;
it rewrites each merged count to `coalesce(sum(count), 0)`, the exact count. If a job sets
`spark.experimental.extraOptimizations` itself after reading a Flight table, it replaces the rule: append to the list
instead.

`LIKE` filters: Spark turns only `LIKE 'abc%'`, `'%abc'` and `'%abc%'` into connector predicates. A second rule
(`FlightLikePushdown`, registered the same way) moves other `[NOT] LIKE` filters on a string column into the
server's WHERE clause after Spark's push-down (e.g. TPC-H Q13's `o_comment NOT LIKE '%special%requests%'`) when the
pattern has only literal characters and `%` (Spark's escapes resolved; patterns with `_` stay in Spark), and stops
reading columns only those filters used. The scan is planned once more on the server. Not for scans with a pushed
aggregation (Spark pushes those only when every filter translates). `spark.flight.likePushdown=false` turns it off.

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

## Parallelism

A scan without `partition.*` options gets one Spark task per Flight endpoint, and the server plans
`arrowflight.cluster.endpointsPerServer` endpoints per server node (default 1). A big table is then read, and its
rows shuffled, by as many tasks as there are server nodes, whatever the executor count (TPC-H SF1 on 4 cores: Q5
1.33x of Spark reading the Parquet directly with 1 endpoint, 0.94x with 4). Two ways to more tasks:

- Server side (every job profits): raise `endpointsPerServer` (`FLIGHT_ENDPOINTS_PER_SERVER`), e.g. to the node's
  cores.
- Per scan: a range split, one Spark task per slice, each slice a query of its own on the server:

```scala
spark.read.format("flight") /* host, port, user, password */
  .option("table", "tpch.lineitem")
  .option("partition.byColumn", "l_orderkey")
  .option("partition.size", "8")
  .option("partition.lowerBound", "1").option("partition.upperBound", "6000001")
  .load()
```

  The column must be an integer, floating-point, decimal, date or timestamp column; the bounds only set the stride
  (as in Spark's JDBC source): the first slice also reads the keys below its upper edge and the null keys, the last
  the keys from its lower edge on, so every row is read once whatever the bounds. Bounds narrower than `size`
  values give fewer slices. Pick bounds close to the key's real range (the server's
  `arrowflight.table_statistics`, or `min`/`max`), or the outer slices get most rows. Alternatively
  `partition.predicates` (SQL conditions separated by `;`, `|` or `,`, so none inside a condition; or one
  `partition.predicate.<name>` option per condition) gives one task per condition; the conditions must then cover
  every row once themselves.
- `partition.byColumn` without bounds asks for a hash split (`partition.hashFunc`, default `hash`), which the server
  does not parse at the moment: the scan fails (see `docs/STATUS.md`, Open questions).

No default split is applied: it needs a suitable key and its bounds per table.

## Scan statistics

Each scan reports the server's size estimate (Parquet input bytes and rows) to Spark; a pushed global aggregate
reports one row. The server's bytes cover whole Parquet files, so a scan that reads only some columns reports their
share of the row width (Spark's default type sizes, as Spark estimates its own Parquet relation under a projection):
a narrow dimension table can then be broadcast as when Spark reads the files directly. `statistics.columnWidth=false`
(default `true`) reports the whole files as before. With `statistics.filterSelectivity=true` (default `false`) the estimate is also scaled by the share
of rows the pushed filters keep, so Spark can broadcast a filtered table. For a plain `schema.table` source the
connector asks the server's `arrowflight.table_statistics` action for the filtered columns (footer bounds, null
counts, distinct counts when the server has them): a range filter then keeps its share of the value range, a value
outside the bounds keeps nothing, `IS NULL` uses the null count. Without the action (or for queries, string
columns, unknown shapes) fixed defaults apply: equality 0.1, range 1/3, `LIKE` patterns 0.25. Estimates never
change results.

## Runtime filtering

Opt-in, Spark session settings. A join of a filtered Flight table with a big Flight table can prune the big scan at
run time: Spark's dynamic pruning collects the join keys of the filtered side and the connector plans the big scan
again on the server with `key IN (...)`. Spark still performs the join, so results never change.

```sh
--conf spark.flight.retainPushedFilters=true            # or auto; default false
--conf spark.sql.extensions=pro.surpin.data.arrowflight.client.spark.FlightAqeGuard
```

- `spark.flight.retainPushedFilters`: Spark only prunes with a side whose filter it sees, and filters the server
  takes are normally removed from Spark's plan. `true` returns them to Spark as well (the server still applies
  them; Spark re-checks the already filtered rows). `auto` does this only for a plain `schema.table` that the
  server's statistics show is at most `spark.flight.retainMaxRows` rows (default 50000000) with filters keeping at
  most `spark.flight.retainMaxSelectivity` of them (default 0.1, estimated as in "Scan statistics"). A kept filter
  stops Spark from pushing an aggregation into that scan.
- The pruned side (only while the setting is not `false`): integer (`int`/`bigint`) columns of scans whose server row estimate is at least
  `spark.flight.runtimeFilterMinRows` (default 50000000), without a pushed aggregation. Up to
  `spark.flight.runtimeFilterRangeMinKeys` keys (default 1000) are sent as an IN list, more as `key >= min and key
  <= max`.
- `FlightAqeGuard` (recommended with retained filters): Spark 3.5's adaptive execution can deadlock on a plan that
  scans one table twice with a retained filter (a filtered self-join). The extension switches
  `spark.sql.adaptive.enabled` off for exactly those queries and back on when they end (logged as warnings).

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

## Performance (TPC-H)

`scripts/perf/tpch.sh` (after `scripts/install-kestrel.sh`) generates TPC-H Parquet data (`tpchgen-cli`, scale
factor `SF`, default 1), starts the server (built at `kestrel.ref`) on it as a separate process, and runs the 22
queries in Spark local mode (`TpchPerfTest`, `-Pperf`) three ways: Spark reading the Parquet files directly (the
reference), the connector (`format("flight")`), and Spark's JDBC data source with the Flight SQL JDBC driver
(baseline; big tables split into key ranges); mode `catalog` reads the connector through `FlightCatalog`
instead, and `ANSI=true|false` sets `spark.sql.ansi.enabled` for every mode (see above). `QUERIES=probes` (or e.g. `p1,5`) runs probe scans
instead of / besides TPC-H queries: the bare `lineitem` stream (`p01`: Q5's four columns, `p02`: the same shuffled by
`l_orderkey`, `p03`: all columns; `src/test/resources/tpch-queries/pNN.sql`). Every mode's rows are compared with the reference. Results in
`target/perf`: `times.csv` (every run), `summary.md` (medians, totals, ratios; then the columns a mode types
differently from the reference; the connector has none, the JDBC baseline reads `bigint` as `decimal(20,0)`, `int` as
`bigint` and `decimal(15,2)` as `decimal(38,18)` from the driver's metadata — values compare equal, but Spark computes
on the wider types there), `plans/` (executed plans),
`env.txt`, `server.log`. In GitHub Actions: workflow `Perf` (manual, inputs `sf`, `runs`, `modes`, `queries`, `ansi`,
`endpoints_per_server`, `flight_options`, `kestrel_ref`; dispatches run in parallel), results kept as an artifact.

Results at SF10 (GitHub runner, 4 cores, server and Spark on the same host, `ANSI=true`, medians of 3; total time of
the 22 queries relative to Spark reading the Parquet files directly): the connector takes 0.79x with range splits in
4 of the five big tables (`FLIGHT_OPTIONS`, see "Parallelism") and 0.78x with `ENDPOINTS_PER_SERVER=4`; more slices
than cores are slower (8: 0.85x, 16: 0.95x). Single queries range from 0.07x (Q1, aggregation pushed to the server)
to about 1.1-1.3x (Q5, Q8, Q9: joins that shuffle `lineitem`). The JDBC baseline takes 2.82x of direct, 3.3x the
connector's time (one run). Details and run ids in `docs/STATUS.md` (Log, 2026-10-29).

## License

Apache License 2.0 (see `LICENSE`).
