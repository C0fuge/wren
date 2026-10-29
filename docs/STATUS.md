# Status

## Context

- Origin: the `flight-spark` module of the server repository C0fuge/kestrel, split with history on 2026-10-28
  (see Log). The server repository no longer develops `flight-spark` (its plugins lane item P3 moved here); the
  module may still exist there, but this repository is where the connector changes.
- Build: `scripts/install-kestrel.sh` installs kestrel's parent pom, `arrow-flight-client-core` and
  `arrow-flight-server` (+ test-jar) at the commit in `kestrel.ref`; then `./mvnw verify`. The parent's properties
  (Arrow 19.0.0, Spark 3.5.9, JUnit, Java 21) are vendored in `pom.xml`. CI: `.github/workflows/ci.yml`.
- Tests at the split: unit + Spark local mode + in-process server (`TestFlightServerHelper` from the server
  test-jar); 175 tests, all green in CI at the split.
- Server builds for end-to-end/perf work: kestrel pre-releases `server-<version>-<sha>` (linux jar, `libs` set,
  plugins dir, `START-NOTES.md` = how to start it).
- Production target (maintainer): Hadoop 3.1.3 HA with ZooKeeper failover, Kerberos, Ranger; Spark on YARN.

## Next steps (one per session; small and finished)

> **Maintainer, 2026-10-28 15:00:** keep the connector native (Arrow batches straight into Spark's columnar
> reader, no JDBC layer anywhere). In S5 add Spark's built-in JDBC data source with the Flight SQL JDBC driver as a
> baseline so the native gain is measured.

(S1 Kerberos on YARN executors: done 2026-10-28, see Log.)

(S2 Build matrix: done 2026-10-28, see Log. Possible follow-up, not scheduled: carry the Spark/Scala line in
the shaded jar's name, e.g. `-spark3.5_2.12-shaded.jar`; changes the documented jar name.)

(S3 Spark catalog: done 2026-10-28, see Log. Possible follow-ups, not scheduled: case-insensitive name
matching, `CREATE TABLE ... AS SELECT` through the catalog, column comments / table statistics in `DESCRIBE`.)

S4. **Upstream ideas.** Upstream github.com/VadimSurpin/arrow-flight-server branch
    `feat/starrocks-native-integration`, `flight-spark` (compared 2026-10-28 at upstream 1de5f6b). Part 1 done
    (server column statistics in the filter estimate, see Log). Only (c) remains and it waits on the server, so the
    next session takes S5:
    (b) Runtime filtering: done 2026-10-28 (see Log). Possible follow-ups, not scheduled: check whether Spark 4.0
        still deadlocks without `FlightAqeGuard` (not reproduced here; the guard is installed on both lines);
        runtime filters on `date` keys.
    (c) Spark CBO column statistics (`estimateStatistics().columnStats()`, upstream `FlightColumnStatistics.convert`):
        only useful once the server sends distinct counts (kestrel's action sends `ndv: null`; upstream reports a
        column only with ndv + min + max, a missing bound makes Spark estimate empty joins). Waits on the server
        (Open questions).
    Not portable without server changes (Open questions): runtime bloom filters in the ticket, complete
    aggregation push-down (`supportCompletePushDown`).
S5. **Perf.** TPC-H via Spark against a separate server process vs Spark reading the same Parquet directly, with
    Spark's JDBC data source + Flight SQL JDBC driver as baseline (maintainer, 15:00). Part 1 done (harness, see
    Log): `scripts/perf/tpch.sh`, `TpchPerfTest` (-Pperf; modes parquet/flight/catalog/jdbc, `ANSI=`), workflow
    `Perf` (manual; all files as artifact). Next:
    (a) Q1 explained (2026-10-28, see Log): Spark pushes arithmetic aggregates only with
        `spark.sql.ansi.enabled=true`; views and `FlightCatalog` behave the same. Q13 done 2026-10-29
        (`FlightLikePushdown`, see Log: 1.46x -> 0.75x of direct). Q5 plan fixed 2026-10-29 (`statistics.columnWidth`,
        see Log): Flight and direct plans now have the same joins; Perf run 40802602269 (SF1, ANSI on, best of 3,
        direct vs Flight ms): Q3 1602/1817, Q5 1942/3016, Q7 1666/1765, Q10 1416/1215. Next session starts here:
        Q5's remaining 1.5x is reading `lineitem` (6M rows, 4 columns, no filter) over Flight vs Spark's local
        vectorized Parquet read (same as Q3, which is ~1.1x: measure where Q5's extra time goes — per-stage times
        from the Spark UI/event log of one run, `partition.size` 6 vs cores, server log; try
        `spark.flight.retainPushedFilters=auto` so the filtered `orders` keys prune `lineitem` at run time);
    (a2) Spark 3.5 + ANSI Q17 failure: done 2026-10-28 (`FlightPushedCounts`, see Log). Possible follow-up, not
        scheduled: a full ANSI-on Perf run (all queries) to refresh the numbers with Q17 included;
    (b) JDBC baseline types BIGINT keys as `decimal(20,0)` (Spark JDBC + driver metadata); harmless for the
        comparison (numbers compared by value) but worth one line in the report;
    (c) optionally SF10 (`sf` input; runner disk/heap permitting) and a run with `partition.size`/result-mode
        variations.

## Open questions

- For the maintainer (FYI, 2026-10-29, S5 a): scans now report bytes scaled to the read columns' share of the row
  width by default (`statistics.columnWidth=true`), as Spark does for Parquet: narrow reads of tables up to roughly
  5-10x `spark.sql.autoBroadcastJoinThreshold` on disk can now be broadcast where they were shuffled before. Results
  are unchanged; set `statistics.columnWidth=false` (per scan or as a `FlightCatalog` default) for the old plans. Say
  if the default should be `false` instead.

- For the maintainer (found 2026-10-28 by S5 a): on Spark 3.5 (`spark.sql.ansi.enabled=false` by default) no
  aggregate with `+ - * /` is pushed (TPC-H Q1 ~6x slower at SF1). Options: run production jobs with ANSI mode
  (also changes Spark's own overflow/cast behaviour: errors instead of nulls), or an opt-in connector optimizer
  rule that pushes such aggregates anyway — but then overflow semantics differ (Spark non-ANSI: null; DuckDB:
  error), so it would need the maintainer's go-ahead. Is ANSI mode an option on the cluster?

- For the server (found 2026-10-28 by S4, comparing upstream `feat/starrocks-native-integration`): three upstream
  Spark features need server support kestrel (at `kestrel.ref`) lacks. (1) Runtime bloom filters: upstream appends
  an "ARF2" block (split-block bloom filters with witness keys, upstream `RuntimeBloom`) to each endpoint ticket as
  protobuf field 15, which upstream's server (`RuntimeFilters`, `SplitBlockBloom`) applies to the rows it returns;
  a ticket format change, so not ported. (2) Complete aggregation push-down: upstream's statistics action reports
  `separableGroups` per column (file groups sharing no value) and `PushAggregation.setCompleteBy` makes the server
  keep those files in one endpoint, so Spark skips its final aggregation; needs both in kestrel. (3) Distinct counts
  (`ndv`) in `arrowflight.table_statistics` (kestrel sends null): needed for Spark CBO column statistics and
  sharper equality estimates. Does the maintainer want any of these in kestrel?

- Spark's standalone backend never creates a delegation token manager (only YARN and Kubernetes do), so shipped
  Flight sessions work on YARN/K8s only; standalone clusters with Kerberos need a TGT on every executor. Tests use
  a local cluster whose backend creates the manager as YARN does (`TokenManagingLocalClusterManager`).

- Spark versions on the production cluster (pending from the maintainer) — decides the default profile in S2.
  Also useful: whether Spark there is a "Hadoop free" build on the cluster's Hadoop 3.1.3 jars (what
  `ShadedJarHadoop313IT` models) or Spark's own Hadoop 3.3 client; the shaded jar is meant to work with both.
- For the server/client core (found 2026-10-28 by the S2 matrix): `arrow-flight-client-core` is Java 21 bytecode
  and uses Java 21 language features (pattern matching in `switch`: `Expr`, `SqlRenderer`), so the connector runs
  on JDK 21 only. Spark 3.3 and 3.4 cannot start on JDK 21 (`org.apache.spark.unsafe.Platform` looks up
  `DirectByteBuffer(long,int)`, removed in JDK 21; fixed in Spark 3.5), so those lines compile here (CI checks them)
  but are untestable and unusable. Supporting them needs the client core (and this connector) built for release 17;
  the server-side work is replacing the `switch` patterns. Also, if they are ever tested: Spark 3.3 brings Parquet
  1.12 (no `LZ4_RAW`), which breaks the in-process server on the test class path (pin Parquet in that profile).
- For the server/client core (forward, no change needed now): executors build one pooled `Client` per session
  (the client core pools by credentials and never evicts), so a long job leaves ~8 idle clients per executor and
  hour (sessions last 10 min, re-obtained every ~7.5). A client-core hook to swap the bearer of a pooled Kerberos
  client (or pool eviction) would remove that; a configurable session lifetime on the server would reduce it.
- For the client core (found 2026-10-28 by `FlightKerberosKdcTest`; matters for drivers that serve several users
  with `doAs`, e.g. proxy users / Thrift server, not for executors with a shipped session): `Client.getOrCreate`
  keys the pool by `UserGroupInformation.getCurrentUser()` on the caller thread, but `KerberosCredentialWriter`
  runs as gRPC `CallCredentials` on a gRPC executor thread, where `getCurrentUser()` is the process login user or
  the Kerberos login of whatever `doAs` created that pool thread. So inside `ugi.doAs` (without a process login)
  SPNEGO fails, or — if the gRPC threads were created inside another user's `doAs` — authenticates as that other
  user. Proposed fix: compute the `Negotiate` header on the caller thread before the handshake and pass the fixed
  value (as `FlightDelegationTokenProvider.handshake` does here). Connector workaround if wanted earlier: have
  `FlightSessionTokens.connect` obtain the session itself on the caller thread for Kerberos configs (needs a
  session cache per user/host/port to avoid a new pooled client per call).

## Log

- 2026-10-28 (maintainer): repo created from kestrel flight-spark (subtree split at kestrel b7cc47a600df, split head
  cd64bb2ed512; commit identities normalised to C0fuge). Standalone pom (parent properties vendored),
  `scripts/install-kestrel.sh` + `kestrel.ref` for the server artifacts, `ci.yml`; the integration tests' two small
  Parquet sets (`test_db`, `lineitem_tiny`) copied from the server module into `src/test/resources`. CI run
  40669479250 green: 175 tests, 0 failures, Checkstyle 0, SpotBugs 0 (kestrel install ~1 min, build+tests ~4.5 min).
- 2026-10-28 (session): S1 part 1 — `FlightDelegationTokenProvider` + `FlightSessionTokens` (driver obtains server
  sessions, executors use them instead of SPNEGO; settings `spark.flight.kerberos.*`, README "Kerberos on YARN"),
  12 new tests (stub-ticket TLS server with the server's Kerberos authenticator). Local `verify`: 187 tests, 0
  failures, Checkstyle 0, SpotBugs 0; CI run 40671758401 green (job logs not readable from the session: blob host
  blocked by the session proxy, step results only). Local build note: `./mvnw` could not download Maven 3.10.0 in
  the session; system `mvn` 3.9.11 worked (also for `scripts/install-kestrel.sh`'s build step). Next: S1 (a)/(b).
- 2026-10-28 (session): S1 (a) — `FlightKerberosKdcTest` (in-process Kerby KDC, real SPNEGO tickets against the
  server's GSS acceptor; test dep `kerb-simplekdc` 2.0.3), 4 new tests; `KerberosTestAuthenticator.gss`,
  `KerberosFlightServerFixture(CallHeaderAuthenticator)`. Found that the client core takes the Kerberos identity
  from the gRPC thread, not the caller's `doAs` (Open questions; tests ordered and the client-core SPNEGO case uses a
  process login). Local `verify` (system `mvn`): 191 tests, 0 failures, Checkstyle 0, SpotBugs 0; CI run
  40681503257 green. Next: S1 (b).
- 2026-10-28 (session): S1 done (Kerberos on YARN executors). `SparkKerberosTokenShippingTest`: Spark's real
  delegation token manager (`spark.kerberos.principal/keytab`, in-process KDC) ships the session to executor JVMs
  without any Kerberos login, which read a table from a TLS Flight server with the server's GSS acceptor in front of
  the in-process producer (`complete` answers, endpoints without location); no SPNEGO during the read. Plain
  `local-cluster` never creates a token manager (standalone), so the test registers an `ExternalClusterManager`
  (`TokenManagingLocalClusterManager`) whose backend creates it as YARN does. Found and fixed: driver-side schema
  inference and endpoint planning (`Table.initialize`/`initializeSchema` -> client-core `TableScan`) ignored the
  shipped session and did SPNEGO; they now use `FlightSessionTokens.resolve`. KDC setup shared in
  `KerberosKdcFixture`. Local `verify` (system `mvn`): 192 tests, 0 failures, Checkstyle 0, SpotBugs 0; CI run
  40686668051 green. Next: S2 (build matrix).
- 2026-10-28 (session): S2 part 1 — Maven profiles `spark-3.3` (3.3.4), `spark-3.4` (3.4.4), `spark-3.5` (3.5.9,
  default) and `spark-4.0` (4.0.5, Scala 2.13; `scala.binary.version` property), CI matrix over them (3.3/3.4 with
  `-DskipTests`: they cannot start on JDK 21, see Open questions). Fixes: `GenericInternalRow` instead of
  `InternalRow.fromSeq` (Scala 2.13), TimestampNTZ literal matched by type name (3.3); tests set `spark.driver.host`
  to the bound address (Spark 4 executors fetch session classes from the driver: connection refused otherwise) and
  cast explicitly in the boolean = tinyint join (Spark 4 ANSI default). Local (system `mvn`): 3.5 and 4.0 each 192
  tests, 0 failures, Checkstyle 0, SpotBugs 0; 3.3/3.4 compile + checks. CI run 40706483617 green (4 jobs). Commits
  carry no `Co-Authored-By` trailer (CLAUDE.md: no trailer e-mail). Next: S2 part 2 (Hadoop 3.1.3 / shading).
- 2026-10-28 (session): S2 done (part 2: shaded jar vs Hadoop 3.1.3). New `ShadedJarHadoop313IT` (failsafe, after
  `package`; tag `shaded`): a separate JVM runs Spark of the build line "Hadoop free" plus the Hadoop 3.1.3 client
  (class path resolved by a nested Maven run from `src/test/hadoop-3.1.3/pom.xml`: Guava 27, protobuf 2.5, Spark's
  own Arrow 12 / Netty 4.1 / Jackson 2.15) with only the shaded jar added after it, and reads `test_table` from the
  in-process server (full scan, pushed filter, pushed aggregate; source launcher runs `ShadedJarProbe.java`); a
  second test checks the jar holds no unrelocated copy of the shared packages and that Netty's native libraries are
  renamed. Found: the old jar (Jackson relocated only) hangs on its first read there — Spark's Arrow 12 wins over
  the connector's Arrow 19 (`apply() or fail() already called` in the Flight credentials, then no answer). Fix:
  relocate Arrow, FlatBuffers, Netty (+ `META-INF/native/libnetty_*` renamed: relocated epoll loads), gRPC,
  perfmark, protobuf, Guava, Gson, Apache Commons; slf4j no longer bundled; dead multi-release Jackson entries
  dropped. Passes on Spark 3.5.9 and 4.0.5 next to Hadoop 3.1.3. `scripts/install-kestrel.sh` takes `MVN=mvn`.
  Local (system `mvn`, 3.5): 192 tests + 2 IT, 0 failures, Checkstyle 0, SpotBugs 0; 4.0: the IT green. CI run 40719166518 green (4 jobs; the IT ran on 3.5 and 4.0). Note: the lock commit of this session carries a
  `Co-Authored-By` trailer by mistake (no rewrite of `main`); later commits have none. Next: S3 (Spark catalog).
- 2026-10-28 (session): S3 done (Spark catalog). `FlightCatalog` (`TableCatalog` + `SupportsNamespaces`,
  `spark.sql.catalog.<name>=pro.surpin.data.arrowflight.client.spark.FlightCatalog`, options under that prefix):
  server schemas are one-level namespaces (`GetDbSchemas`), tables come from `GetTables` with the Arrow schema
  (no query), `loadTable` returns the connector's `FlightTable` (same pushdown/columnar reads); the other catalog
  options are scan defaults (`FlightTable(config, table, scanDefaults)`, scan options win); `default.schema` is
  the default namespace; names exact (the server's LIKE `_` wildcard is filtered out client-side), non-plain names
  double-quoted in the FROM clause; DDL rejected (read-only). `FlightSource.configuration(options)` extracted.
  README "Spark catalog". 17 new tests (`FlightCatalogTest` unit, `SparkFlightCatalogTest` Spark local mode:
  `SHOW NAMESPACES`/`SHOW TABLES`/`DESCRIBE`/`SELECT` with pushdown, same rows as `format("flight")`, `USE`,
  missing names, DDL). Local (system `mvn`): 3.5 208 tests + 2 IT, 0 failures, Checkstyle 0, SpotBugs 0 (USE test
  added after: green on 3.5 and 4.0); 3.3 compiles. CI run 40725071252 green (4 jobs). Next: S4 (upstream ideas).
- 2026-10-28 (session): S4 part 1 — compared upstream `feat/starrocks-native-integration` `flight-spark` (at
  1de5f6b) with this repo: runtime filtering + AQE guard, runtime bloom filters, server column statistics, complete
  aggregation push-down; plan and server dependencies in Next steps S4 / Open questions. Ported the statistics part
  that the pinned server supports: with `statistics.filterSelectivity=true` a plain `schema.table` scan asks the
  server's `arrowflight.table_statistics` action (new `ServerColumnStatistics`, through `FlightSessionTokens` so
  shipped Kerberos sessions apply) for the filtered columns; `ScanSelectivity` uses footer bounds for ranges (value
  outside the bounds -> no rows, i.e. the one-row floor), null counts for `IS [NOT] NULL`, distinct counts for
  equality / IN when present; literals in the server's units (dates epoch days, timestamps epoch seconds); any
  failure or other source -> the old fixed defaults (unchanged; off by default, no extra call). README "Scan
  statistics". 9 new tests (`ScanSelectivityTest` stats cases, `SparkFilterStatisticsTest` Spark local mode: `id >
  max` estimated at 1 row, half the range ~half the rows, string column default). Spark 4.0 found two test issues
  (Scala `collectLeaves` signature, `Predicate.references()` order), fixed. Local (system `mvn`): 3.5 219 tests + 2
  IT, 0 failures, Checkstyle 0, SpotBugs 0; 4.0 the new tests green. CI run 40737627818 red (4.0 tests), 40738746483
  green (4 jobs). Next: S4 (b) runtime filtering.
- 2026-10-28 (session): S4 (b) done (runtime filtering, upstream port without bloom filters). Opt-in Spark
  settings: `spark.flight.retainPushedFilters=true|auto` (default `false`) returns accepted predicates to Spark too
  (`auto`: plain table with server statistics, `retainMaxRows` 50M, `retainMaxSelectivity` 0.1); only then does
  `FlightScan` (now `SupportsRuntimeFiltering`, V1 so the 3.3 profile compiles) offer `int`/`bigint` columns of
  scans with at least `runtimeFilterMinRows` (50M) rows to dynamic pruning, and `filter()` re-plans the scan with
  the keys (IN list; from `runtimeFilterRangeMinKeys` (1000) keys a min/max range; > 100k non-integer keys
  dropped; no re-plan for pushed aggregations). `FlightAqeGuard` ported (`spark.sql.extensions`); deviation from
  upstream: it counts distinct scan objects, because the pruning subquery repeats the filtering side's scan and
  upstream's count switched AQE off for every runtime-filtered plan. Found: without AQE Spark reuses the broadcast
  for pruning only when the scans' batches are equal, so `FlightBatch.equals` (same table instance) was added —
  otherwise the runtime filter became `true`. Settings read via `getActiveSession` (Spark 4's `active()` throws a
  checked exception without a session). README "Runtime filtering". New tests: `SparkRuntimeFilterTest` (5, Spark
  local mode: self-join with the guard, AQE path with a query source, range, `auto`, filtered `spark.range` side
  offered nothing by default; results equal to unpruned), `FlightScanTest` (+4), `FlightScanBuilderTest` (+1),
  `FlightAqeGuardTest`. Local (system `mvn`): 3.5 230 tests + 2 IT, 0 failures, Checkstyle 0, SpotBugs 0; 4.0 the
  new tests green; 3.3 compiles. CI run 40745229990 green (4 jobs). Next: S5 (perf).
- 2026-10-28 (session): S5 part 1 — TPC-H harness. `TpchPerfTest` (tag `perf`; `-Pperf`, which now skips the
  JaCoCo agent — it instrumented Spark, Q1 took ~3 min — and adds the Flight SQL JDBC driver 19.0.0 for the
  perf runs only) runs the 22 queries (`src/test/resources/tpch-queries`, validation parameters, checked against
  DuckDB's known SF1 answers) in Spark local mode in three modes: Parquet direct (reference), `format("flight")`,
  Spark JDBC + Flight SQL JDBC driver (big tables split by key into core-count ranges); every mode's rows are
  compared with the reference (numbers by value; 1e-6 for Spark's scale-6 `avg`, relative 1e-9 only with
  doubles; exact decimal sums must match) and a mismatch fails the run. Outputs `times.csv`, `summary.md`,
  `plans/<mode>/qNN.txt`, `env.txt`, `errors.txt`. `scripts/perf/tpch.sh`: data via `tpchgen-cli` 4.0.0 (pip;
  DuckDB's tpch extension is not downloadable from the session), server jar + libs built at `kestrel.ref`
  (`-Plinux-only`, as its release) and started as its own JVM (insecure, local; needs `--add-opens
  java.base/java.nio` or every DoGet fails with `DirectByteBuffer.<init>(long, int) not available`). Workflow
  `Perf` (manual; inputs sf/runs/modes/queries/kestrel_ref; artifact `tpch-sf<sf>-<run id>`, 90 days).
  `TpchPerfHelpersTest` (6 unit tests). Local (system `mvn`): 236 tests + 2 IT, 0 failures, Checkstyle 0,
  SpotBugs 0; local SF1 all 22 x 3 modes equal. CI run 40766073856 green (4 jobs). Perf run 40766081647 (SF1,
  3 runs, 4-core runner, server and Spark on one host; sum of medians): Parquet 20.1 s, connector 22.9 s (1.14x),
  JDBC 45.7 s (2.28x; connector 2.0x faster than JDBC); all rows equal. Connector faster than direct on Q2, Q6,
  Q14, Q17, Q19 (0.48-0.59x: filters/joins pushed to DuckDB), slower on Q1 2.16x, Q5 1.80x, Q13 1.50x, Q7 1.31x.
  This session's lock commit again carries a `Co-Authored-By` trailer by mistake (no rewrite of `main`). Next:
  S5 (a).
- 2026-10-28 (session): S5 (a) — why Q1 was 2.2x slower than direct: Spark translates `+ - * /` into connector
  expressions only in ANSI mode (`spark.sql.ansi.enabled`, default false on 3.5), so Q1's arithmetic SUMs keep the
  whole aggregation in Spark; temp views and `FlightCatalog` behave the same (not a view problem). `TpchPerfTest`:
  mode `catalog` (tables of `FlightCatalog`), `perf.ansi` (`ANSI=` in `tpch.sh`, `ansi` input of `Perf`; recorded
  in `env.txt`). New test `SparkTpchQ1PushdownIntegrationTest.nonAnsiPushesOnlyAggregatesWithoutArithmetic`
  (Q1 not pushed but equal without ANSI, plain-column aggregates pushed). README: ANSI note + known issue. Local
  SF1 (Q1/5/7/13, 3 runs): ANSI off connector 1.85x direct (Q1 2.30x), ANSI on 0.96x (Q1 0.18x). Perf run
  40779266604 (SF1, ANSI on, parquet/flight/catalog, sum of medians without Q17): flight 0.94x, catalog 0.89x of
  direct; Q1 0.12x, slower still Q5 1.66x, Q13 1.46x, Q7 1.29x, Q3 1.27x; all rows equal except Q17, which fails
  in both connector modes (Spark 3.5 bug with pushed AVG in a correlated subquery under ANSI; analysis and repro
  in Next steps S5 (a2); Spark 4.0 passes the repro) — so that Perf run is red. Local `verify` (system `mvn`, 3.5):
  238 tests + 2 IT, 0 failures, Checkstyle 0, SpotBugs 0; the new test also green on 4.0. CI run 40779248644
  green (4 jobs). Commits without `Co-Authored-By` trailer (CLAUDE.md). Next: S5 (a2), then the rest of (a).
- 2026-10-28 (session): S5 (a2) done — Spark 3.5 + ANSI TPC-H Q17 failure fixed. New optimizer rule
  `FlightPushedCounts`: rewrites Spark's merge of pushed partial counts (`sum` of a `bigint` column of a `FlightScan`
  with a pushed aggregation, below Filters/Projects; the connector pushes SUM only for decimal/floating types) to
  `coalesce(sum(count), 0)`, idempotently. Registered via reflection in the session's
  `experimental.extraOptimizations` (the only hook that runs after a subquery's own push-down and before the outer
  `RewriteCorrelatedScalarSubquery`) whenever a `FlightTable` is created (analysis time); failures only logged.
  `FlightScan.aggregated()` added. Side effect: pushed `count(*)` is now non-null as Spark's own. README note replaces
  the "known issue". Tests in `SparkTpchQ1PushdownIntegrationTest`: Q17-shaped correlated `avg` (with and without
  outer rows whose subquery is empty; fails with `ARITHMETIC_OVERFLOW` without the rule, checked) and rule
  idempotence. Spark 4.0 does not push that subquery's avg (results compared only). Local (system `mvn`, 3.5):
  239 tests + 2 IT, 0 failures, Checkstyle 0, SpotBugs 0; the test class green on 4.0. CI run 40788891671 green
  (4 jobs); Perf run 40788895111 (SF1, ANSI on, Q17, parquet/flight/catalog) green, rows equal. Next: rest of S5 (a)
  (Q13).
- 2026-10-29 (session): S5 (a) Q13 — `NOT LIKE '%special%requests%'` is no DSv2 predicate (Spark translates only
  `abc%`/`%abc`/`%abc%`), so every `o_comment` crossed the wire. New optimizer rule `FlightLikePushdown` (registered
  with `FlightPushedCounts` by the new `FlightOptimizations`, in `experimental.extraOptimizations`, i.e. after Spark's
  push-down): `[NOT] LIKE` conjuncts of a Filter directly above a non-aggregated Flight scan, on a plain string column,
  pattern of literals and `%` only (Spark escapes resolved; `_` and invalid escapes stay in Spark), are rendered as
  `col like '...' escape '\'`, added to the scan (`FlightScan.pushConditions`, re-plans on the server) and removed
  from the Filter; with a Project above, scan columns nothing else needs are dropped (relation copied via `makeCopy`,
  since the constructor differs on 3.3). Non-aggregated scans now always have a replanner (runtime filtering is a
  separate flag and keeps the pushed conditions). Setting `spark.flight.likePushdown` (default true); README note.
  Tests: `FlightLikePushdownTest` (pattern conversion), `FlightScanTest` (pushConditions/replans/columns),
  `SparkTpchQ1PushdownIntegrationTest.multiWildcardLikeIsSentToTheServerAndMatchesDirectParquet` (results = direct
  Parquet, case-sensitivity, `_` stays, Q13-shaped left join prunes `l_comment` and leaves no LIKE in Spark, setting
  off). A first version that kept the filter in Spark only reached 1.35x (Perf run 40792865155: `o_comment` still
  read). Perf run 40793985564 (SF1, ANSI on, parquet/flight, all queries green, rows equal): Q13 992 vs 1323 ms
  (0.75x); Q5 1.63x, Q7 1.33x still slower. Also fixed a race in `SparkRuntimeFilterTest` (AQE restore checked
  before the asynchronous query listener ran; failed once on 4.0 in CI run 40794869030): waits for the listener bus.
  Local `verify` (system `mvn`; `./mvnw` cannot download Maven in the session, use `MVN=mvn scripts/install-kestrel.sh`):
  245 tests + 2 IT, 0 failures, Checkstyle 0, SpotBugs 0; new tests green on 4.0. CI run 40795768365 green (4 jobs). Next: S5 (a) Q5.
- 2026-10-29 (session): S5 (a) Q5 — the server's byte estimate covers whole Parquet files, while Spark narrows its own
  Parquet relation under a projection by the row-width share of the read columns (`8 + sum(defaultSize)`), so
  `customer` (14 MB at SF1, 2 of 8 columns read) was broadcast only when read directly and Flight's Q5 plan had an
  extra SortMergeJoin. `FlightScanBuilder` now passes that share (`Table.widthShare`, not for pushed aggregations) to
  `Table.setPushdownWidth`, which scales the reported bytes (rows unchanged). New scan option
  `statistics.columnWidth` (default `true`; `false` = whole-file bytes as before); README "Scan statistics". Local Q5
  at SF1: plans now have the same joins as direct (one SMJ orders-lineitem, AQE broadcasts it in both). Tests:
  `TableTest` (width share, bytes scaling, aggregates), `ScanSelectivityTest` (builder on/off),
  `SparkFilterStatisticsTest.prunedScanReportsItsShareOfTheRowWidth` (bytes = share of full, opt-out, a threshold
  between the two sizes broadcasts only the narrowed scan). `SparkRuntimeFilterTest.offeredToSparksPruningOnlyWhenEnabled`
  pins `statistics.columnWidth=false`: with the narrower estimate the tiny test table no longer looks worth pruning
  to Spark (same rule as for Parquet). CI run 40802560434 failed on that test (3.5, 4.0); fixed. Local `verify`
  (system `mvn`): 249 tests + 2 IT, 0 failures, Checkstyle 0, SpotBugs 0. CI run 40803194370 green (4 jobs). Perf run
  40802602269 (Q3/5/7/10, flight, SF1, ANSI on): Q7 1.33x -> 1.06x, Q5 1.63x -> 1.55x, Q3 1.13x, Q10 0.86x.
  Note: artifacts and job-log downloads through `gh api` are blocked by the session proxy (blob storage); the GitHub
  MCP `get_job_logs` tool works, and `scripts/perf/tpch.sh` runs locally (SF1 data via pip `tpchgen-cli`, ~5 min).
