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

S3. **Spark catalog.** A proper `TableCatalog` + `SupportsNamespaces` over the server's metadata (schemas, tables,
    columns), configured as `spark.sql.catalog.<name>=<class>` with options under that prefix; `SHOW NAMESPACES`,
    `SHOW TABLES`, `DESCRIBE`, `SELECT` through the catalog; tests in Spark local mode.
S4. **Upstream ideas.** Look at upstream github.com/VadimSurpin/arrow-flight-server branch
    `feat/starrocks-native-integration`, Spark side: runtime / bloom filters, AQE guard, post-pushdown statistics
    (part of the statistics is already here). Port clear wins with tests; anything that changes the server ticket
    format goes to Open questions first.
S5. **Perf.** TPC-H SF1 via Spark against a server container (kestrel pre-release jar from its releases) vs Spark
    reading the same Parquet directly; report per-query times and totals in the Log (record all metrics and raw
    files as CI artifacts so runs never need repeating).

## Open questions

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
  Local (system `mvn`, 3.5): 192 tests + 2 IT, 0 failures, Checkstyle 0, SpotBugs 0; 4.0: the IT green. CI run CIRUN. Note: the lock commit of this session carries a
  `Co-Authored-By` trailer by mistake (no rewrite of `main`); later commits have none. Next: S3 (Spark catalog).
