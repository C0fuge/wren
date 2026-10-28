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

(S1 Kerberos on YARN executors: done 2026-10-28, see Log.)

S2. **Build matrix.** Maven profiles for Spark 3.3 / 3.4 / 3.5 (Scala 2.12) and Spark 4.0 (Scala 2.13, JDK 17/21);
    CI matrix over them. Check the shaded jar against a Hadoop 3.1.3 client classpath: Netty, protobuf, Guava and
    Jackson relocated, no clashes (a test that loads the shaded jar next to Hadoop 3.1.3 client jars).
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
