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

S1. **Kerberos on YARN executors.** Today every executor does its own SPNEGO and fails without a TGT. Add a Spark
    `HadoopDelegationTokenProvider` (registered through the service loader) that, on the driver, obtains a server
    session/token with Kerberos SPNEGO (as today) and ships it in the job credentials; executors read it from the
    credentials and use it instead of their own SPNEGO (fall back to today's behaviour when absent). Tests: unit
    tests for the provider and the executor side; CI job with an MIT KDC container + generated keytab running
    `spark-submit --master local[2] --keytab ... --principal ...` against a Kerberos-enabled server; a negative test
    (no token, no TGT → clear error, no silent empty result).
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

- Spark versions on the production cluster (pending from the maintainer) — decides the default profile in S2.

## Log

- 2026-10-28 (maintainer): repo created from kestrel flight-spark (subtree split at kestrel b7cc47a600df, split head
  cd64bb2ed512; commit identities normalised to C0fuge). Standalone pom (parent properties vendored),
  `scripts/install-kestrel.sh` + `kestrel.ref` for the server artifacts, `ci.yml`; the integration tests' two small
  Parquet sets (`test_db`, `lineitem_tiny`) copied from the server module into `src/test/resources`. CI run
  40669479250 green: 175 tests, 0 failures, Checkstyle 0, SpotBugs 0 (kestrel install ~1 min, build+tests ~4.5 min).
