# Working in this repository (instructions for Claude sessions)

Spark DataSource V2 connector for our Arrow Flight SQL server (DuckDB over Parquet on HDFS/S3/lakeFS; Arrow
results; "partial" result mode = one endpoint per node, backup locations for failover). Split on 2026-10-28 from the
`flight-spark` module of the server repository C0fuge/kestrel (history kept). Read first: `README.md` and
`docs/STATUS.md` (state, backlog, decisions, Log).

## Build

- The connector depends on the server repository's `arrow-flight-client-core` (compile) and `arrow-flight-server`
  + its test-jar (in-process server for the tests). `scripts/install-kestrel.sh` builds and installs them into the
  local Maven repository at the commit pinned in `kestrel.ref` (`KESTREL_REF=<sha|branch>` overrides it). Then
  `./mvnw verify` (all tags except `perf` and `real-services`; Spark local mode), `checkstyle:check spotbugs:check`.
- Bump `kestrel.ref` deliberately (one commit, CI green, Log line) when the connector needs a newer client core or
  server; CI can try another ref with the `kestrel_ref` dispatch input first.
- JDK 21, Spark 3.5 / Scala 2.12 today (see Next steps for the matrix). Code conventions as in the server repo:
  Javadoc on every class and non-override method, Checkstyle + SpotBugs clean (exclusions only in
  `spotbugs-exclude.xml` with a reason), JUnit 5 with tags (`unit`, `integration`, `spark`, `smoke`, `perf`).

## Session routine

A recurring cloud session develops this repo; it starts with zero context every time.

1. **Lock.** `git fetch origin`. If `docs/LOCK` holds a UTC time less than 150 minutes old, another session is
   working: stop without changes. Otherwise write `date -u +%FT%TZ` to `docs/LOCK`, commit `[skip ci] lock`, push.
   Empty the file in the last commit of the session.
2. **Branch.** Work on `main` (if pushing to `main` is rejected, use `claude/spark-dev` and keep using it). Never
   force-push `main`; `git pull --rebase` before every push.
3. **CI.** Workflow `ci.yml` (push + `workflow_dispatch`). `gh api` works in sessions; `gh workflow run` may not —
   dispatch with `gh api -X POST repos/C0fuge/wren/actions/workflows/ci.yml/dispatches -f ref=main`, then
   `gh api repos/C0fuge/wren/actions/runs?per_page=3`, `gh run view <id> --log-failed`. Local Maven in the session is
   fine too when it can download dependencies.
4. **Work** on the first open item of "Next steps" in `docs/STATUS.md`; finish one item properly (code + tests + CI
   green + STATUS update) rather than starting several.
5. **Before ending** (≤ ~90 min): update `docs/STATUS.md` (dated Log line with CI run ids, Next steps, Open
   questions), release the lock, commit, push. Nothing may stay only in the session.

## Rules

- The repo is **public** (maintainer's decision: free Actions minutes). Everything pushed — code, docs, releases, CI
  logs — is visible to anyone: never commit secrets, credentials or personal data (names, e-mail addresses, home
  paths, host names of the maintainer's machines or of production systems). Commits use
  `C0fuge <189659128+C0fuge@users.noreply.github.com>` and no other identity or trailer e-mail. Keep the repo
  low-profile: no description, topics or website, no links to it from other projects.
- Commit/push only to this repository. Never change other repositories (the server repository included): what the
  connector needs from the server goes to "Open questions" (or is worked around here) — the maintainer forwards it.
- Tests with every change: new code paths get unit tests, bug fixes a regression test; Spark behaviour is checked in
  Spark local mode against the in-process server.
- Actions minutes are free (public repo), but runners are shared: cache Maven, run the full build once per change
  set, `perf` tests only on request.
- Correctness first: never return wrong or partial results silently.
- Don't change what the server or existing jobs depend on (ticket format, result modes `partial`/`complete`,
  headers `x-flight-result-mode`, `x-flight-location-failover`, option names, auth) without writing it into Open
  questions first. Purely additive features (new optional settings) need no approval: implement, document, Log.
- No secrets in the repo; test credentials (tokens, keytabs, KDC principals) are generated in the job.
- Docs and commit messages in English.
