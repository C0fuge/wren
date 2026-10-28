#!/usr/bin/env bash
# TPC-H through Spark (local mode): the connector against a separate Flight server process, Spark reading
# the same Parquet files directly, and Spark's JDBC data source over the Flight SQL JDBC driver (S5 in
# docs/STATUS.md). Steps: generate the data with tpchgen-cli (pip), build the server's shaded jar + libs at
# kestrel.ref (in .kestrel, as scripts/install-kestrel.sh), start the server on the data, run TpchPerfTest
# (-Pperf), stop the server. Results in $OUT (default target/perf): times.csv, summary.md, env.txt, plans/,
# errors.txt, server.log.
#
#   scripts/install-kestrel.sh && scripts/perf/tpch.sh
#   SF=1 RUNS=3 MODES=parquet,flight,jdbc QUERIES=all MVN=mvn scripts/perf/tpch.sh
#
# Needs JDK 21, python3 with pip (tpchgen-cli), and scripts/install-kestrel.sh run before (its checkout in
# .kestrel). Extra test JVM options for Spark: TEST_JVM_OPTIONS (e.g. -Xmx8g).
set -euo pipefail

root="$(cd "$(dirname "$0")/../.." && pwd)"
sf="${SF:-1}"
runs="${RUNS:-3}"
modes="${MODES:-parquet,flight,jdbc}"
queries="${QUERIES:-all}"
port="${PORT:-32010}"
mvn="${MVN:-$root/mvnw}"
kestrel="${KESTREL_DIR:-$root/.kestrel}"
out="${OUT:-$root/target/perf}"
data="${DATA:-$root/target/tpch-sf$sf}"
server_heap="${SERVER_HEAP:-4g}"

[ -d "$kestrel/flight-server" ] || { echo "no server checkout in $kestrel: run scripts/install-kestrel.sh" >&2; exit 1; }
mkdir -p "$out"

if [ ! -d "$data/tpch/lineitem" ]; then
  command -v tpchgen-cli >/dev/null || python3 -m pip install --quiet "tpchgen-cli==${TPCHGEN_VERSION:-4.0.0}"
  # One directory per table (<data>/tpch/<table>/<table>.<n>.parquet): the server's schema.table layout.
  tpchgen-cli parquet -s "$sf" --parts=4 --output-dir="$data/tpch"
fi
du -sh "$data/tpch"/* | tee "$out/data-sizes.txt"

jar="$(ls "$kestrel"/flight-server/target/arrow-flight-server-*-shaded.jar 2>/dev/null | head -1 || true)"
if [ -z "$jar" ] || [ ! -d "$kestrel/flight-server/target/libs" ]; then
  # Linux-only server jar plus the Hadoop client libraries it leaves to the deployment (as its release).
  (cd "$kestrel" && ${MVN_KESTREL:-./mvnw} --batch-mode --no-transfer-progress -q \
    -pl flight-server -am package -Plinux-only -DskipTests)
  jar="$(ls "$kestrel"/flight-server/target/arrow-flight-server-*-shaded.jar | head -1)"
fi
echo "server jar: $jar (kestrel $(git -C "$kestrel" rev-parse HEAD))" | tee "$out/server.txt"

# No TLS and no authentication: a local benchmark only. Netty (Arrow's allocator) needs java.nio opened.
FLIGHT_ALLOW_INSECURE=true java "-Xmx$server_heap" \
  --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
  -Dio.netty.tryReflectionSetAccessible=true \
  -cp "$jar:$kestrel/flight-server/target/libs/*" pro.surpin.data.arrowflight.server.HadoopArrowFlightServer \
  --data-dir "$data" --localhost 127.0.0.1 --port "$port" --hosts 127.0.0.1 > "$out/server.log" 2>&1 &
server=$!
trap 'kill "$server" 2>/dev/null || true; wait "$server" 2>/dev/null || true' EXIT
for _ in $(seq 1 120); do
  grep -q "Server Arrow Flight SQL started" "$out/server.log" && break
  kill -0 "$server" 2>/dev/null || { cat "$out/server.log" >&2; echo "server did not start" >&2; exit 1; }
  sleep 1
done
grep -q "Server Arrow Flight SQL started" "$out/server.log" || { echo "server start timed out" >&2; exit 1; }

cd "$root"
"$mvn" --batch-mode --no-transfer-progress -Pperf test -Dtest=TpchPerfTest -Dsurefire.failIfNoSpecifiedTests=false \
  -Dperf.tpch.data="$data" -Dperf.flight.port="$port" -Dperf.runs="$runs" -Dperf.modes="$modes" \
  -Dperf.queries="$queries" -Dperf.out="$out" -Dperf.sf="$sf" ${TEST_JVM_OPTIONS:+"-DargLine=$TEST_JVM_OPTIONS"}
