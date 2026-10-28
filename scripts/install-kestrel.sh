#!/usr/bin/env bash
# Builds the server repository's artifacts this connector needs and installs them into the local
# Maven repository: the parent pom, arrow-flight-client-core (compile dependency) and
# arrow-flight-server with its test-jar (in-process server for the tests). The commit is pinned
# in kestrel.ref; override with KESTREL_REF=<sha|branch>. Used by CI and for local builds.
#
#   scripts/install-kestrel.sh            # build at kestrel.ref
#   KESTREL_REF=main scripts/install-kestrel.sh
#   MVN=mvn scripts/install-kestrel.sh    # build with another Maven than the wrapper
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
ref="${KESTREL_REF:-$(tr -d '[:space:]' < "$root/kestrel.ref")}"
repo="${KESTREL_REPO:-https://github.com/C0fuge/kestrel.git}"
dir="${KESTREL_DIR:-$root/.kestrel}"

if [ ! -d "$dir/.git" ]; then
  git init -q "$dir"
  git -C "$dir" remote add origin "$repo"
fi
git -C "$dir" fetch -q --depth 1 origin "$ref"
git -C "$dir" checkout -q --force FETCH_HEAD
echo "kestrel at $(git -C "$dir" rev-parse HEAD)"

# -DskipTests still compiles and packages the test classes (the server test-jar is needed).
(cd "$dir" && ${MVN:-./mvnw} --batch-mode --no-transfer-progress -q \
  -pl flight-client-core,flight-server -am install -DskipTests)
