#!/usr/bin/env bash
# Copyright 2024 RustFS Team
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Runs the spike test suite in the two environments that matter for P2
# (backlog#894), and — crucially — ASSERTS which path each leg actually took
# (rustfs/backlog#1067), so neither leg can silently degenerate into a vacuous
# pass:
#
#   leg 1  io_uring blocked by an EXPLICIT seccomp profile (not the host
#          Docker default), reproducing the #4313 restricted environment.
#          Every test MUST degrade to a graceful skip; if none skip, io_uring
#          was not actually blocked and the leg FAILS.
#   leg 2  seccomp=unconfined — real io_uring against the host kernel. NO test
#          may skip; a single skip means io_uring did not really run and the
#          leg FAILS.
set -euo pipefail
cd "$(dirname "$0")"

IMG="${SPIKE_IMAGE:-rust:1-bookworm}"
CACHE_REG=uring-spike-cargo-registry
CACHE_TARGET=uring-spike-target
SECCOMP_PROFILE="$PWD/seccomp-block-uring.json"

# Run the suite once, capturing stdout+stderr (so the tests' "SKIP" lines,
# printed to stderr, are inspectable).
run_and_capture() {
    docker run --rm "$@" \
        -v "$PWD":/spike:ro \
        -v "$CACHE_REG":/usr/local/cargo/registry \
        -v "$CACHE_TARGET":/spike-target \
        -e CARGO_TARGET_DIR=/spike-target \
        -e CARGO_TERM_COLOR=always \
        -w /spike \
        "$IMG" \
        cargo test --release -- --nocapture --test-threads=1 2>&1
}

echo "=================================================================="
echo "== leg 1: io_uring blocked by explicit seccomp profile (degradation expected)"
echo "=================================================================="
leg1_out=$(run_and_capture --security-opt seccomp="$SECCOMP_PROFILE") || {
    echo "$leg1_out"
    echo "leg 1 FAILED: the suite errored (a blocked probe must degrade, not fail)"
    exit 1
}
echo "$leg1_out"
if ! grep -q "SKIP " <<<"$leg1_out"; then
    echo "leg 1 FAILED: expected graceful-degradation SKIPs but saw none — io_uring was NOT blocked"
    exit 1
fi

echo "=================================================================="
echo "== leg 2: seccomp=unconfined (real io_uring)"
echo "=================================================================="
leg2_out=$(run_and_capture --security-opt seccomp=unconfined) || {
    echo "$leg2_out"
    echo "leg 2 FAILED: the suite errored"
    exit 1
}
echo "$leg2_out"
if grep -q "SKIP " <<<"$leg2_out"; then
    echo "leg 2 FAILED: a test skipped — io_uring did not actually run here (vacuous pass)"
    exit 1
fi

echo "both legs passed (leg 1 degraded gracefully, leg 2 ran real io_uring)"
