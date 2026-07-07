#!/usr/bin/env bash
# Copyright 2026 RustFS Team
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
# (backlog#894):
#
#   leg 1  default Docker seccomp — io_uring is (usually) blocked, which is
#          exactly the #4313 incident environment. Tests must degrade to a
#          graceful skip via the probe, never fail.
#   leg 2  seccomp=unconfined — real io_uring against the host kernel.
#          The full cancel-safety suite runs.
set -euo pipefail
cd "$(dirname "$0")"

IMG="${SPIKE_IMAGE:-rust:1-bookworm}"
CACHE_REG=uring-spike-cargo-registry
CACHE_TARGET=uring-spike-target

run_leg() {
    local title="$1"
    shift
    echo "=================================================================="
    echo "== $title"
    echo "=================================================================="
    docker run --rm "$@" \
        -v "$PWD":/spike:ro \
        -v "$CACHE_REG":/usr/local/cargo/registry \
        -v "$CACHE_TARGET":/spike-target \
        -e CARGO_TARGET_DIR=/spike-target \
        -e CARGO_TERM_COLOR=always \
        -w /spike \
        "$IMG" \
        cargo test --release -- --nocapture --test-threads=1
}

run_leg "leg 1: default seccomp (restricted env expected)"
run_leg "leg 2: seccomp=unconfined (real io_uring)" --security-opt seccomp=unconfined
echo "both legs passed"
