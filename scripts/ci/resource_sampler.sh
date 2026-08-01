#!/usr/bin/env bash
# Background resource sampler for the Test and Lint lane (issue #5394).
#
# The post-mortem `pgrep` in ci.yml runs only after `timeout` has already TERM'd
# the whole cargo process group, so it can never name a wedged process. This
# samples every 60s instead; the last samples before a timeout show what was
# stuck — rustc, the linker, a build script, memory pressure.
#
# WHY THE CGROUP READINGS MATTER
#
# The runners are ARC (Actions Runner Controller) pods, so /proc/loadavg,
# /proc/pressure/*, `free` and `df` are all *node*-level: they include every
# other runner pod scheduled on the same Kubernetes node. A sample showing high
# I/O pressure says nothing about whether *this* job caused it. Measured
# evidence: one sample reported loadavg 6.67 with 832 threads node-wide while
# `ps` inside the pod showed about 10 processes.
#
# Only the /sys/fs/cgroup/* values and `ps` are attributable to this job, so
# those are what any CARGO_BUILD_JOBS experiment must be judged on. The
# node-level readings are kept — co-tenancy is itself a real cause of stalls,
# and a 9m57s `git checkout` was traced to it — but labelled NODE-LEVEL so
# nobody reads them as this pod's own load.
#
# Usage:
#   scripts/ci/resource_sampler.sh start <phase>   # e.g. clippy, nextest, doctest
#   scripts/ci/resource_sampler.sh stop
#
# Safe to call `stop` when nothing is running, and safe to `start` a new phase
# while another is sampling — the previous one is stopped first. Never fails the
# calling step: this is diagnostics, not a gate.
set -uo pipefail

# GNU date on the runners; the fallback keeps local smoke tests on macOS quiet.
now() { date --utc --iso-8601=seconds 2>/dev/null || date -u +%Y-%m-%dT%H:%M:%S+00:00; }

LOG_DIR="artifacts/test-and-lint"
LOG_FILE="${LOG_DIR}/sampler.log"
PID_FILE="${LOG_DIR}/.sampler.pid"
INTERVAL="${SAMPLER_INTERVAL_SECS:-60}"

stop_sampler() {
    [ -f "$PID_FILE" ] || return 0
    local pid
    pid="$(cat "$PID_FILE" 2>/dev/null || true)"
    if [ -n "${pid:-}" ]; then
        kill "$pid" 2>/dev/null || true
    fi
    rm -f "$PID_FILE"
}

sample_once() {
    echo "=== $(now)"

    # Attributable to this pod: cgroup v2 accounting and our own process table.
    echo "--- POD cpu/mem limits"
    grep -H . /sys/fs/cgroup/cpu.max /sys/fs/cgroup/memory.max \
              /sys/fs/cgroup/memory.peak /sys/fs/cgroup/memory.current 2>/dev/null || true
    echo "--- POD pressure (cgroup v2, this pod only)"
    grep -H . /sys/fs/cgroup/cpu.pressure /sys/fs/cgroup/io.pressure \
              /sys/fs/cgroup/memory.pressure 2>/dev/null || true
    echo "--- POD nproc"; nproc 2>/dev/null || true

    # NODE-LEVEL: shared with every other runner pod on this Kubernetes node.
    # Useful for spotting co-tenancy, useless for attributing load to this job.
    echo "--- NODE-LEVEL load (shared with co-tenant runners)"; cat /proc/loadavg 2>/dev/null || true
    echo "--- NODE-LEVEL psi (shared with co-tenant runners)"
    grep -H . /proc/pressure/* 2>/dev/null || true
    echo "--- NODE-LEVEL mem (shared with co-tenant runners)"; free -m 2>/dev/null || true
    echo "--- NODE-LEVEL disk (shared with co-tenant runners)"
    df -h / /home/runner 2>/dev/null || df -h / 2>/dev/null || true

    echo "--- top-rss"
    ps -eo pid,ppid,stat,etime,rss,pcpu,args --sort=-rss 2>/dev/null | head -15 || true
    echo "--- build/test processes"
    ps -eo pid,ppid,stat,etime,rss,pcpu,args 2>/dev/null \
        | grep -E '[c]argo|[r]ustc|[n]extest|[c]ollect2|rust-ll[d]|[b]uild-script|deps[/]' || true
    echo "--- d-state (uninterruptible IO)"
    ps -eo pid,stat,etime,args 2>/dev/null | awk 'NR > 1 && $2 ~ /D/' || true
    echo
}

case "${1:-}" in
    start)
        phase="${2:-unknown}"
        mkdir -p "$LOG_DIR"
        stop_sampler
        {
            echo "--- phase=${phase} started_at=$(now) runner=${RUNNER_NAME:-unknown}"
        } >> "$LOG_FILE" 2>&1 || true
        (
            while true; do
                sample_once >> "$LOG_FILE" 2>&1 || true
                sleep "$INTERVAL"
            done
        ) &
        echo $! > "$PID_FILE"
        ;;
    stop)
        stop_sampler
        ;;
    *)
        echo "usage: $0 start <phase> | stop" >&2
        exit 2
        ;;
esac

exit 0
