# Legacy Heal Outcome Compatibility

This fixture executes the real `madmin-go` HTTP decoder and a pinned `mc` binary against synthetic v3 responses. Rust owner, admin adapter, and SDK tests validate the same JSON cases in `crates/madmin/tests/fixtures/heal-outcome-v3.json`. It does not run a storage repair or prove distributed recovery.

Pinned primary sources:

- `mc` release `RELEASE.2025-08-13T08-35-41Z`, commit `7394ce0dd2a80935aded936b09fa12cbb3cb8096`: [polling implementation](https://github.com/minio/mc/blob/7394ce0dd2a80935aded936b09fa12cbb3cb8096/cmd/admin-heal-ui.go#L414).
- Its `madmin-go/v3` dependency is `v3.0.107-0.20250415152934-4b504b82db63`: [decoder and response type](https://github.com/minio/madmin-go/blob/4b504b82db633e978a57d49443b2be75824244c3/heal-commands.go#L101).

The old decoder ignores additional JSON fields. The old poller returns success for `finished` without examining `detail`; only `stopped` returns a terminal error. Consequently `completed_with_errors` retains its canonical outcome and complete traversal coverage, but uses legacy summary `stopped`. `completed` describes execution only: unknown storage receipts remain `unknown`, not `repaired`.

Run from the repository root with an isolated tool cache and binary directory:

```sh
(
set -eu
compat_dir=$(mktemp -d)
trap 'rm -rf "$compat_dir"' EXIT
export GOPATH="$compat_dir/gopath" GOMODCACHE="$compat_dir/mod" GOCACHE="$compat_dir/cache" GOBIN="$compat_dir/bin"
export CGO_ENABLED=0 GOTOOLCHAIN=local GOMAXPROCS=2
go install github.com/minio/mc@v0.0.0-20250813083541-7394ce0dd2a8
cd scripts/compat/heal-outcome
MC_BINARY="$compat_dir/bin/mc" NO_PROXY=127.0.0.1,localhost go test -mod=readonly -p 2 -count=1 -v ./...
)
```

The subshell keeps the calling shell unchanged. `MC_BINARY` is mandatory and its Go build metadata must identify the pinned commit. Each subprocess gets a temporary mc configuration directory and synthetic credentials; it never edits the user's mc configuration. Loopback socket permission is required. The Go tests do not skip unavailable prerequisites.

Six cases cover completed traversal, unknown repair proof, completed traversal with failures, cancellation, deadline, and untraversable listing. Two receiver cases carry a remote `finished` summary that contradicts an aborted or completed-with-errors outcome. The admin test applies the heal owner's wire validator to each `remoteResponse` and must produce the corresponding public `response`; the old CLI must then exit with an error. Unknown extension fields remain intact. Unknown or missing execution fields cannot validate a successful summary.

New counters do not replace or reinterpret legacy progress. Outcome is a cumulative snapshot, not a page delta; `sinceSeq` only pages legacy result items. Result cursors and truncation markers remain separate from execution and traversal coverage.

Two existing CLI limitations remain explicit: this mc does not terminate on `notFound`, and `-f` polling sends `forceStart` together with `clientToken`, a combination the RustFS v3 request contract rejects. The fixture uses the standard non-force polling flow. Neither limitation is hidden by emitting a new summary string or reporting a missing task as completed.
