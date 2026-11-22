# SSE black-box suite

[中文](README.zh-CN.md)

Business-level tests of server-side encryption against a real `rustfs` process. A case sees only what a client sees: S3 responses, admin API responses, environment variables, and the two startup events the product promises. The suite is written in Python with the standard library only.

## What it answers

- Does each SSE scheme (SSE-S3, SSE-KMS, SSE-C) round-trip bytes and answer with exactly the response headers it should, under every key source the process can start with?
- Are malformed, mismatched and unauthorized requests refused explicitly, with nothing stored?
- Do two server binaries behave the same? Every HTTP exchange inside a case is recorded as an observation vector, and `diff` compares two runs field by field.

## Run

```bash
uv run --python 3.12 --no-project scripts/test/sse_blackbox/suite.py bin add --name head-default --path target/debug/rustfs --commit "$(git rev-parse --short HEAD)" --build default
uv run --python 3.12 --no-project scripts/test/sse_blackbox/suite.py run --binary head-default
uv run --python 3.12 --no-project scripts/test/sse_blackbox/suite.py diff <run-a> <run-b>
```

`bin add` copies the binary into `target/sse-bb/bin/<name>/` and records its SHA-256, so a later rebuild of `target/debug/rustfs` cannot change what a run tests. A binary built before the `sse_local_master_key` startup event existed is registered with `--no-master-key-event`; the event is then recorded but not asserted.

A run starts one server per process configuration, one after another on the same port, each on a fresh data directory under `target/sse-bb/runs/<run id>/`. The port defaults to 20105; set `SSE_BB_PORT` or pass `--port` to change it. A busy port is an error: the suite does not move to another port and does not stop the process that holds it.

## Process configurations

| Name | Key source |
|---|---|
| `e0` | No KMS, no local master key |
| `e1` | No KMS, local master key loaded |
| `e1x-text`, `e1x-short` | No KMS, local master key set to an invalid value |
| `e2` | Local KMS backend, per-key authorization off |
| `e2e` | Local KMS backend, per-key authorization enforced |
| `e2e-audit` | As `e2e`, with audit entries delivered to a receiver inside the driver |
| `e2m` | Local KMS backend and a loaded local master key |
| `e3` | Vault Transit backend |
| `e4` | Vault KV2 backend |
| `e8` | Static backend: one key from the environment |

`e3` and `e4` need the `vault` binary on `PATH` (or at the path in `SSE_BB_VAULT_BIN`). The first of them to start launches one Vault development server on `SSE_BB_VAULT_PORT` (default 20108; Vault also takes the next port for its cluster listener) as a child of the driver, and the run stops it at its end; nothing has to be running beforehand and Docker is not used. The root token is generated for the run, reaches the server under test through its environment, and is redacted from `vault/vault.log` in the run directory. Without the binary, or with the port taken, every case of these configurations reports `env-error`. `backends.py` holds these configurations and the table of what each backend can do; a case skips only by that table.

`e2e-audit` starts an HTTP receiver on `SSE_BB_AUX_PORT` (default 20107) in a thread of the driver and points the server's audit webhook at it. With the port taken, its cases report `env-error`.

## Leak scans

Every secret a run uses is a canary: each SSE-C key a request sends, the local master key (also an invalid value, including its first and last characters), every other secret in a server's environment, the material of each Local-backend key file, and each plaintext data key the admin API returns. A canary is searched for in standard base64, URL-safe base64, hex and as raw bytes, and by the first 12 characters of each spelling. The registry lives in memory only; a report names a canary by kind and digest, never by value.

`archive.py` keeps every HTTP exchange of the run in `archive/exchanges.jsonl`: response headers and documents in full, of a larger object body its hash and its first and last bytes. The full response is searched when it arrives. Request bodies are kept as hash and length, the value of a secret request header as a digest, and a canary found in a response as a marker.

Four cases of the configuration `run` judge the run as a whole after its last site stopped: no response contains a canary (`SEC-01`), no server log does (`SEC-02`), no response carries a storage-internal header (`SEC-03`), and every 500 says one of a fixed set of texts (`SEC-06`). The one accepted occurrence of a canary is a generated data key in the body of the response that returned it. Before scanning, the run plants a throwaway canary in a file and requires the scanner to find it; a scan with nothing to search for reports `skip`, not `pass`.

## Output

Each run directory holds `run.json` (binary identity, configurations, counts), `results.jsonl` (one verdict per case and per table row), `observations.jsonl` (the observation vectors), `report.md`, the server logs under `sites/`, the exchange archive under `archive/`, and `DONE` with the overall status. A run without `DONE` did not finish and cannot be compared.

A verdict is `pass`, `fail` (an expectation about the product does not hold) or `env-error` (the site is down or a dependency is missing). An environment error is never a pass and never a product failure, and a run with one does not succeed.

Two more verdicts cover behavior the product is known not to meet yet. An expectation written with `known_deviation` stays in the case as the intended behavior: while it does not hold the case reports `known-deviation` and the run still succeeds; once it holds the case reports `deviation-fixed` and the run fails until the marker is turned into a plain `check`.

## Comparing two binaries

`diff` reports three things: verdicts that differ, observations that differ, and cases or exchanges present on one side only. The differences it accepts form a closed list in `diffing.py`; anything else makes the comparison fail. It exits 0 when nothing blocking differs, 1 otherwise, and 2 when the two runs share no exchange, so an empty result cannot be mistaken for agreement.

An observation keeps what is deterministic (status, error code and message, SSE headers, the set of response header names, body hash, the encryption metadata key names stored for the object) and reduces random values (ETags of encrypted objects, version ids, upload ids) to their shape.

Equal behavior of two binaries does not show that one can read what the other wrote. `cross` covers that: the writer binary stores every form the configuration can produce (each scheme, empty to multi-megabyte, single and multipart), the server is stopped, and the reader binary is started on the same data directory and must read every object back.

```bash
uv run --python 3.12 --no-project scripts/test/sse_blackbox/suite.py cross --writer base-default --reader head-default
```

## Stored form

The `SF` cases form a second layer: they read the drives of the site instead of its responses. They state which metadata keys an encrypted object has, which of their values are fixed and what shape the random ones have, that every drive holds the same copy, and that no file on any drive contains the plaintext, the customer key, a data key or an object key. A failure in this layer means the stored form changed, not that a request misbehaved.

The cases decode `xl.meta` with the repository's own reader, built once with `cargo build -p rustfs-filemeta --example dump_fileinfo`; `SSE_BB_DUMP_FILEINFO` points the suite at another copy. Without a reader the cases that need one report `env-error`. Keep using a reader built from a commit whose format you trust: one rebuilt together with the server under test follows a format change instead of exposing it. `SF-01` searches raw bytes, needs no reader, and runs at the default level.

What a build must store is written down in `gold/<build>.json`: one entry per scheme and layout, listing every metadata key with its exact value or, where the value is random by design, its shape. `SF-02` names every key that is missing, extra or different. `stored.py` keeps the one table of keys that count as random; a key outside it is compared verbatim, so a new random value shows up as a difference instead of vanishing into a shape.

A manifest is never edited to make a run pass. When the stored form changes, someone confirms that the change is intended and that other readers (an older release, the other build) still cope; the manifest is then replaced by what the new binary stores:

```bash
uv run --python 3.12 --no-project scripts/test/sse_blackbox/stored.py propose <run dir> > scripts/test/sse_blackbox/gold/<build>.json
```

Each of these cases also records a `stored-form` observation per object, and `cross` records one for every object the writer stored. `diff` over two runs, or over two `cross` runs, therefore compares what two binaries put on the drives: key names and fixed values exactly, random values by shape.

## Checking that the suite can fail

`suite.py selftest` needs no server. It checks the suite's own judgment: that an observation changes when a listed part size, count or order changes and does not change with upload ids or timestamps, that a known deviation tolerates only its one documented difference, and that `diff` reports what it must. Run it after changing `observe.py`, `diffing.py` or `framework.py`.

Two switches exist only to prove that a green run means something:

```bash
# Start the e1 cases on a process without a master key: RT-01, BD-01 and KS-02 must fail.
uv run --python 3.12 --no-project scripts/test/sse_blackbox/suite.py run --binary head-default --configs e1 --run-as e1=e0
# Stop the server before the cases: every verdict must be env-error.
uv run --python 3.12 --no-project scripts/test/sse_blackbox/suite.py run --binary head-default --configs e1 --sabotage kill-site
```

## Adding a case

Register a function with `@case(id, title, configs)` in one of the `cases_*.py` files. State expectations through the context (`check`, `refused`, `projection`, `roundtrip`, `absent`): each records the evidence that decided it, and a refused write is always followed by a check that nothing was stored. Wrap each input of a table-driven case in `with c.row(name)` so it gets its own verdict. Keep the request sequence deterministic, and put polling inside `observe.quiet()`, because the comparison matches exchanges by their position in the case.
