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

## Output

Each run directory holds `run.json` (binary identity, configurations, counts), `results.jsonl` (one verdict per case and per table row), `observations.jsonl` (the observation vectors), `report.md`, the server logs under `sites/`, and `DONE` with the overall status. A run without `DONE` did not finish and cannot be compared.

A verdict is `pass`, `fail` (an expectation about the product does not hold) or `env-error` (the site is down or a dependency is missing). An environment error is never a pass and never a product failure, and a run with one does not succeed.

Two more verdicts cover behavior the product is known not to meet yet. An expectation written with `known_deviation` stays in the case as the intended behavior: while it does not hold the case reports `known-deviation` and the run still succeeds; once it holds the case reports `deviation-fixed` and the run fails until the marker is turned into a plain `check`.

## Comparing two binaries

`diff` reports three things: verdicts that differ, observations that differ, and cases or exchanges present on one side only. The differences it accepts form a closed list in `diffing.py`; anything else makes the comparison fail. It exits 0 when nothing blocking differs, 1 otherwise, and 2 when the two runs share no exchange, so an empty result cannot be mistaken for agreement.

An observation keeps what is deterministic (status, error code and message, SSE headers, the set of response header names, body hash, the encryption metadata key names stored for the object) and reduces random values (ETags of encrypted objects, version ids, upload ids) to their shape.

Equal behavior of two binaries does not show that one can read what the other wrote. `cross` covers that: the writer binary stores every form the configuration can produce (each scheme, empty to multi-megabyte, single and multipart), the server is stopped, and the reader binary is started on the same data directory and must read every object back.

```bash
uv run --python 3.12 --no-project scripts/test/sse_blackbox/suite.py cross --writer base-default --reader head-default
```

## Checking that the suite can fail

Two switches exist only to prove that a green run means something:

```bash
# Start the e1 cases on a process without a master key: RT-01, BD-01 and KS-02 must fail.
uv run --python 3.12 --no-project scripts/test/sse_blackbox/suite.py run --binary head-default --configs e1 --run-as e1=e0
# Stop the server before the cases: every verdict must be env-error.
uv run --python 3.12 --no-project scripts/test/sse_blackbox/suite.py run --binary head-default --configs e1 --sabotage kill-site
```

## Adding a case

Register a function with `@case(id, title, configs)` in one of the `cases_*.py` files. State expectations through the context (`check`, `refused`, `projection`, `roundtrip`, `absent`): each records the evidence that decided it, and a refused write is always followed by a check that nothing was stored. Wrap each input of a table-driven case in `with c.row(name)` so it gets its own verdict. Keep the request sequence deterministic, and put polling inside `observe.quiet()`, because the comparison matches exchanges by their position in the case.
