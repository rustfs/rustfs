# MinIO Fixture Lab

This lab captures real MinIO backend artifacts into a repeatable local layout for RustFS compatibility tests.

It now supports two workflows:

- manual capture from an already exported backend tree
- automated capture from a disposable local MinIO run

## Scope

Use the manual path after you already have:

- a running MinIO instance
- one or more uploaded objects you want to preserve as fixtures
- the backend object tree or object-version directory you want to export

Use the automated path when you want the lab to:

- locate a local `minio` binary
- start a disposable MinIO instance
- upload a predefined SSE fixture case
- export the generated backend tree into the lab layout

## Layout

The default root is `artifacts/minio-fixture-lab`, which is already ignored by the repository.

Each case is stored under:

```text
artifacts/minio-fixture-lab/
  cases/
    <case-id>/
      backend/
      request.json
      head.json
      plaintext.sha256
      manifest.json
```

`manifest.json` is the source of truth for the captured case.

## Commands

Initialize the lab root:

```powershell
uv run python D:\Github\rustfs\crates\rio-v2\tests\minio_fixture_lab\lab.py init
```

Capture one case from an existing MinIO backend tree:

```powershell
uv run python D:\Github\rustfs\crates\rio-v2\tests\minio_fixture_lab\lab.py add-case `
  --case-id sse-kms-singlepart-64k `
  --bucket demo `
  --object dir/object.bin `
  --source-tree D:\minio-data-export\case-tree `
  --head-json D:\minio-data-export\head.json `
  --request-json D:\minio-data-export\request.json `
  --plaintext-sha256 D:\minio-data-export\plaintext.sha256
```

Capture the default automated matrix:

```powershell
uv run python D:\Github\rustfs\crates\rio-v2\tests\minio_fixture_lab\lab.py capture-matrix `
  --root D:\Github\rustfs\artifacts\minio-fixture-lab `
  --minio-binary D:\go\bin\minio.exe `
  --endpoint https://127.0.0.1:19000
```

Capture only one automated case:

```powershell
uv run python D:\Github\rustfs\crates\rio-v2\tests\minio_fixture_lab\lab.py capture-matrix `
  --root D:\Github\rustfs\artifacts\minio-fixture-lab `
  --minio-binary D:\Github\rustfs\tmp\minio.windows-amd64.RELEASE.2025-09-07T16-13-09Z.exe `
  --endpoint https://127.0.0.1:19000 `
  --case-id sse-s3-singlepart-64k
```

The automated default matrix is intentionally small:

- `sse-s3-singlepart-64k`
- `sse-kms-singlepart-64k`
- `sse-c-singlepart-64k`
- `sse-s3-multipart-8m`
- `sse-kms-multipart-8m`
- `sse-c-multipart-8m`

`64 KiB multipart` is intentionally excluded because S3 multipart semantics require a larger non-final part size.

## Automated Runner Prerequisites

The automated runner expects:

- either `minio` in `PATH`, the bundled `D:\Github\rustfs\tmp\minio.windows-amd64.RELEASE.2025-09-07T16-13-09Z.exe`, `--minio-binary`, or `--minio-root` pointing at a directory containing `minio.exe`
- a free local endpoint port

When the selected matrix includes SSE-C cases, use an `https://` endpoint. The lab will mint a short-lived local self-signed certificate and use its built-in SigV4 S3 client with certificate verification disabled for the disposable local MinIO run.

The runner provisions backend directories under `--work-root` and exports the resulting backend tree into the lab.

For local static-KMS runs, pass `--kms-secret-key` or set
`MINIO_FIXTURE_LAB_KMS_SECRET_KEY` using MinIO's
`<key-id>:<base64-32byte-key>` format. The runner derives the SSE-KMS request
key id from that configured key name automatically.

On some Windows MinIO builds, a multi-disk backend may not come online when all disk directories live on the same volume. If you only want a local smoke run of the upload/export pipeline, try:

```powershell
uv run python D:\Github\rustfs\crates\rio-v2\tests\minio_fixture_lab\lab.py capture-matrix `
  --root D:\Github\rustfs\artifacts\minio-fixture-lab `
  --minio-binary D:\go\bin\minio.exe `
  --endpoint https://127.0.0.1:19000 `
  --disk-count 1 `
  --case-id sse-s3-singlepart-64k
```

That is a runner smoke path only. Real compatibility fixtures should still prefer the intended multi-disk backend layout when the local environment can support it.

## Via Docker (Linux/macOS, no local `minio` install)

When you don't have a `minio` binary on the host, `capture_via_docker.sh`
generates the fixtures the ignored round-trip tests consume using Docker only.
It builds a throwaway image (the official MinIO server binary pulled from
`minio/minio` plus this lab on a small Python base — `lab.py` drives MinIO's S3
API directly, so no `mc` is needed) and runs `capture-matrix` inside it, writing
fixtures under `crates/rio-v2/tests/fixtures/minio-generated/` (the root the Rust
tests read):

```bash
# Default: the two 8 MiB multipart cases the round-trip tests need.
# Pass case ids to override, or "all" for the full default matrix.
./capture_via_docker.sh

RUSTFS_MINIO_STATIC_KMS_KEY=minio-default-key:IyqsU3kMFloCNup4BsZtf/rmfHVcTgznO2F25CkEH1g= \
  cargo test -p rustfs-ecstore --features rio-v2 --test minio_generated_read_test -- --ignored
```

This is exactly what the nightly `minio-interop` GitHub Actions workflow runs
(`.github/workflows/minio-interop.yml`), so the local and CI paths stay in sync.
SSE-C cases still need the host-`minio` + TLS path above; the Docker helper
targets the SSE-S3 / SSE-KMS multipart cases the interop tests assert on.

## RustFS beta.5 KMS Compatibility Fixture

The same CI gate also downloads the pinned RustFS `1.0.0-beta.5` release,
verifies its published archive SHA-256, starts it with the production local KMS
backend, writes a real SSE-KMS object, and exports the resulting four-disk
backend plus its one-time KMS key directory:

```bash
uv run python ./capture_rustfs_beta5.py

RUSTFS_BETA5_FIXTURE_ROOT=../fixtures/rustfs-beta5-generated \
  cargo test -p rustfs-ecstore --features rio-v2 \
    --test minio_generated_read_test \
    reads_real_rustfs_beta5_sse_kms_fixture_through_production_reader \
    -- --ignored
```

Outside Linux x86_64, pass `--rustfs-binary` with the matching beta.5 binary.
The generated fixture and KMS key stay under the ignored fixture root and are
recreated for every CI run; neither key material nor plaintext keys are logged
or committed.

## Capture Guidance

For each case, preserve these inputs when possible:

- the exact backend tree that contains `xl.meta` and part files
- the request shape used to create the object
- the API metadata returned by `HEAD Object`
- the plaintext digest used for byte-for-byte read verification

Recommended early matrix:

- singlepart SSE-S3
- singlepart SSE-KMS
- multipart SSE-S3
- multipart SSE-KMS
- compressed + encrypted
- range-sensitive sizes around `64 KiB` and `8 MiB`

## Next Stage

The current runner covers launch, upload, HEAD capture, and backend export.

The next iteration should focus on:

- proving the multi-disk backend path in the target local environment
- adding compressed fixtures to the automated matrix
- tightening exported tree selection if a narrower object-level slice becomes practical
