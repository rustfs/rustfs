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
