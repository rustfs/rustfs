use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use rustfs_filemeta::{FileInfo, FileInfoOpts, get_file_info};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct RequestRecord {
    encryption: String,
    headers: HashMap<String, String>,
    multipart: bool,
    size_bytes: u64,
    object: String,
}

#[derive(Debug, Deserialize)]
struct HeadRecord {
    #[serde(rename = "ContentLength")]
    content_length: u64,
    #[serde(rename = "ServerSideEncryption")]
    server_side_encryption: Option<String>,
    #[serde(rename = "SSEKMSKeyId")]
    sse_kms_key_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ManifestRecord {
    capture: Option<CaptureRecord>,
}

#[derive(Debug, Deserialize)]
struct CaptureRecord {
    kms_key_id: Option<String>,
}

fn fixture_root() -> PathBuf {
    std::env::var_os("RUSTFS_MINIO_FIXTURE_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/minio-generated"))
}

fn case_dir(case_id: &str) -> PathBuf {
    fixture_root().join("cases").join(case_id)
}

fn read_json<T: for<'de> Deserialize<'de>>(path: &Path) -> T {
    let text = fs::read_to_string(path).unwrap_or_else(|err| panic!("read {}: {err}", path.display()));
    serde_json::from_str(&text).unwrap_or_else(|err| panic!("parse {}: {err}", path.display()))
}

fn find_object_xl_meta(case_dir: &Path) -> PathBuf {
    let backend = case_dir.join("backend");
    for entry in walkdir::WalkDir::new(&backend) {
        let entry = entry.unwrap_or_else(|err| panic!("walk {}: {err}", backend.display()));
        if !entry.file_type().is_file() {
            continue;
        }
        if entry.file_name() != "xl.meta" {
            continue;
        }
        let path = entry.path();
        let path_text = path.to_string_lossy();
        if path_text.contains(".minio.sys") {
            continue;
        }
        return path.to_path_buf();
    }
    panic!("object xl.meta not found under {}", backend.display());
}

fn load_file_info(case_id: &str) -> FileInfo {
    let case = case_dir(case_id);
    let xl_meta = find_object_xl_meta(&case);
    let data = fs::read(&xl_meta).unwrap_or_else(|err| panic!("read {}: {err}", xl_meta.display()));
    get_file_info(
        &data,
        "fixture-bucket",
        "fixture-object",
        "",
        FileInfoOpts {
            data: false,
            include_free_versions: true,
        },
    )
    .unwrap_or_else(|err| panic!("decode {}: {err}", xl_meta.display()))
}

fn read_manifest(case_id: &str) -> ManifestRecord {
    read_json(&case_dir(case_id).join("manifest.json"))
}

fn require_generated_fixture_root() {
    let root = fixture_root();
    assert!(
        root.join("cases").is_dir(),
        "generated fixture root missing: {}. Run scripts/minio_fixture_lab/lab.py capture-matrix first.",
        root.display()
    );
}

fn metadata_keys(fi: &FileInfo) -> HashMap<&str, &str> {
    fi.metadata.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect()
}

fn expected_fixture_kms_key_id(case_id: &str) -> String {
    read_manifest(case_id)
        .capture
        .and_then(|capture| capture.kms_key_id)
        .unwrap_or_else(|| panic!("fixture {case_id} missing capture.kms_key_id"))
}

#[test]
#[ignore = "requires generated MinIO fixture data"]
fn parses_singlepart_sse_s3_fixture() {
    require_generated_fixture_root();

    let request: RequestRecord = read_json(&case_dir("sse-s3-singlepart-64k").join("request.json"));
    let head: HeadRecord = read_json(&case_dir("sse-s3-singlepart-64k").join("head.json"));
    let fi = load_file_info("sse-s3-singlepart-64k");
    let metadata = metadata_keys(&fi);

    assert_eq!(request.encryption, "SSE-S3");
    assert!(!request.multipart);
    assert_eq!(request.object, "dir/object.bin");
    assert_eq!(head.content_length, request.size_bytes);
    assert_eq!(head.server_side_encryption.as_deref(), Some("AES256"));
    assert_eq!(fi.parts.len(), 1);
    assert_eq!(fi.parts[0].actual_size as u64, request.size_bytes);
    assert!(metadata.contains_key("X-Minio-Internal-Server-Side-Encryption-S3-Sealed-Key"));
    assert_eq!(metadata.get("content-type"), Some(&"binary/octet-stream"));
}

#[test]
#[ignore = "requires generated MinIO fixture data"]
fn parses_singlepart_sse_kms_fixture() {
    require_generated_fixture_root();

    let case_id = "sse-kms-singlepart-64k";
    let request: RequestRecord = read_json(&case_dir(case_id).join("request.json"));
    let head: HeadRecord = read_json(&case_dir(case_id).join("head.json"));
    let fi = load_file_info(case_id);
    let metadata = metadata_keys(&fi);
    let expected_key_id = expected_fixture_kms_key_id(case_id);

    assert_eq!(request.encryption, "SSE-KMS");
    assert!(!request.multipart);
    assert_eq!(head.content_length, request.size_bytes);
    assert_eq!(head.server_side_encryption.as_deref(), Some("aws:kms"));
    assert_eq!(request.headers.get("x-amz-server-side-encryption"), Some(&"aws:kms".to_string()));
    assert_eq!(request.headers.get("x-amz-server-side-encryption-aws-kms-key-id"), Some(&expected_key_id));
    assert_eq!(head.sse_kms_key_id.as_deref(), Some(format!("arn:aws:kms:{expected_key_id}").as_str()));
    assert_eq!(fi.parts.len(), 1);
    assert_eq!(fi.parts[0].actual_size as u64, request.size_bytes);
    assert_eq!(
        metadata.get("X-Minio-Internal-Server-Side-Encryption-S3-Kms-Key-Id"),
        Some(&expected_key_id.as_str())
    );
    assert_eq!(
        metadata.get("X-Minio-Internal-Server-Side-Encryption-Context").copied(),
        request
            .headers
            .get("x-amz-server-side-encryption-context")
            .map(String::as_str)
    );
    assert!(metadata.contains_key("X-Minio-Internal-Server-Side-Encryption-Kms-Sealed-Key"));
}

#[test]
#[ignore = "requires generated MinIO fixture data"]
fn parses_multipart_sse_s3_fixture() {
    require_generated_fixture_root();

    let case_id = "sse-s3-multipart-8m";
    let request: RequestRecord = read_json(&case_dir(case_id).join("request.json"));
    let head: HeadRecord = read_json(&case_dir(case_id).join("head.json"));
    let fi = load_file_info(case_id);
    let metadata = metadata_keys(&fi);
    let expected_key_id = expected_fixture_kms_key_id(case_id);

    assert_eq!(request.encryption, "SSE-S3");
    assert!(request.multipart);
    assert_eq!(head.content_length, request.size_bytes);
    assert_eq!(head.server_side_encryption.as_deref(), Some("AES256"));
    assert_eq!(fi.parts.len(), 2);
    assert_eq!(fi.parts.iter().map(|part| part.actual_size as u64).sum::<u64>(), request.size_bytes);
    assert_eq!(
        metadata.get("X-Minio-Internal-Server-Side-Encryption-S3-Kms-Key-Id"),
        Some(&expected_key_id.as_str())
    );
    assert!(metadata.contains_key("X-Minio-Internal-Server-Side-Encryption-S3-Sealed-Key"));
    assert!(metadata.contains_key("X-Minio-Internal-Encrypted-Multipart"));
    assert_eq!(metadata.get("X-Minio-Internal-actual-size"), Some(&"8388608"));
}

#[test]
#[ignore = "requires generated MinIO fixture data"]
fn parses_multipart_sse_kms_fixture() {
    require_generated_fixture_root();

    let case_id = "sse-kms-multipart-8m";
    let request: RequestRecord = read_json(&case_dir(case_id).join("request.json"));
    let head: HeadRecord = read_json(&case_dir(case_id).join("head.json"));
    let fi = load_file_info(case_id);
    let metadata = metadata_keys(&fi);
    let expected_key_id = expected_fixture_kms_key_id(case_id);

    assert_eq!(request.encryption, "SSE-KMS");
    assert!(request.multipart);
    assert_eq!(head.content_length, request.size_bytes);
    assert_eq!(head.server_side_encryption.as_deref(), Some("aws:kms"));
    assert_eq!(request.headers.get("x-amz-server-side-encryption-aws-kms-key-id"), Some(&expected_key_id));
    assert_eq!(head.sse_kms_key_id.as_deref(), Some(format!("arn:aws:kms:{expected_key_id}").as_str()));
    assert_eq!(fi.parts.len(), 2);
    assert_eq!(fi.parts.iter().map(|part| part.actual_size as u64).sum::<u64>(), request.size_bytes);
    assert_eq!(
        metadata.get("X-Minio-Internal-Server-Side-Encryption-Context").copied(),
        request
            .headers
            .get("x-amz-server-side-encryption-context")
            .map(String::as_str)
    );
    assert!(metadata.contains_key("X-Minio-Internal-Encrypted-Multipart"));
    assert_eq!(metadata.get("X-Minio-Internal-actual-size"), Some(&"8388608"));
}
