// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::io::Cursor;

use futures::StreamExt;
use rustfs_zip::CompressionFormat;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use tar_codec::{Archive as _, DecodePolicy, Member, MemberPayload as _, PaxDecodePolicy, PaxVendorExtensionPolicy, TarArchive};
use tar_framing::{
    FrameError, FrameErrorInner, PaxKeyword, PaxRecord, PaxValue, StreamPolicy, UstarKind,
    logical::{MemberExtensions, PaxState, TarReader},
};
use tokio::io::AsyncReadExt;

const FIXTURE_ROOT: &str = "fixtures/snowball/minio-go-v7.3.0";
const RAW_FIXTURE: &[u8] = include_bytes!("fixtures/snowball/minio-go-v7.3.0/snowball.tar");
const S2_FIXTURE: &[u8] = include_bytes!("fixtures/snowball/minio-go-v7.3.0/snowball.tar.s2");
const MANIFEST: &[u8] = include_bytes!("fixtures/snowball/minio-go-v7.3.0/manifest.json");

#[derive(Debug, Deserialize)]
struct FixtureManifest {
    generator: String,
    minio_go: String,
    generated_at: String,
    objects: Vec<FixtureObject>,
    archives: Vec<FixtureArchive>,
}

#[derive(Debug, Deserialize)]
struct FixtureObject {
    key: String,
    body: String,
    mod_time: String,
    #[serde(default)]
    version_id: String,
    #[serde(default)]
    headers: BTreeMap<String, Vec<String>>,
}

#[derive(Debug, Deserialize)]
struct FixtureArchive {
    file: String,
    compressed: bool,
    length: usize,
    sha256: String,
}

#[derive(Debug, Eq, PartialEq)]
struct ParsedMember {
    path: String,
    size: u64,
    mtime: Option<u64>,
    body: Vec<u8>,
    minio_pax: BTreeMap<String, Option<Vec<u8>>>,
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut encoded = String::with_capacity(64);
    for byte in Sha256::digest(bytes) {
        write!(&mut encoded, "{byte:02x}").expect("writing to a String should not fail");
    }
    encoded
}

async fn decode_s2(bytes: &[u8]) -> Vec<u8> {
    let mut decoder = CompressionFormat::S2
        .get_decoder(Cursor::new(bytes.to_vec()))
        .expect("S2 fixture decoder should be available");
    let mut decoded = Vec::new();
    decoder.read_to_end(&mut decoded).await.expect("S2 fixture should decode");
    decoded
}

async fn parse_with_tokio_tar(bytes: &[u8]) -> Vec<ParsedMember> {
    let mut archive = tokio_tar::Archive::new(Cursor::new(bytes.to_vec()));
    let mut entries = archive.entries().expect("tokio-tar should create an entry stream");
    let mut parsed = Vec::new();

    while let Some(entry) = entries.next().await {
        let mut entry = entry.expect("tokio-tar should parse the fixture member");
        let kind = entry.header().entry_type();
        if kind == tokio_tar::EntryType::XGlobalHeader {
            continue;
        }

        let path_bytes = entry.path_bytes().expect("tokio-tar should resolve the fixture path");
        let path = std::str::from_utf8(path_bytes.as_ref())
            .expect("fixture paths should be UTF-8")
            .to_owned();
        let size = entry.effective_size();
        let mtime = entry.header().mtime().ok();
        let mut minio_pax = BTreeMap::new();
        if let Some(extensions) = entry
            .pax_extensions()
            .await
            .expect("tokio-tar should parse local PAX records")
        {
            for extension in extensions {
                let extension = extension.expect("fixture PAX record should be valid");
                let key = extension.key().expect("fixture PAX keys should be UTF-8");
                if key.starts_with("minio.") {
                    minio_pax.insert(key.to_owned(), Some(extension.value_bytes().to_vec()));
                }
            }
        }
        let mut body = Vec::new();
        entry
            .read_to_end(&mut body)
            .await
            .expect("tokio-tar should read the fixture body");
        parsed.push(ParsedMember {
            path,
            size,
            mtime,
            body,
            minio_pax,
        });
    }
    parsed
}

fn effective_minio_pax(state: &PaxState<'_>, known_keywords: &mut Vec<PaxKeyword>) -> BTreeMap<String, Option<Vec<u8>>> {
    for extension in state.extensions() {
        for record in extension.records() {
            let keyword = record.keyword();
            if matches!(&keyword, PaxKeyword::Vendor { vendor, .. } if vendor.as_ref() == "minio")
                && !known_keywords.contains(&keyword)
            {
                known_keywords.push(keyword);
            }
        }
    }

    known_keywords
        .iter()
        .filter_map(|keyword| {
            let record = state.effective_record(keyword)?;
            let PaxRecord::Vendor { vendor, name, value } = record else {
                return None;
            };
            let key = format!("{vendor}.{name}");
            let value = match value {
                PaxValue::Value(value) => Some(value.to_vec()),
                PaxValue::Deleted => None,
            };
            Some((key, value))
        })
        .collect()
}

fn effective_mtime(header_mtime: Option<u64>, extensions: &MemberExtensions<'_>) -> Option<u64> {
    let MemberExtensions::Pax(state) = extensions else {
        return header_mtime;
    };
    match state.effective_record(&PaxKeyword::Mtime) {
        Some(PaxRecord::Mtime(PaxValue::Value(value))) => Some(*value),
        Some(PaxRecord::Mtime(PaxValue::Deleted)) => None,
        _ => header_mtime,
    }
}

fn padded_member_end(position: u64, size: u64) -> u64 {
    let padded_size = size.checked_add(511).expect("fixture member size should not overflow") / 512 * 512;
    position
        .checked_add(512)
        .and_then(|position| position.checked_add(padded_size))
        .expect("fixture member end should not overflow")
}

fn is_authenticated_footerless_end(error: &FrameError, last_member_end: Option<u64>, request_body_complete: bool) -> bool {
    // The production gate must source `request_body_complete` from RustFS's
    // length, checksum, and trailing-header validation state.
    request_body_complete && matches!(&error.inner, FrameErrorInner::MissingEndMarker) && last_member_end == Some(error.position)
}

fn candidate_snowball_decode_policy() -> DecodePolicy {
    DecodePolicy::default()
        .allow_gnu(true)
        .allow_all_nul_numeric_fields(true)
        .max_gnu_extension_size(1_048_576)
        .pax_policy(
            PaxDecodePolicy::default()
                .max_extension_size(1_048_576)
                .max_global_extensions_size(67_108_864)
                .allow_global_pax_extensions(false)
                .allow_non_utf8_pax_vendor_values(false)
                .allow_duplicate_pax_records(false)
                .allow_global_pax_member_metadata(false)
                .vendor_extension_policy(PaxVendorExtensionPolicy::ignore(["minio"])),
        )
}

async fn parse_with_tar_framing(bytes: &[u8]) -> (Vec<ParsedMember>, Option<FrameError>, Option<u64>) {
    let policy = StreamPolicy::default()
        .max_pax_extension_size(1024 * 1024)
        .max_global_pax_extensions_size(4 * 1024 * 1024)
        .max_gnu_extension_size(128 * 1024);
    let mut reader = TarReader::new(Cursor::new(bytes.to_vec())).with_policy(policy);
    let mut parsed = Vec::new();
    let mut known_minio_keywords = Vec::new();
    let mut last_member_end = None;

    loop {
        let mut frame = match reader.next_frame().await {
            Ok(Some(frame)) => frame,
            Ok(None) => return (parsed, None, last_member_end),
            Err(error) => return (parsed, Some(error), last_member_end),
        };
        assert_eq!(frame.header.kind, UstarKind::Regular);
        let path = String::from_utf8(
            frame
                .effective_path()
                .expect("tar-framing should resolve the fixture path")
                .into_owned(),
        )
        .expect("fixture paths should be UTF-8");
        let size = frame.header.effective_size;
        let mtime = effective_mtime(frame.header.mtime, &frame.extensions);
        let minio_pax = match &frame.extensions {
            MemberExtensions::Pax(state) => effective_minio_pax(state, &mut known_minio_keywords),
            MemberExtensions::Gnu { .. } => BTreeMap::new(),
        };
        let mut body = Vec::new();
        let mut chunk = Vec::new();
        while frame
            .payload
            .next_chunk(&mut chunk, 64 * 1024)
            .await
            .expect("tar-framing should read the fixture body")
        {
            body.extend_from_slice(&chunk);
        }
        last_member_end = Some(padded_member_end(frame.header.position, size));
        parsed.push(ParsedMember {
            path,
            size,
            mtime,
            body,
            minio_pax,
        });
    }
}

#[test]
fn checked_in_fixtures_match_the_minio_go_manifest() {
    let manifest: FixtureManifest = serde_json::from_slice(MANIFEST).expect("fixture manifest should be valid JSON");
    assert_eq!(manifest.generator, "github.com/minio/minio-go/v7.Client.PutObjectsSnowball");
    assert_eq!(manifest.minio_go, "v7.3.0");
    assert_eq!(manifest.generated_at, "2026-09-05T00:00:00Z");
    assert_eq!(manifest.objects.len(), 2);
    assert_eq!(manifest.objects[0].key, "alpha.txt");
    assert_eq!(manifest.objects[0].body, "alpha-body");
    assert_eq!(manifest.objects[0].mod_time, "2024-01-02T03:04:05Z");
    assert_eq!(manifest.objects[0].version_id, "018cc251-f400-7c22-9e8d-8b1800000001");
    assert_eq!(
        manifest.objects[0].headers.get("X-Amz-Meta-Owner"),
        Some(&vec!["snowball-fixture".to_owned()])
    );

    for archive in &manifest.archives {
        let bytes = match archive.file.as_str() {
            "snowball.tar" => RAW_FIXTURE,
            "snowball.tar.s2" => S2_FIXTURE,
            file => panic!("unexpected archive in {FIXTURE_ROOT}/manifest.json: {file}"),
        };
        assert_eq!(bytes.len(), archive.length);
        assert_eq!(sha256_hex(bytes), archive.sha256);
        assert_eq!(archive.compressed, archive.file.ends_with(".s2"));
    }
}

#[tokio::test]
async fn minio_go_raw_and_s2_fixtures_have_identical_footerless_tar_data() {
    assert_eq!(decode_s2(S2_FIXTURE).await, RAW_FIXTURE);
    assert_eq!(RAW_FIXTURE.len() % 512, 0);
    assert!(RAW_FIXTURE.len() >= 1024);
    assert!(
        !RAW_FIXTURE[RAW_FIXTURE.len() - 1024..].iter().all(|byte| *byte == 0),
        "minio-go Flush output should not contain the standard two-block terminator"
    );
}

#[tokio::test]
async fn tar_framing_matches_tokio_tar_before_rejecting_the_missing_terminator() {
    let expected = parse_with_tokio_tar(RAW_FIXTURE).await;
    let (actual, error, last_member_end) = parse_with_tar_framing(RAW_FIXTURE).await;
    let error = error.expect("footerless minio-go fixture should fail strict termination");

    assert_eq!(actual, expected);
    assert_eq!(
        actual,
        [
            ParsedMember {
                path: "alpha.txt".to_owned(),
                size: 10,
                mtime: Some(1_704_164_645),
                body: b"alpha-body".to_vec(),
                minio_pax: BTreeMap::from([
                    ("minio.metadata.Content-Type".to_owned(), Some(b"text/plain".to_vec()),),
                    ("minio.metadata.X-Amz-Meta-Owner".to_owned(), Some(b"snowball-fixture".to_vec()),),
                    (
                        "minio.metadata.X-Amz-Tagging".to_owned(),
                        Some(b"project=rustfs&source=minio-go".to_vec()),
                    ),
                    ("minio.versionId".to_owned(), Some(b"018cc251-f400-7c22-9e8d-8b1800000001".to_vec()),),
                ]),
            },
            ParsedMember {
                path: "nested/世界.txt".to_owned(),
                size: 10,
                mtime: Some(1_704_164_705),
                body: b"bravo-body".to_vec(),
                minio_pax: BTreeMap::from([
                    ("minio.metadata.Content-Language".to_owned(), Some(b"zh-CN".to_vec()),),
                    ("minio.metadata.X-Amz-Meta-Note".to_owned(), Some(b"unicode-path".to_vec()),),
                ]),
            },
        ]
    );
    assert!(matches!(&error.inner, FrameErrorInner::MissingEndMarker));
    assert_eq!(
        error.position,
        u64::try_from(RAW_FIXTURE.len()).expect("fixture length should fit in u64")
    );
    assert_eq!(last_member_end, Some(error.position));
}

#[tokio::test]
async fn footerless_compatibility_requires_authenticated_eof_at_the_member_boundary() {
    let (_, error, last_member_end) = parse_with_tar_framing(RAW_FIXTURE).await;
    let error = error.expect("the real fixture should be footerless");
    assert!(is_authenticated_footerless_end(&error, last_member_end, true));
    assert!(!is_authenticated_footerless_end(&error, last_member_end, false));

    let mut one_zero_block = RAW_FIXTURE.to_vec();
    one_zero_block.extend([0; 512]);
    let (_, error, last_member_end) = parse_with_tar_framing(&one_zero_block).await;
    let error = error.expect("one zero block is not a valid TAR terminator");
    assert!(matches!(&error.inner, FrameErrorInner::MissingEndMarker));
    assert_eq!(
        last_member_end,
        Some(u64::try_from(RAW_FIXTURE.len()).expect("fixture length should fit in u64"))
    );
    assert_eq!(
        error.position,
        u64::try_from(one_zero_block.len()).expect("fixture length should fit in u64")
    );
    assert!(!is_authenticated_footerless_end(&error, last_member_end, true));
}

#[tokio::test]
async fn tar_codec_policy_accepts_only_the_explicit_minio_vendor_namespace() {
    let default_error = match TarArchive::new(Cursor::new(RAW_FIXTURE.to_vec())).members().next().await {
        Err(error) => error,
        Ok(_) => panic!("the default policy should reject minio vendor records"),
    };
    assert!(default_error.to_string().contains("pax vendor extension minio."));

    let mut members = TarArchive::new(Cursor::new(RAW_FIXTURE.to_vec()))
        .with_policy(candidate_snowball_decode_policy())
        .members();
    let mut bodies = Vec::new();
    loop {
        let member = match members.next().await {
            Ok(Some(member)) => member,
            Ok(None) => panic!("footerless minio-go fixture should not report a valid archive end"),
            Err(error) => {
                assert!(error.to_string().contains("missing two-block end-of-archive marker"));
                break;
            }
        };
        let Member::File { mut payload, .. } = member else {
            panic!("fixture should contain only regular files");
        };
        let mut body = Vec::new();
        let mut chunk = Vec::new();
        while payload
            .next_chunk(&mut chunk, 64 * 1024)
            .await
            .expect("tar-codec should read the fixture body")
        {
            body.extend_from_slice(&chunk);
        }
        bodies.push(body);
    }
    assert_eq!(bodies, [b"alpha-body".to_vec(), b"bravo-body".to_vec()]);
    assert!(
        members
            .next()
            .await
            .expect("the member cursor should be fused after an error")
            .is_none()
    );
}

fn pax_record(key: &str, value: &str) -> Vec<u8> {
    let payload = format!("{key}={value}\n");
    let mut len = payload.len() + 3;
    loop {
        let record = format!("{len} {payload}");
        if record.len() == len {
            return record.into_bytes();
        }
        len = record.len();
    }
}

async fn append_pax_header(
    builder: &mut tokio_tar::Builder<Cursor<Vec<u8>>>,
    entry_type: tokio_tar::EntryType,
    records: &[(&str, &str)],
) {
    let mut payload = Vec::new();
    for (key, value) in records {
        payload.extend(pax_record(key, value));
    }
    let mut header = tokio_tar::Header::new_ustar();
    header.set_entry_type(entry_type);
    header.set_size(u64::try_from(payload.len()).expect("PAX test payload should fit in u64"));
    header.set_mode(0o644);
    header.set_cksum();
    builder
        .append_data(&mut header, "PaxHeaders.X/snowball", Cursor::new(payload))
        .await
        .expect("PAX test header should be written");
}

async fn append_regular(builder: &mut tokio_tar::Builder<Cursor<Vec<u8>>>, path: &str) {
    let body = path.as_bytes();
    let mut header = tokio_tar::Header::new_ustar();
    header.set_entry_type(tokio_tar::EntryType::Regular);
    header.set_size(u64::try_from(body.len()).expect("test member body should fit in u64"));
    header.set_mode(0o644);
    header.set_mtime(1_704_164_645);
    header.set_cksum();
    builder
        .append_data(&mut header, path, Cursor::new(body))
        .await
        .expect("ordinary test member should be written");
}

async fn archive_with_local_pax(records: &[(&str, &str)]) -> Vec<u8> {
    let mut builder = tokio_tar::Builder::new(Cursor::new(Vec::new()));
    append_pax_header(&mut builder, tokio_tar::EntryType::XHeader, records).await;
    append_regular(&mut builder, "member.txt").await;
    builder.into_inner().await.expect("policy archive should finish").into_inner()
}

#[tokio::test]
async fn candidate_policy_rejects_unknown_vendor_and_duplicate_pax_records() {
    let unknown_vendor = archive_with_local_pax(&[("acme.metadata.owner", "mallory")]).await;
    let error = match TarArchive::new(Cursor::new(unknown_vendor))
        .with_policy(candidate_snowball_decode_policy())
        .members()
        .next()
        .await
    {
        Err(error) => error,
        Ok(_) => panic!("the candidate Snowball policy should reject unknown vendors"),
    };
    assert!(
        error
            .to_string()
            .contains("pax vendor extension acme.metadata.owner is not allowed")
    );

    let duplicate = archive_with_local_pax(&[
        ("minio.metadata.x-amz-meta-owner", "first"),
        ("minio.metadata.x-amz-meta-owner", "second"),
    ])
    .await;
    let error = match TarArchive::new(Cursor::new(duplicate))
        .with_policy(candidate_snowball_decode_policy())
        .members()
        .next()
        .await
    {
        Err(error) => error,
        Ok(_) => panic!("the candidate Snowball policy should reject duplicate PAX records"),
    };
    assert!(
        error
            .to_string()
            .contains("pax extended header contains duplicate record minio.metadata.x-amz-meta-owner")
    );
}

#[tokio::test]
async fn global_minio_pax_inheritance_is_an_explicit_migration_difference() {
    let mut builder = tokio_tar::Builder::new(Cursor::new(Vec::new()));
    append_pax_header(
        &mut builder,
        tokio_tar::EntryType::XGlobalHeader,
        &[("minio.metadata.x-amz-meta-owner", "global")],
    )
    .await;
    append_pax_header(
        &mut builder,
        tokio_tar::EntryType::XHeader,
        &[("minio.metadata.x-amz-meta-owner", "local")],
    )
    .await;
    append_regular(&mut builder, "local.txt").await;
    append_regular(&mut builder, "inherited.txt").await;
    let archive = builder
        .into_inner()
        .await
        .expect("precedence archive should finish")
        .into_inner();

    let legacy = parse_with_tokio_tar(&archive).await;
    let (framing, error, _) = parse_with_tar_framing(&archive).await;
    assert!(error.is_none());
    assert_eq!(legacy.len(), 2);
    assert_eq!(framing.len(), 2);

    let owner_key = "minio.metadata.x-amz-meta-owner";
    assert_eq!(legacy[0].minio_pax.get(owner_key), Some(&Some(b"local".to_vec())));
    assert!(!legacy[1].minio_pax.contains_key(owner_key));
    assert_eq!(framing[0].minio_pax.get(owner_key), Some(&Some(b"local".to_vec())));
    assert_eq!(framing[1].minio_pax.get(owner_key), Some(&Some(b"global".to_vec())));

    let error = match TarArchive::new(Cursor::new(archive))
        .with_policy(candidate_snowball_decode_policy())
        .members()
        .next()
        .await
    {
        Err(error) => error,
        Ok(_) => panic!("the candidate Snowball policy should reject global PAX state"),
    };
    assert!(error.to_string().contains("global pax extended headers are not allowed"));
}
