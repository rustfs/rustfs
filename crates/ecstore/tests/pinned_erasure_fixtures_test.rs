// Copyright 2026 RustFS Team
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

mod storage_api;

use rustfs_filemeta::{FileInfoOpts, get_file_info};
use rustfs_utils::HashAlgorithm;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, HashSet},
    fs,
    io::{Cursor, ErrorKind},
    path::PathBuf,
    sync::Arc,
    time::Duration,
};
use storage_api::pinned_erasure_fixtures::{
    BitrotReader, BucketOperations, ECStore, Endpoint, EndpointServerPools, Endpoints, Erasure, Error, HTTPRangeSpec,
    InstanceContext, MakeBucketOptions, ObjectIO, ObjectOptions, PoolEndpoints, init_bucket_metadata_sys,
    init_local_disks_with_instance_ctx,
};
use tokio::io::AsyncReadExt;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

const PLAINTEXT_SHA256: &str = "b5a83332327961a31b65a8731c64559fd04ff8ac9f6124022201dbbdba5ffa01";

#[derive(Deserialize)]
struct Source {
    release: String,
    source_commit: String,
    archive_sha256: String,
    binary_sha256: String,
    uses_legacy: bool,
}

#[derive(Deserialize)]
struct Manifest {
    schema_version: usize,
    source: Source,
    bucket: String,
    object: String,
    data_shards: usize,
    parity_shards: usize,
    block_size: usize,
    plaintext_size: usize,
    plaintext_sha256: String,
    files: BTreeMap<String, String>,
}

struct Fixture {
    case: PathBuf,
    manifest: Manifest,
    slot_paths: Vec<String>,
    plaintext: Vec<u8>,
    shards: Vec<Vec<u8>>,
    uses_legacy: bool,
    hash: HashAlgorithm,
}

fn sha256(bytes: &[u8]) -> String {
    rustfs_utils::crypto::hex(Sha256::digest(bytes))
}

fn load_fixture(kind: &str) -> Fixture {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/erasure-shards");
    let case = root.join(kind);
    let manifest: Manifest = serde_json::from_slice(&fs::read(case.join("manifest.json")).expect("required fixture manifest"))
        .expect("parse pinned fixture manifest");
    assert_eq!(manifest.schema_version, 1);
    assert_eq!((manifest.data_shards, manifest.parity_shards, manifest.block_size), (6, 6, 1_048_576));
    assert_eq!(manifest.files.len(), 24, "all twelve metadata and external shard files are required");
    let (release, commit, archive, binary, uses_legacy) = match kind {
        "minio" => (
            "RELEASE.2025-07-23T15-54-02Z",
            "7ced9663e6a791fef9dc6be798ff24cda9c730ac",
            "0939ce5553ce9e6451b69e049fbf399794368276b61da8166f84cbd8c7f2d641",
            "0939ce5553ce9e6451b69e049fbf399794368276b61da8166f84cbd8c7f2d641",
            false,
        ),
        "legacy" => (
            "1.0.0-alpha.37",
            "371119f7336b74fdbcee37a8fcccb9d91c276f47",
            "c7d1d5b060fad53ea3c9eac9deb6d1ea20a22d441ec3494cd5a18fe6f7bbb215",
            "158b5e0a96c6caf201a72e8c20e417124c190f295160e7c8610bf54228b5292a",
            true,
        ),
        _ => panic!("unknown fixture source"),
    };
    assert_eq!(manifest.source.release, release);
    assert_eq!(manifest.source.source_commit, commit);
    assert_eq!(manifest.source.archive_sha256, archive);
    assert_eq!(manifest.source.binary_sha256, binary);
    assert_eq!(manifest.source.uses_legacy, uses_legacy);

    let plaintext = fs::read(root.join("plaintext.bin")).expect("required independent plaintext oracle");
    assert_eq!(plaintext.len(), 6 * 128 * 1024 + 1);
    assert_eq!(manifest.plaintext_size, plaintext.len());
    assert_eq!(manifest.plaintext_sha256, PLAINTEXT_SHA256);
    assert_eq!(sha256(&plaintext), PLAINTEXT_SHA256);
    let read_file = |relative: &str| {
        let expected_hash = manifest.files.get(relative).expect("file must be pinned in the manifest");
        let bytes = fs::read(case.join(relative)).unwrap_or_else(|error| panic!("required fixture {relative}: {error}"));
        assert_eq!(sha256(&bytes), *expected_hash, "fixture SHA256 mismatch: {relative}");
        bytes
    };
    let mut shards = vec![None; 12];
    let mut slot_paths = vec![String::new(); 12];
    let mut selected_legacy = None;
    for disk in 1..=12 {
        let object_path = format!("disk{disk}/{}/{}", manifest.bucket, manifest.object);
        let metadata = read_file(&format!("{object_path}/xl.meta"));
        let fi = get_file_info(
            &metadata,
            &manifest.bucket,
            &manifest.object,
            "",
            FileInfoOpts {
                data: true,
                include_free_versions: false,
                include_part_checksums: true,
            },
        )
        .expect("decode actual producer xl.meta");
        fi.validate_for_metadata_read()
            .expect("producer metadata must describe a valid payload");
        assert!(!fi.inline_data(), "capture must exercise external part files");
        assert!(!fi.is_remote(), "transitioned layout is outside this corpus");
        assert_eq!(fi.uses_legacy_checksum, uses_legacy);
        selected_legacy = Some(fi.uses_legacy_checksum);
        assert_eq!(
            (fi.erasure.data_blocks, fi.erasure.parity_blocks, fi.erasure.block_size),
            (6, 6, 1_048_576)
        );
        assert_eq!(fi.parts.len(), 1);
        assert_eq!((fi.parts[0].number, fi.parts[0].size), (1, plaintext.len()));
        assert_eq!(fi.erasure.get_checksum_info(1).algorithm, HashAlgorithm::HighwayHash256S);
        let data_dir = fi.data_dir.expect("external part data directory");
        let relative = format!("{object_path}/{data_dir}/part.1");
        let part = read_file(&relative);
        assert_eq!(part.len(), if uses_legacy { 131_074 + 32 } else { 131_073 + 32 });
        let slot = fi.erasure.index.checked_sub(1).expect("one-based erasure index");
        let target = shards.get_mut(slot).expect("erasure index is within the captured set");
        assert!(target.replace(part).is_none(), "duplicate captured erasure slot");
        slot_paths[slot] = relative;
    }
    let shards: Vec<_> = shards
        .into_iter()
        .map(|part| part.expect("all erasure slots are present"))
        .collect();
    assert_eq!(
        shards.iter().map(|part| sha256(part)).collect::<HashSet<_>>().len(),
        12,
        "shards must be distinguishable"
    );
    Fixture {
        case,
        manifest,
        slot_paths,
        plaintext,
        shards,
        uses_legacy: selected_legacy.expect("format selected from real xl.meta"),
        hash: if selected_legacy.expect("checksum variant selected from real xl.meta") {
            HashAlgorithm::HighwayHash256SLegacy
        } else {
            HashAlgorithm::HighwayHash256S
        },
    }
}

async fn decode(
    fixture: &Fixture,
    missing: &[usize],
    uses_legacy: bool,
    offset: usize,
    length: usize,
) -> (Vec<u8>, Option<std::io::Error>) {
    let erasure = Erasure::try_new_with_options(6, 6, 1_048_576, uses_legacy).expect("supported captured geometry");
    let readers = fixture
        .shards
        .iter()
        .enumerate()
        .map(|(slot, part)| {
            (!missing.contains(&slot))
                .then(|| BitrotReader::new(Cursor::new(part.clone()), erasure.shard_size(), fixture.hash.clone(), false))
        })
        .collect();
    let mut plaintext = Vec::new();
    let (written, error) = erasure
        .decode(&mut plaintext, readers, offset, length, fixture.plaintext.len())
        .await;
    assert_eq!(written, plaintext.len());
    (plaintext, error)
}

async fn assert_complete_and_reconstructed(kind: &str) {
    let fixture = load_fixture(kind);
    let erasure = Erasure::try_new_with_options(6, 6, 1_048_576, fixture.uses_legacy).expect("captured codec");
    assert_eq!(erasure.shard_size(), if fixture.uses_legacy { 174_764 } else { 174_763 });
    for missing_data in [false, true] {
        let mut dirs = Vec::with_capacity(12);
        let mut endpoints = Vec::with_capacity(12);
        for disk in 0..12 {
            let dir = tempfile::tempdir().expect("isolated fixture disk");
            let mut endpoint = Endpoint::try_from(dir.path().to_str().expect("disk path UTF-8")).expect("local endpoint");
            endpoint.set_pool_index(0);
            endpoint.set_set_index(0);
            endpoint.set_disk_index(disk);
            dirs.push(dir);
            endpoints.push(endpoint);
        }
        let endpoint_pools = EndpointServerPools(vec![PoolEndpoints {
            legacy: false,
            set_count: 1,
            drives_per_set: 12,
            endpoints: Endpoints::from(endpoints),
            cmd_line: "pinned-erasure-fixture-test".to_string(),
            platform: "test".to_string(),
        }]);
        let instance = Arc::new(InstanceContext::new());
        init_local_disks_with_instance_ctx(&instance, endpoint_pools.clone())
            .await
            .expect("initialize fixture disks");
        let cancel = CancellationToken::new();
        let _cancel_on_drop = cancel.clone().drop_guard();
        let store = ECStore::new_with_instance_ctx(
            "127.0.0.1:0".parse().expect("local address"),
            endpoint_pools,
            cancel.clone(),
            instance,
        )
        .await
        .expect("initialize isolated twelve-drive store");
        init_bucket_metadata_sys(Arc::clone(&store), Vec::new()).await;
        store
            .make_bucket(&fixture.manifest.bucket, &MakeBucketOptions::default())
            .await
            .expect("create fixture namespace");
        for relative in fixture.manifest.files.keys() {
            let (disk, path) = relative.split_once('/').expect("captured disk path");
            let disk: usize = disk.strip_prefix("disk").expect("disk prefix").parse().expect("disk number");
            let target = dirs[disk - 1].path().join(path);
            fs::create_dir_all(target.parent().expect("object directory")).expect("create captured object directory");
            fs::copy(fixture.case.join(relative), target).expect("copy actual producer file without re-encoding");
        }
        if missing_data {
            let (disk, path) = fixture.slot_paths[0].split_once('/').expect("data shard disk path");
            let disk: usize = disk.strip_prefix("disk").expect("disk prefix").parse().expect("disk number");
            let target = dirs[disk - 1].path().join(path);
            fs::remove_file(&target).expect("remove actual data slot zero before the first GET");
            assert!(!target.exists());
        }
        for (offset, length) in [
            (0, fixture.plaintext.len()),
            (0, 17),
            (131_070, 37),
            (fixture.plaintext.len() - 47, 47),
        ] {
            let range = (length != fixture.plaintext.len()).then(|| HTTPRangeSpec {
                is_suffix_length: false,
                start: offset.try_into().expect("range start"),
                end: (offset + length - 1).try_into().expect("range end"),
            });
            let mut reader = store
                .get_object_reader(
                    &fixture.manifest.bucket,
                    &fixture.manifest.object,
                    range,
                    Default::default(),
                    &ObjectOptions::default(),
                )
                .await
                .unwrap_or_else(|error| {
                    panic!("production GET dispatch: {kind} missing_data={missing_data} range={offset}+{length}: {error}")
                });
            let mut actual = Vec::new();
            reader.read_to_end(&mut actual).await.unwrap_or_else(|error| {
                panic!("production GET body: {kind} missing_data={missing_data} range={offset}+{length}: {error}")
            });
            assert_eq!(
                actual,
                fixture.plaintext[offset..offset + length],
                "{kind} missing_data={missing_data} range={offset}+{length}"
            );
        }
        cancel.cancel();
    }
}

#[tokio::test]
async fn pinned_minio_shards_decode_and_reconstruct_plaintext() {
    timeout(Duration::from_secs(30), assert_complete_and_reconstructed("minio"))
        .await
        .expect("pinned MinIO GET and reconstruction schedule must finish");
}

#[tokio::test]
async fn pinned_legacy_shards_decode_and_reconstruct_plaintext() {
    timeout(Duration::from_secs(30), assert_complete_and_reconstructed("legacy"))
        .await
        .expect("pinned legacy GET and reconstruction schedule must finish");
}

#[tokio::test]
async fn pinned_erasure_shards_reject_bitrot_corruption() {
    for kind in ["minio", "legacy"] {
        let fixture = load_fixture(kind);
        let erasure = Erasure::try_new_with_options(6, 6, 1_048_576, fixture.uses_legacy).expect("captured codec");
        let mut corrupt = fixture.shards[0].clone();
        corrupt[32] ^= 0x80;
        let mut reader = BitrotReader::new(Cursor::new(corrupt), erasure.shard_size(), fixture.hash, false);
        let mut output = vec![0xa5; fixture.shards[0].len() - 32];
        let error = reader
            .read(&mut output)
            .await
            .expect_err("real captured shard corruption must fail bitrot verification");
        assert_eq!(error.kind(), ErrorKind::InvalidData, "{kind}: {error}");
        assert!(output.iter().all(|byte| *byte == 0xa5), "unverified bytes must not reach the caller");
    }
}

#[tokio::test]
async fn pinned_erasure_shards_reject_insufficient_quorum_and_wrong_codec() {
    for kind in ["minio", "legacy"] {
        let fixture = load_fixture(kind);
        let missing = [0, 1, 2, 3, 4, 5, 6];
        let (actual, error) = decode(&fixture, &missing, fixture.uses_legacy, 0, fixture.plaintext.len()).await;
        let error = error.expect("five remaining shards cannot satisfy six-data-shard quorum");
        assert_eq!(error.to_string(), Error::ErasureReadQuorum.to_string(), "{kind}");
        assert!(actual.len() < fixture.plaintext.len());
        let (actual, error) = decode(&fixture, &[0], !fixture.uses_legacy, 0, fixture.plaintext.len()).await;
        assert!(
            error.is_some() || actual != fixture.plaintext,
            "the opposite codec cannot reconstruct the pinned plaintext: {kind}"
        );
    }
}
