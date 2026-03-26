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

//! Integration test: verify create_bitrot_reader with HighwayHash256SLegacy reads main-version objects.
//!
//! Supports both EC (part files) and inline objects. Reads xl.meta to compute params.
//! Uses create_bitrot_reader only (no ECStore/get_object_reader) to isolate bitrot compatibility.
//!
//! Run from workspace root:
//!   cargo test -p rustfs-ecstore test_legacy_bitrot_read -- --nocapture
//!
//! Environment:
//!   RUSTFS_LEGACY_TEST_ROOT  - Test data root (default: workspace root)
//!   RUSTFS_LEGACY_TEST_DISK  - Disk name under root (default: "test1" for rustfs, "test" for minio)
//!   RUSTFS_SKIP_LEGACY_TEST  - Set to 1 to skip
//!
//! For MinIO data: RUSTFS_LEGACY_TEST_ROOT=/path/to/minio (disk "test" and .minio.sys auto-detected).

use rustfs_ecstore::bitrot::create_bitrot_reader;
use rustfs_ecstore::disk::endpoint::Endpoint;
use rustfs_ecstore::disk::{DiskOption, STORAGE_FORMAT_FILE, new_disk};
use rustfs_filemeta::{FileInfoOpts, get_file_info};
use rustfs_utils::HashAlgorithm;
use std::path::PathBuf;
use tokio::fs;

fn workspace_root() -> PathBuf {
    let manifest = std::env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".into());
    PathBuf::from(&manifest)
        .ancestors()
        .nth(2)
        .unwrap_or(std::path::Path::new("."))
        .to_path_buf()
}

fn legacy_test_data_exists() -> bool {
    if std::env::var("RUSTFS_SKIP_LEGACY_TEST").unwrap_or_default() == "1" {
        return false;
    }
    let root = workspace_root();
    let format_path = root.join("test1/.rustfs.sys/format.json");
    let ktvzip_meta = root.join("test1/vvvv/ktvzip.tar.gz").join(STORAGE_FORMAT_FILE);
    let path_traversal_meta = root.join("test1/vvvv/path_traversal.md").join(STORAGE_FORMAT_FILE);
    format_path.exists() && (ktvzip_meta.exists() || path_traversal_meta.exists())
}

async fn run_legacy_bitrot_test_for_object(root: &std::path::Path, disk_name: &str, bucket: &str, object: &str) -> bool {
    let xl_meta_path = root.join(disk_name).join(bucket).join(object).join(STORAGE_FORMAT_FILE);
    if !xl_meta_path.exists() {
        eprintln!("xl_meta_path not found: {:?}", xl_meta_path);
        return false;
    }

    let buf = match fs::read(&xl_meta_path).await {
        Ok(b) => b,
        Err(_) => {
            eprintln!("Failed to read xl_meta_path: {:?}", xl_meta_path);
            return false;
        }
    };

    let fi = match get_file_info(
        &buf,
        bucket,
        object,
        "",
        FileInfoOpts {
            data: true, // need inline data for inline objects
            include_free_versions: false,
        },
    ) {
        Ok(f) => f,
        Err(_) => {
            eprintln!("Failed to get file info: {:?}", xl_meta_path);
            return false;
        }
    };

    if fi.deleted || fi.parts.is_empty() {
        eprintln!("File is deleted or has no parts: {:?}", xl_meta_path);
        return false;
    }

    let shard_size = fi.erasure.shard_size();
    let part_number = 1;
    let checksum_info = fi.erasure.get_checksum_info(part_number);
    let checksum_algo = if fi.uses_legacy_checksum && checksum_info.algorithm == HashAlgorithm::HighwayHash256S {
        HashAlgorithm::HighwayHash256SLegacy
    } else {
        checksum_info.algorithm
    };

    eprintln!("fi.inline_data(): {:?}", fi.inline_data());
    eprintln!("fi.is_remote(): {:?}", fi.metadata);

    // Inline path: use fi.data
    if fi.inline_data() {
        let inline_bytes = match &fi.data {
            Some(b) if !b.is_empty() => b.as_ref(),
            _ => {
                eprintln!("Inline data is empty: {:?}", xl_meta_path);
                return false;
            }
        };
        let read_length = fi.size as usize;
        if read_length == 0 {
            eprintln!("Read length is 0: {:?}", xl_meta_path);
            return false;
        }

        let mut reader = match create_bitrot_reader(
            Some(inline_bytes),
            None,
            bucket,
            "",
            0,
            read_length,
            shard_size,
            checksum_algo.clone(),
            false,
            false, // use_zero_copy
        )
        .await
        {
            Ok(Some(r)) => r,
            _ => {
                eprintln!("Failed to create bitrot reader for inline data: {:?}", xl_meta_path);
                return false;
            }
        };

        let mut buf = vec![0u8; shard_size];
        match reader.read(&mut buf).await {
            Ok(n) if n > 0 => {
                eprintln!("Successfully read {} bytes (inline) via create_bitrot_reader with {:?}", n, checksum_algo);
                return true;
            }
            _ => {
                eprintln!("Failed to read inline data: {:?}", xl_meta_path);
                return false;
            }
        }
    }

    // EC path: use disk + part file
    let data_dir = match fi.data_dir {
        Some(d) => d,
        None => {
            eprintln!("Data dir is empty: {:?}", xl_meta_path);
            return false;
        }
    };
    let path = format!("{object}/{data_dir}/part.{}", part_number);
    let part_path = root.join(disk_name).join(bucket).join(&path);
    if !part_path.exists() {
        eprintln!("Part file not found: {:?}", part_path);
        return false;
    }

    let disk_path = root.join(disk_name);
    let path_str = disk_path.to_str().expect("path");
    let mut ep = Endpoint::try_from(path_str).expect("endpoint");
    ep.set_pool_index(0);
    ep.set_set_index(0);
    ep.set_disk_index(0);
    let opt = DiskOption {
        cleanup: false,
        health_check: false,
    };
    let disk = match new_disk(&ep, &opt).await {
        Ok(d) => d,
        Err(_) => {
            eprintln!("Failed to create disk: {:?}", disk_path);
            return false;
        }
    };

    let read_length = shard_size;
    let mut reader = match create_bitrot_reader(
        None,
        Some(&disk),
        bucket,
        &path,
        0,
        read_length,
        shard_size,
        checksum_algo.clone(),
        false,
        false,
    ) // use_zero_copy
    .await
    {
        Ok(Some(r)) => r,
        _ => {
            eprintln!("Failed to create bitrot reader for EC part: {:?}", part_path);
            return false;
        }
    };

    let mut buf = vec![0u8; shard_size];
    match reader.read(&mut buf).await {
        Ok(n) if n > 0 => {
            eprintln!(
                "Successfully read {} bytes (EC part) via create_bitrot_reader with {:?}",
                n, checksum_algo
            );
            true
        }
        _ => {
            eprintln!("Failed to read EC part: {:?}", part_path);
            false
        }
    }
}

#[tokio::test]
async fn test_legacy_bitrot_read() {
    if !legacy_test_data_exists() {
        eprintln!("Skipping legacy bitrot test: test1/vvvv xl.meta or RUSTFS_SKIP_LEGACY_TEST");
        return;
    }

    // let root = workspace_root();
    let root = PathBuf::from("/Users/weisd/project/minio");

    let disk_name = "test";
    let bucket = "vvvv";

    // Try both EC (part files) and inline objects
    let ok = run_legacy_bitrot_test_for_object(&root, disk_name, bucket, "ktvzip.tar.gz").await;

    eprintln!("ok: {:?}", ok);

    assert!(ok, "create_bitrot_reader  failed for both ktvzip.tar.gz and path_traversal.md");

    let ok = run_legacy_bitrot_test_for_object(&root, disk_name, bucket, "path_traversal.md").await;
    assert!(ok, "create_bitrot_reader  failed for path_traversal.md");
}
