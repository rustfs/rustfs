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

use rustfs_filemeta::{FileInfo, FileMeta};
use rustfs_heal_contracts::heal_channel::{
    HealAdmissionReceipt, HealAdmissionResult, HealReceiptCommand, HealRequestSource, init_heal_channels,
};
use rustfs_scanner::DataUsageCacheSource;
use rustfs_scanner::scanner_folder::ScannerItem;
use rustfs_scanner::scanner_io::ScannerIODisk;
use tokio::sync::mpsc;

use super::storage_api::lifecycle::{DiskOption, Endpoint, STORAGE_FORMAT_FILE, new_disk};

#[test]
fn scanner_object_requests_bind_the_actual_disk_scope() {
    const CHILD: &str = "RUSTFS_TEST_SCANNER_SCOPE_CHILD";
    if std::env::var_os(CHILD).is_none() {
        let status = std::process::Command::new(std::env::current_exe().expect("locate integration test executable"))
            .args([
                "--exact",
                "scanner_heal_scope::scanner_object_requests_bind_the_actual_disk_scope",
                "--nocapture",
            ])
            .env(CHILD, "1")
            .status()
            .expect("run scanner scope regression in an isolated process");
        assert!(status.success(), "isolated scanner scope regression failed: {status}");
        return;
    }
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build isolated scanner runtime")
        .block_on(check_scanner_object_scope());
}

async fn check_scanner_object_scope() {
    let (_commands, mut receipts) = init_heal_channels().expect("initialize isolated scanner request channels");
    let (captured, mut requests) = mpsc::unbounded_channel();
    let responder = tokio::spawn(async move {
        while let Some(command) = receipts.recv().await {
            let HealReceiptCommand { request, response_tx } = command;
            let receipt = HealAdmissionReceipt {
                result: HealAdmissionResult::Accepted,
                task_id: request.id.clone(),
            };
            captured.send(request).expect("retain scanner request");
            let _ = response_tx.send(Ok(receipt));
        }
    });

    for (pool, set, disk_index) in [
        (Some(0), Some(0), Some(1)),
        (Some(2), Some(3), Some(4)),
        (None, Some(3), Some(4)),
        (Some(2), None, Some(4)),
        (Some(2), Some(3), None),
        (None, None, None),
    ] {
        for version_id in [None, Some(uuid::Uuid::from_u128(42))] {
            let root = tempfile::tempdir().expect("create scanner disk");
            let bucket = "scanner-scope-bucket";
            let object = "object";
            let path = root.path().join(bucket).join(object).join(STORAGE_FORMAT_FILE);
            tokio::fs::create_dir_all(path.parent().expect("metadata parent"))
                .await
                .expect("create object directory");
            let mut info = FileInfo::new(object, 4, 2);
            info.volume = bucket.to_string();
            info.name = object.to_string();
            info.size = 1;
            info.version_id = version_id;
            info.data_dir = Some(uuid::Uuid::new_v4());
            info.erasure.index = 1;
            info.mod_time = Some(time::OffsetDateTime::from_unix_timestamp(1).expect("valid object timestamp"));
            info.add_object_part(1, "fixture-etag".to_string(), 1, info.mod_time, 1, None, None);
            let mut meta = FileMeta::new();
            meta.add_version(info).expect("encode original null version");
            let original = meta.marshal_msg().expect("marshal original metadata");
            tokio::fs::write(&path, &original).await.expect("write original metadata");
            let mut endpoint = Endpoint::try_from(root.path().to_string_lossy().as_ref()).expect("parse scanning disk endpoint");
            if let Some(pool) = pool {
                endpoint.set_pool_index(pool);
            }
            if let Some(set) = set {
                endpoint.set_set_index(set);
            }
            if let Some(disk_index) = disk_index {
                endpoint.set_disk_index(disk_index);
            }
            let disk = new_disk(
                &endpoint,
                &DiskOption {
                    cleanup: false,
                    health_check: false,
                },
            )
            .await
            .expect("open actual scanning disk");
            let item = ScannerItem {
                path: path.to_string_lossy().to_string(),
                bucket: bucket.to_string(),
                prefix: object.to_string(),
                object_name: STORAGE_FORMAT_FILE.to_string(),
                file_type: tokio::fs::metadata(&path).await.expect("stat original metadata").file_type(),
                lifecycle: None,
                object_lock: None,
                replication: None,
                // This foreign caller-provided scope must never reach admission.
                heal_scope: Some(DataUsageCacheSource::new(99, 98)),
                heal_enabled: true,
                heal_bitrot: true,
                debug: false,
            };
            disk.get_size_with_tier_names(item, &[]).await.expect("scan original object");
            if let (Some(pool), Some(set), Some(_)) = (pool, set, disk_index) {
                let request = requests.try_recv().expect("scanner submitted one real object request");
                assert_eq!(request.pool_index, Some(pool));
                assert_eq!(request.set_index, Some(set));
                assert_eq!(request.bucket, bucket);
                assert_eq!(request.object_prefix.as_deref(), Some(object));
                assert_eq!(request.object_version_id, version_id.map(|value| value.to_string()));
                assert_eq!(request.source, HealRequestSource::Scanner);
                assert_eq!(request.no_lock, None);
                assert_eq!(request.recreate_missing, Some(false));
            }
            assert!(requests.try_recv().is_err(), "no duplicate or unscoped request may be admitted");
            assert_eq!(tokio::fs::read(&path).await.expect("read original metadata"), original);
        }
    }
    responder.abort();
}
