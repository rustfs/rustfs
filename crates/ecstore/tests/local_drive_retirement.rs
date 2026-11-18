// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

mod storage_api;
use storage_api::local_drive_retirement::{Disk, DiskAPI, DiskOption, Endpoint, new_disk};

#[tokio::test]
async fn local_wrapper_retirement_is_shared_and_independent_of_monitoring() {
    let dir = tempfile::tempdir().expect("retirement fixture directory");
    let endpoint = Endpoint::try_from(dir.path().to_str().expect("UTF-8 fixture path")).expect("local endpoint");
    let disk = new_disk(
        &endpoint,
        &DiskOption {
            cleanup: false,
            health_check: false,
        },
    )
    .await
    .expect("local disk");
    let Disk::Local(wrapper) = disk.as_ref() else { panic!("fixture must use a local disk") };
    let cloned = wrapper.clone();
    let observer = disk.local_retirement_token().expect("local retirement observer");
    let cancelled_observer = disk.local_retirement_token().expect("another observer");
    cancelled_observer.cancel();
    assert!(!observer.is_cancelled(), "observers cannot retire the wrapper");
    wrapper.stop_monitoring().await;
    assert!(!observer.is_cancelled(), "monitoring policy is not retirement");
    cloned.close().await.expect("close shared wrapper");
    assert!(observer.is_cancelled(), "clones share retirement");
    assert!(
        disk.local_retirement_token().expect("late observer").is_cancelled(),
        "late observers see retirement"
    );
    disk.close().await.expect("retirement is idempotent");
}
