// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::disk::endpoint::Endpoint;
use futures::FutureExt;

#[inline(never)]
fn heap_scope<F: std::future::Future>(build: impl FnOnce() -> F) -> std::pin::Pin<Box<F>> {
    Box::pin(build())
}

#[tokio::test]
async fn generic_delete_recovery_cannot_restore_or_prepare_any_physical_multipart_scope() {
    let temporary = tempfile::tempdir().expect("recovery scope disk");
    let endpoint = Endpoint::try_from(temporary.path().to_string_lossy().as_ref()).expect("local endpoint");
    let disk = heap_scope(|| LocalDisk::new(&endpoint, false)).await.expect("local disk");
    let hash = "a".repeat(64);
    let upload = Uuid::new_v4().to_string();
    let object = format!("{hash}/{upload}");
    let source = disk
        .io_get_object_path(RUSTFS_META_MULTIPART_BUCKET, &object)
        .expect("actual source root");
    std::fs::create_dir_all(&source).expect("existing source root");
    // Scope rejection precedes any attempt to interpret or replace these bytes.
    let head = source.join(STORAGE_FORMAT_FILE);
    std::fs::write(&head, b"existing source head must not be read or replaced").expect("existing head bytes");
    let original = std::fs::read(&head).expect("original physical bytes");
    let logical = disk
        .get_object_path(RUSTFS_META_MULTIPART_BUCKET, &object)
        .expect("configured metadata key");
    let owner: Option<Arc<dyn Send + Sync>> = Some(os::acquire_metadata_mutation_lease(&logical, None).await);
    let transaction = Uuid::new_v4();
    let aliases = [
        (RUSTFS_META_MULTIPART_BUCKET.to_owned(), object.clone()),
        (format!("{RUSTFS_META_MULTIPART_BUCKET}/."), object.clone()),
        (format!("{RUSTFS_META_MULTIPART_BUCKET}/{hash}"), upload),
        (crate::disk::RUSTFS_META_BUCKET.to_owned(), format!("multipart/{object}")),
    ];
    for (volume, object) in aliases {
        let result = heap_scope(|| {
            disk.delete_recovery_settle(&volume, &object, transaction, [0; 32], DeleteRecoveryAction::Rollback, owner.clone())
        })
        .now_or_never()
        .expect("scope is rejected before metadata lock acquisition");
        assert_eq!(
            result.expect_err("generic recovery cannot mutate MPI"),
            DiskError::ConditionalDeleteConflict
        );
        let result = heap_scope(|| {
            disk.delete_recovery_settle_owned(
                &volume,
                &object,
                transaction,
                [0; 32],
                DeleteRecoveryAction::Rollback,
                owner.clone(),
            )
        })
        .now_or_never()
        .expect("owned rollback is rejected before I/O");
        assert_eq!(
            result.expect_err("owned recovery cannot restore MPI head"),
            DiskError::ConditionalDeleteConflict
        );
        let result = heap_scope(|| {
            disk.prepare_delete_recovery(
                &volume,
                &object,
                Some(&original),
                &[],
                None,
                DeletePreparationContext {
                    transaction,
                    bucket_incarnation: None,
                    namespace_owner: owner.clone(),
                },
            )
        })
        .now_or_never()
        .expect("preparation is rejected before decoding or writing metadata");
        assert_eq!(
            result.expect_err("generic recovery cannot prepare MPI"),
            DiskError::ConditionalDeleteConflict
        );
        assert_eq!(std::fs::read(&head).expect("source remains present"), original);
        assert_eq!(std::fs::read_dir(&source).expect("source inventory").count(), 1);
    }
    assert!(
        disk.reject_multipart_delete_recovery_mutation("ordinary-user-bucket", "object")
            .is_ok()
    );
}
