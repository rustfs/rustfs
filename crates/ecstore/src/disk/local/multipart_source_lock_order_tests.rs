// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::disk::{DeleteOptions, endpoint::Endpoint};
use futures::FutureExt;

#[inline(never)]
fn heap<F: std::future::Future>(build: impl FnOnce() -> F) -> std::pin::Pin<Box<F>> {
    Box::pin(build())
}

#[tokio::test]
async fn legacy_mpi_residue_under_metadata_ownership_never_waits_for_source_rw_or_deletes_bytes() {
    let temporary = tempfile::tempdir().expect("legacy residue disk");
    let endpoint = Endpoint::try_from(temporary.path().to_string_lossy().as_ref()).expect("local endpoint");
    let disk = heap(|| LocalDisk::new(&endpoint, false)).await.expect("local disk");
    let path = format!("{}/{}", "a".repeat(64), Uuid::new_v4());
    let source = disk
        .io_get_object_path(RUSTFS_META_MULTIPART_BUCKET, &path)
        .expect("pinned source path");
    let data_dir = Uuid::new_v4();
    let transaction = Uuid::new_v4();
    let data = source.join(data_dir.to_string());
    let receipt = source.join(transaction.to_string());
    std::fs::create_dir_all(&data).expect("legacy payload directory");
    std::fs::create_dir_all(&receipt).expect("legacy transaction directory");
    let payload = data.join("part.1");
    std::fs::write(&payload, b"retained legacy payload").expect("legacy payload");
    let committed = data.join(format!("{}{transaction}", super::super::DELETE_DATA_DIR_MARKER_PREFIX));
    let quorum = receipt.join(super::super::DELETE_QUORUM_COMMITTED_FILE);
    std::fs::write(&committed, []).expect("legacy data receipt");
    std::fs::write(&quorum, []).expect("legacy quorum receipt");
    let transactions = Box::pin(disk.committed_delete_transactions(RUSTFS_META_MULTIPART_BUCKET, &path))
        .await
        .expect("actual metadata-less legacy inventory");
    assert!(transactions.contains(&transaction), "fixture must enter the actual residue branch");

    // These are the two real lock domains in the reported inversion. A None
    // token does not grant permission to wait for the already-held SourceRW.
    let source_writer = disk
        .multipart_source_owner(RUSTFS_META_MULTIPART_BUCKET, &path, false, true, None)
        .await
        .expect("independent source writer");
    let logical = disk
        .get_object_path(RUSTFS_META_MULTIPART_BUCKET, &path)
        .expect("configured metadata key");
    let metadata: Option<Arc<dyn Send + Sync>> = Some(os::acquire_metadata_mutation_lease(&logical, None).await);
    let options = DeleteOptions {
        recursive: true,
        immediate: true,
        ..Default::default()
    };
    let transaction_path = format!("{path}/{transaction}");
    let data_path = format!("{path}/{data_dir}");
    let result = heap(|| {
        disk.delete_data_dir_with_namespace_owner(RUSTFS_META_MULTIPART_BUCKET, &data_path, options.clone(), metadata.clone())
    })
    .now_or_never()
    .expect("metadata-owned data-dir cleanup cannot wait for SourceRW");
    assert_eq!(
        result.expect_err("public or typed source entry is required"),
        DiskError::ConditionalDeleteConflict
    );

    let result = heap(|| {
        disk.delete_unleased_with_namespace_owner(
            RUSTFS_META_MULTIPART_BUCKET,
            &transaction_path,
            &options,
            metadata.clone(),
            None,
        )
    })
    .now_or_never()
    .expect("residue refusal must not await SourceRW");
    assert_eq!(result.expect_err("unowned MPI residue is retained"), DiskError::ConditionalDeleteConflict);
    let result =
        heap(|| disk.finish_version_delete(RUSTFS_META_MULTIPART_BUCKET, &path, transaction, false, metadata.clone(), None))
            .now_or_never()
            .expect("version cleanup refuses before any lock wait");
    assert_eq!(
        result.expect_err("typed source ownership is required"),
        DiskError::ConditionalDeleteConflict
    );
    let result = heap(|| {
        disk.finish_delete_transaction_with_namespace_owner(
            RUSTFS_META_MULTIPART_BUCKET,
            &path,
            transaction,
            metadata.clone(),
            None,
        )
    })
    .now_or_never()
    .expect("transaction cleanup refuses before any lock wait");
    assert_eq!(
        result.expect_err("typed source ownership is required"),
        DiskError::ConditionalDeleteConflict
    );
    let finished = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        Box::pin(disk.finish_internal_delete_residue(RUSTFS_META_MULTIPART_BUCKET, &path, metadata)),
    )
    .await
    .expect("metadata-owned residue must not deadlock");
    assert_eq!(finished, 0);
    assert_eq!(std::fs::read(&payload).expect("retained payload"), b"retained legacy payload");
    assert_eq!(std::fs::read(&committed).expect("retained committed receipt"), b"");
    assert_eq!(std::fs::read(&quorum).expect("retained quorum receipt"), b"");
    assert!(!source.join(STORAGE_FORMAT_FILE).exists());
    drop(source_writer);
}
