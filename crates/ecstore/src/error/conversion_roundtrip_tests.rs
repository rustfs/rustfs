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

//! Round-trip preservation tests for the error variants that heal, replication
//! and quorum aggregation match on (backlog#1845 PR1).
//!
//! These tests pin the *current* behaviour of every conversion seam an error
//! can cross before a classifier looks at it:
//!
//! - `DiskError` ⇄ `StorageError` (in-process layer boundary)
//! - `DiskError` ⇄ `node_service::Error` (internode wire format)
//! - `DiskError`/`StorageError` ⇄ `std::io::Error` (the intentional identity
//!   bridge: a typed error boxed through `io::Error` must downcast back)
//! - `StorageError` ⇄ `rustfs_filemeta::Error`
//!
//! They also document the known lossy edges as they exist today — most
//! importantly the `SlowDown` collapse and the wire re-wrap of `Io` payloads —
//! so that later refactors (typed variants, `narrow_to_disk()`) change these
//! expectations *deliberately* rather than silently.

use crate::disk::error::DiskError;
use crate::disk::error_reduce::{OBJECT_OP_IGNORED_ERRS, reduce_errs};
use crate::error::StorageError;
use rustfs_protos::proto_gen::node_service::Error as WireError;

/// The variants heal / replication / quorum repair match on, and that must
/// survive the DiskError → StorageError → DiskError loop unchanged.
fn heal_matched_disk_variants() -> Vec<DiskError> {
    vec![
        DiskError::FileNotFound,
        DiskError::FileVersionNotFound,
        DiskError::ErasureReadQuorum,
        DiskError::ErasureWriteQuorum,
    ]
}

#[test]
fn disk_to_storage_to_disk_preserves_heal_matched_variants() {
    for disk_err in heal_matched_disk_variants() {
        let storage: StorageError = disk_err.clone().into();
        let back: DiskError = storage.narrow_to_disk().expect("heal-matched variants must narrow");
        assert_eq!(back, disk_err, "DiskError → StorageError → DiskError must be identity for {disk_err:?}");
    }
}

#[test]
fn storage_to_disk_to_storage_preserves_heal_matched_variants() {
    let variants = vec![
        StorageError::FileNotFound,
        StorageError::FileVersionNotFound,
        StorageError::ErasureReadQuorum,
        StorageError::ErasureWriteQuorum,
    ];
    for storage_err in variants {
        let disk: DiskError = storage_err
            .clone()
            .narrow_to_disk()
            .expect("heal-matched variants must narrow");
        let back: StorageError = disk.into();
        assert_eq!(
            back, storage_err,
            "StorageError → DiskError → StorageError must be identity for {storage_err:?}"
        );
    }
}

/// Documents the `SlowDown` collapse: the disk layer has no SlowDown variant,
/// so narrowing maps it onto `TooManyOpenFiles` and the identity is lost.
/// A classifier on the far side of the disk boundary can no longer tell
/// backpressure ("please slow down") apart from fd exhaustion.
///
/// The fallible `narrow_to_disk()` keeps this collapse as a documented arm;
/// only variants with no disk-layer identity at all narrow to `Err`.
#[test]
fn slowdown_collapses_to_too_many_open_files_across_disk_boundary() {
    let disk: DiskError = StorageError::SlowDown.narrow_to_disk().expect("SlowDown narrows, lossily");
    assert_eq!(disk, DiskError::TooManyOpenFiles);

    let back: StorageError = disk.into();
    assert_eq!(
        back,
        StorageError::TooManyOpenFiles,
        "SlowDown identity is lost after one disk-boundary loop"
    );
}

/// Same shape as the SlowDown collapse: `StorageFull` narrows to `DiskFull`
/// and comes back as `DiskFull`.
#[test]
fn storage_full_collapses_to_disk_full_across_disk_boundary() {
    let disk: DiskError = StorageError::StorageFull
        .narrow_to_disk()
        .expect("StorageFull narrows, lossily");
    assert_eq!(disk, DiskError::DiskFull);

    let back: StorageError = disk.into();
    assert_eq!(back, StorageError::DiskFull);
}

#[test]
fn wire_roundtrip_preserves_typed_variants() {
    // TooManyOpenFiles rides along because SlowDown degrades to it before
    // the wire is involved.
    let mut variants = heal_matched_disk_variants();
    variants.push(DiskError::TooManyOpenFiles);

    for disk_err in variants {
        let wire: WireError = disk_err.clone().into();
        let back: DiskError = wire.into();
        assert_eq!(back, disk_err, "DiskError → wire → DiskError must be identity for {disk_err:?}");
    }
}

/// Documents the wire behaviour of the `Io` catch-all (code 0x24): only the
/// rendered message crosses the wire, the `io::ErrorKind` does not, and each
/// hop re-wraps the message with the `io error ` display prefix. Two hops
/// therefore double the prefix. Quorum aggregation on the receiving node
/// buckets these by (kind, message), so the re-wrap alone changes bucketing
/// relative to the sending node.
#[test]
fn wire_roundtrip_rewraps_io_payload_and_drops_kind() {
    let original = DiskError::Io(std::io::Error::new(std::io::ErrorKind::NotFound, "open failed"));

    let wire: WireError = original.into();
    assert_eq!(wire.code, 0x24);
    assert_eq!(wire.error_info, "io error open failed");

    let after_one_hop: DiskError = wire.into();
    let DiskError::Io(io_err) = &after_one_hop else {
        panic!("wire Io code must decode to DiskError::Io, got {after_one_hop:?}");
    };
    // The typed kind is gone…
    assert_eq!(io_err.kind(), std::io::ErrorKind::Other);
    // …and the message gained the display prefix.
    assert_eq!(io_err.to_string(), "io error open failed");

    let wire_again: WireError = after_one_hop.into();
    assert_eq!(
        wire_again.error_info, "io error io error open failed",
        "each wire hop re-wraps the rendered message"
    );
}

/// The intentional identity bridge (kept by design, see backlog#1845): a typed
/// `DiskError` boxed through `io::Error` must recover its identity on the way
/// back instead of degrading to `DiskError::Io`.
#[test]
fn io_error_bridge_recovers_disk_error_identity() {
    for disk_err in heal_matched_disk_variants() {
        let io_err: std::io::Error = disk_err.clone().into();
        let back: DiskError = io_err.into();
        assert_eq!(back, disk_err, "DiskError → io::Error → DiskError must recover identity for {disk_err:?}");
    }
}

#[test]
fn io_error_bridge_recovers_storage_error_identity() {
    let variants = vec![
        StorageError::FileNotFound,
        StorageError::FileVersionNotFound,
        StorageError::ErasureReadQuorum,
        StorageError::ErasureWriteQuorum,
        StorageError::SlowDown,
    ];
    for storage_err in variants {
        let io_err: std::io::Error = storage_err.clone().into();
        let back: StorageError = io_err.into();
        assert_eq!(
            back, storage_err,
            "StorageError → io::Error → StorageError must recover identity for {storage_err:?}"
        );
    }
}

/// A `StorageError` boxed through `io::Error` and then narrowed at the *disk*
/// layer must also come back typed (this crosses `From<io::Error> for
/// DiskError`, which downcasts through both error types).
#[test]
fn io_error_bridge_recovers_identity_across_layers() {
    let io_err: std::io::Error = StorageError::FileVersionNotFound.into();
    let disk: DiskError = io_err.into();
    assert_eq!(disk, DiskError::FileVersionNotFound);

    let io_err: std::io::Error = DiskError::ErasureReadQuorum.into();
    let storage: StorageError = io_err.into();
    assert_eq!(storage, StorageError::ErasureReadQuorum);
}

#[test]
fn storage_to_filemeta_to_storage_preserves_matched_variants() {
    let variants = vec![
        StorageError::FileNotFound,
        StorageError::FileVersionNotFound,
        StorageError::FileCorrupt,
        StorageError::MethodNotAllowed,
        StorageError::VolumeNotFound,
        StorageError::DoneForNow,
        StorageError::Unexpected,
    ];
    for storage_err in variants {
        let filemeta: rustfs_filemeta::Error = storage_err
            .clone()
            .narrow_to_filemeta()
            .expect("matched variants must narrow");
        let back: StorageError = filemeta.into();
        assert_eq!(
            back, storage_err,
            "StorageError → filemeta::Error → StorageError must be identity for {storage_err:?}"
        );
    }
}

/// A variant filemeta has no arm for falls into `filemeta::Error::other`,
/// which boxes the original `StorageError` inside an `io::Error` — and the
/// identity bridge recovers it on the way back. Pinned so the "downcast
/// recovers identity" property of the by-design bridge stays load-bearing.
#[test]
fn storage_to_filemeta_other_recovers_identity_via_io_bridge() {
    // A variant with no filemeta identity narrows to Err; callers that need a
    // total conversion fold it into the io-backed other(), and the identity
    // bridge recovers the boxed StorageError on the way back.
    let refused = StorageError::SlowDown
        .narrow_to_filemeta()
        .expect_err("SlowDown has no filemeta identity");
    let filemeta = rustfs_filemeta::Error::other(refused);
    assert!(
        matches!(filemeta, rustfs_filemeta::Error::Io(_)),
        "unmatched variants fold into the io-backed other()"
    );

    let back: StorageError = filemeta.into();
    assert_eq!(back, StorageError::SlowDown, "the io bridge must recover the boxed StorageError identity");
}

/// The quorum-aggregation premise of backlog#1845, pinned as a test: N disks
/// failing for the same *reason* but with per-disk detail formatted into an
/// `other()` message are counted as N distinct errors by `reduce_errs`, while
/// N typed failures are counted as one error with weight N.
#[test]
fn reduce_errs_splits_other_messages_with_per_disk_detail() {
    let typed: Vec<Option<DiskError>> = (0..3).map(|_| Some(DiskError::FaultyDisk)).collect();
    let (count, err) = reduce_errs(&typed, &[]);
    assert_eq!(count, 3);
    assert_eq!(err, Some(DiskError::FaultyDisk));

    // Same failure, but the message embeds which peer failed — as the
    // "can not get client, err: …" family does today.
    let fragmented: Vec<Option<DiskError>> = (0..3)
        .map(|peer| {
            let message = format!("can not get client, err: connection refused to peer {peer}");
            Some(DiskError::other(message))
        })
        .collect();
    let (count, _) = reduce_errs(&fragmented, OBJECT_OP_IGNORED_ERRS);
    assert_eq!(
        count, 1,
        "per-disk detail in other() messages fragments quorum buckets: 3 same-cause failures count as 1+1+1"
    );
}

/// Identical `other()` messages do still bucket together — the fragmentation
/// is caused by the formatted-in detail, not by `other()` itself.
#[test]
fn reduce_errs_buckets_identical_other_messages_together() {
    let same: Vec<Option<DiskError>> = (0..3).map(|_| Some(DiskError::other("can not get client"))).collect();
    let (count, err) = reduce_errs(&same, OBJECT_OP_IGNORED_ERRS);
    assert_eq!(count, 3);
    assert_eq!(err, Some(DiskError::other("can not get client")));
}
