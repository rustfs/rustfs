// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

//! A candidate index never replaces multipart metadata or quorum validation.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub(crate) const RUSTFS_MULTIPART_BUCKET_KEY: &str = "x-rustfs-internal-multipart-bucket";
pub(crate) const RUSTFS_MULTIPART_OBJECT_KEY: &str = "x-rustfs-internal-multipart-object";

pub(crate) const MULTIPART_INDEX_PAGE_SIZE: usize = 256;
/// Pages one listing accepts from one disk. Pruned pages may carry few roots, so
/// the cursor alone no longer bounds a peer's answer; a disk that keeps
/// continuing past this many pages contributes no coverage.
pub(crate) const MULTIPART_INDEX_MAX_PAGES_PER_DISK: usize = 4096;

/// A bounded page from one disk's continuously maintained, boot-local index.
/// `epoch` changes whenever coverage must be rebuilt; callers must not combine
/// pages from different epochs or physical disk identities.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MultipartIndexPage {
    pub bucket: String,
    pub disk_id: Uuid,
    pub epoch: Uuid,
    pub roots: Vec<String>,
    pub next_marker: Option<String>,
}

pub const MULTIPART_INDEX_RPC_MAX_BYTES: usize = 64 * 1024;
pub const MULTIPART_INDEX_RPC_MAX_REQUEST_BYTES: usize = 4096;

/// Additive operation inside the legacy read-only DiskInfo JSON envelope.
/// Old peers may ignore this field, but their ordinary response cannot decode
/// as the required signed, challenge-bound MultipartIndexRpcReply.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MultipartIndexRpcRequest {
    pub disk_id: String,
    pub metrics: bool,
    pub noop: bool,
    pub multipart_index: MultipartIndexQuery,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MultipartIndexQuery {
    pub operation: String,
    pub version: u32,
    pub challenge: Uuid,
    pub bucket: String,
    pub marker: Option<String>,
    pub page_cap: usize,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MultipartIndexRpcReply {
    pub operation: String,
    pub version: u32,
    pub challenge: Uuid,
    pub page: MultipartIndexPage,
    pub proof: Vec<u8>,
}

impl MultipartIndexRpcRequest {
    pub fn new(bucket: &str, marker: Option<&str>) -> Self {
        Self {
            disk_id: String::new(),
            metrics: false,
            noop: true,
            multipart_index: MultipartIndexQuery {
                operation: "list_multipart_index_roots".into(),
                version: 1,
                challenge: Uuid::new_v4(),
                bucket: bucket.to_owned(),
                marker: marker.map(str::to_owned),
                page_cap: MULTIPART_INDEX_PAGE_SIZE,
            },
        }
    }

    pub fn validate(&self) -> crate::disk::error::Result<()> {
        let query = &self.multipart_index;
        if !self.disk_id.is_empty()
            || self.metrics
            || !self.noop
            || query.operation != "list_multipart_index_roots"
            || query.version != 1
            || query.challenge.is_nil()
            || query.bucket.is_empty()
            || query.bucket.len() > 255
            || query.bucket.contains('/')
            || matches!(query.bucket.as_str(), "." | "..")
            || query.page_cap != MULTIPART_INDEX_PAGE_SIZE
            || query.marker.as_ref().is_some_and(|marker| !valid_root(marker))
        {
            return Err(crate::disk::error::DiskError::MethodNotAllowed);
        }
        Ok(())
    }

    pub fn canonical_request(&self, disk: &str) -> crate::disk::error::Result<Vec<u8>> {
        self.validate()?;
        if disk.len() > MULTIPART_INDEX_RPC_MAX_REQUEST_BYTES {
            return Err(crate::disk::error::DiskError::FileCorrupt);
        }
        Ok(serde_json::to_vec(&("rustfs-multipart-index-request-v1", disk, self))?)
    }

    pub fn canonical_response(&self, disk: &str, page: &MultipartIndexPage) -> crate::disk::error::Result<Vec<u8>> {
        self.validate()?;
        Ok(serde_json::to_vec(&("rustfs-multipart-index-response-v1", disk, self, page))?)
    }
}

fn valid_root(root: &str) -> bool {
    root.len() == 64 && root.bytes().all(|byte| byte.is_ascii_hexdigit())
}

impl MultipartIndexPage {
    pub fn validate(&self, bucket: &str, marker: Option<&str>, expected_disk: Uuid) -> crate::disk::error::Result<()> {
        if expected_disk.is_nil()
            || self.disk_id != expected_disk
            || self.epoch.is_nil()
            || self.bucket != bucket
            || self.roots.len() > MULTIPART_INDEX_PAGE_SIZE
            || self
                .roots
                .iter()
                .any(|root| !valid_root(root) || marker.is_some_and(|marker| root.as_str() <= marker))
            || self.roots.windows(2).any(|pair| pair[0] >= pair[1])
            // The cursor names the last examined marker; pruning may leave fewer
            // live roots than examined names, so it only has to bound them.
            || self.next_marker.as_ref().is_some_and(|next| {
                !valid_root(next)
                    || marker.is_some_and(|marker| next.as_str() <= marker)
                    || self.roots.last().is_some_and(|last| last > next)
            })
        {
            return Err(crate::disk::error::DiskError::FileCorrupt);
        }
        Ok(())
    }
}

impl MultipartIndexRpcReply {
    /// This is structural validation only; the caller must also verify `proof`.
    pub fn validate(&self, request: &MultipartIndexRpcRequest, expected_disk: Uuid) -> crate::disk::error::Result<()> {
        let query = &request.multipart_index;
        request.validate()?;
        if self.operation != query.operation
            || self.version != query.version
            || self.challenge != query.challenge
            || self.proof.len() != 32
        {
            return Err(crate::disk::error::DiskError::FileCorrupt);
        }
        self.page.validate(&query.bucket, query.marker.as_deref(), expected_disk)
    }
}

/// A disk can attest coverage only after every page belongs to the same physical
/// identity and index generation. An interrupted/changed stream contributes no
/// partial candidates to the erasure set's coverage count.
pub(crate) async fn collect_pages<F, Fut>(bucket: &str, identity: Uuid, mut fetch: F) -> crate::disk::error::Result<Vec<String>>
where
    F: FnMut(Option<String>) -> Fut,
    Fut: std::future::Future<Output = crate::disk::error::Result<MultipartIndexPage>>,
{
    let mut epoch = None;
    let mut marker = None;
    let mut roots = Vec::new();
    let mut pages = 0_usize;
    loop {
        if pages >= MULTIPART_INDEX_MAX_PAGES_PER_DISK {
            return Err(crate::disk::error::DiskError::FileCorrupt);
        }
        pages += 1;
        let page = fetch(marker.clone()).await?;
        page.validate(bucket, marker.as_deref(), identity)?;
        if epoch.is_some_and(|epoch| epoch != page.epoch) {
            return Err(crate::disk::error::DiskError::DiskNotFound);
        }
        epoch = Some(page.epoch);
        roots.extend(page.roots);
        marker = page.next_marker;
        if marker.is_none() {
            return Ok(roots);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn multipart_index_reply_rejects_challenge_disk_cursor_and_unbounded_pages() {
        let request = MultipartIndexRpcRequest::new("bucket", None);
        let id = Uuid::new_v4();
        let mut reply = MultipartIndexRpcReply {
            operation: request.multipart_index.operation.clone(),
            version: 1,
            challenge: request.multipart_index.challenge,
            page: MultipartIndexPage {
                bucket: "bucket".into(),
                disk_id: id,
                epoch: Uuid::new_v4(),
                roots: vec!["a".repeat(64)],
                next_marker: None,
            },
            proof: vec![0; 32],
        };
        reply.validate(&request, id).expect("valid structural envelope");
        assert!(
            reply.validate(&MultipartIndexRpcRequest::new("bucket", None), id).is_err(),
            "old challenge cannot replay"
        );
        assert!(reply.validate(&request, Uuid::new_v4()).is_err(), "physical disk identity cannot change");
        reply.page.next_marker = Some("9".repeat(64));
        assert!(reply.validate(&request, id).is_err(), "a cursor below the last root would re-list it");
        reply.page.next_marker = Some("a".repeat(64));
        reply
            .validate(&request, id)
            .expect("a pruned short page continues from its last examined name");
        reply.page.next_marker = None;
        reply.page.roots = vec!["a".repeat(64); MULTIPART_INDEX_PAGE_SIZE + 1];
        assert!(reply.validate(&request, id).is_err(), "peer cannot enlarge the fixed page cap");
    }

    #[test]
    fn multipart_index_response_proof_binds_the_complete_query_and_page() {
        crate::runtime::sources::ensure_test_rpc_secret();
        let mut request = MultipartIndexRpcRequest::new("bucket", None);
        let mut page = MultipartIndexPage {
            bucket: "bucket".into(),
            disk_id: Uuid::new_v4(),
            epoch: Uuid::new_v4(),
            roots: vec![],
            next_marker: None,
        };
        let canonical = request.canonical_response("disk", &page).expect("canonical response");
        let proof = crate::cluster::rpc::sign_tonic_rpc_response_proof(&canonical).expect("signed response");
        let verify = |request: &MultipartIndexRpcRequest, page: &MultipartIndexPage| {
            crate::cluster::rpc::verify_tonic_rpc_response_proof(
                &request.canonical_response("disk", page).expect("canonical"),
                &proof,
            )
        };
        verify(&request, &page).expect("exact response");
        request.multipart_index.marker = Some("0".repeat(64));
        assert!(verify(&request, &page).is_err(), "cursor is signed");
        request.multipart_index.marker = None;
        page.epoch = Uuid::new_v4();
        assert!(verify(&request, &page).is_err(), "boot-local coverage epoch is signed");
    }
    #[test]
    fn multipart_index_page_cursor_bounds_roots_without_requiring_a_full_page() {
        let disk = Uuid::new_v4();
        let page = |roots: Vec<String>, next_marker: Option<String>| MultipartIndexPage {
            bucket: "bucket".into(),
            disk_id: disk,
            epoch: Uuid::new_v4(),
            roots,
            next_marker,
        };
        // Pruning leaves fewer live roots than examined names; the cursor still advances.
        page(vec![format!("{:064x}", 1)], Some(format!("{:064x}", 5)))
            .validate("bucket", None, disk)
            .expect("short page with a cursor");
        page(vec![], Some(format!("{:064x}", 5)))
            .validate("bucket", Some(&format!("{:064x}", 2)), disk)
            .expect("all examined names pruned");
        assert!(
            page(vec![format!("{:064x}", 6)], Some(format!("{:064x}", 5)))
                .validate("bucket", None, disk)
                .is_err(),
            "a root beyond the cursor would be re-listed"
        );
        assert!(
            page(vec![], Some(format!("{:064x}", 2)))
                .validate("bucket", Some(&format!("{:064x}", 2)), disk)
                .is_err(),
            "the cursor must advance past the request marker"
        );
        assert!(
            page(vec![], Some("not-a-sha".to_owned()))
                .validate("bucket", None, disk)
                .is_err(),
            "the cursor is a marker name"
        );
    }

    #[tokio::test]
    async fn multipart_index_page_collection_bounds_the_pages_one_disk_may_answer() {
        let id = Uuid::new_v4();
        let epoch = Uuid::new_v4();
        let mut served = 0_usize;
        let result = collect_pages("bucket", id, |marker| {
            served += 1;
            let next = marker.map_or(0_u64, |marker| u64::from_str_radix(&marker[48..], 16).expect("hex cursor") + 1);
            std::future::ready(Ok(MultipartIndexPage {
                bucket: "bucket".into(),
                disk_id: id,
                epoch,
                roots: vec![],
                next_marker: Some(format!("{next:064x}")),
            }))
        })
        .await;
        assert!(matches!(result, Err(crate::disk::error::DiskError::FileCorrupt)), "{result:?}");
        assert_eq!(served, MULTIPART_INDEX_MAX_PAGES_PER_DISK, "the peer is cut off at the page cap");
    }

    #[tokio::test]
    async fn multipart_index_page_collection_rejects_replaced_disk_or_restarted_epoch() {
        let id = Uuid::new_v4();
        let epoch = Uuid::new_v4();
        for replace_disk in [false, true] {
            let first = MultipartIndexPage {
                bucket: "bucket".into(),
                disk_id: id,
                epoch,
                roots: (0..MULTIPART_INDEX_PAGE_SIZE).map(|index| format!("{index:064x}")).collect(),
                next_marker: Some(format!("{:064x}", MULTIPART_INDEX_PAGE_SIZE - 1)),
            };
            let second = MultipartIndexPage {
                bucket: "bucket".into(),
                disk_id: if replace_disk { Uuid::new_v4() } else { id },
                epoch: if replace_disk { epoch } else { Uuid::new_v4() },
                roots: vec![],
                next_marker: None,
            };
            let mut replies = std::collections::VecDeque::from([first, second]);
            let result = collect_pages("bucket", id, |marker| {
                let page = replies.pop_front().expect("bounded fixture pages");
                if marker.is_some() {
                    assert!(page.roots.is_empty());
                }
                std::future::ready(Ok(page))
            })
            .await;
            // A replaced disk fails the page's identity check; a restarted
            // epoch fails the cross-page continuity check.
            let expected_corrupt = replace_disk;
            assert!(
                match &result {
                    Err(crate::disk::error::DiskError::FileCorrupt) => expected_corrupt,
                    Err(crate::disk::error::DiskError::DiskNotFound) => !expected_corrupt,
                    _ => false,
                },
                "changed physical source cannot contribute partial coverage: {result:?}"
            );
            assert!(replies.is_empty(), "exercise the continuation response");
        }
    }
}
