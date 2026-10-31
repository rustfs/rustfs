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

use super::{
    TableCatalogObjectBackend, TableCatalogObjectMutationFence, TableCatalogPutPrecondition, TableCatalogStoreError,
    TableCatalogStoreResult, TableWarehouseIndexEntry,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

const NODE_CAPACITY: usize = 32;
const MAX_HEIGHT: u8 = 16;
const MAX_NODE_BYTES: usize = 1024 * 1024;
// Reserve space for node framing and the larger internal child references.
const MAX_ENTRY_BYTES: usize = MAX_NODE_BYTES / NODE_CAPACITY - 256;
const NODE_VERSION: u16 = 1;

type TreeEntry = (String, TableWarehouseIndexEntry);
type Neighbors = (Option<TreeEntry>, Option<TreeEntry>);

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RegistrationTreeRoot {
    root: Option<NodeRef>,
    height: u8,
}

impl RegistrationTreeRoot {
    pub(super) fn validate(&self) -> TableCatalogStoreResult<()> {
        match &self.root {
            None if self.height == 0 => Ok(()),
            Some(root) if (1..=MAX_HEIGHT).contains(&self.height) => root.validate(),
            _ => Err(invalid("invalid registration tree root height")),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct NodeRef {
    id: Uuid,
    digest: String,
    max_key: String,
}

impl NodeRef {
    fn validate(&self) -> TableCatalogStoreResult<()> {
        if self.id.is_nil()
            || self.max_key.is_empty()
            || self.digest.len() != 64
            || !self
                .digest
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(invalid("invalid registration tree node reference"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
enum Node {
    Leaf {
        version: u16,
        entries: Vec<TreeEntry>,
    },
    Internal {
        version: u16,
        height: u8,
        children: Vec<NodeRef>,
    },
}

impl Node {
    fn height(&self) -> u8 {
        match self {
            Self::Leaf { .. } => 1,
            Self::Internal { height, .. } => *height,
        }
    }

    fn len(&self) -> usize {
        match self {
            Self::Leaf { entries, .. } => entries.len(),
            Self::Internal { children, .. } => children.len(),
        }
    }

    fn max_key(&self) -> Option<&str> {
        match self {
            Self::Leaf { entries, .. } => entries.last().map(|(key, _)| key.as_str()),
            Self::Internal { children, .. } => children.last().map(|child| child.max_key.as_str()),
        }
    }

    fn validate(&self, lower_bound: Option<&str>) -> TableCatalogStoreResult<()> {
        if self.len() == 0 || self.len() > NODE_CAPACITY {
            return Err(invalid("invalid registration tree node capacity"));
        }
        let (version, keys) = match self {
            Self::Leaf { version, entries } => (*version, entries.iter().map(|(key, _)| key.as_str()).collect::<Vec<_>>()),
            Self::Internal {
                version,
                height,
                children,
            } => {
                if !(2..=MAX_HEIGHT).contains(height) || children.len() < 2 {
                    return Err(invalid("invalid registration tree internal height or capacity"));
                }
                for child in children {
                    child.validate()?;
                }
                (*version, children.iter().map(|child| child.max_key.as_str()).collect())
            }
        };
        if version != NODE_VERSION
            || keys.iter().any(|key| key.is_empty())
            || keys.windows(2).any(|pair| pair[0] >= pair[1])
            || lower_bound.is_some_and(|lower| keys[0] <= lower)
        {
            return Err(invalid("invalid registration tree node version or key order"));
        }
        Ok(())
    }

    fn split(&mut self) -> Option<Self> {
        if self.len() <= NODE_CAPACITY {
            return None;
        }
        let middle = self.len() / 2;
        Some(match self {
            Self::Leaf { version, entries } => Self::Leaf {
                version: *version,
                entries: entries.split_off(middle),
            },
            Self::Internal {
                version,
                height,
                children,
            } => Self::Internal {
                version: *version,
                height: *height,
                children: children.split_off(middle),
            },
        })
    }
}

fn invalid(message: &str) -> TableCatalogStoreError {
    TableCatalogStoreError::Invalid(message.to_string())
}

fn node_prefix(prefix: &str) -> String {
    format!("{}/nodes/", prefix.trim_end_matches('/'))
}

fn digest(bytes: &[u8]) -> String {
    hex_simd::encode_to_string(Sha256::digest(bytes), hex_simd::AsciiCase::Lower)
}

fn decode_node(bytes: &[u8]) -> TableCatalogStoreResult<Node> {
    if bytes.len() > MAX_NODE_BYTES {
        return Err(invalid("registration tree node exceeds its size limit"));
    }
    let node: Node = serde_json::from_slice(bytes).map_err(|_| invalid("invalid registration tree node JSON"))?;
    node.validate(None)?;
    Ok(node)
}

/// Bounds both leaf entries and internal references before publishing a table.
pub(super) fn registration_tree_entry_supported(key: &str, owner: &TableWarehouseIndexEntry) -> bool {
    !key.is_empty() && serde_json::to_vec(&(key, owner)).is_ok_and(|bytes| bytes.len() <= MAX_ENTRY_BYTES)
}

/// Recognizes tree nodes for catalog cleanup, including unreachable split nodes.
pub(super) fn validate_cleanup_object(prefix: &str, key: &str, bytes: &[u8]) -> TableCatalogStoreResult<bool> {
    let prefix = node_prefix(prefix);
    let Some(name) = key.strip_prefix(&prefix) else {
        return Ok(false);
    };
    name.strip_suffix(".json")
        .and_then(|id| Uuid::parse_str(id).ok())
        .filter(|id| !id.is_nil() && format!("{id}.json") == name)
        .ok_or_else(|| invalid("invalid registration tree node object name"))?;
    decode_node(bytes)?;
    Ok(true)
}

/// The caller holds the bucket publication fence for every operation.
///
/// Writes update existing node IDs. Before inserting, the caller durably marks
/// its root dirty; only a successful insert followed by publishing the new root
/// makes it ready again. Cancellation or a failed write can leave partial nodes
/// and requires rebuilding from the authoritative catalog before another read.
pub(super) struct RegistrationTree<'a, B> {
    backend: &'a B,
    catalog_bucket: &'a str,
    prefix: &'a str,
    fence: &'a TableCatalogObjectMutationFence,
}

impl<'a, B: TableCatalogObjectBackend> RegistrationTree<'a, B> {
    pub(super) fn new(
        backend: &'a B,
        catalog_bucket: &'a str,
        prefix: &'a str,
        fence: &'a TableCatalogObjectMutationFence,
    ) -> Self {
        Self {
            backend,
            catalog_bucket,
            prefix,
            fence,
        }
    }

    fn object_key(&self, id: Uuid) -> String {
        format!("{}{id}.json", node_prefix(self.prefix))
    }

    async fn read_node(&self, reference: &NodeRef, height: u8, lower_bound: Option<&str>) -> TableCatalogStoreResult<Node> {
        self.fence.ensure_held()?;
        reference.validate()?;
        let object = self
            .backend
            .read_object_limited(self.catalog_bucket, &self.object_key(reference.id), MAX_NODE_BYTES)
            .await?
            .ok_or_else(|| invalid("referenced registration tree node is missing"))?;
        self.fence.ensure_held()?;
        if digest(&object.data) != reference.digest {
            return Err(invalid("registration tree node digest mismatch"));
        }
        let node = decode_node(&object.data)?;
        if node.height() != height || node.max_key() != Some(reference.max_key.as_str()) {
            return Err(invalid("registration tree node height or maximum key mismatch"));
        }
        node.validate(lower_bound)?;
        Ok(node)
    }

    async fn write_node(&self, id: Option<Uuid>, node: &Node) -> TableCatalogStoreResult<NodeRef> {
        node.validate(None)?;
        let bytes = serde_json::to_vec(node).map_err(|_| invalid("could not encode registration tree node"))?;
        if bytes.len() > MAX_NODE_BYTES {
            return Err(invalid("registration tree node exceeds its size limit"));
        }
        let reference = NodeRef {
            id: id.unwrap_or_else(Uuid::new_v4),
            digest: digest(&bytes),
            max_key: node
                .max_key()
                .ok_or_else(|| invalid("registration tree node is empty"))?
                .to_string(),
        };
        self.backend
            .put_object_fenced(
                self.catalog_bucket,
                &self.object_key(reference.id),
                bytes,
                if id.is_some() {
                    TableCatalogPutPrecondition::Any
                } else {
                    TableCatalogPutPrecondition::IfAbsent
                },
                self.fence,
            )
            .await?;
        Ok(reference)
    }

    async fn write_split(&self, id: Option<Uuid>, mut node: Node) -> TableCatalogStoreResult<(NodeRef, Option<NodeRef>)> {
        let right = node.split();
        let left = self.write_node(id, &node).await?;
        let right = match right {
            Some(right) => Some(self.write_node(None, &right).await?),
            None => None,
        };
        Ok((left, right))
    }

    pub(super) async fn get(
        &self,
        root: &RegistrationTreeRoot,
        key: &str,
    ) -> TableCatalogStoreResult<Option<TableWarehouseIndexEntry>> {
        Ok(self
            .find_neighbor(root, key, None, false)
            .await?
            .filter(|(candidate, _)| candidate == key)
            .map(|(_, owner)| owner))
    }

    /// Returns the greatest key below `key` and the least key at or above it.
    /// Excluded owners are skipped across leaf boundaries as well.
    pub(super) async fn neighbors(
        &self,
        root: &RegistrationTreeRoot,
        key: &str,
        excluded_table_id: Option<&str>,
    ) -> TableCatalogStoreResult<Neighbors> {
        let predecessor = self.find_neighbor(root, key, excluded_table_id, true).await?;
        let successor = self.find_neighbor(root, key, excluded_table_id, false).await?;
        Ok((predecessor, successor))
    }

    async fn find_neighbor(
        &self,
        root: &RegistrationTreeRoot,
        key: &str,
        excluded_table_id: Option<&str>,
        predecessor: bool,
    ) -> TableCatalogStoreResult<Option<TreeEntry>> {
        root.validate()?;
        let Some(reference) = &root.root else {
            return Ok(None);
        };
        let mut pending = vec![(reference.clone(), root.height, None::<String>)];
        while let Some((reference, height, lower_bound)) = pending.pop() {
            let node = self.read_node(&reference, height, lower_bound.as_deref()).await?;
            match node {
                Node::Leaf { entries, .. } => {
                    let mut candidates = entries.into_iter().filter(|(candidate, owner)| {
                        if excluded_table_id.is_some_and(|id| owner.table_id == id) {
                            return false;
                        }
                        if predecessor {
                            candidate.as_str() < key
                        } else {
                            candidate.as_str() >= key
                        }
                    });
                    let result = if predecessor {
                        candidates.next_back()
                    } else {
                        candidates.next()
                    };
                    if result.is_some() {
                        return Ok(result);
                    }
                }
                Node::Internal { children, .. } => {
                    let mut ranges = Vec::with_capacity(children.len());
                    let mut lower = lower_bound;
                    for child in children {
                        let eligible = if predecessor {
                            lower.as_deref().is_none_or(|lower| lower < key)
                        } else {
                            child.max_key.as_str() >= key
                        };
                        let next_lower = Some(child.max_key.clone());
                        if eligible {
                            ranges.push((child, height - 1, lower));
                        }
                        lower = next_lower;
                    }
                    if !predecessor {
                        ranges.reverse();
                    }
                    pending.extend(ranges);
                }
            }
        }
        Ok(None)
    }

    /// Inserts or replaces an exact key. Ownership checks belong to the caller.
    pub(super) async fn insert(
        &self,
        root: &mut RegistrationTreeRoot,
        key: String,
        owner: TableWarehouseIndexEntry,
    ) -> TableCatalogStoreResult<()> {
        root.validate()?;
        if key.is_empty() {
            return Err(invalid("registration tree key cannot be empty"));
        }
        if !registration_tree_entry_supported(&key, &owner) {
            return Err(TableCatalogStoreError::Unsupported(
                "registration tree entry exceeds its bounded node capacity".to_string(),
            ));
        }
        let Some(mut reference) = root.root.clone() else {
            root.root = Some(
                self.write_node(
                    None,
                    &Node::Leaf {
                        version: NODE_VERSION,
                        entries: vec![(key, owner)],
                    },
                )
                .await?,
            );
            root.height = 1;
            return Ok(());
        };
        let mut height = root.height;
        let mut lower_bound = None::<String>;
        let mut ancestors = Vec::new();
        let mut replacement = loop {
            match self.read_node(&reference, height, lower_bound.as_deref()).await? {
                Node::Leaf { version, mut entries } => {
                    match entries.binary_search_by(|(candidate, _)| candidate.cmp(&key)) {
                        Ok(index) => entries[index] = (key, owner),
                        Err(index) => entries.insert(index, (key, owner)),
                    }
                    break self.write_split(Some(reference.id), Node::Leaf { version, entries }).await?;
                }
                Node::Internal { children, .. } => {
                    let index = children.partition_point(|child| child.max_key < key).min(children.len() - 1);
                    if index > 0 {
                        lower_bound = Some(children[index - 1].max_key.clone());
                    }
                    let child = children[index].clone();
                    ancestors.push((reference.id, height, children, index));
                    reference = child;
                    height -= 1;
                }
            }
        };
        while let Some((id, height, mut children, index)) = ancestors.pop() {
            children[index] = replacement.0;
            if let Some(right) = replacement.1 {
                children.insert(index + 1, right);
            }
            replacement = self
                .write_split(
                    Some(id),
                    Node::Internal {
                        version: NODE_VERSION,
                        height,
                        children,
                    },
                )
                .await?;
        }
        let (reference, height) = if let Some(right) = replacement.1 {
            if root.height == MAX_HEIGHT {
                return Err(invalid("registration tree exceeds its height limit"));
            }
            let height = root.height + 1;
            let reference = self
                .write_node(
                    None,
                    &Node::Internal {
                        version: NODE_VERSION,
                        height,
                        children: vec![replacement.0, right],
                    },
                )
                .await?;
            (reference, height)
        } else {
            (replacement.0, root.height)
        };
        *root = RegistrationTreeRoot {
            root: Some(reference),
            height,
        };
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table_catalog::TableCatalogEntryState;
    use crate::table_catalog::test_support::TestCatalogObjectBackend;

    const BUCKET: &str = ".rustfs.sys";
    const PREFIX: &str = "catalog/registration";

    fn owner(index: usize) -> TableWarehouseIndexEntry {
        TableWarehouseIndexEntry {
            version: 1,
            table_bucket: "analytics".to_string(),
            namespace: "sales".to_string(),
            table: format!("table{index:05}"),
            table_id: format!("id{index:05}"),
            warehouse_object_prefix: format!("data/{index:05}"),
            state: TableCatalogEntryState::Active,
        }
    }

    #[tokio::test]
    async fn registration_tree_splits_and_preserves_sorted_neighbors() {
        let backend = TestCatalogObjectBackend::default();
        let fence = TableCatalogObjectMutationFence::default();
        let tree = RegistrationTree::new(&backend, BUCKET, PREFIX, &fence);
        let mut root = RegistrationTreeRoot::default();
        for index in (0..600).rev() {
            tree.insert(&mut root, format!("{index:05}"), owner(index)).await.unwrap();
        }
        assert!(root.height >= 3);
        for index in [0, 15, 16, 31, 32, 300, 599] {
            assert_eq!(tree.get(&root, &format!("{index:05}")).await.unwrap(), Some(owner(index)));
        }
        let (previous, next) = tree.neighbors(&root, "00300", Some("id00300")).await.unwrap();
        assert_eq!(previous.unwrap().0, "00299");
        assert_eq!(next.unwrap().0, "00301");
        assert!(tree.neighbors(&root, "00000", None).await.unwrap().0.is_none());
        assert!(tree.neighbors(&root, "99999", None).await.unwrap().1.is_none());
        let encoded = serde_json::to_vec(&root).unwrap();
        let restored = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(root, restored);
    }

    #[tokio::test]
    async fn registration_tree_skips_excluded_owners_across_leaves() {
        let backend = TestCatalogObjectBackend::default();
        let fence = TableCatalogObjectMutationFence::default();
        let tree = RegistrationTree::new(&backend, BUCKET, PREFIX, &fence);
        let mut root = RegistrationTreeRoot::default();
        for index in 0..96 {
            let mut entry = owner(index);
            if (16..80).contains(&index) {
                entry.table_id = "relocating".to_string();
            }
            tree.insert(&mut root, format!("{index:05}"), entry).await.unwrap();
        }
        let (previous, next) = tree.neighbors(&root, "00050", Some("relocating")).await.unwrap();
        assert_eq!(previous.unwrap().0, "00015");
        assert_eq!(next.unwrap().0, "00080");
    }

    #[tokio::test]
    async fn registration_tree_reads_grow_with_height() {
        let backend = TestCatalogObjectBackend::default();
        let fence = TableCatalogObjectMutationFence::default();
        let tree = RegistrationTree::new(&backend, BUCKET, PREFIX, &fence);
        let mut root = RegistrationTreeRoot::default();
        for index in 0..1200 {
            tree.insert(&mut root, format!("{index:05}"), owner(index)).await.unwrap();
            if [31, 599, 1199].contains(&index) {
                backend.reset_call_counts().await;
                assert_eq!(tree.get(&root, "00015").await.unwrap(), Some(owner(15)));
                assert_eq!(backend.read_call_count().await, usize::from(root.height));
                let read_bytes = backend.state.lock().await.read_bytes;
                assert!(read_bytes <= usize::from(root.height) * 16 * 1024);
                backend.reset_call_counts().await;
                tree.insert(&mut root, "00015".to_string(), owner(15)).await.unwrap();
                assert_eq!(backend.read_call_count().await, usize::from(root.height));
                assert_eq!(backend.list_call_count().await, 0);
            }
        }
        assert!(root.height <= 3);
    }

    #[tokio::test]
    async fn registration_tree_rejects_missing_nodes_and_digest_damage() {
        let backend = TestCatalogObjectBackend::default();
        let fence = TableCatalogObjectMutationFence::default();
        let tree = RegistrationTree::new(&backend, BUCKET, PREFIX, &fence);
        let mut root = RegistrationTreeRoot::default();
        tree.insert(&mut root, "key".to_string(), owner(1)).await.unwrap();
        let key = tree.object_key(root.root.as_ref().unwrap().id);
        let bytes = backend.read_object(BUCKET, &key).await.unwrap().unwrap().data;
        backend
            .put_object(BUCKET, &key, b"{}".to_vec(), TableCatalogPutPrecondition::Any)
            .await
            .unwrap();
        assert!(matches!(tree.get(&root, "key").await, Err(TableCatalogStoreError::Invalid(_))));
        backend.delete_object(BUCKET, &key).await.unwrap();
        assert!(matches!(tree.get(&root, "key").await, Err(TableCatalogStoreError::Invalid(_))));
        assert!(validate_cleanup_object(PREFIX, &key, &bytes).unwrap());
    }

    #[tokio::test]
    async fn registration_tree_rejects_invalid_height_and_key_order() {
        let backend = TestCatalogObjectBackend::default();
        let fence = TableCatalogObjectMutationFence::default();
        let tree = RegistrationTree::new(&backend, BUCKET, PREFIX, &fence);
        let node = Node::Leaf {
            version: NODE_VERSION,
            entries: vec![("z".to_string(), owner(1)), ("a".to_string(), owner(2))],
        };
        let bytes = serde_json::to_vec(&node).unwrap();
        let id = Uuid::new_v4();
        backend
            .put_object(BUCKET, &tree.object_key(id), bytes.clone(), TableCatalogPutPrecondition::Any)
            .await
            .unwrap();
        let mut root = RegistrationTreeRoot {
            root: Some(NodeRef {
                id,
                digest: digest(&bytes),
                max_key: "a".to_string(),
            }),
            height: 1,
        };
        assert!(matches!(tree.get(&root, "a").await, Err(TableCatalogStoreError::Invalid(_))));
        root.height = MAX_HEIGHT + 1;
        backend.reset_call_counts().await;
        assert!(matches!(tree.get(&root, "a").await, Err(TableCatalogStoreError::Invalid(_))));
        assert_eq!(backend.read_call_count().await, 0);
        root.height = 0;
        assert!(matches!(tree.get(&root, "a").await, Err(TableCatalogStoreError::Invalid(_))));
    }

    #[test]
    fn registration_tree_cleanup_rejects_unknown_fields_and_oversized_nodes() {
        let key = format!("{}{id}.json", node_prefix(PREFIX), id = Uuid::new_v4());
        let node = Node::Leaf {
            version: NODE_VERSION,
            entries: vec![("key".to_string(), owner(1))],
        };
        let mut json = serde_json::to_value(node).unwrap();
        json["unexpected"] = serde_json::json!(true);
        assert!(validate_cleanup_object(PREFIX, &key, &serde_json::to_vec(&json).unwrap()).is_err());
        assert!(validate_cleanup_object(PREFIX, &key, &vec![b' '; MAX_NODE_BYTES + 1]).is_err());
        assert!(!validate_cleanup_object(PREFIX, "unrelated.json", b"{}").unwrap());
    }

    #[tokio::test]
    async fn registration_tree_rejects_large_entries_before_writing() {
        let backend = TestCatalogObjectBackend::default();
        let fence = TableCatalogObjectMutationFence::default();
        let tree = RegistrationTree::new(&backend, BUCKET, PREFIX, &fence);
        let mut root = RegistrationTreeRoot::default();
        let key = "a".repeat(MAX_ENTRY_BYTES);
        assert!(!registration_tree_entry_supported(&key, &owner(1)));
        assert!(matches!(
            tree.insert(&mut root, key, owner(1)).await,
            Err(TableCatalogStoreError::Unsupported(_))
        ));
        assert!(backend.state.lock().await.objects.is_empty());
        assert_eq!(root, RegistrationTreeRoot::default());
    }
}
