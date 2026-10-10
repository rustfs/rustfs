//! Exact OpenRaft values and an independent CMEM v1 membership codec.
use super::{CaptureNode, CaptureNodeError, LogId, VoteRecord};
use std::collections::{BTreeMap, BTreeSet};

type Stored = openraft::StoredMembership<u64, CaptureNode>;

impl From<VoteRecord> for openraft::Vote<u64> {
    fn from(value: VoteRecord) -> Self {
        Self {
            leader_id: openraft::LeaderId::new(value.term, value.voted_node),
            committed: value.committed,
        }
    }
}
impl From<openraft::Vote<u64>> for VoteRecord {
    fn from(value: openraft::Vote<u64>) -> Self {
        Self {
            term: value.leader_id.term,
            voted_node: value.leader_id.node_id,
            committed: value.committed,
        }
    }
}
impl From<LogId> for openraft::LogId<u64> {
    fn from(value: LogId) -> Self {
        Self::new(openraft::CommittedLeaderId::new(value.term, value.leader_node), value.index)
    }
}
impl From<openraft::LogId<u64>> for LogId {
    fn from(value: openraft::LogId<u64>) -> Self {
        Self {
            term: value.leader_id.term,
            leader_node: value.leader_id.node_id,
            index: value.index,
        }
    }
}
#[derive(Debug, thiserror::Error)]
pub enum MembershipError {
    #[error("missing voter metadata")]
    MissingVoter,
    #[error("invalid node metadata")]
    InvalidNode(#[source] CaptureNodeError),
    #[error("membership construction changed values")]
    ChangedMembership,
    #[error("invalid membership magic")]
    InvalidMagic,
    #[error("unsupported membership version")]
    UnsupportedVersion,
    #[error("invalid membership log ID tag")]
    InvalidLogIdTag,
    #[error("truncated membership record")]
    Truncated,
    #[error("invalid membership length or count")]
    InvalidLength,
    #[error("membership IDs are not strictly increasing")]
    UnorderedIds,
    #[error("trailing membership bytes")]
    TrailingBytes,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CaptureMembership {
    log_id: Option<LogId>,
    configs: Vec<BTreeSet<u64>>,
    nodes: BTreeMap<u64, CaptureNode>,
}
impl CaptureMembership {
    pub fn new(
        log_id: Option<LogId>,
        configs: Vec<BTreeSet<u64>>,
        nodes: BTreeMap<u64, CaptureNode>,
    ) -> Result<Self, MembershipError> {
        let value = Self { log_id, configs, nodes };
        value.validate()?;
        Ok(value)
    }
    fn validate(&self) -> Result<(), MembershipError> {
        for group in &self.configs {
            for voter in group {
                if !self.nodes.contains_key(voter) {
                    return Err(MembershipError::MissingVoter);
                }
            }
        }
        for node in self.nodes.values() {
            node.validate().map_err(MembershipError::InvalidNode)?;
        }
        Ok(())
    }
    pub fn log_id(&self) -> &Option<LogId> {
        &self.log_id
    }
    pub fn configs(&self) -> &[BTreeSet<u64>] {
        &self.configs
    }
    pub fn nodes(&self) -> &BTreeMap<u64, CaptureNode> {
        &self.nodes
    }

    pub fn encode(&self) -> Result<Vec<u8>, MembershipError> {
        self.validate()?;
        let mut bytes = b"CMEM\x01".to_vec();
        match self.log_id {
            None => bytes.push(0),
            Some(id) => {
                bytes.push(1);
                bytes.extend_from_slice(&super::encode_id(id));
            }
        }
        put_count(&mut bytes, self.configs.len())?;
        for group in &self.configs {
            put_count(&mut bytes, group.len())?;
            for id in group {
                bytes.extend_from_slice(&id.to_be_bytes());
            }
        }
        put_count(&mut bytes, self.nodes.len())?;
        for (id, node) in &self.nodes {
            let record = node.encode().map_err(MembershipError::InvalidNode)?;
            bytes.extend_from_slice(&id.to_be_bytes());
            put_count(&mut bytes, record.len())?;
            bytes.extend_from_slice(&record);
        }
        Ok(bytes)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, MembershipError> {
        let mut reader = Reader { remaining: bytes };
        if reader.take(4)? != b"CMEM" {
            return Err(MembershipError::InvalidMagic);
        }
        if reader.take(1)? != [1] {
            return Err(MembershipError::UnsupportedVersion);
        }
        let log_id = match reader.take(1)? {
            [0] => None,
            [1] => Some(super::decode_id(reader.take(24)?).map_err(|_| MembershipError::InvalidLength)?),
            _ => return Err(MembershipError::InvalidLogIdTag),
        };
        let group_count = reader.count(4)?;
        let mut configs = Vec::new();
        for _ in 0..group_count {
            let voter_count = reader.count(8)?;
            let mut group = BTreeSet::new();
            let mut previous = None;
            for _ in 0..voter_count {
                let id = reader.id()?;
                increasing(previous, id)?;
                previous = Some(id);
                group.insert(id);
            }
            configs.push(group);
        }
        // A node requires an ID, length, and at least the CNOD header.
        let node_count = reader.count(21)?;
        let mut nodes = BTreeMap::new();
        let mut previous = None;
        for _ in 0..node_count {
            let id = reader.id()?;
            increasing(previous, id)?;
            previous = Some(id);
            let length = reader.count(1)?;
            let node = CaptureNode::decode(reader.take(length)?).map_err(MembershipError::InvalidNode)?;
            nodes.insert(id, node);
        }
        if !reader.remaining.is_empty() {
            return Err(MembershipError::TrailingBytes);
        }
        Self::new(log_id, configs, nodes)
    }
}
impl TryFrom<&Stored> for CaptureMembership {
    type Error = MembershipError;
    fn try_from(value: &Stored) -> Result<Self, Self::Error> {
        Self::new(
            value.log_id().map(Into::into),
            value.membership().get_joint_config().clone(),
            value.nodes().map(|(id, node)| (*id, node.clone())).collect(),
        )
    }
}
impl TryFrom<&CaptureMembership> for Stored {
    type Error = MembershipError;
    fn try_from(value: &CaptureMembership) -> Result<Self, Self::Error> {
        value.validate()?;
        let membership = openraft::Membership::new(value.configs.clone(), value.nodes.clone());
        let nodes: BTreeMap<_, _> = membership.nodes().map(|(id, node)| (*id, node.clone())).collect();
        if membership.get_joint_config() != &value.configs || nodes != value.nodes {
            return Err(MembershipError::ChangedMembership);
        }
        Ok(Self::new(value.log_id.map(Into::into), membership))
    }
}
fn put_count(bytes: &mut Vec<u8>, count: usize) -> Result<(), MembershipError> {
    let count = u32::try_from(count).map_err(|_| MembershipError::InvalidLength)?;
    bytes.extend_from_slice(&count.to_be_bytes());
    Ok(())
}
fn increasing(previous: Option<u64>, id: u64) -> Result<(), MembershipError> {
    if previous.is_some_and(|previous| previous >= id) {
        return Err(MembershipError::UnorderedIds);
    }
    Ok(())
}
struct Reader<'a> {
    remaining: &'a [u8],
}
impl<'a> Reader<'a> {
    fn take(&mut self, length: usize) -> Result<&'a [u8], MembershipError> {
        let value = self.remaining.get(..length).ok_or(MembershipError::Truncated)?;
        self.remaining = self.remaining.get(length..).ok_or(MembershipError::Truncated)?;
        Ok(value)
    }
    fn count(&mut self, minimum: usize) -> Result<usize, MembershipError> {
        let bytes = self.take(4)?.try_into().map_err(|_| MembershipError::InvalidLength)?;
        let count = usize::try_from(u32::from_be_bytes(bytes)).map_err(|_| MembershipError::InvalidLength)?;
        let required = count.checked_mul(minimum).ok_or(MembershipError::InvalidLength)?;
        if required > self.remaining.len() {
            return Err(MembershipError::InvalidLength);
        }
        Ok(count)
    }
    fn id(&mut self) -> Result<u64, MembershipError> {
        Ok(u64::from_be_bytes(self.take(8)?.try_into().map_err(|_| MembershipError::InvalidLength)?))
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error;

    const EMPTY: &[u8] = b"CMEM\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00";
    const JOINT: &[u8] = b"CMEM\x01\x01\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\x00\x00\x00\x02\x00\x00\x00\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x02\x00\x00\x00\x00\x00\x00\x00\x01\xff\xff\xff\xff\xff\xff\xff\xff\x00\x00\x00\x05\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x17CNOD\x01\x00\x00\x00\x0ehttp://node:80\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x1aCNOD\x01\x00\x00\x00\x11https://node:443/\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00\x1bCNOD\x01\x00\x00\x00\x12https://[::1]:443/\x00\x00\x00\x00\x00\x00\x00\x03\x00\x00\x00\x1eCNOD\x01\x00\x00\x00\x15http://127.0.0.1:9000\xff\xff\xff\xff\xff\xff\xff\xff\x00\x00\x00\x19CNOD\x01\x00\x00\x00\x10https://node:443";

    fn id() -> LogId {
        LogId {
            term: u64::MAX,
            leader_node: u64::MAX,
            index: u64::MAX,
        }
    }
    fn nodes() -> BTreeMap<u64, CaptureNode> {
        [
            (0, "http://node:80"),
            (1, "https://node:443/"),
            (2, "https://[::1]:443/"),
            (3, "http://127.0.0.1:9000"),
            (u64::MAX, "https://node:443"),
        ]
        .into_iter()
        .map(|(id, uri)| (id, CaptureNode::new(uri.to_owned()).expect("fixture URI")))
        .collect()
    }
    fn groups() -> Vec<BTreeSet<u64>> {
        vec![BTreeSet::from([0, 1]), BTreeSet::from([1, u64::MAX])]
    }
    fn fixture() -> CaptureMembership {
        CaptureMembership::new(Some(id()), groups(), nodes()).expect("valid joint fixture")
    }
    fn roundtrip(value: &CaptureMembership) {
        let library = Stored::try_from(value).expect("native to library");
        assert_eq!(library.membership().get_joint_config().as_slice(), value.configs());
        assert_eq!(
            library
                .nodes()
                .map(|(id, node)| (*id, node.clone()))
                .collect::<BTreeMap<_, _>>(),
            *value.nodes()
        );
        assert_eq!(*library.log_id(), value.log_id().map(Into::into));
        assert_eq!(CaptureMembership::try_from(&library).expect("library to native"), *value);
        let extracted = CaptureMembership::try_from(&library).expect("extract complete library");
        assert_eq!(Stored::try_from(&extracted).expect("back to library"), library);
    }
    #[test]
    fn vote_roundtrip_full_width_and_commit() {
        for term in [0, u64::MAX] {
            for node in [0, u64::MAX] {
                for committed in [false, true] {
                    let native = VoteRecord {
                        term,
                        voted_node: node,
                        committed,
                    };
                    let library = openraft::Vote::<u64>::from(native);
                    assert_eq!(
                        (library.leader_id.term, library.leader_id.node_id, library.committed),
                        (term, node, committed)
                    );
                    assert_eq!(VoteRecord::from(library), native);
                    let actual = if committed {
                        openraft::Vote::new_committed(term, node)
                    } else {
                        openraft::Vote::new(term, node)
                    };
                    assert_eq!(openraft::Vote::<u64>::from(VoteRecord::from(actual)), actual);
                }
            }
        }
    }
    #[test]
    fn log_id_roundtrip_same_term_distinct_leaders() {
        for term in [0, u64::MAX] {
            for index in [0, u64::MAX] {
                let values = [0, u64::MAX].map(|leader_node| LogId {
                    term,
                    leader_node,
                    index,
                });
                let libraries = values.map(openraft::LogId::<u64>::from);
                assert_ne!(libraries[0], libraries[1], "same term leaders must stay distinct");
                for (native, library) in values.into_iter().zip(libraries) {
                    assert_eq!(
                        (library.leader_id.term, library.leader_id.node_id, library.index),
                        (native.term, native.leader_node, native.index)
                    );
                    assert_eq!(LogId::from(library), native);
                    let actual = openraft::LogId::new(openraft::CommittedLeaderId::new(term, native.leader_node), index);
                    assert_eq!(openraft::LogId::<u64>::from(LogId::from(actual)), actual);
                }
            }
        }
    }
    #[test]
    fn legacy_vote_log_golden_bytes() {
        let vote = VoteRecord {
            term: 0x0102030405060708,
            voted_node: u64::MAX,
            committed: true,
        };
        let log = LogId {
            term: 0x0102030405060708,
            leader_node: u64::MAX,
            index: 0,
        };
        assert_eq!(
            super::super::encode_vote(VoteRecord::from(openraft::Vote::<u64>::from(vote))),
            b"\x01\x02\x03\x04\x05\x06\x07\x08\xff\xff\xff\xff\xff\xff\xff\xff\x01"
        );
        assert_eq!(
            super::super::encode_id(LogId::from(openraft::LogId::<u64>::from(log))),
            b"\x01\x02\x03\x04\x05\x06\x07\x08\xff\xff\xff\xff\xff\xff\xff\xff\x00\x00\x00\x00\x00\x00\x00\x00"
        );
    }
    #[test]
    fn membership_joint_learners_roundtrip() {
        let value = fixture();
        roundtrip(&value);
        let mut swapped = groups();
        swapped.reverse();
        assert_ne!(value, CaptureMembership::new(Some(id()), swapped, nodes()).expect("swapped groups"));
        let library = Stored::new(Some(id().into()), openraft::Membership::new(groups(), nodes()));
        assert_eq!(CaptureMembership::try_from(&library).expect("library fixture"), value);
        assert_eq!(Stored::try_from(&value).expect("native fixture"), library);
    }
    #[test]
    fn membership_optional_log_id_roundtrip() {
        for log_id in [None, Some(id())] {
            let value = CaptureMembership::new(log_id, groups(), nodes()).expect("optional ID");
            roundtrip(&value);
            assert_eq!(
                CaptureMembership::decode(&value.encode().expect("encode optional ID")).expect("decode optional ID"),
                value
            );
        }
    }
    #[test]
    fn membership_library_default_and_empty_groups() {
        let library = Stored::default();
        assert_eq!(library.membership().get_joint_config(), &Vec::<BTreeSet<u64>>::new());
        assert_eq!(library.nodes().count(), 0);
        assert_eq!(*library.log_id(), None);
        let native = CaptureMembership::try_from(&library).expect("actual default extraction");
        roundtrip(&native);
        for configs in [
            vec![BTreeSet::new()],
            vec![BTreeSet::new(), BTreeSet::new()],
            vec![BTreeSet::from([0]), BTreeSet::from([0])],
        ] {
            let map = if configs.iter().any(|g| !g.is_empty()) {
                BTreeMap::from([(0, nodes()[&0].clone())])
            } else {
                BTreeMap::new()
            };
            let native = CaptureMembership::new(None, configs.clone(), map).expect("empty or repeated groups");
            roundtrip(&native);
            assert_eq!(
                CaptureMembership::decode(&native.encode().expect("encode groups")).expect("decode groups"),
                native
            );
            assert_eq!(native.configs(), configs);
        }
        roundtrip(&CaptureMembership::new(None, vec![], BTreeMap::from([(0, nodes()[&0].clone())])).expect("valid bootstrap"));
    }
    #[test]
    fn membership_rejects_missing_voters() {
        assert!(matches!(
            CaptureMembership::new(None, vec![BTreeSet::from([0])], BTreeMap::new()),
            Err(MembershipError::MissingVoter)
        ));
        let library: Stored = serde_json::from_str(r#"{"log_id":null,"membership":{"configs":[[0]],"nodes":{}}}"#)
            .expect("real serde can omit voter node");
        assert!(matches!(CaptureMembership::try_from(&library), Err(MembershipError::MissingVoter)));
    }
    #[test]
    fn membership_rejects_default_metadata_without_log_id() {
        let library = Stored::new(
            None,
            openraft::Membership::new(vec![BTreeSet::from([0])], BTreeMap::<u64, CaptureNode>::new()),
        );
        assert!(matches!(CaptureMembership::try_from(&library), Err(MembershipError::InvalidNode(_))));
        for log_id in [None, Some(id())] {
            assert!(matches!(
                CaptureMembership::new(log_id, vec![], BTreeMap::from([(0, CaptureNode::default())])),
                Err(MembershipError::InvalidNode(_))
            ));
        }
    }
    #[test]
    fn membership_uri_bytes_survive_conversion() {
        let value = fixture();
        roundtrip(&value);
        let decoded = CaptureMembership::decode(&value.encode().expect("encode URI fixture")).expect("decode URI fixture");
        for (id, node) in nodes() {
            assert_eq!(decoded.nodes()[&id].rpc_uri().as_bytes(), node.rpc_uri().as_bytes());
        }
    }
    #[test]
    fn membership_codec_golden_and_roundtrip() {
        for (value, golden) in [
            (CaptureMembership::new(None, vec![], BTreeMap::new()).expect("empty membership"), EMPTY),
            (fixture(), JOINT),
        ] {
            assert_eq!(value.encode().expect("golden encoding"), golden, "fixed CMEM bytes");
            assert_eq!(CaptureMembership::decode(golden).expect("golden decoding"), value);
        }
    }
    fn record(configs: &[Vec<u64>], entries: &[(u64, Vec<u8>)]) -> Vec<u8> {
        let mut bytes = b"CMEM\x01\x00".to_vec();
        bytes.extend_from_slice(&u32::try_from(configs.len()).expect("groups count").to_be_bytes());
        for group in configs {
            bytes.extend_from_slice(&u32::try_from(group.len()).expect("voter count").to_be_bytes());
            for id in group {
                bytes.extend_from_slice(&id.to_be_bytes());
            }
        }
        bytes.extend_from_slice(&u32::try_from(entries.len()).expect("node count").to_be_bytes());
        for (id, node) in entries {
            bytes.extend_from_slice(&id.to_be_bytes());
            bytes.extend_from_slice(&u32::try_from(node.len()).expect("node length").to_be_bytes());
            bytes.extend_from_slice(node);
        }
        bytes
    }
    #[test]
    fn membership_codec_rejects_duplicate_and_unsorted_ids() {
        let node = nodes()[&0].encode().expect("CNOD fixture");
        for ids in [vec![0, 0], vec![1, 0]] {
            assert!(matches!(
                CaptureMembership::decode(&record(&[ids.clone()], &[(0, node.clone()), (1, node.clone())])),
                Err(MembershipError::UnorderedIds)
            ));
            assert!(matches!(
                CaptureMembership::decode(&record(&[], &ids.into_iter().map(|id| (id, node.clone())).collect::<Vec<_>>())),
                Err(MembershipError::UnorderedIds)
            ));
        }
    }
    #[test]
    fn membership_codec_rejects_malformed_records() {
        let mut bad = Vec::<Vec<u8>>::new();
        for end in 0..EMPTY.len() {
            bad.push(EMPTY[..end].to_vec());
        }
        for end in 0..JOINT.len() {
            bad.push(JOINT[..end].to_vec());
        }
        for (offset, value) in [(0, b'X'), (4, 2), (5, 2)] {
            let mut b = EMPTY.to_vec();
            b[offset] = value;
            bad.push(b);
        }
        let mut trailing = EMPTY.to_vec();
        trailing.push(0);
        bad.push(trailing);
        let mut trailing = JOINT.to_vec();
        trailing.extend_from_slice(b"tail");
        bad.push(trailing);
        for count_offset in [6, 10] {
            let mut b = EMPTY.to_vec();
            b[count_offset..count_offset + 4].copy_from_slice(&u32::MAX.to_be_bytes());
            bad.push(b);
        }
        bad.push(b"CMEM\x01\x00\x00\x00\x00\x01\xff\xff\xff\xff\x00\x00\x00\x00".to_vec());
        bad.push(record(&[vec![0]], &[]));
        for cnod in [
            b"CNOD\x01\x00\x00\x00\x00".to_vec(),
            b"CNOD\x01\x00\x00\x00\x01\xff".to_vec(),
            b"CNOD\x02\x00\x00\x00\x0ehttp://node:80".to_vec(),
            b"CNOD\x01\x00\x00\x00\x0ehttp://node:80x".to_vec(),
            Vec::new(),
            b"CNOD\x01\xff\xff\xff\xff".to_vec(),
            b"CNOD\x01\x00\x00\x00\x01x".to_vec(),
        ] {
            bad.push(record(&[], &[(0, cnod)]));
        }
        let mut b = record(&[], &[(0, nodes()[&0].encode().expect("CNOD length fixture"))]);
        b[22..26].copy_from_slice(&u32::MAX.to_be_bytes());
        bad.push(b);
        for (index, bytes) in bad.iter().enumerate() {
            assert!(CaptureMembership::decode(bytes).is_err(), "malformed case {index} must reject");
        }
        let corrupt = record(&[], &[(0, b"CNOD\x01\x00\x00\x00\x01\xff".to_vec())]);
        let error = CaptureMembership::decode(&corrupt).expect_err("typed CNOD error");
        assert!(matches!(error, MembershipError::InvalidNode(_)));
        assert!(error.source().is_some());
        assert!(!format!("{error:?} {error}").contains("http://"));
    }
}
