//! Exact OpenRaft values and an independent CMEM v1 membership codec.
use super::{CaptureNode, CaptureNodeError, CreatedApplyResult, CreatedResult, DecideCreated, LogId, StoreError, VoteRecord};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CaptureCommand(DecideCreated);
impl CaptureCommand {
    pub fn try_created(command: DecideCreated) -> Result<Self, StoreError> {
        command.encode()?;
        Ok(Self(command))
    }
    pub fn created(&self) -> &DecideCreated {
        &self.0
    }
}
impl Serialize for CaptureCommand {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let bytes = self
            .0
            .encode()
            .map_err(|_| serde::ser::Error::custom("invalid capture command"))?;
        bytes.serialize(serializer)
    }
}
impl<'de> Deserialize<'de> for CaptureCommand {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let bytes = Vec::<u8>::deserialize(deserializer).map_err(|_| serde::de::Error::custom("invalid capture command"))?;
        let command = DecideCreated::decode(&bytes).map_err(|_| serde::de::Error::custom("invalid capture command"))?;
        Self::try_created(command).map_err(|_| serde::de::Error::custom("invalid capture command"))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CaptureResponseKind {
    Blank,
    Created,
    Membership,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CaptureResponse {
    log_id: LogId,
    kind: CaptureResponseKind,
    result: Option<CreatedResult>,
}
impl CaptureResponse {
    pub fn blank(log_id: LogId) -> Result<Self, StoreError> {
        Ok(Self {
            log_id,
            kind: CaptureResponseKind::Blank,
            result: None,
        })
    }
    pub fn created(applied: CreatedApplyResult) -> Result<Self, StoreError> {
        Self::decode_result(&super::created::result_bytes(&applied.result))?;
        Ok(Self {
            log_id: applied.log_id,
            kind: CaptureResponseKind::Created,
            result: Some(applied.result),
        })
    }
    pub fn membership(log_id: LogId) -> Result<Self, StoreError> {
        Ok(Self {
            log_id,
            kind: CaptureResponseKind::Membership,
            result: None,
        })
    }
    fn decode_result(bytes: &[u8]) -> Result<CreatedResult, StoreError> {
        let mut decoder = super::created::Decoder::new(bytes);
        let result = decoder.result()?;
        decoder.finish()?;
        Ok(result)
    }
    pub fn kind(&self) -> CaptureResponseKind {
        self.kind
    }
    pub fn log_id(&self) -> &LogId {
        &self.log_id
    }
    pub fn created_result(&self) -> Option<&CreatedResult> {
        self.result.as_ref()
    }
}
#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum ResponseWire {
    Blank { version: u8, log_id: Vec<u8> },
    Created { version: u8, log_id: Vec<u8>, result: Vec<u8> },
    Membership { version: u8, log_id: Vec<u8> },
}
impl Serialize for CaptureResponse {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let log_id = super::encode_id(self.log_id);
        let wire = match (self.kind, &self.result) {
            (CaptureResponseKind::Blank, None) => ResponseWire::Blank { version: 1, log_id },
            (CaptureResponseKind::Membership, None) => ResponseWire::Membership { version: 1, log_id },
            (CaptureResponseKind::Created, Some(result)) => {
                let result = super::created::result_bytes(result);
                Self::decode_result(&result).map_err(|_| serde::ser::Error::custom("invalid capture response"))?;
                ResponseWire::Created {
                    version: 1,
                    log_id,
                    result,
                }
            }
            _ => return Err(serde::ser::Error::custom("invalid capture response")),
        };
        wire.serialize(serializer)
    }
}
impl<'de> Deserialize<'de> for CaptureResponse {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let wire = ResponseWire::deserialize(deserializer).map_err(|_| serde::de::Error::custom("invalid capture response"))?;
        let (version, log_id, kind, result) = match wire {
            ResponseWire::Blank { version, log_id } => (version, log_id, CaptureResponseKind::Blank, None),
            ResponseWire::Membership { version, log_id } => (version, log_id, CaptureResponseKind::Membership, None),
            ResponseWire::Created { version, log_id, result } => {
                let result = Self::decode_result(&result).map_err(|_| serde::de::Error::custom("invalid capture response"))?;
                (version, log_id, CaptureResponseKind::Created, Some(result))
            }
        };
        if version != 1 {
            return Err(serde::de::Error::custom("invalid capture response"));
        }
        let log_id = super::decode_id(&log_id).map_err(|_| serde::de::Error::custom("invalid capture response"))?;
        Ok(Self { log_id, kind, result })
    }
}

openraft::declare_raft_types!(
    pub CaptureTypeConfig:
        D = CaptureCommand,
        R = CaptureResponse,
        NodeId = u64,
        Node = CaptureNode,
        Entry = openraft::Entry<Self>,
        SnapshotData = std::io::Cursor<Vec<u8>>,
        AsyncRuntime = openraft::TokioRuntime,
        Responder = openraft::impls::OneshotResponder<Self>,
);
pub type CaptureRaftEntry = openraft::Entry<CaptureTypeConfig>;

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
    fn command_fixture() -> DecideCreated {
        DecideCreated {
            operation_id: [1; 16],
            object: crate::ObjectIdentity {
                bucket_incarnation: [2; 16],
                key: b"k".to_vec(),
            },
            expected_head: Some([3; 16]),
            prepared: crate::PreparedIdentity {
                version_id: [4; 16],
                preparation_id: [5; 16],
                content_digest: [6; 32],
                content_length: u64::MAX,
            },
            binding_revision: 17,
        }
    }
    fn checked_command(command: DecideCreated) -> CaptureCommand {
        let result = CaptureCommand::try_created(command);
        assert!(result.is_ok(), "valid command must be admitted");
        result.expect("asserted valid command")
    }
    fn checked_response(applied: CreatedApplyResult) -> CaptureResponse {
        let result = CaptureResponse::created(applied);
        assert!(result.is_ok(), "valid Created result must be admitted");
        result.expect("asserted valid response")
    }
    fn serde_roundtrip<T>(value: &T) -> T
    where
        T: Serialize + serde::de::DeserializeOwned,
    {
        let encoded = serde_json::to_vec(value);
        assert!(encoded.is_ok(), "valid value must serialize");
        let recovered = serde_json::from_slice(&encoded.expect("asserted valid serialization"));
        assert!(recovered.is_ok(), "valid value must deserialize");
        recovered.expect("asserted valid deserialization")
    }
    fn reject_command(bytes: &[u8]) {
        let json = serde_json::to_vec(bytes).expect("bytes JSON");
        let result = serde_json::from_slice::<CaptureCommand>(&json);
        assert!(result.is_err(), "corrupt command must reject");
        let error = result.expect_err("asserted corrupt command rejection");
        assert!(error.to_string().starts_with("invalid capture command"));
    }
    fn response_wire(kind: &str, result: Option<Vec<u8>>) -> serde_json::Value {
        let mut wire = serde_json::json!({"version": 1, "kind": kind, "log_id": crate::encode_id(id())});
        if let Some(result) = result {
            wire["result"] = serde_json::json!(result);
        }
        wire
    }
    fn reject_response(wire: serde_json::Value) {
        let result = serde_json::from_value::<CaptureResponse>(wire);
        assert!(result.is_err(), "corrupt response must reject");
        let error = result.expect_err("asserted corrupt response rejection");
        assert_eq!(error.to_string(), "invalid capture response");
    }
    #[test]
    fn capture_command_canonical_serde_roundtrip() {
        for key in [Vec::new(), b"k".to_vec(), vec![255; crate::MAX_OBJECT_KEY_LENGTH]] {
            for expected_head in [None, Some([3; 16])] {
                let mut input = command_fixture();
                input.object.key = key.clone();
                input.expected_head = expected_head;
                let command = checked_command(input.clone());
                assert_eq!(command.created(), &input);
                let recovered: CaptureCommand = serde_roundtrip(&command);
                assert_eq!(recovered.created(), &input);
                assert_eq!(
                    recovered.created().encode().expect("canonical bytes"),
                    input.encode().expect("original bytes")
                );
            }
        }
    }
    #[test]
    fn capture_command_serde_rejects_corruption() {
        let bytes = command_fixture().encode().expect("fixture bytes");
        for offset in [5, 21, 43, 59, 75] {
            let mut corrupt = bytes.clone();
            corrupt[offset..offset + 16].fill(0);
            reject_command(&corrupt);
        }
        let mut bad_version = bytes.clone();
        bad_version[4] = 2;
        reject_command(&bad_version);
        let mut oversized = bytes.clone();
        oversized[37..41].copy_from_slice(&4097u32.to_be_bytes());
        reject_command(&oversized);
        for length in 0..bytes.len() {
            reject_command(&bytes[..length]);
        }
        let mut trailing = bytes;
        trailing.push(0);
        reject_command(&trailing);
        let error = serde_json::from_value::<CaptureCommand>(serde_json::json!({"secret-object-key": "payload"}))
            .expect_err("wrong shape");
        assert_eq!(error.to_string(), "invalid capture command");
    }
    #[test]
    fn capture_command_constructor_rejects_invalid_values() {
        for field in 0..6 {
            let mut command = command_fixture();
            match field {
                0 => command.operation_id = [0; 16],
                1 => command.object.bucket_incarnation = [0; 16],
                2 => command.prepared.version_id = [0; 16],
                3 => command.prepared.preparation_id = [0; 16],
                4 => command.expected_head = Some([0; 16]),
                _ => command.object.key = vec![0; crate::MAX_OBJECT_KEY_LENGTH + 1],
            }
            assert!(CaptureCommand::try_created(command).is_err());
        }
        let mut valid = command_fixture();
        valid.object.key.clear();
        valid.prepared.content_digest = [0; 32];
        assert_eq!(checked_command(valid.clone()).created(), &valid);
    }
    #[test]
    fn capture_response_all_created_results_roundtrip() {
        let original = LogId {
            term: 7,
            leader_node: 0,
            index: 3,
        };
        for result in [
            CreatedResult::Created {
                version_id: [4; 16],
                event_id: [8; 16],
                decision_log_id: original,
            },
            CreatedResult::HeadMismatch { actual_head: None },
            CreatedResult::HeadMismatch {
                actual_head: Some([9; 16]),
            },
            CreatedResult::OperationConflict,
            CreatedResult::BindingMismatch,
            CreatedResult::VersionConflict,
        ] {
            let response = checked_response(CreatedApplyResult {
                log_id: id(),
                result: result.clone(),
            });
            assert_eq!(response.kind(), CaptureResponseKind::Created);
            assert_eq!(*response.log_id(), id());
            assert_eq!(response.created_result(), Some(&result));
            let recovered: CaptureResponse = serde_roundtrip(&response);
            assert_eq!(recovered, response);
            assert_eq!(recovered.created_result(), Some(&result));
            let wire = serde_json::to_value(&response).expect("response wire");
            assert_eq!(wire["result"], serde_json::json!(crate::created::result_bytes(&result)));
        }
    }
    #[test]
    fn capture_response_control_kinds_distinct() {
        let blank = CaptureResponse::blank(id());
        let membership = CaptureResponse::membership(id());
        assert!(blank.is_ok(), "Blank response must be admitted");
        assert!(membership.is_ok(), "Membership response must be admitted");
        let blank = blank.expect("asserted Blank");
        let membership = membership.expect("asserted Membership");
        assert_ne!(blank, membership);
        for (response, kind) in [
            (blank, CaptureResponseKind::Blank),
            (membership, CaptureResponseKind::Membership),
        ] {
            assert_eq!(response.kind(), kind);
            assert_eq!(*response.log_id(), id());
            assert!(response.created_result().is_none());
            let recovered: CaptureResponse = serde_roundtrip(&response);
            assert_eq!(recovered, response);
        }
    }
    #[test]
    fn capture_response_serde_rejects_corruption() {
        let valid_result = crate::created::result_bytes(&CreatedResult::Created {
            version_id: [4; 16],
            event_id: [8; 16],
            decision_log_id: id(),
        });
        for field in ["version", "kind", "log_id", "result"] {
            let mut wire = response_wire("created", Some(valid_result.clone()));
            wire.as_object_mut().expect("wire object").remove(field);
            reject_response(wire);
        }
        for kind in ["unknown", "secret-object-key"] {
            reject_response(response_wire(kind, None));
        }
        let mut wire = response_wire("created", Some(valid_result.clone()));
        wire["version"] = serde_json::json!(2);
        reject_response(wire);
        let mut wire = response_wire("created", Some(valid_result.clone()));
        wire["secret-object-key"] = serde_json::json!("secret-payload");
        reject_response(wire);
        for kind in ["blank", "membership"] {
            reject_response(response_wire(kind, Some(vec![2])));
            reject_response(response_wire(kind, Some(Vec::new())));
        }
        for bytes in [
            vec![255],
            vec![0],
            {
                let mut b = valid_result.clone();
                b[1..17].fill(0);
                b
            },
            {
                let mut b = valid_result.clone();
                b[17..33].fill(0);
                b
            },
            vec![1, 1],
            vec![1, 2],
        ] {
            reject_response(response_wire("created", Some(bytes)));
        }
        for length in 0..valid_result.len() {
            reject_response(response_wire("created", Some(valid_result[..length].to_vec())));
        }
        for result in [valid_result, vec![1, 0], vec![2], vec![3], vec![4]] {
            let mut trailing = result;
            trailing.push(0);
            reject_response(response_wire("created", Some(trailing)));
        }
        for length in [0, 23, 25] {
            let mut wire = response_wire("blank", None);
            wire["log_id"] = serde_json::json!(vec![0; length]);
            reject_response(wire);
        }
        for result in [
            CreatedResult::Created {
                version_id: [0; 16],
                event_id: [8; 16],
                decision_log_id: id(),
            },
            CreatedResult::Created {
                version_id: [4; 16],
                event_id: [0; 16],
                decision_log_id: id(),
            },
            CreatedResult::HeadMismatch {
                actual_head: Some([0; 16]),
            },
        ] {
            assert!(
                CaptureResponse::created(CreatedApplyResult {
                    log_id: id(),
                    result: result.clone()
                })
                .is_err()
            );
            let unchecked = CaptureResponse {
                log_id: id(),
                kind: CaptureResponseKind::Created,
                result: Some(result),
            };
            let error = serde_json::to_value(&unchecked).expect_err("serializer validates malformed private fixture");
            assert_eq!(error.to_string(), "invalid capture response");
        }
    }
    #[test]
    fn capture_type_config_exact_associated_types() {
        fn exact<
            C: openraft::RaftTypeConfig<
                    D = CaptureCommand,
                    R = CaptureResponse,
                    NodeId = u64,
                    Node = CaptureNode,
                    Entry = CaptureRaftEntry,
                    SnapshotData = std::io::Cursor<Vec<u8>>,
                    AsyncRuntime = openraft::TokioRuntime,
                    Responder = openraft::impls::OneshotResponder<CaptureTypeConfig>,
                >,
        >() {
        }
        exact::<CaptureTypeConfig>();
        let command = checked_command(command_fixture());
        assert!(!command.created().object.key.is_empty());
        let responses = [
            CaptureResponse::blank(id()),
            CaptureResponse::membership(id()),
            CaptureResponse::created(CreatedApplyResult {
                log_id: id(),
                result: CreatedResult::BindingMismatch,
            }),
        ];
        for response in responses {
            assert!(response.is_ok(), "concrete response binding admits valid kinds");
        }
    }
    #[test]
    fn capture_raft_entry_normal_roundtrip() {
        for leader_node in [0, u64::MAX] {
            let native_id = LogId {
                term: u64::MAX,
                leader_node,
                index: u64::MAX,
            };
            let input = command_fixture();
            let entry = CaptureRaftEntry {
                log_id: native_id.into(),
                payload: openraft::EntryPayload::Normal(checked_command(input.clone())),
            };
            let recovered: CaptureRaftEntry = serde_roundtrip(&entry);
            assert_eq!(recovered.log_id, entry.log_id);
            assert_eq!(recovered, entry);
            assert!(matches!(&recovered.payload,openraft::EntryPayload::Normal(command) if command.created()==&input));
            if let openraft::EntryPayload::Normal(command) = recovered.payload {
                assert_eq!(command.created().prepared.content_digest, input.prepared.content_digest);
                assert_eq!(
                    command.created().encode().expect("entry command"),
                    input.encode().expect("legacy command")
                );
            }
        }
    }
    #[test]
    fn capture_raft_entry_control_roundtrip() {
        let native = fixture();
        let stored = Stored::try_from(&native).expect("valid membership conversion");
        for payload in [
            openraft::EntryPayload::Blank,
            openraft::EntryPayload::Membership(stored.membership().clone()),
        ] {
            let entry = CaptureRaftEntry {
                log_id: id().into(),
                payload,
            };
            let recovered: CaptureRaftEntry = serde_roundtrip(&entry);
            assert_eq!(recovered, entry);
            match recovered.payload {
                openraft::EntryPayload::Blank => assert!(matches!(entry.payload, openraft::EntryPayload::Blank)),
                openraft::EntryPayload::Membership(membership) => {
                    let recovered = CaptureMembership::try_from(&Stored::new(Some(recovered.log_id), membership))
                        .expect("recovered metadata admission");
                    assert_eq!(recovered, native);
                    assert_eq!(recovered.configs(), groups());
                    assert_eq!(recovered.nodes(), &nodes());
                }
                openraft::EntryPayload::Normal(_) => panic!("control became business command"),
            }
        }
        let invalid = openraft::Membership::new(vec![BTreeSet::from([0])], BTreeMap::from([(0, CaptureNode::default())]));
        let entry = CaptureRaftEntry {
            log_id: id().into(),
            payload: openraft::EntryPayload::Membership(invalid),
        };
        assert!(serde_json::to_value(&entry).is_err(), "invalid node metadata must reject serialization");
        let entry = CaptureRaftEntry {
            log_id: id().into(),
            payload: openraft::EntryPayload::Membership(stored.membership().clone()),
        };
        let mut wire = serde_json::to_value(&entry).expect("valid membership entry wire");
        let removed = wire["payload"]["Membership"]["nodes"]
            .as_object_mut()
            .expect("membership nodes wire")
            .remove("0");
        assert!(removed.is_some(), "remove actual voter metadata");
        let recovered = serde_json::from_value::<CaptureRaftEntry>(wire);
        assert!(recovered.is_ok(), "library serde permits missing voter metadata");
        let recovered = recovered.expect("asserted library serde recovery");
        if let openraft::EntryPayload::Membership(membership) = recovered.payload {
            assert!(matches!(
                CaptureMembership::try_from(&Stored::new(Some(recovered.log_id), membership)),
                Err(MembershipError::MissingVoter)
            ));
        } else {
            panic!("membership must remain membership");
        }
    }
    #[test]
    fn capture_serde_preserves_legacy_codecs() {
        let mut command = command_fixture();
        command.expected_head = None;
        command.prepared.content_length = 7;
        command.binding_revision = 8;
        const COMMAND:&[u8]=b"CRTD\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x02\x02\x02\x02\x02\x02\x02\x02\x02\x02\x02\x02\x02\x02\x02\x02\x00\x00\x00\x01k\x00\x04\x04\x04\x04\x04\x04\x04\x04\x04\x04\x04\x04\x04\x04\x04\x04\x05\x05\x05\x05\x05\x05\x05\x05\x05\x05\x05\x05\x05\x05\x05\x05\x06\x06\x06\x06\x06\x06\x06\x06\x06\x06\x06\x06\x06\x06\x06\x06\x06\x06\x06\x06\x06\x06\x06\x06\x06\x06\x06\x06\x06\x06\x06\x06\x00\x00\x00\x00\x00\x00\x00\x07\x00\x00\x00\x00\x00\x00\x00\x08";
        assert_eq!(command.encode().expect("legacy encode"), COMMAND);
        assert_eq!(DecideCreated::decode(COMMAND).expect("golden decode"), command);
        let typed = checked_command(command);
        let recovered: CaptureCommand = serde_roundtrip(&typed);
        assert_eq!(recovered.created().encode().expect("typed golden"), COMMAND);
        const RESULT:&[u8]=b"\x00\x04\x04\x04\x04\x04\x04\x04\x04\x04\x04\x04\x04\x04\x04\x04\x04\x08\x08\x08\x08\x08\x08\x08\x08\x08\x08\x08\x08\x08\x08\x08\x08\x00\x00\x00\x00\x00\x00\x00\x07\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x03";
        let result = CreatedResult::Created {
            version_id: [4; 16],
            event_id: [8; 16],
            decision_log_id: LogId {
                term: 7,
                leader_node: 0,
                index: 3,
            },
        };
        assert_eq!(crate::created::result_bytes(&result), RESULT);
        let response = checked_response(CreatedApplyResult { log_id: id(), result });
        let recovered: CaptureResponse = serde_roundtrip(&response);
        assert_eq!(crate::created::result_bytes(recovered.created_result().expect("Created result")), RESULT);
    }
}
