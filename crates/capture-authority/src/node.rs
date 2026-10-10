//! Validated capture peer metadata and its independent CNOD v1 codec.
use serde::{Deserialize, Deserializer, Serialize, Serializer, ser::SerializeStruct};

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CaptureNode {
    rpc_uri: String,
}

#[derive(Debug, thiserror::Error)]
pub enum CaptureNodeError {
    #[error("invalid RPC URI")]
    InvalidRpcUri,
    #[error("invalid RPC URI")]
    InvalidRpcUriParse(#[source] url::ParseError),
    #[error("RPC URI too long")]
    RpcUriTooLong,
    #[error("invalid node magic")]
    InvalidNodeMagic,
    #[error("unsupported node version")]
    UnsupportedNodeVersion,
    #[error("invalid node length")]
    InvalidNodeLength,
    #[error("truncated node record")]
    TruncatedNodeRecord,
    #[error("invalid node UTF-8")]
    InvalidNodeUtf8(#[source] std::str::Utf8Error),
    #[error("trailing node bytes")]
    TrailingNodeBytes,
}
impl CaptureNodeError {
    pub fn code(&self) -> &'static str {
        match self {
            Self::InvalidRpcUri | Self::InvalidRpcUriParse(_) => "invalid_rpc_uri",
            Self::RpcUriTooLong => "rpc_uri_too_long",
            Self::InvalidNodeMagic => "invalid_node_magic",
            Self::UnsupportedNodeVersion => "unsupported_node_version",
            Self::InvalidNodeLength => "invalid_node_length",
            Self::TruncatedNodeRecord => "truncated_node_record",
            Self::InvalidNodeUtf8(_) => "invalid_node_utf8",
            Self::TrailingNodeBytes => "trailing_node_bytes",
        }
    }
}
impl CaptureNode {
    pub fn new(rpc_uri: String) -> Result<Self, CaptureNodeError> {
        let node = Self { rpc_uri };
        node.validate()?;
        Ok(node)
    }

    pub fn rpc_uri(&self) -> &str {
        &self.rpc_uri
    }

    pub fn validate(&self) -> Result<(), CaptureNodeError> {
        let uri = &self.rpc_uri;
        if uri.len() > 4096 {
            return Err(CaptureNodeError::RpcUriTooLong);
        }
        if uri.is_empty() || uri.chars().any(|c| c.is_whitespace() || c.is_control() || c == '\\') {
            return Err(CaptureNodeError::InvalidRpcUri);
        }
        let rest = uri
            .strip_prefix("http://")
            .or_else(|| uri.strip_prefix("https://"))
            .ok_or(CaptureNodeError::InvalidRpcUri)?;
        if uri.contains('?') || uri.contains('#') {
            return Err(CaptureNodeError::InvalidRpcUri);
        }
        let (authority, path) = rest.split_once('/').map_or((rest, ""), |(a, p)| (a, p));
        // An empty suffix after the first slash is the only allowed path.
        if !path.is_empty() || authority.contains('@') {
            return Err(CaptureNodeError::InvalidRpcUri);
        }
        let (host, port) = if authority.starts_with('[') {
            let end = authority.find(']').ok_or(CaptureNodeError::InvalidRpcUri)?;
            let port = authority
                .get(end + 1..)
                .and_then(|suffix| suffix.strip_prefix(':'))
                .ok_or(CaptureNodeError::InvalidRpcUri)?;
            (&authority[..=end], port)
        } else {
            authority.split_once(':').ok_or(CaptureNodeError::InvalidRpcUri)?
        };
        if host.is_empty() || port.is_empty() || !port.bytes().all(|b| b.is_ascii_digit()) {
            return Err(CaptureNodeError::InvalidRpcUri);
        }
        let port = port.parse::<u16>().map_err(|_| CaptureNodeError::InvalidRpcUri)?;
        if port == 0 {
            return Err(CaptureNodeError::InvalidRpcUri);
        }
        let parsed_url = url::Url::parse(uri).map_err(CaptureNodeError::InvalidRpcUriParse)?;
        if parsed_url.host().is_none() {
            return Err(CaptureNodeError::InvalidRpcUri);
        }
        Ok(())
    }

    pub fn encode(&self) -> Result<Vec<u8>, CaptureNodeError> {
        self.validate()?;
        let length = u32::try_from(self.rpc_uri.len()).map_err(|_| CaptureNodeError::InvalidNodeLength)?;
        let mut bytes = b"CNOD\x01".to_vec();
        bytes.extend_from_slice(&length.to_be_bytes());
        bytes.extend_from_slice(self.rpc_uri.as_bytes());
        Ok(bytes)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, CaptureNodeError> {
        if bytes.len() < 9 {
            return Err(CaptureNodeError::TruncatedNodeRecord);
        }
        if &bytes[..4] != b"CNOD" {
            return Err(CaptureNodeError::InvalidNodeMagic);
        }
        if bytes[4] != 1 {
            return Err(CaptureNodeError::UnsupportedNodeVersion);
        }
        let length = u32::from_be_bytes(bytes[5..9].try_into().map_err(|_| CaptureNodeError::InvalidNodeLength)?);
        if !(1..=4096).contains(&length) {
            return Err(CaptureNodeError::InvalidNodeLength);
        }
        let length = usize::try_from(length).map_err(|_| CaptureNodeError::InvalidNodeLength)?;
        let end = 9_usize.checked_add(length).ok_or(CaptureNodeError::InvalidNodeLength)?;
        if bytes.len() < end {
            return Err(CaptureNodeError::TruncatedNodeRecord);
        }
        if bytes.len() != end {
            return Err(CaptureNodeError::TrailingNodeBytes);
        }
        let uri = std::str::from_utf8(&bytes[9..end]).map_err(CaptureNodeError::InvalidNodeUtf8)?;
        Self::new(uri.to_owned())
    }
}
impl Serialize for CaptureNode {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.validate().map_err(serde::ser::Error::custom)?;
        let mut state = serializer.serialize_struct("CaptureNode", 1)?;
        state.serialize_field("rpc_uri", &self.rpc_uri)?;
        state.end()
    }
}
impl<'de> Deserialize<'de> for CaptureNode {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Fields {
            rpc_uri: String,
        }
        let fields = Fields::deserialize(deserializer).map_err(|_| serde::de::Error::custom("invalid capture node fields"))?;
        Self::new(fields.rpc_uri).map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error;

    fn record(payload: &[u8]) -> Vec<u8> {
        let mut bytes = b"CNOD\x01".to_vec();
        bytes.extend_from_slice(&u32::try_from(payload.len()).expect("test payload length").to_be_bytes());
        bytes.extend_from_slice(payload);
        bytes
    }
    fn reject_uri(uri: &str) {
        let result = CaptureNode::new(uri.to_owned());
        assert!(result.is_err(), "URI should reject: {uri:?}");
        assert_eq!(result.expect_err("rejected URI").code(), "invalid_rpc_uri");
    }
    fn reject_record(bytes: &[u8], code: &str) {
        let result = CaptureNode::decode(bytes);
        assert!(result.is_err(), "record should reject with {code}");
        assert_eq!(result.expect_err("rejected record").code(), code);
    }
    #[test]
    fn actual_openraft_node_bound() {
        fn require_node<T: openraft::Node>() {}
        require_node::<CaptureNode>();
        assert_eq!(CaptureNode::default().rpc_uri(), "");
    }
    #[test]
    fn default_and_empty_rejected_at_every_boundary() {
        let node = CaptureNode::default();
        assert_eq!(node.rpc_uri(), "");
        reject_uri("");
        assert_eq!(node.validate().expect_err("empty validation").code(), "invalid_rpc_uri");
        assert!(serde_json::to_string(&node).is_err());
        assert!(serde_json::from_str::<CaptureNode>(r#"{"rpc_uri":""}"#).is_err());
        assert_eq!(node.encode().expect_err("empty encoding").code(), "invalid_rpc_uri");
        reject_record(&record(b""), "invalid_node_length");
    }
    #[test]
    fn accepted_uris_preserve_exact_bytes() {
        for uri in [
            "http://node:80",
            "https://node:443",
            "http://node:1",
            "https://node:65535/",
            "http://127.0.0.1:9000",
            "https://[::1]:443/",
            "http://[2001:db8::1]:1",
            "http://NoDe:00080/",
        ] {
            let result = CaptureNode::new(uri.to_owned());
            assert!(result.is_ok(), "valid URI construction: {uri}");
            let node = result.expect("valid URI");
            assert_eq!(node.rpc_uri(), uri, "preserve explicit default port and raw URI");
            assert_eq!(node.encode().expect("encode URI"), record(uri.as_bytes()));
            assert_eq!(CaptureNode::decode(&record(uri.as_bytes())).expect("decode URI"), node);
        }
    }
    #[test]
    fn golden_cnod_bytes() {
        let node = CaptureNode {
            rpc_uri: "http://node:80".to_owned(),
        };
        let golden = b"CNOD\x01\x00\x00\x00\x0ehttp://node:80";
        assert_eq!(node.encode().ok().as_deref(), Some(golden.as_slice()), "golden CNOD bytes");
    }
    #[test]
    fn valid_cnod_decode() {
        let golden = b"CNOD\x01\x00\x00\x00\x0ehttp://node:80";
        let decoded = CaptureNode::decode(golden);
        assert!(decoded.is_ok(), "golden decode succeeds");
        assert_eq!(decoded.expect("golden decoded node").rpc_uri(), "http://node:80");
    }
    #[test]
    fn serde_shape_roundtrip_and_strict_fields() {
        let node = CaptureNode::new("https://node:443/".to_owned()).expect("serde node");
        let json = r#"{"rpc_uri":"https://node:443/"}"#;
        assert_eq!(serde_json::to_string(&node).expect("serialize node"), json);
        assert_eq!(serde_json::from_str::<CaptureNode>(json).expect("deserialize node"), node);
        for json in [
            r#"{}"#,
            r#"{"rpc_uri":1}"#,
            r#"{"rpc_uri":null}"#,
            r#"{"rpc_uri":"http://node:1","extra":0}"#,
            r#"{"rpc_uri":"http://node:1","rpc_uri":"http://node:2"}"#,
        ] {
            assert!(serde_json::from_str::<CaptureNode>(json).is_err(), "strict fields: {json}");
        }
    }
    #[test]
    fn credentials_and_raw_uri_shapes_reject() {
        for uri in [
            "http://user@node:1",
            "http://user:password@node:1",
            "http://@node:1",
            "HTTP://node:1",
            "Https://node:1",
            "http://node",
            "http://node:",
            "http://node:0",
            "http://node:-1",
            "http://node:+1",
            "http://node:65536",
            "http://node:999999999999999999999",
            "http://[::1:1",
            "http://[::1]extra:1",
            "http://[invalid]:1",
            "http://::1:1",
            "http://:1",
            "/disk/path",
            "file:///disk/path",
            "http://node:1/./",
            "http://node:1/../",
            "http://node:1/%2e/",
            "http://node:1/%2E%2e/",
            "http://node:1/path",
            "http://node:1\\",
            " http://node:1",
            "http://node:1\n",
            "http://no\tde:1",
            "http://node:1\u{00a0}",
            "http://node:1\u{0085}",
            "http://node:1\0",
        ] {
            reject_uri(uri);
        }
    }
    #[test]
    fn query_and_fragment_markers_reject() {
        for uri in [
            "http://node:1?",
            "http://node:1?x=y",
            "http://node:1/?",
            "http://node:1#",
            "http://node:1#fragment",
            "http://node:1/#",
        ] {
            reject_uri(uri);
        }
    }
    #[test]
    fn utf8_byte_length_boundaries() {
        for length in [4095, 4096] {
            let prefix = "http://node:";
            let uri = format!("{prefix}{}1", "0".repeat(length - prefix.len() - 1));
            assert_eq!(uri.len(), length);
            assert!(url::Url::parse(&uri).is_ok(), "boundary fixture accepted by actual URL parser");
            let node = CaptureNode::new(uri.clone()).expect("boundary node");
            assert_eq!(node.rpc_uri(), uri);
            assert_eq!(
                CaptureNode::decode(&node.encode().expect("boundary encoding")).expect("boundary decoding"),
                node
            );
        }
        let uri = format!("http://node:{}1", "0".repeat(4097 - "http://node:".len() - 1));
        assert_eq!(CaptureNode::new(uri).expect_err("oversize URI").code(), "rpc_uri_too_long");
        let unicode = format!("http://{}:1", "é".repeat(2044));
        assert!(unicode.chars().count() < 4096);
        assert!(unicode.len() > 4096);
        assert_eq!(CaptureNode::new(unicode).expect_err("UTF-8 byte limit").code(), "rpc_uri_too_long");
    }
    #[test]
    fn cnod_headers_and_lengths_reject_exact_codes() {
        let valid = record(b"http://node:80");
        for end in 0..9 {
            reject_record(&valid[..end], "truncated_node_record");
        }
        let mut bad = valid.clone();
        bad[0] = b'X';
        reject_record(&bad, "invalid_node_magic");
        bad = valid.clone();
        bad[4] = 2;
        reject_record(&bad, "unsupported_node_version");
        for length in [0_u32, 4097, u32::MAX] {
            bad = b"CNOD\x01".to_vec();
            bad.extend_from_slice(&length.to_be_bytes());
            reject_record(&bad, "invalid_node_length");
        }
        for end in 9..valid.len() {
            reject_record(&valid[..end], "truncated_node_record");
        }
        reject_record(&record(&[0xff]), "invalid_node_utf8");
        reject_record(&record(&[0xc3, 0x28]), "invalid_node_utf8");
        reject_record(&record(b"invalid URI"), "invalid_rpc_uri");
    }
    #[test]
    fn trailing_bytes_reject() {
        for tail in [&b"x"[..], &b"xyz"[..]] {
            let mut bytes = record(b"http://node:80");
            bytes.extend_from_slice(tail);
            reject_record(&bytes, "trailing_node_bytes");
        }
    }
    #[test]
    fn errors_and_sources_redact_input() {
        let sentinel = "http://SECRET_USER:SECRET_PASSWORD@node:1";
        let error = CaptureNode::new(sentinel.to_owned()).expect_err("credentials reject");
        let mut current: &dyn Error = &error;
        loop {
            let rendered = format!("{current} {current:?}");
            assert!(!rendered.contains("SECRET"));
            match current.source() {
                Some(source) => current = source,
                None => break,
            }
        }
        let json = format!(r#"{{"rpc_uri":"{sentinel}"}}"#);
        let error = serde_json::from_str::<CaptureNode>(&json).expect_err("serde credentials reject");
        assert!(!format!("{error} {error:?}").contains("SECRET"));
        let json = format!(r#"{{"{sentinel}":0}}"#);
        let error = serde_json::from_str::<CaptureNode>(&json).expect_err("unknown fields reject");
        assert!(!format!("{error} {error:?}").contains("SECRET"));
        let error = CaptureNode::new("http://[bad]:1".to_owned()).expect_err("parser rejection");
        assert!(error.source().is_some(), "retain URL parser source");
        let error = CaptureNode::decode(&record(&[0xff])).expect_err("UTF-8 rejection");
        assert!(error.source().is_some(), "retain UTF-8 source");
    }
}
