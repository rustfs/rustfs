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

//! Structural wire codec for heal-control commands.
//!
//! Decoding proves only that a payload has a supported, bounded shape. It does
//! not establish coordinator authority, freshness, or replay protection. The
//! decoded fields intentionally remain opaque until an execution layer adds
//! contextual lease and nonce validation.

use rmp_serde::Deserializer;
use rustfs_common::heal_channel::{
    HealAdmissionDropReason, HealAdmissionResult, HealChannelPriority, HealChannelRequest, HealChannelResponse,
    HealRequestSource, HealScanMode,
};
use serde::{Deserialize, Serialize, de::SeqAccess, de::Visitor};
use std::{fmt, io::Cursor, io::Write};

const ENVELOPE_VERSION: u8 = 1;
pub const ENVELOPE_MAX_SIZE: usize = 64 * 1024;
pub const RESULT_MAX_SIZE: usize = 16 * 1024 * 1024;
pub const NONCE_SIZE: usize = 16;
pub const MAX_LIFETIME_MS: i64 = 30_000;
const MAX_CLOCK_SKEW_MS: i64 = 5_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RequestMetadata {
    pub nonce: [u8; NONCE_SIZE],
    pub issued_at_unix_ms: i64,
    pub expires_at_unix_ms: i64,
    pub coordinator_epoch: u64,
}

impl RequestMetadata {
    pub const fn new(nonce: [u8; NONCE_SIZE], issued_at_unix_ms: i64, expires_at_unix_ms: i64, coordinator_epoch: u64) -> Self {
        Self {
            nonce,
            issued_at_unix_ms,
            expires_at_unix_ms,
            coordinator_epoch,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Priority {
    Low,
    Normal,
    High,
    Critical,
}

impl From<HealChannelPriority> for Priority {
    fn from(priority: HealChannelPriority) -> Self {
        match priority {
            HealChannelPriority::Low => Self::Low,
            HealChannelPriority::Normal => Self::Normal,
            HealChannelPriority::High => Self::High,
            HealChannelPriority::Critical => Self::Critical,
        }
    }
}

impl From<Priority> for HealChannelPriority {
    fn from(priority: Priority) -> Self {
        match priority {
            Priority::Low => Self::Low,
            Priority::Normal => Self::Normal,
            Priority::High => Self::High,
            Priority::Critical => Self::Critical,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct StartCommand {
    disk: Option<String>,
    bucket: String,
    object_prefix: Option<String>,
    object_version_id: Option<String>,
    force_start: bool,
    priority: Priority,
    pool_index: Option<u64>,
    set_index: Option<u64>,
    scan_mode: Option<HealScanMode>,
    remove_corrupted: Option<bool>,
    recreate_missing: Option<bool>,
    update_parity: Option<bool>,
    recursive: Option<bool>,
    dry_run: Option<bool>,
    timeout_seconds: Option<u64>,
    source: HealRequestSource,
}

impl TryFrom<HealChannelRequest> for StartCommand {
    type Error = String;

    fn try_from(request: HealChannelRequest) -> Result<Self, Self::Error> {
        Ok(Self {
            disk: request.disk,
            bucket: request.bucket,
            object_prefix: request.object_prefix,
            object_version_id: request.object_version_id,
            force_start: request.force_start,
            priority: request.priority.into(),
            pool_index: request
                .pool_index
                .map(u64::try_from)
                .transpose()
                .map_err(|_| "heal pool index exceeds wire range".to_string())?,
            set_index: request
                .set_index
                .map(u64::try_from)
                .transpose()
                .map_err(|_| "heal set index exceeds wire range".to_string())?,
            scan_mode: request.scan_mode,
            remove_corrupted: request.remove_corrupted,
            recreate_missing: request.recreate_missing,
            update_parity: request.update_parity,
            recursive: request.recursive,
            dry_run: request.dry_run,
            timeout_seconds: request.timeout_seconds,
            source: request.source,
        })
    }
}

impl StartCommand {
    fn into_channel_request(self, request_id: String) -> Result<HealChannelRequest, String> {
        Ok(HealChannelRequest {
            id: request_id,
            disk: self.disk,
            bucket: self.bucket,
            object_prefix: self.object_prefix,
            object_version_id: self.object_version_id,
            force_start: self.force_start,
            priority: self.priority.into(),
            pool_index: self
                .pool_index
                .map(usize::try_from)
                .transpose()
                .map_err(|_| "heal pool index exceeds platform range".to_string())?,
            set_index: self
                .set_index
                .map(usize::try_from)
                .transpose()
                .map_err(|_| "heal set index exceeds platform range".to_string())?,
            scan_mode: self.scan_mode,
            remove_corrupted: self.remove_corrupted,
            recreate_missing: self.recreate_missing,
            update_parity: self.update_parity,
            recursive: self.recursive,
            dry_run: self.dry_run,
            timeout_seconds: self.timeout_seconds,
            source: self.source,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum Command {
    Start { request: StartCommand },
    Query { heal_path: String, client_token: String },
    Cancel { heal_path: String, client_token: String },
}

#[derive(Debug)]
pub enum ExecutableCommand {
    Start { request: HealChannelRequest },
    Query { heal_path: String, client_token: String },
    Cancel { heal_path: String, client_token: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Envelope {
    version: u8,
    request_id: String,
    nonce: [u8; NONCE_SIZE],
    issued_at_unix_ms: i64,
    expires_at_unix_ms: i64,
    coordinator_epoch: u64,
    command: Command,
}

impl Envelope {
    fn new(request_id: String, metadata: RequestMetadata, command: Command) -> Result<Self, String> {
        let envelope = Self {
            version: ENVELOPE_VERSION,
            request_id,
            nonce: metadata.nonce,
            issued_at_unix_ms: metadata.issued_at_unix_ms,
            expires_at_unix_ms: metadata.expires_at_unix_ms,
            coordinator_epoch: metadata.coordinator_epoch,
            command,
        };
        envelope.validate()?;
        Ok(envelope)
    }

    pub fn start(request: HealChannelRequest, metadata: RequestMetadata) -> Result<Self, String> {
        let request_id = request.id.clone();
        Self::new(
            request_id,
            metadata,
            Command::Start {
                request: request.try_into()?,
            },
        )
    }

    pub fn query(request_id: String, metadata: RequestMetadata, heal_path: String, client_token: String) -> Result<Self, String> {
        Self::new(request_id, metadata, Command::Query { heal_path, client_token })
    }

    pub fn cancel(
        request_id: String,
        metadata: RequestMetadata,
        heal_path: String,
        client_token: String,
    ) -> Result<Self, String> {
        Self::new(request_id, metadata, Command::Cancel { heal_path, client_token })
    }

    fn validate(&self) -> Result<(), String> {
        if self.version != ENVELOPE_VERSION {
            return Err(format!("unsupported heal control envelope version: {}", self.version));
        }
        validate_uuid(&self.request_id, "request")?;
        if self.coordinator_epoch == 0 {
            return Err("heal control coordinator epoch is zero".to_string());
        }
        if self.nonce.iter().all(|byte| *byte == 0) {
            return Err("heal control nonce is zero".to_string());
        }
        if self.issued_at_unix_ms < 0 || self.expires_at_unix_ms <= self.issued_at_unix_ms {
            return Err("heal control request lifetime is invalid".to_string());
        }
        if self.expires_at_unix_ms - self.issued_at_unix_ms > MAX_LIFETIME_MS {
            return Err("heal control request lifetime exceeds limit".to_string());
        }
        match &self.command {
            Command::Start { .. } => {}
            Command::Query { client_token, .. } if client_token.is_empty() => {
                return Err("heal control client token is empty".to_string());
            }
            Command::Query { .. } | Command::Cancel { .. } => {}
        }
        Ok(())
    }

    pub fn validate_execution(&self, now_unix_ms: i64, expected_coordinator_epoch: u64) -> Result<(), String> {
        self.validate()?;
        if self.coordinator_epoch != expected_coordinator_epoch {
            return Err("heal control coordinator epoch does not match".to_string());
        }
        if self.issued_at_unix_ms > now_unix_ms.saturating_add(MAX_CLOCK_SKEW_MS) {
            return Err("heal control request was issued in the future".to_string());
        }
        if self.expires_at_unix_ms <= now_unix_ms {
            return Err("heal control request expired".to_string());
        }
        Ok(())
    }

    pub fn into_execution(self) -> Result<(String, u64, ExecutableCommand), String> {
        let command = match self.command {
            Command::Start { request } => ExecutableCommand::Start {
                request: request.into_channel_request(self.request_id.clone())?,
            },
            Command::Query { heal_path, client_token } => ExecutableCommand::Query { heal_path, client_token },
            Command::Cancel { heal_path, client_token } => ExecutableCommand::Cancel { heal_path, client_token },
        };
        Ok((self.request_id, self.coordinator_epoch, command))
    }

    pub const fn expires_at_unix_ms(&self) -> i64 {
        self.expires_at_unix_ms
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Admission {
    Accepted,
    Merged,
    Full,
    DroppedQueueFull,
    DroppedPolicy,
}

impl From<HealAdmissionResult> for Admission {
    fn from(result: HealAdmissionResult) -> Self {
        match result {
            HealAdmissionResult::Accepted => Self::Accepted,
            HealAdmissionResult::Merged => Self::Merged,
            HealAdmissionResult::Full => Self::Full,
            HealAdmissionResult::Dropped(HealAdmissionDropReason::QueueFull) => Self::DroppedQueueFull,
            HealAdmissionResult::Dropped(HealAdmissionDropReason::PolicyDropped) => Self::DroppedPolicy,
        }
    }
}

impl Admission {
    pub const fn is_admitted(self) -> bool {
        matches!(self, Self::Accepted | Self::Merged)
    }

    pub const fn into_heal_admission_result(self) -> HealAdmissionResult {
        match self {
            Self::Accepted => HealAdmissionResult::Accepted,
            Self::Merged => HealAdmissionResult::Merged,
            Self::Full => HealAdmissionResult::Full,
            Self::DroppedQueueFull => HealAdmissionResult::Dropped(HealAdmissionDropReason::QueueFull),
            Self::DroppedPolicy => HealAdmissionResult::Dropped(HealAdmissionDropReason::PolicyDropped),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "result", rename_all = "snake_case", deny_unknown_fields)]
pub enum Outcome {
    Start {
        task_id: String,
        admission: Admission,
    },
    Channel {
        success: bool,
        #[serde(deserialize_with = "deserialize_bounded_optional_bytes")]
        data: Option<Vec<u8>>,
        error: Option<String>,
    },
}

#[derive(Debug)]
struct BoundedBytes(Vec<u8>);
struct BoundedBytesVisitor;

impl<'de> Visitor<'de> for BoundedBytesVisitor {
    type Value = BoundedBytes;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "at most {RESULT_MAX_SIZE} bytes")
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        if sequence.size_hint().is_some_and(|length| length > RESULT_MAX_SIZE) {
            return Err(serde::de::Error::custom("heal control response data exceeds size limit"));
        }

        let mut bytes = Vec::new();
        while let Some(byte) = sequence.next_element()? {
            if bytes.len() == RESULT_MAX_SIZE {
                return Err(serde::de::Error::custom("heal control response data exceeds size limit"));
            }
            bytes.push(byte);
        }
        Ok(BoundedBytes(bytes))
    }
}

impl<'de> Deserialize<'de> for BoundedBytes {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_seq(BoundedBytesVisitor)
    }
}

fn deserialize_bounded_optional_bytes<'de, D>(deserializer: D) -> Result<Option<Vec<u8>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Ok(Option::<BoundedBytes>::deserialize(deserializer)?.map(|bytes| bytes.0))
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ResultEnvelope {
    version: u8,
    request_id: String,
    coordinator_epoch: u64,
    outcome: Outcome,
}

impl ResultEnvelope {
    pub fn new(request_id: String, coordinator_epoch: u64, outcome: Outcome) -> Result<Self, String> {
        let result = Self {
            version: ENVELOPE_VERSION,
            request_id,
            coordinator_epoch,
            outcome,
        };
        result.validate()?;
        Ok(result)
    }

    pub fn channel(request_id: String, coordinator_epoch: u64, response: HealChannelResponse) -> Result<Self, String> {
        if response.request_id != request_id {
            return Err("heal control channel response request ID does not match".to_string());
        }
        Self::new(
            request_id,
            coordinator_epoch,
            Outcome::Channel {
                success: response.success,
                data: response.data,
                error: response.error,
            },
        )
    }

    fn validate(&self) -> Result<(), String> {
        if self.version != ENVELOPE_VERSION {
            return Err(format!("unsupported heal control result version: {}", self.version));
        }
        if self.coordinator_epoch == 0 {
            return Err("heal control result coordinator epoch is zero".to_string());
        }
        validate_uuid(&self.request_id, "result request")?;
        match &self.outcome {
            Outcome::Start { task_id, .. } => validate_uuid(task_id, "result task")?,
            Outcome::Channel {
                success: false,
                error: None,
                ..
            } => {
                return Err("failed heal control result has no error".to_string());
            }
            Outcome::Channel {
                success: false,
                data: Some(_),
                ..
            } => {
                return Err("failed heal control result contains data".to_string());
            }
            Outcome::Channel {
                success: false,
                error: Some(error),
                ..
            } if error.is_empty() => {
                return Err("failed heal control result has an empty error".to_string());
            }
            Outcome::Channel { .. } => {}
        }
        Ok(())
    }

    pub fn into_outcome(self, expected_request_id: &str, expected_epoch: u64) -> Result<Outcome, String> {
        self.validate()?;
        if self.request_id != expected_request_id {
            return Err("heal control result request ID does not match".to_string());
        }
        if self.coordinator_epoch != expected_epoch {
            return Err("heal control result coordinator epoch does not match".to_string());
        }
        Ok(self.outcome)
    }
}

fn validate_uuid(value: &str, field: &str) -> Result<(), String> {
    let parsed = uuid::Uuid::parse_str(value).map_err(|_| format!("heal control {field} ID is not a UUID"))?;
    if parsed.is_nil() || parsed.to_string() != value {
        return Err(format!("heal control {field} ID is not canonical"));
    }
    Ok(())
}

fn decode<T: for<'de> Deserialize<'de>>(data: &[u8], value_name: &str, max_size: usize) -> Result<T, String> {
    if data.len() > max_size {
        return Err(format!("{value_name} exceeds size limit"));
    }
    let mut deserializer = Deserializer::new(Cursor::new(data));
    let value = T::deserialize(&mut deserializer).map_err(|err| format!("failed to decode {value_name}: {err}"))?;
    if usize::try_from(deserializer.get_ref().position()).ok() != Some(data.len()) {
        return Err(format!("{value_name} contains trailing data"));
    }
    Ok(value)
}

pub fn encode_envelope(envelope: &Envelope) -> Result<Vec<u8>, String> {
    envelope.validate()?;
    encode_bounded(envelope, "heal control envelope", ENVELOPE_MAX_SIZE)
}

pub fn decode_envelope(data: &[u8]) -> Result<Envelope, String> {
    let envelope: Envelope = decode(data, "heal control envelope", ENVELOPE_MAX_SIZE)?;
    envelope.validate()?;
    Ok(envelope)
}

pub fn encode_result(result: &ResultEnvelope) -> Result<Vec<u8>, String> {
    result.validate()?;
    encode_bounded(result, "heal control result", RESULT_MAX_SIZE)
}

pub fn decode_result(data: &[u8]) -> Result<ResultEnvelope, String> {
    let result: ResultEnvelope = decode(data, "heal control result", RESULT_MAX_SIZE)?;
    result.validate()?;
    Ok(result)
}

fn encode_bounded(value: &impl Serialize, value_name: &str, max_size: usize) -> Result<Vec<u8>, String> {
    struct BoundedWriter {
        bytes: Vec<u8>,
        exceeded: bool,
        max_size: usize,
    }

    impl Write for BoundedWriter {
        fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
            let remaining = self.max_size.saturating_sub(self.bytes.len());
            if data.len() > remaining {
                self.exceeded = true;
                return Err(std::io::Error::other("heal control value exceeds size limit"));
            }
            self.bytes.extend_from_slice(data);
            Ok(data.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    let mut writer = BoundedWriter {
        bytes: Vec::with_capacity(1024),
        exceeded: false,
        max_size,
    };
    let result = value.serialize(&mut rmp_serde::Serializer::new(&mut writer).with_struct_map());
    if writer.exceeded {
        return Err(format!("{value_name} exceeds size limit"));
    }
    result.map_err(|err| format!("failed to encode {value_name}: {err}"))?;
    Ok(writer.bytes)
}

#[cfg(test)]
mod tests {
    use super::{
        Admission, ENVELOPE_MAX_SIZE, Envelope, Outcome, RESULT_MAX_SIZE, RequestMetadata, ResultEnvelope, decode_envelope,
        decode_result, encode_result,
    };
    use rustfs_common::heal_channel::{HealChannelRequest, HealChannelResponse, HealRequestSource};
    use serde::de::{DeserializeSeed, SeqAccess, Visitor, value::Error as ValueError};

    fn test_request(request_id: String) -> HealChannelRequest {
        HealChannelRequest {
            id: request_id,
            bucket: "bucket".to_string(),
            object_prefix: Some("prefix".to_string()),
            pool_index: Some(1),
            set_index: Some(2),
            source: HealRequestSource::Admin,
            ..Default::default()
        }
    }

    fn metadata(byte: u8, epoch: u64) -> RequestMetadata {
        RequestMetadata::new([byte; 16], 1_000, 2_000, epoch)
    }

    #[test]
    fn round_trips_all_commands_and_results() {
        let request_id = uuid::Uuid::new_v4().to_string();
        let start = Envelope::start(test_request(request_id), metadata(1, 7)).unwrap();
        let query = Envelope::query(
            uuid::Uuid::new_v4().to_string(),
            metadata(2, 7),
            "bucket/prefix".to_string(),
            "token".to_string(),
        )
        .unwrap();
        let cancel = Envelope::cancel(
            uuid::Uuid::new_v4().to_string(),
            metadata(3, 7),
            "bucket/prefix".to_string(),
            "token".to_string(),
        )
        .unwrap();

        for envelope in [start, query, cancel] {
            let encoded = super::encode_envelope(&envelope).unwrap();
            assert_eq!(decode_envelope(&encoded).unwrap(), envelope);
        }

        let result = ResultEnvelope::new(
            uuid::Uuid::new_v4().to_string(),
            7,
            Outcome::Start {
                task_id: uuid::Uuid::new_v4().to_string(),
                admission: Admission::Merged,
            },
        )
        .unwrap();
        let encoded = super::encode_result(&result).unwrap();
        assert_eq!(decode_result(&encoded).unwrap(), result);

        let channel_request_id = uuid::Uuid::new_v4().to_string();
        let channel = ResultEnvelope::channel(
            channel_request_id.clone(),
            7,
            HealChannelResponse {
                request_id: channel_request_id,
                success: true,
                data: Some(vec![1, 2, 3]),
                error: None,
            },
        )
        .unwrap();
        let encoded = super::encode_result(&channel).unwrap();
        assert_eq!(decode_result(&encoded).unwrap(), channel);
    }

    #[test]
    fn v1_query_and_result_wire_fixtures_are_stable() {
        let request_id = "00112233-4455-6677-8899-aabbccddeeff".to_string();
        let start = Envelope::start(
            test_request(request_id.clone()),
            RequestMetadata::new([0x11; 16], 1_700_000_000_000, 1_700_000_030_000, 9),
        )
        .unwrap();
        let envelope = Envelope::query(
            request_id.clone(),
            RequestMetadata::new([0x11; 16], 1_700_000_000_000, 1_700_000_030_000, 9),
            "bucket/prefix".to_string(),
            "client-token".to_string(),
        )
        .unwrap();
        let cancel = Envelope::cancel(
            request_id.clone(),
            RequestMetadata::new([0x11; 16], 1_700_000_000_000, 1_700_000_030_000, 9),
            "bucket/prefix".to_string(),
            "client-token".to_string(),
        )
        .unwrap();
        let start_result = ResultEnvelope::new(
            request_id.clone(),
            9,
            Outcome::Start {
                task_id: "ffeeddcc-bbaa-9988-7766-554433221100".to_string(),
                admission: Admission::DroppedPolicy,
            },
        )
        .unwrap();
        let result = ResultEnvelope::new(
            request_id,
            9,
            Outcome::Channel {
                success: true,
                data: Some(vec![1, 2, 3]),
                error: None,
            },
        )
        .unwrap();

        let envelope_hex = super::encode_envelope(&envelope)
            .unwrap()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        let result_hex = super::encode_result(&result)
            .unwrap()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        let start_hex = super::encode_envelope(&start)
            .unwrap()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        let cancel_hex = super::encode_envelope(&cancel)
            .unwrap()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        let start_result_hex = super::encode_result(&start_result)
            .unwrap()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        assert_eq!(
            start_hex,
            "87a776657273696f6e01a9726571756573744964d92430303131323233332d343435352d363637372d383839392d616162626363646465656666a56e6f6e6365dc001011111111111111111111111111111111ae6973737565644174556e69784d73cf0000018bcfe56800af657870697265734174556e69784d73cf0000018bcfe5dd30b0636f6f7264696e61746f7245706f636809a7636f6d6d616e6482a6616374696f6ea57374617274a772657175657374de0010a46469736bc0a66275636b6574a66275636b6574ac6f626a656374507265666978a6707265666978af6f626a65637456657273696f6e4964c0aa666f7263655374617274c2a87072696f72697479a66e6f726d616ca9706f6f6c496e64657801a8736574496e64657802a87363616e4d6f6465c0af72656d6f7665436f72727570746564c0af72656372656174654d697373696e67c0ac757064617465506172697479c0a9726563757273697665c0a664727952756ec0ae74696d656f75745365636f6e6473c0a6736f75726365a561646d696e"
        );
        assert_eq!(
            cancel_hex,
            "87a776657273696f6e01a9726571756573744964d92430303131323233332d343435352d363637372d383839392d616162626363646465656666a56e6f6e6365dc001011111111111111111111111111111111ae6973737565644174556e69784d73cf0000018bcfe56800af657870697265734174556e69784d73cf0000018bcfe5dd30b0636f6f7264696e61746f7245706f636809a7636f6d6d616e6483a6616374696f6ea663616e63656ca96865616c5f70617468ad6275636b65742f707265666978ac636c69656e745f746f6b656eac636c69656e742d746f6b656e"
        );
        assert_eq!(
            start_result_hex,
            "84a776657273696f6e01a9726571756573744964d92430303131323233332d343435352d363637372d383839392d616162626363646465656666b0636f6f7264696e61746f7245706f636809a76f7574636f6d6583a6726573756c74a57374617274a77461736b5f6964d92466666565646463632d626261612d393938382d373736362d353534343333323231313030a961646d697373696f6eae64726f707065645f706f6c696379"
        );
        assert_eq!(
            envelope_hex,
            "87a776657273696f6e01a9726571756573744964d92430303131323233332d343435352d363637372d383839392d616162626363646465656666a56e6f6e6365dc001011111111111111111111111111111111ae6973737565644174556e69784d73cf0000018bcfe56800af657870697265734174556e69784d73cf0000018bcfe5dd30b0636f6f7264696e61746f7245706f636809a7636f6d6d616e6483a6616374696f6ea57175657279a96865616c5f70617468ad6275636b65742f707265666978ac636c69656e745f746f6b656eac636c69656e742d746f6b656e"
        );
        assert_eq!(
            result_hex,
            "84a776657273696f6e01a9726571756573744964d92430303131323233332d343435352d363637372d383839392d616162626363646465656666b0636f6f7264696e61746f7245706f636809a76f7574636f6d6584a6726573756c74a76368616e6e656ca773756363657373c3a46461746193010203a56572726f72c0"
        );
    }

    #[test]
    fn rejects_invalid_and_untrusted_shapes() {
        let request_id = uuid::Uuid::new_v4().to_string();
        assert!(Envelope::start(test_request(request_id.clone()), metadata(0, 7)).is_err());
        assert!(Envelope::start(test_request(request_id.clone()), metadata(1, 0)).is_err());
        assert!(Envelope::start(test_request(request_id.clone()), RequestMetadata::new([1; 16], 1_000, 31_001, 7),).is_err());
        assert!(Envelope::query(request_id.clone(), metadata(1, 7), String::new(), String::new()).is_err());
        assert!(Envelope::cancel(request_id.clone(), metadata(1, 7), String::new(), String::new()).is_ok());

        let mut noncanonical_request = test_request(request_id.to_uppercase());
        assert!(Envelope::start(noncanonical_request.clone(), metadata(1, 7)).is_err());
        noncanonical_request.id = uuid::Uuid::nil().to_string();
        assert!(Envelope::start(noncanonical_request, metadata(1, 7)).is_err());

        let boundary = RequestMetadata::new([1; 16], 1_000, 1_000 + super::MAX_LIFETIME_MS, 7);
        assert!(Envelope::start(test_request(request_id.clone()), boundary).is_ok());
        assert!(
            Envelope::start(
                test_request(request_id.clone()),
                RequestMetadata::new([1; 16], 1_000, 1_001 + super::MAX_LIFETIME_MS, 7),
            )
            .is_err()
        );

        let envelope = Envelope::start(test_request(request_id.clone()), metadata(1, 7)).unwrap();
        let mut trailing = super::encode_envelope(&envelope).unwrap();
        trailing.push(0);
        assert!(decode_envelope(&trailing).unwrap_err().contains("trailing data"));
        assert!(
            decode_envelope(&vec![0; ENVELOPE_MAX_SIZE + 1])
                .unwrap_err()
                .contains("size limit")
        );

        let oversized = Envelope::query(
            uuid::Uuid::new_v4().to_string(),
            metadata(1, 7),
            "x".repeat(ENVELOPE_MAX_SIZE),
            "token".to_string(),
        )
        .unwrap();
        let error = super::encode_envelope(&oversized).unwrap_err();
        assert!(error.contains("size limit"), "{error}");

        let mut unknown = serde_json::to_value(&envelope).unwrap();
        unknown
            .as_object_mut()
            .unwrap()
            .insert("unknown".to_string(), serde_json::Value::Bool(true));
        let unknown = rmp_serde::to_vec_named(&unknown).unwrap();
        assert!(decode_envelope(&unknown).unwrap_err().contains("unknown field"));

        let executable =
            Envelope::start(test_request(request_id.clone()), RequestMetadata::new([1; 16], 10_000, 20_000, 7)).unwrap();
        assert!(executable.validate_execution(15_000, 7).is_ok());
        assert!(executable.validate_execution(20_000, 7).unwrap_err().contains("expired"));
        assert!(executable.validate_execution(4_999, 7).unwrap_err().contains("future"));
        let wrong_epoch =
            Envelope::start(test_request(request_id.clone()), RequestMetadata::new([1; 16], 10_000, 20_000, 8)).unwrap();
        assert!(wrong_epoch.validate_execution(15_000, 7).unwrap_err().contains("epoch"));

        assert!(
            ResultEnvelope::channel(
                uuid::Uuid::new_v4().to_string(),
                7,
                HealChannelResponse {
                    request_id: uuid::Uuid::new_v4().to_string(),
                    success: true,
                    data: None,
                    error: None,
                },
            )
            .unwrap_err()
            .contains("does not match")
        );

        assert!(
            ResultEnvelope::new(
                request_id.clone(),
                7,
                Outcome::Channel {
                    success: true,
                    data: None,
                    error: Some("status detail".to_string()),
                },
            )
            .is_ok()
        );

        let large_result = ResultEnvelope::new(
            request_id.clone(),
            7,
            Outcome::Channel {
                success: true,
                data: Some(vec![7; ENVELOPE_MAX_SIZE + 1]),
                error: None,
            },
        )
        .unwrap();
        let large_result = encode_result(&large_result).expect("status results may exceed the request envelope limit");
        assert!(large_result.len() > ENVELOPE_MAX_SIZE);
        assert!(decode_result(&large_result).is_ok());
        assert!(
            ResultEnvelope::new(
                request_id.clone(),
                7,
                Outcome::Channel {
                    success: false,
                    data: None,
                    error: None,
                },
            )
            .unwrap_err()
            .contains("has no error")
        );
        assert!(
            ResultEnvelope::new(
                request_id.clone(),
                0,
                Outcome::Channel {
                    success: true,
                    data: None,
                    error: None,
                },
            )
            .unwrap_err()
            .contains("epoch is zero")
        );
        assert!(
            ResultEnvelope::new(
                "not-a-uuid".to_string(),
                7,
                Outcome::Channel {
                    success: true,
                    data: None,
                    error: None,
                },
            )
            .unwrap_err()
            .contains("not a UUID")
        );
        assert!(
            ResultEnvelope::new(
                request_id.clone(),
                7,
                Outcome::Start {
                    task_id: "not-a-uuid".to_string(),
                    admission: Admission::Accepted,
                },
            )
            .unwrap_err()
            .contains("not a UUID")
        );
        assert!(
            ResultEnvelope::new(
                request_id.clone(),
                7,
                Outcome::Channel {
                    success: false,
                    data: Some(vec![1]),
                    error: Some("failed".to_string()),
                },
            )
            .is_err()
        );
        assert!(
            ResultEnvelope::new(
                request_id.clone(),
                7,
                Outcome::Channel {
                    success: false,
                    data: None,
                    error: Some(String::new()),
                },
            )
            .is_err()
        );

        let valid_result = ResultEnvelope::new(
            request_id,
            7,
            Outcome::Channel {
                success: true,
                data: Some(vec![1]),
                error: None,
            },
        )
        .unwrap();
        let mut length_bomb = rmp_serde::to_vec_named(&valid_result).unwrap();
        let data_key = length_bomb
            .windows(6)
            .position(|window| window == b"\xa4data\x91")
            .expect("fixture must encode data as a one-element array");
        length_bomb.splice(data_key + 5..=data_key + 5, [0xdd, 0xff, 0xff, 0xff, 0xff]);
        assert!(decode_result(&length_bomb).is_err());

        struct OversizedHint;

        impl<'de> SeqAccess<'de> for OversizedHint {
            type Error = ValueError;

            fn next_element_seed<T>(&mut self, _seed: T) -> Result<Option<T::Value>, Self::Error>
            where
                T: DeserializeSeed<'de>,
            {
                Ok(None)
            }

            fn size_hint(&self) -> Option<usize> {
                Some(RESULT_MAX_SIZE + 1)
            }
        }

        let error = super::BoundedBytesVisitor.visit_seq(OversizedHint).unwrap_err();
        assert!(error.to_string().contains("size limit"));
    }
}
