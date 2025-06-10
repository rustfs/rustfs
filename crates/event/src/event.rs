use crate::Error;
use reqwest::dns::Name;
use serde::{Deserialize, Serialize};
use serde_with::{DeserializeFromStr, SerializeDisplay};
use smallvec::{smallvec, SmallVec};
use std::borrow::Cow;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};
use strum::{Display, EnumString};
use uuid::Uuid;

/// A struct representing the identity of the user
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Identity {
    #[serde(rename = "principalId")]
    pub principal_id: String,
}

impl Identity {
    /// Create a new Identity instance
    pub fn new(principal_id: String) -> Self {
        Self { principal_id }
    }

    /// Set the principal ID
    pub fn set_principal_id(&mut self, principal_id: String) {
        self.principal_id = principal_id;
    }
}

/// A struct representing the bucket information
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Bucket {
    pub name: String,
    #[serde(rename = "ownerIdentity")]
    pub owner_identity: Identity,
    pub arn: String,
}

impl Bucket {
    /// Create a new Bucket instance
    pub fn new(name: String, owner_identity: Identity, arn: String) -> Self {
        Self {
            name,
            owner_identity,
            arn,
        }
    }

    /// Set the name of the bucket
    pub fn set_name(&mut self, name: String) {
        self.name = name;
    }

    /// Set the ARN of the bucket
    pub fn set_arn(&mut self, arn: String) {
        self.arn = arn;
    }

    /// Set the owner identity of the bucket
    pub fn set_owner_identity(&mut self, owner_identity: Identity) {
        self.owner_identity = owner_identity;
    }
}

/// A struct representing the object information
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Object {
    pub key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none", rename = "eTag")]
    pub etag: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none", rename = "contentType")]
    pub content_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none", rename = "userMetadata")]
    pub user_metadata: Option<HashMap<String, String>>,
    #[serde(default, skip_serializing_if = "Option::is_none", rename = "versionId")]
    pub version_id: Option<String>,
    pub sequencer: String,
}

impl Object {
    /// Create a new Object instance
    pub fn new(
        key: String,
        size: Option<i64>,
        etag: Option<String>,
        content_type: Option<String>,
        user_metadata: Option<HashMap<String, String>>,
        version_id: Option<String>,
        sequencer: String,
    ) -> Self {
        Self {
            key,
            size,
            etag,
            content_type,
            user_metadata,
            version_id,
            sequencer,
        }
    }

    /// Set the key
    pub fn set_key(&mut self, key: String) {
        self.key = key;
    }

    /// Set the size
    pub fn set_size(&mut self, size: Option<i64>) {
        self.size = size;
    }

    /// Set the etag
    pub fn set_etag(&mut self, etag: Option<String>) {
        self.etag = etag;
    }

    /// Set the content type
    pub fn set_content_type(&mut self, content_type: Option<String>) {
        self.content_type = content_type;
    }

    /// Set the user metadata
    pub fn set_user_metadata(&mut self, user_metadata: Option<HashMap<String, String>>) {
        self.user_metadata = user_metadata;
    }

    /// Set the version ID
    pub fn set_version_id(&mut self, version_id: Option<String>) {
        self.version_id = version_id;
    }

    /// Set the sequencer
    pub fn set_sequencer(&mut self, sequencer: String) {
        self.sequencer = sequencer;
    }
}

/// A struct representing the metadata of the event
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Metadata {
    #[serde(rename = "s3SchemaVersion")]
    pub schema_version: String,
    #[serde(rename = "configurationId")]
    pub configuration_id: String,
    pub bucket: Bucket,
    pub object: Object,
}

impl Default for Metadata {
    fn default() -> Self {
        Self::new()
    }
}
impl Metadata {
    /// Create a new Metadata instance with default values
    pub fn new() -> Self {
        Self {
            schema_version: "1.0".to_string(),
            configuration_id: "default".to_string(),
            bucket: Bucket::new(
                "default".to_string(),
                Identity::new("default".to_string()),
                "arn:aws:s3:::default".to_string(),
            ),
            object: Object::new("default".to_string(), None, None, None, None, None, "default".to_string()),
        }
    }

    /// Create a new Metadata instance
    pub fn create(schema_version: String, configuration_id: String, bucket: Bucket, object: Object) -> Self {
        Self {
            schema_version,
            configuration_id,
            bucket,
            object,
        }
    }

    /// Set the schema version
    pub fn set_schema_version(&mut self, schema_version: String) {
        self.schema_version = schema_version;
    }

    /// Set the configuration ID
    pub fn set_configuration_id(&mut self, configuration_id: String) {
        self.configuration_id = configuration_id;
    }

    /// Set the bucket
    pub fn set_bucket(&mut self, bucket: Bucket) {
        self.bucket = bucket;
    }

    /// Set the object
    pub fn set_object(&mut self, object: Object) {
        self.object = object;
    }
}

/// A struct representing the source of the event
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Source {
    pub host: String,
    pub port: String,
    #[serde(rename = "userAgent")]
    pub user_agent: String,
}

impl Source {
    /// Create a new Source instance
    pub fn new(host: String, port: String, user_agent: String) -> Self {
        Self { host, port, user_agent }
    }

    /// Set the host
    pub fn set_host(&mut self, host: String) {
        self.host = host;
    }

    /// Set the port
    pub fn set_port(&mut self, port: String) {
        self.port = port;
    }

    /// Set the user agent
    pub fn set_user_agent(&mut self, user_agent: String) {
        self.user_agent = user_agent;
    }
}

/// Builder for creating an Event.
///
/// This struct is used to build an Event object with various parameters.
/// It provides methods to set each parameter and a build method to create the Event.
#[derive(Default, Clone)]
pub struct EventBuilder {
    event_version: Option<String>,
    event_source: Option<String>,
    aws_region: Option<String>,
    event_time: Option<String>,
    event_name: Option<Name>,
    user_identity: Option<Identity>,
    request_parameters: Option<HashMap<String, String>>,
    response_elements: Option<HashMap<String, String>>,
    s3: Option<Metadata>,
    source: Option<Source>,
    channels: Option<SmallVec<[String; 2]>>,
}

impl EventBuilder {
    /// create a builder that pre filled default values
    pub fn new() -> Self {
        Self {
            event_version: Some(Cow::Borrowed("2.0").to_string()),
            event_source: Some(Cow::Borrowed("aws:s3").to_string()),
            aws_region: Some("us-east-1".to_string()),
            event_time: Some(SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs().to_string()),
            event_name: None,
            user_identity: Some(Identity {
                principal_id: "anonymous".to_string(),
            }),
            request_parameters: Some(HashMap::new()),
            response_elements: Some(HashMap::new()),
            s3: None,
            source: None,
            channels: Some(Vec::new().into()),
        }
    }

    /// verify and set the event version
    pub fn event_version(mut self, event_version: impl Into<String>) -> Self {
        let event_version = event_version.into();
        if !event_version.is_empty() {
            self.event_version = Some(event_version);
        }
        self
    }

    /// verify and set the event source
    pub fn event_source(mut self, event_source: impl Into<String>) -> Self {
        let event_source = event_source.into();
        if !event_source.is_empty() {
            self.event_source = Some(event_source);
        }
        self
    }

    /// set up aws regions
    pub fn aws_region(mut self, aws_region: impl Into<String>) -> Self {
        self.aws_region = Some(aws_region.into());
        self
    }

    /// set event time
    pub fn event_time(mut self, event_time: impl Into<String>) -> Self {
        self.event_time = Some(event_time.into());
        self
    }

    /// set event name
    pub fn event_name(mut self, event_name: Name) -> Self {
        self.event_name = Some(event_name);
        self
    }

    /// set user identity
    pub fn user_identity(mut self, user_identity: Identity) -> Self {
        self.user_identity = Some(user_identity);
        self
    }

    /// set request parameters
    pub fn request_parameters(mut self, request_parameters: HashMap<String, String>) -> Self {
        self.request_parameters = Some(request_parameters);
        self
    }

    /// set response elements
    pub fn response_elements(mut self, response_elements: HashMap<String, String>) -> Self {
        self.response_elements = Some(response_elements);
        self
    }

    /// setting up s3 metadata
    pub fn s3(mut self, s3: Metadata) -> Self {
        self.s3 = Some(s3);
        self
    }

    /// set event source information
    pub fn source(mut self, source: Source) -> Self {
        self.source = Some(source);
        self
    }

    /// set up the sending channel
    pub fn channels(mut self, channels: Vec<String>) -> Self {
        self.channels = Some(channels.into());
        self
    }

    /// Create a preconfigured builder for common object event scenarios
    pub fn for_object_creation(s3: Metadata, source: Source) -> Self {
        Self::new().event_name(Name::ObjectCreatedPut).s3(s3).source(source)
    }

    /// Create a preconfigured builder for object deletion events
    pub fn for_object_removal(s3: Metadata, source: Source) -> Self {
        Self::new().event_name(Name::ObjectRemovedDelete).s3(s3).source(source)
    }

    /// build event instance
    ///
    /// Verify the required fields and create a complete Event object
    pub fn build(self) -> Result<Event, Error> {
        let event_version = self.event_version.ok_or(Error::MissingField("event_version"))?;

        let event_source = self.event_source.ok_or(Error::MissingField("event_source"))?;

        let aws_region = self.aws_region.ok_or(Error::MissingField("aws_region"))?;

        let event_time = self.event_time.ok_or(Error::MissingField("event_time"))?;

        let event_name = self.event_name.ok_or(Error::MissingField("event_name"))?;

        let user_identity = self.user_identity.ok_or(Error::MissingField("user_identity"))?;

        let request_parameters = self.request_parameters.unwrap_or_default();
        let response_elements = self.response_elements.unwrap_or_default();

        let s3 = self.s3.ok_or(Error::MissingField("s3"))?;

        let source = self.source.ok_or(Error::MissingField("source"))?;

        let channels = self.channels.unwrap_or_else(|| smallvec![]);

        Ok(Event {
            event_version,
            event_source,
            aws_region,
            event_time,
            event_name,
            user_identity,
            request_parameters,
            response_elements,
            s3,
            source,
            id: Uuid::new_v4(),
            timestamp: SystemTime::now(),
            channels,
        })
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Event {
    #[serde(rename = "eventVersion")]
    pub event_version: String,
    #[serde(rename = "eventSource")]
    pub event_source: String,
    #[serde(rename = "awsRegion")]
    pub aws_region: String,
    #[serde(rename = "eventTime")]
    pub event_time: String,
    #[serde(rename = "eventName")]
    pub event_name: Name,
    #[serde(rename = "userIdentity")]
    pub user_identity: Identity,
    #[serde(rename = "requestParameters")]
    pub request_parameters: HashMap<String, String>,
    #[serde(rename = "responseElements")]
    pub response_elements: HashMap<String, String>,
    pub s3: Metadata,
    pub source: Source,
    pub id: Uuid,
    pub timestamp: SystemTime,
    pub channels: SmallVec<[String; 2]>,
}

impl Event {
    /// create a new event builder
    ///
    /// Returns an EventBuilder instance pre-filled with default values
    pub fn builder() -> EventBuilder {
        EventBuilder::new()
    }

    /// Quickly create Event instances with necessary fields
    ///
    /// suitable for common s3 event scenarios
    pub fn create(event_name: Name, s3: Metadata, source: Source, channels: Vec<String>) -> Self {
        Self::builder()
            .event_name(event_name)
            .s3(s3)
            .source(source)
            .channels(channels)
            .build()
            .expect("Failed to create event, missing necessary parameters")
    }

    /// a convenient way to create a preconfigured builder
    pub fn for_object_creation(s3: Metadata, source: Source) -> EventBuilder {
        EventBuilder::for_object_creation(s3, source)
    }

    /// a convenient way to create a preconfigured builder
    pub fn for_object_removal(s3: Metadata, source: Source) -> EventBuilder {
        EventBuilder::for_object_removal(s3, source)
    }

    /// Determine whether an event belongs to a specific type
    pub fn is_type(&self, event_type: Name) -> bool {
        let mask = event_type.mask();
        (self.event_name.mask() & mask) != 0
    }

    /// Determine whether an event needs to be sent to a specific channel
    pub fn is_for_channel(&self, channel: &str) -> bool {
        self.channels.iter().any(|c| c == channel)
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Log {
    #[serde(rename = "eventName")]
    pub event_name: Name,
    pub key: String,
    pub records: Vec<Event>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, SerializeDisplay, DeserializeFromStr, Display, EnumString)]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
pub enum Name {
    ObjectAccessedGet,
    ObjectAccessedGetRetention,
    ObjectAccessedGetLegalHold,
    ObjectAccessedHead,
    ObjectAccessedAttributes,
    ObjectCreatedCompleteMultipartUpload,
    ObjectCreatedCopy,
    ObjectCreatedPost,
    ObjectCreatedPut,
    ObjectCreatedPutRetention,
    ObjectCreatedPutLegalHold,
    ObjectCreatedPutTagging,
    ObjectCreatedDeleteTagging,
    ObjectRemovedDelete,
    ObjectRemovedDeleteMarkerCreated,
    ObjectRemovedDeleteAllVersions,
    ObjectRemovedNoOp,
    BucketCreated,
    BucketRemoved,
    ObjectReplicationFailed,
    ObjectReplicationComplete,
    ObjectReplicationMissedThreshold,
    ObjectReplicationReplicatedAfterThreshold,
    ObjectReplicationNotTracked,
    ObjectRestorePost,
    ObjectRestoreCompleted,
    ObjectTransitionFailed,
    ObjectTransitionComplete,
    ObjectManyVersions,
    ObjectLargeVersions,
    PrefixManyFolders,
    IlmDelMarkerExpirationDelete,
    ObjectAccessedAll,
    ObjectCreatedAll,
    ObjectRemovedAll,
    ObjectReplicationAll,
    ObjectRestoreAll,
    ObjectTransitionAll,
    ObjectScannerAll,
    Everything,
}

impl Name {
    pub fn expand(&self) -> Vec<Name> {
        match self {
            Name::ObjectAccessedAll => vec![
                Name::ObjectAccessedGet,
                Name::ObjectAccessedHead,
                Name::ObjectAccessedGetRetention,
                Name::ObjectAccessedGetLegalHold,
                Name::ObjectAccessedAttributes,
            ],
            Name::ObjectCreatedAll => vec![
                Name::ObjectCreatedCompleteMultipartUpload,
                Name::ObjectCreatedCopy,
                Name::ObjectCreatedPost,
                Name::ObjectCreatedPut,
                Name::ObjectCreatedPutRetention,
                Name::ObjectCreatedPutLegalHold,
                Name::ObjectCreatedPutTagging,
                Name::ObjectCreatedDeleteTagging,
            ],
            Name::ObjectRemovedAll => vec![
                Name::ObjectRemovedDelete,
                Name::ObjectRemovedDeleteMarkerCreated,
                Name::ObjectRemovedNoOp,
                Name::ObjectRemovedDeleteAllVersions,
            ],
            Name::ObjectReplicationAll => vec![
                Name::ObjectReplicationFailed,
                Name::ObjectReplicationComplete,
                Name::ObjectReplicationNotTracked,
                Name::ObjectReplicationMissedThreshold,
                Name::ObjectReplicationReplicatedAfterThreshold,
            ],
            Name::ObjectRestoreAll => vec![Name::ObjectRestorePost, Name::ObjectRestoreCompleted],
            Name::ObjectTransitionAll => {
                vec![Name::ObjectTransitionFailed, Name::ObjectTransitionComplete]
            }
            Name::ObjectScannerAll => vec![Name::ObjectManyVersions, Name::ObjectLargeVersions, Name::PrefixManyFolders],
            Name::Everything => (1..=Name::IlmDelMarkerExpirationDelete as u32)
                .map(|i| Name::from_repr(i).unwrap())
                .collect(),
            _ => vec![*self],
        }
    }

    pub fn mask(&self) -> u64 {
        if (*self as u32) < Name::ObjectAccessedAll as u32 {
            1 << (*self as u32 - 1)
        } else {
            self.expand().iter().fold(0, |acc, n| acc | (1 << (*n as u32 - 1)))
        }
    }

    fn from_repr(discriminant: u32) -> Option<Self> {
        match discriminant {
            1 => Some(Name::ObjectAccessedGet),
            2 => Some(Name::ObjectAccessedGetRetention),
            3 => Some(Name::ObjectAccessedGetLegalHold),
            4 => Some(Name::ObjectAccessedHead),
            5 => Some(Name::ObjectAccessedAttributes),
            6 => Some(Name::ObjectCreatedCompleteMultipartUpload),
            7 => Some(Name::ObjectCreatedCopy),
            8 => Some(Name::ObjectCreatedPost),
            9 => Some(Name::ObjectCreatedPut),
            10 => Some(Name::ObjectCreatedPutRetention),
            11 => Some(Name::ObjectCreatedPutLegalHold),
            12 => Some(Name::ObjectCreatedPutTagging),
            13 => Some(Name::ObjectCreatedDeleteTagging),
            14 => Some(Name::ObjectRemovedDelete),
            15 => Some(Name::ObjectRemovedDeleteMarkerCreated),
            16 => Some(Name::ObjectRemovedDeleteAllVersions),
            17 => Some(Name::ObjectRemovedNoOp),
            18 => Some(Name::BucketCreated),
            19 => Some(Name::BucketRemoved),
            20 => Some(Name::ObjectReplicationFailed),
            21 => Some(Name::ObjectReplicationComplete),
            22 => Some(Name::ObjectReplicationMissedThreshold),
            23 => Some(Name::ObjectReplicationReplicatedAfterThreshold),
            24 => Some(Name::ObjectReplicationNotTracked),
            25 => Some(Name::ObjectRestorePost),
            26 => Some(Name::ObjectRestoreCompleted),
            27 => Some(Name::ObjectTransitionFailed),
            28 => Some(Name::ObjectTransitionComplete),
            29 => Some(Name::ObjectManyVersions),
            30 => Some(Name::ObjectLargeVersions),
            31 => Some(Name::PrefixManyFolders),
            32 => Some(Name::IlmDelMarkerExpirationDelete),
            33 => Some(Name::ObjectAccessedAll),
            34 => Some(Name::ObjectCreatedAll),
            35 => Some(Name::ObjectRemovedAll),
            36 => Some(Name::ObjectReplicationAll),
            37 => Some(Name::ObjectRestoreAll),
            38 => Some(Name::ObjectTransitionAll),
            39 => Some(Name::ObjectScannerAll),
            40 => Some(Name::Everything),
            _ => None,
        }
    }
}
