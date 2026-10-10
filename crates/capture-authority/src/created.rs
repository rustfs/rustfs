use crate::{LogId, StoreError, decode_id, encode_id};

pub const MAX_OBJECT_KEY_LENGTH: usize = 4096;
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectIdentity {
    pub bucket_incarnation: [u8; 16],
    pub key: Vec<u8>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedIdentity {
    pub version_id: [u8; 16],
    pub preparation_id: [u8; 16],
    pub content_digest: [u8; 32],
    pub content_length: u64,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecideCreated {
    pub operation_id: [u8; 16],
    pub object: ObjectIdentity,
    pub expected_head: Option<[u8; 16]>,
    pub prepared: PreparedIdentity,
    pub binding_revision: u64,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CaptureBinding {
    pub revision: u64,
    pub binding_id: [u8; 16],
    pub target_id: [u8; 16],
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CreatedResult {
    Created {
        version_id: [u8; 16],
        event_id: [u8; 16],
        decision_log_id: LogId,
    },
    HeadMismatch {
        actual_head: Option<[u8; 16]>,
    },
    OperationConflict,
    BindingMismatch,
    VersionConflict,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreatedVersion {
    pub prepared: PreparedIdentity,
    pub decision_log_id: LogId,
    pub binding_revision: u64,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreatedEvent {
    pub object: ObjectIdentity,
    pub prepared: PreparedIdentity,
    pub decision_log_id: LogId,
    pub binding: CaptureBinding,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreatedOperation {
    pub canonical_command: Vec<u8>,
    pub result: CreatedResult,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreatedApplyResult {
    pub log_id: LogId,
    pub result: CreatedResult,
}

pub(crate) struct Decoder<'a> {
    bytes: &'a [u8],
}
impl<'a> Decoder<'a> {
    pub(crate) fn new(bytes: &'a [u8]) -> Self {
        Self { bytes }
    }
    pub(crate) fn take<const N: usize>(&mut self) -> Result<[u8; N], StoreError> {
        let a = self
            .bytes
            .get(..N)
            .ok_or(StoreError::CorruptRecord)?
            .try_into()
            .map_err(|_| StoreError::CorruptRecord)?;
        self.bytes = &self.bytes[N..];
        Ok(a)
    }
    pub(crate) fn u64(&mut self) -> Result<u64, StoreError> {
        Ok(u64::from_be_bytes(self.take()?))
    }
    pub(crate) fn uuid(&mut self) -> Result<[u8; 16], StoreError> {
        let id = self.take()?;
        if id == [0; 16] {
            return Err(StoreError::CorruptRecord);
        }
        Ok(id)
    }
    pub(crate) fn option(&mut self) -> Result<Option<[u8; 16]>, StoreError> {
        match self.take::<1>()?[0] {
            0 => Ok(None),
            1 => Ok(Some(self.uuid()?)),
            _ => Err(StoreError::CorruptRecord),
        }
    }
    pub(crate) fn object(&mut self) -> Result<ObjectIdentity, StoreError> {
        let bucket_incarnation = self.uuid()?;
        let len = usize::try_from(u32::from_be_bytes(self.take()?)).map_err(|_| StoreError::CorruptRecord)?;
        if len > MAX_OBJECT_KEY_LENGTH {
            return Err(StoreError::CorruptRecord);
        }
        let key = self.bytes.get(..len).ok_or(StoreError::CorruptRecord)?.to_vec();
        self.bytes = &self.bytes[len..];
        Ok(ObjectIdentity { bucket_incarnation, key })
    }
    pub(crate) fn prepared(&mut self) -> Result<PreparedIdentity, StoreError> {
        Ok(PreparedIdentity {
            version_id: self.uuid()?,
            preparation_id: self.uuid()?,
            content_digest: self.take()?,
            content_length: self.u64()?,
        })
    }
    pub(crate) fn id(&mut self) -> Result<LogId, StoreError> {
        decode_id(&self.take::<24>()?)
    }
    pub(crate) fn binding(&mut self) -> Result<CaptureBinding, StoreError> {
        Ok(CaptureBinding {
            revision: self.u64()?,
            binding_id: self.uuid()?,
            target_id: self.uuid()?,
        })
    }
    pub(crate) fn finish(self) -> Result<(), StoreError> {
        if self.bytes.is_empty() {
            Ok(())
        } else {
            Err(StoreError::CorruptRecord)
        }
    }
    pub(crate) fn result(&mut self) -> Result<CreatedResult, StoreError> {
        match self.take::<1>()?[0] {
            0 => Ok(CreatedResult::Created {
                version_id: self.uuid()?,
                event_id: self.uuid()?,
                decision_log_id: self.id()?,
            }),
            1 => Ok(CreatedResult::HeadMismatch {
                actual_head: self.option()?,
            }),
            2 => Ok(CreatedResult::OperationConflict),
            3 => Ok(CreatedResult::BindingMismatch),
            4 => Ok(CreatedResult::VersionConflict),
            _ => Err(StoreError::CorruptRecord),
        }
    }
}
pub(crate) fn put_option(b: &mut Vec<u8>, id: Option<[u8; 16]>) {
    b.push(u8::from(id.is_some()));
    if let Some(id) = id {
        b.extend(id)
    }
}
pub(crate) fn object_bytes(o: &ObjectIdentity) -> Result<Vec<u8>, StoreError> {
    if o.bucket_incarnation == [0; 16] || o.key.len() > MAX_OBJECT_KEY_LENGTH {
        return Err(StoreError::CorruptRecord);
    }
    let capacity = 20usize.checked_add(o.key.len()).ok_or(StoreError::CorruptRecord)?;
    let mut b = Vec::with_capacity(capacity);
    b.extend(o.bucket_incarnation);
    b.extend(
        u32::try_from(o.key.len())
            .map_err(|_| StoreError::CorruptRecord)?
            .to_be_bytes(),
    );
    b.extend(&o.key);
    Ok(b)
}
pub(crate) fn put_prepared(b: &mut Vec<u8>, p: &PreparedIdentity) {
    b.extend(p.version_id);
    b.extend(p.preparation_id);
    b.extend(p.content_digest);
    b.extend(p.content_length.to_be_bytes())
}
pub(crate) fn binding_bytes(v: &CaptureBinding) -> Result<Vec<u8>, StoreError> {
    let mut b = v.revision.to_be_bytes().to_vec();
    b.extend(v.binding_id);
    b.extend(v.target_id);
    let mut d = Decoder::new(&b);
    d.binding()?;
    d.finish()?;
    Ok(b)
}
pub(crate) fn result_bytes(r: &CreatedResult) -> Vec<u8> {
    let mut b = Vec::new();
    match r {
        CreatedResult::Created {
            version_id,
            event_id,
            decision_log_id,
        } => {
            b.push(0);
            b.extend(version_id);
            b.extend(event_id);
            b.extend(encode_id(*decision_log_id))
        }
        CreatedResult::HeadMismatch { actual_head } => {
            b.push(1);
            put_option(&mut b, *actual_head)
        }
        CreatedResult::OperationConflict => b.push(2),
        CreatedResult::BindingMismatch => b.push(3),
        CreatedResult::VersionConflict => b.push(4),
    }
    b
}
impl DecideCreated {
    pub fn encode(&self) -> Result<Vec<u8>, StoreError> {
        let object = object_bytes(&self.object)?;
        let capacity = object.len().checked_add(134).ok_or(StoreError::CorruptRecord)?;
        let mut b = Vec::with_capacity(capacity);
        b.extend(b"CRTD\x01");
        b.extend(self.operation_id);
        b.extend(object);
        put_option(&mut b, self.expected_head);
        put_prepared(&mut b, &self.prepared);
        b.extend(self.binding_revision.to_be_bytes());
        Self::decode(&b)?;
        Ok(b)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, StoreError> {
        let mut d = Decoder::new(bytes);
        if d.take::<5>()? != *b"CRTD\x01" {
            return Err(StoreError::CorruptRecord);
        }
        let c = Self {
            operation_id: d.uuid()?,
            object: d.object()?,
            expected_head: d.option()?,
            prepared: d.prepared()?,
            binding_revision: d.u64()?,
        };
        d.finish()?;
        Ok(c)
    }
}
