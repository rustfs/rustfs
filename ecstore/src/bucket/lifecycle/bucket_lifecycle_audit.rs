use super::lifecycle;

#[derive(Debug, Clone, Default)]
pub enum LcEventSrc {
    #[default]
    None,
    Heal,
    Scanner,
    Decom,
    Rebal,
    S3HeadObject,
    S3GetObject,
    S3ListObjects,
    S3PutObject,
    S3CopyObject,
    S3CompleteMultipartUpload,
}

#[derive(Clone, Debug, Default)]
pub struct LcAuditEvent {
    pub event: lifecycle::Event,
    pub source: LcEventSrc,
}

impl LcAuditEvent {
    pub fn new(event: lifecycle::Event, source: LcEventSrc) -> Self {
        Self { event, source }
    }
}
