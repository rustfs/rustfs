//! RustFS-specific OpenTelemetry attribute names and bounded values.
//!
//! These constants are for traces only. Do not reuse request identifiers,
//! bucket names, object keys, upload IDs, credentials, headers, or peer
//! addresses as metric labels.

/// S3 operation name, for example `put_object`.
pub const OPERATION: &str = "rustfs.operation";
/// Internal pipeline stage name.
pub const STAGE: &str = "rustfs.stage";
/// Direction of a streaming pipeline.
pub const STREAM_DIRECTION: &str = "rustfs.stream.direction";
/// Declared or resolved stream size in bytes.
pub const STREAM_EXPECTED_BYTES: &str = "rustfs.stream.expected_bytes";
/// Buffer capacity selected for the stream.
pub const STREAM_BUFFER_BYTES: &str = "rustfs.stream.buffer_bytes";
/// Number of erasure data shards.
pub const EC_DATA_SHARDS: &str = "rustfs.ec.data_shards";
/// Number of erasure parity shards.
pub const EC_PARITY_SHARDS: &str = "rustfs.ec.parity_shards";
/// Required successful erasure writes.
pub const EC_WRITE_QUORUM: &str = "rustfs.ec.write_quorum";
/// Erasure block size in bytes.
pub const EC_BLOCK_BYTES: &str = "rustfs.ec.block_bytes";
/// Number of shard targets receiving a distributed write.
pub const DISTRIBUTION_TARGETS: &str = "rustfs.distribution.targets";
/// Bounded transport category for distributed work.
pub const DISTRIBUTION_TRANSPORT: &str = "rustfs.distribution.transport";

/// Bounded stream direction values.
pub mod stream_direction {
    /// Client request body flowing into storage.
    pub const INGRESS: &str = "ingress";
    /// Source-object body flowing through UploadPartCopy.
    pub const COPY: &str = "copy";
}

/// Bounded distributed transport values.
pub mod distribution_transport {
    /// Erasure shard writer setup and dispatch.
    pub const ERASURE: &str = "erasure";
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn attribute_names_use_the_rustfs_namespace() {
        for name in [
            OPERATION,
            STAGE,
            STREAM_DIRECTION,
            STREAM_EXPECTED_BYTES,
            STREAM_BUFFER_BYTES,
            EC_DATA_SHARDS,
            EC_PARITY_SHARDS,
            EC_WRITE_QUORUM,
            EC_BLOCK_BYTES,
            DISTRIBUTION_TARGETS,
            DISTRIBUTION_TRANSPORT,
        ] {
            assert!(name.starts_with("rustfs."));
        }
    }
}
