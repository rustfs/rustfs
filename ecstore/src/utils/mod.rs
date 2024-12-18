use crate::bucket::error::BucketMetadataError;
use crate::config::error::ConfigError;
use crate::disk::error::DiskError;
use crate::error::Error;
use crate::quorum::QuorumError;
use crate::store_err::StorageError;
use crate::store_init::ErasureError;

pub mod bool_flag;
pub mod crypto;
pub mod ellipses;
pub mod fs;
pub mod hash;
pub mod net;
pub mod os;
pub mod path;
pub mod wildcard;
pub mod xml;

const DISK_ERROR_MASK: u32 = 0x00001000;
const STORAGE_ERROR_MASK: u8 = 0x20;
const BUCKET_METADATA_ERROR_MASK: u8 = 0x30;
const CONFIG_ERROR_MASK: u8 = 0x40;
const QUORUM_ERROR_MASK: u8 = 0x50;
const ERASURE_ERROR_MASK: u8 = 0x60;
// error to u8
pub fn error_to_u8(err: &Error) -> u8 {
    if let Some(e) = err.downcast_ref::<DiskError>() {
    } else if let Some(e) = err.downcast_ref::<StorageError>() {
    } else if let Some(e) = err.downcast_ref::<BucketMetadataError>() {
    } else if let Some(e) = err.downcast_ref::<ConfigError>() {
    } else if let Some(e) = err.downcast_ref::<QuorumError>() {
    } else if let Some(e) = err.downcast_ref::<ErasureError>() {
    }
    0
}
