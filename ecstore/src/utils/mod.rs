pub mod bool_flag;
pub mod crypto;
pub mod ellipses;
pub mod fs;
pub mod hash;
pub mod net;
// pub mod os;
pub mod path;
pub mod wildcard;
pub mod xml;

// use crate::bucket::error::BucketMetadataError;
// use crate::disk::error::DiskError;
// use crate::error::StorageError;
// use protos::proto_gen::node_service::Error as Proto_Error;

// const ERROR_MODULE_MASK: u32 = 0xFF00;
// pub const ERROR_TYPE_MASK: u32 = 0x00FF;
// const DISK_ERROR_MASK: u32 = 0x0100;
// const STORAGE_ERROR_MASK: u32 = 0x0200;
// const BUCKET_METADATA_ERROR_MASK: u32 = 0x0300;
// const CONFIG_ERROR_MASK: u32 = 0x04000;
// const QUORUM_ERROR_MASK: u32 = 0x0500;
// const ERASURE_ERROR_MASK: u32 = 0x0600;

// // error to u8
// pub fn error_to_u32(err: &Error) -> u32 {
//     if let Some(e) = err.downcast_ref::<DiskError>() {
//         DISK_ERROR_MASK | e.to_u32()
//     } else if let Some(e) = err.downcast_ref::<StorageError>() {
//         STORAGE_ERROR_MASK | e.to_u32()
//     } else if let Some(e) = err.downcast_ref::<BucketMetadataError>() {
//         BUCKET_METADATA_ERROR_MASK | e.to_u32()
//     } else if let Some(e) = err.downcast_ref::<ConfigError>() {
//         CONFIG_ERROR_MASK | e.to_u32()
//     } else if let Some(e) = err.downcast_ref::<QuorumError>() {
//         QUORUM_ERROR_MASK | e.to_u32()
//     } else if let Some(e) = err.downcast_ref::<ErasureError>() {
//         ERASURE_ERROR_MASK | e.to_u32()
//     } else {
//         0
//     }
// }

// pub fn u32_to_error(e: u32) -> Option<Error> {
//     match e & ERROR_MODULE_MASK {
//         DISK_ERROR_MASK => DiskError::from_u32(e & ERROR_TYPE_MASK).map(|e| Error::new(e)),
//         STORAGE_ERROR_MASK => StorageError::from_u32(e & ERROR_TYPE_MASK).map(|e| Error::new(e)),
//         BUCKET_METADATA_ERROR_MASK => BucketMetadataError::from_u32(e & ERROR_TYPE_MASK).map(|e| Error::new(e)),
//         CONFIG_ERROR_MASK => ConfigError::from_u32(e & ERROR_TYPE_MASK).map(|e| Error::new(e)),
//         QUORUM_ERROR_MASK => QuorumError::from_u32(e & ERROR_TYPE_MASK).map(|e| Error::new(e)),
//         ERASURE_ERROR_MASK => ErasureError::from_u32(e & ERROR_TYPE_MASK).map(|e| Error::new(e)),
//         _ => None,
//     }
// }

// pub fn err_to_proto_err(err: &Error, msg: &str) -> Proto_Error {
//     let num = error_to_u32(err);
//     Proto_Error {
//         code: num,
//         error_info: msg.to_string(),
//     }
// }

// pub fn proto_err_to_err(err: &Proto_Error) -> Error {
//     if let Some(e) = u32_to_error(err.code) {
//         e
//     } else {
//         Error::from_string(err.error_info.clone())
//     }
// }

// #[test]
// fn test_u32_to_error() {
//     let error = Error::new(DiskError::FileCorrupt);
//     let num = error_to_u32(&error);
//     let new_error = u32_to_error(num);
//     assert!(new_error.is_some());
//     assert_eq!(new_error.unwrap().downcast_ref::<DiskError>(), Some(&DiskError::FileCorrupt));

//     let error = Error::new(StorageError::BucketNotEmpty(Default::default()));
//     let num = error_to_u32(&error);
//     let new_error = u32_to_error(num);
//     assert!(new_error.is_some());
//     assert_eq!(
//         new_error.unwrap().downcast_ref::<StorageError>(),
//         Some(&StorageError::BucketNotEmpty(Default::default()))
//     );

//     let error = Error::new(BucketMetadataError::BucketObjectLockConfigNotFound);
//     let num = error_to_u32(&error);
//     let new_error = u32_to_error(num);
//     assert!(new_error.is_some());
//     assert_eq!(
//         new_error.unwrap().downcast_ref::<BucketMetadataError>(),
//         Some(&BucketMetadataError::BucketObjectLockConfigNotFound)
//     );

//     let error = Error::new(ConfigError::NotFound);
//     let num = error_to_u32(&error);
//     let new_error = u32_to_error(num);
//     assert!(new_error.is_some());
//     assert_eq!(new_error.unwrap().downcast_ref::<ConfigError>(), Some(&ConfigError::NotFound));

//     let error = Error::new(QuorumError::Read);
//     let num = error_to_u32(&error);
//     let new_error = u32_to_error(num);
//     assert!(new_error.is_some());
//     assert_eq!(new_error.unwrap().downcast_ref::<QuorumError>(), Some(&QuorumError::Read));

//     let error = Error::new(ErasureError::ErasureReadQuorum);
//     let num = error_to_u32(&error);
//     let new_error = u32_to_error(num);
//     assert!(new_error.is_some());
//     assert_eq!(new_error.unwrap().downcast_ref::<ErasureError>(), Some(&ErasureError::ErasureReadQuorum));
// }
