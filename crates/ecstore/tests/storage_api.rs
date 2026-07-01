#![allow(unused_imports)]

pub(crate) use rustfs_ecstore::api::bitrot::create_bitrot_reader;
pub(crate) use rustfs_ecstore::api::disk::{DiskAPI, DiskOption, DiskStore, STORAGE_FORMAT_FILE, endpoint::Endpoint, new_disk};
pub(crate) use rustfs_ecstore::api::erasure::Erasure;
pub(crate) use rustfs_ecstore::api::object::{GetObjectReader, ObjectInfo, ObjectOptions, PutObjReader};
pub(crate) use rustfs_ecstore::api::{error::Error, set_disk::SetDisks, storage::ECStore};
use rustfs_storage_api as storage_contracts;

pub(crate) mod contract_compat {
    pub(crate) use super::storage_contracts::{
        BucketOperations as StorageBucketOperations, CompletePart, DeletedObject, HTTPRangeSpec,
        HealOperations as StorageHealOperations, ListMultipartsInfo, ListObjectVersionsInfo as StorageListObjectVersionsInfo,
        ListObjectsV2Info as StorageListObjectsV2Info, ListOperations as StorageListOperations, ListPartsInfo, MultipartInfo,
        MultipartOperations as StorageMultipartOperations, MultipartUploadResult, NamespaceLocking as StorageNamespaceLocking,
        ObjectIO as StorageObjectIO, ObjectInfoOrErr as StorageObjectInfoOrErr, ObjectOperations as StorageObjectOperations,
        ObjectToDelete, PartInfo, StorageAdminApi, WalkOptions as StorageWalkOptions,
    };

    pub(crate) use super::{DiskStore, ECStore, Error, GetObjectReader, ObjectInfo, ObjectOptions, PutObjReader, SetDisks};
}

pub(crate) mod replication_compat {
    pub(crate) use rustfs_ecstore::api::bucket::replication::{
        BucketStats, DeletedObjectReplicationInfo, DynReplicationPool, ObjectOpts, ReplicationConfigurationExt,
        ReplicationObjectBridge, ReplicationStats, ResyncStatusType,
    };
}

pub(crate) mod legacy_bitrot_read {
    pub(crate) use super::{DiskOption, Endpoint, STORAGE_FORMAT_FILE, create_bitrot_reader, new_disk};
}

pub(crate) mod minio_generated_read {
    pub(crate) use super::{
        DiskAPI, DiskOption, Endpoint, Erasure, GetObjectReader, ObjectInfo, ObjectOptions, create_bitrot_reader, new_disk,
    };
}
