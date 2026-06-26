use crate::error::Error;
use crate::object_api::{GetObjectReader, ObjectInfo, ObjectOptions, PutObjReader};
use rustfs_filemeta::FileInfo;
use std::fmt::Debug;

pub(crate) mod admin {
    pub(crate) use rustfs_storage_api::{DiskSetSelector, StorageAdminApi};
}

pub(crate) mod bucket {
    pub(crate) use rustfs_storage_api::{BucketInfo, BucketOperations, BucketOptions, DeleteBucketOptions, MakeBucketOptions};
}

pub(crate) mod error {
    pub(crate) use rustfs_storage_api::StorageErrorCode;
}

pub(crate) mod heal {
    pub(crate) use rustfs_storage_api::HealOperations;
}

pub(crate) mod lifecycle {
    pub use rustfs_storage_api::{ExpirationOptions, TransitionedObject};
}

pub(crate) mod list {
    use super::{Debug, Error, FileInfo, ObjectInfo};
    pub(crate) use rustfs_storage_api::{
        ListObjectVersionsInfo as StorageListObjectVersionsInfo, ListObjectsInfo, ListObjectsV2Info as StorageListObjectsV2Info,
        ListOperations, ObjectInfoOrErr as StorageObjectInfoOrErr, VersionMarker, WalkOptions as StorageWalkOptions,
        WalkVersionsSortOrder,
    };
    use tokio_util::sync::CancellationToken;

    type ListObjectsV2Info = StorageListObjectsV2Info<ObjectInfo>;
    type ListObjectVersionsInfo = StorageListObjectVersionsInfo<ObjectInfo>;
    type ObjectInfoOrErr = StorageObjectInfoOrErr<ObjectInfo, Error>;
    type WalkOptions = StorageWalkOptions<fn(&FileInfo) -> bool>;

    pub(crate) trait EcstoreListOperations:
        ListOperations<
            Error = Error,
            ListObjectsV2Info = ListObjectsV2Info,
            ListObjectVersionsInfo = ListObjectVersionsInfo,
            ObjectInfoOrErr = ObjectInfoOrErr,
            WalkOptions = WalkOptions,
            WalkCancellation = CancellationToken,
            WalkResultSender = tokio::sync::mpsc::Sender<ObjectInfoOrErr>,
        > + Send
        + Sync
        + Debug
    {
    }

    impl<T> EcstoreListOperations for T where
        T: ListOperations<
                Error = Error,
                ListObjectsV2Info = ListObjectsV2Info,
                ListObjectVersionsInfo = ListObjectVersionsInfo,
                ObjectInfoOrErr = ObjectInfoOrErr,
                WalkOptions = WalkOptions,
                WalkCancellation = CancellationToken,
                WalkResultSender = tokio::sync::mpsc::Sender<ObjectInfoOrErr>,
            > + Send
            + Sync
            + Debug
    {
    }
}

pub(crate) mod multipart {
    pub(crate) use rustfs_storage_api::{
        CompletePart, ListMultipartsInfo, ListPartsInfo, MultipartInfo, MultipartOperations, MultipartUploadResult, PartInfo,
    };
}

pub(crate) mod namespace {
    pub(crate) use rustfs_storage_api::NamespaceLocking;
}

pub(crate) mod object {
    use super::{Debug, Error, FileInfo, GetObjectReader, ObjectInfo, ObjectOptions, PutObjReader};
    use crate::storage_api_contracts::range::HTTPRangeSpec;
    pub(crate) use rustfs_storage_api::{
        DeletedObject, HTTPPreconditions, ObjectIO, ObjectLockRetentionOptions, ObjectOperations, ObjectPreconditionError,
        ObjectPreconditionPart, ObjectPreconditionState, ObjectToDelete,
    };

    pub(crate) trait EcstoreObjectIO:
        ObjectIO<
            Error = Error,
            RangeSpec = HTTPRangeSpec,
            HeaderMap = http::HeaderMap,
            ObjectOptions = ObjectOptions,
            ObjectInfo = ObjectInfo,
            GetObjectReader = GetObjectReader,
            PutObjectReader = PutObjReader,
        > + Send
        + Sync
        + Debug
        + 'static
    {
    }

    impl<T> EcstoreObjectIO for T where
        T: ObjectIO<
                Error = Error,
                RangeSpec = HTTPRangeSpec,
                HeaderMap = http::HeaderMap,
                ObjectOptions = ObjectOptions,
                ObjectInfo = ObjectInfo,
                GetObjectReader = GetObjectReader,
                PutObjectReader = PutObjReader,
            > + Send
            + Sync
            + Debug
            + 'static
    {
    }

    pub(crate) trait EcstoreObjectOperations:
        ObjectOperations<
            Error = Error,
            ObjectInfo = ObjectInfo,
            ObjectOptions = ObjectOptions,
            FileInfo = FileInfo,
            ObjectToDelete = ObjectToDelete,
            DeletedObject = DeletedObject,
        > + Send
        + Sync
        + Debug
    {
    }

    impl<T> EcstoreObjectOperations for T where
        T: ObjectOperations<
                Error = Error,
                ObjectInfo = ObjectInfo,
                ObjectOptions = ObjectOptions,
                FileInfo = FileInfo,
                ObjectToDelete = ObjectToDelete,
                DeletedObject = DeletedObject,
            > + Send
            + Sync
            + Debug
    {
    }
}

pub(crate) mod range {
    pub(crate) use rustfs_storage_api::{HTTPRangeError, HTTPRangeSpec};
}

pub(crate) mod topology {
    pub(crate) use rustfs_storage_api::{
        CapabilityStatus, DiskCapabilities, TopologyCapabilities, TopologyDisk, TopologyLabels, TopologyPool, TopologySet,
        TopologySnapshot,
    };
}
