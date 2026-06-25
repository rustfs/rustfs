use crate::error::Error;
use crate::object_api::{GetObjectReader, ObjectInfo, ObjectOptions, PutObjReader};
use rustfs_filemeta::FileInfo;
pub use rustfs_storage_api::{
    BucketInfo, BucketOperations, BucketOptions, CapabilityStatus, CompletePart, DeleteBucketOptions, DeletedObject,
    DiskCapabilities, DiskSetSelector, ExpirationOptions, HTTPPreconditions, HTTPRangeError, HTTPRangeSpec, HealOperations,
    ListMultipartsInfo, ListObjectVersionsInfo as StorageListObjectVersionsInfo, ListObjectsInfo,
    ListObjectsV2Info as StorageListObjectsV2Info, ListOperations, ListPartsInfo, MakeBucketOptions, MultipartInfo,
    MultipartOperations, MultipartUploadResult, NamespaceLocking, ObjectIO, ObjectInfoOrErr as StorageObjectInfoOrErr,
    ObjectLockRetentionOptions, ObjectOperations, ObjectPreconditionError, ObjectPreconditionPart, ObjectPreconditionState,
    ObjectToDelete, PartInfo, StorageAdminApi, StorageErrorCode, TopologyCapabilities, TopologyDisk, TopologyLabels,
    TopologyPool, TopologySet, TopologySnapshot, TransitionedObject, VersionMarker, WalkOptions as StorageWalkOptions,
    WalkVersionsSortOrder,
};
use std::fmt::Debug;
use tokio_util::sync::CancellationToken;

type ListObjectsV2Info = StorageListObjectsV2Info<ObjectInfo>;
type ListObjectVersionsInfo = StorageListObjectVersionsInfo<ObjectInfo>;
type ObjectInfoOrErr = StorageObjectInfoOrErr<ObjectInfo, Error>;
type WalkOptions = StorageWalkOptions<fn(&FileInfo) -> bool>;

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
