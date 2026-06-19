use super::*;
use rustfs_storage_api::{
    CompletePart, DeletedObject, ListMultipartsInfo, ListPartsInfo, MultipartInfo, MultipartUploadResult, ObjectToDelete,
    PartInfo,
};

pub trait ObjectIO:
    rustfs_storage_api::ObjectIO<
        Error = Error,
        RangeSpec = HTTPRangeSpec,
        HeaderMap = HeaderMap,
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

impl<T> ObjectIO for T where
    T: rustfs_storage_api::ObjectIO<
            Error = Error,
            RangeSpec = HTTPRangeSpec,
            HeaderMap = HeaderMap,
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

/// Object-level storage operations (beyond basic I/O in ObjectIO).
pub trait ObjectOperations:
    rustfs_storage_api::ObjectOperations<
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

impl<T> ObjectOperations for T where
    T: rustfs_storage_api::ObjectOperations<
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

/// Listing and walking operations.
pub trait ListOperations:
    rustfs_storage_api::ListOperations<
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

impl<T> ListOperations for T where
    T: rustfs_storage_api::ListOperations<
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

/// Multipart upload operations.
pub trait MultipartOperations:
    rustfs_storage_api::MultipartOperations<
        Error = Error,
        ObjectInfo = ObjectInfo,
        ObjectOptions = ObjectOptions,
        PutObjectReader = PutObjReader,
        CompletePart = CompletePart,
        ListMultipartsInfo = ListMultipartsInfo,
        MultipartUploadResult = MultipartUploadResult,
        PartInfo = PartInfo,
        MultipartInfo = MultipartInfo,
        ListPartsInfo = ListPartsInfo,
    > + Send
    + Sync
    + Debug
{
}

impl<T> MultipartOperations for T where
    T: rustfs_storage_api::MultipartOperations<
            Error = Error,
            ObjectInfo = ObjectInfo,
            ObjectOptions = ObjectOptions,
            PutObjectReader = PutObjReader,
            CompletePart = CompletePart,
            ListMultipartsInfo = ListMultipartsInfo,
            MultipartUploadResult = MultipartUploadResult,
            PartInfo = PartInfo,
            MultipartInfo = MultipartInfo,
            ListPartsInfo = ListPartsInfo,
        > + Send
        + Sync
        + Debug
{
}
