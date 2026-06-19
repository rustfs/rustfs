use crate::error::Error;
use crate::object_api::{
    GetObjectReader, ListObjectVersionsInfo, ListObjectsV2Info, ObjectInfo, ObjectInfoOrErr, ObjectOptions, PutObjReader,
    WalkOptions,
};
use rustfs_filemeta::FileInfo;
use rustfs_storage_api::{DeletedObject, HTTPRangeSpec, ObjectToDelete};
use std::fmt::Debug;
use tokio_util::sync::CancellationToken;

pub(crate) trait EcstoreObjectIO:
    rustfs_storage_api::ObjectIO<
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
    T: rustfs_storage_api::ObjectIO<
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

impl<T> EcstoreObjectOperations for T where
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

pub(crate) trait EcstoreListOperations:
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

impl<T> EcstoreListOperations for T where
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
