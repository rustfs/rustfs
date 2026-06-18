use super::*;
use rustfs_storage_api::{CompletePart, ListMultipartsInfo, ListPartsInfo, MultipartInfo, MultipartUploadResult, PartInfo};

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

/// Healing and repair operations.
#[async_trait::async_trait]
pub trait HealOperations: Send + Sync + Debug {
    async fn heal_format(&self, dry_run: bool) -> Result<(HealResultItem, Option<Error>)>;
    async fn heal_bucket(&self, bucket: &str, opts: &HealOpts) -> Result<HealResultItem>;
    async fn heal_object(
        &self,
        bucket: &str,
        object: &str,
        version_id: &str,
        opts: &HealOpts,
    ) -> Result<(HealResultItem, Option<Error>)>;
    async fn get_pool_and_set(&self, id: &str) -> Result<(Option<usize>, Option<usize>, Option<usize>)>;
    async fn check_abandoned_parts(&self, bucket: &str, object: &str, opts: &HealOpts) -> Result<()>;
}

/// Namespace lock operations needed by consumers that only coordinate object
/// mutations but do not require the full storage API surface.
#[async_trait::async_trait]
pub trait NamespaceLocking: Send + Sync + Debug + 'static {
    async fn new_ns_lock(&self, bucket: &str, object: &str) -> Result<NamespaceLockWrapper>;
}
