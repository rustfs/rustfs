use super::*;

#[async_trait::async_trait]
pub trait ObjectIO: Send + Sync + Debug + 'static {
    // GetObjectNInfo FIXME:
    async fn get_object_reader(
        &self,
        bucket: &str,
        object: &str,
        range: Option<HTTPRangeSpec>,
        h: HeaderMap,
        opts: &ObjectOptions,
    ) -> Result<GetObjectReader>;
    // PutObject
    async fn put_object(&self, bucket: &str, object: &str, data: &mut PutObjReader, opts: &ObjectOptions) -> Result<ObjectInfo>;
}

#[async_trait::async_trait]
#[allow(clippy::too_many_arguments)]
pub trait StorageAPI: ObjectIO + Debug {
    async fn new_ns_lock(&self, bucket: &str, object: &str) -> Result<NamespaceLockWrapper>;

    async fn backend_info(&self) -> rustfs_madmin::BackendInfo;
    async fn storage_info(&self) -> rustfs_madmin::StorageInfo;
    async fn local_storage_info(&self) -> rustfs_madmin::StorageInfo;

    async fn make_bucket(&self, bucket: &str, opts: &MakeBucketOptions) -> Result<()>;
    async fn get_bucket_info(&self, bucket: &str, opts: &BucketOptions) -> Result<BucketInfo>;
    async fn list_bucket(&self, opts: &BucketOptions) -> Result<Vec<BucketInfo>>;
    async fn delete_bucket(&self, bucket: &str, opts: &DeleteBucketOptions) -> Result<()>;
    // ListObjects TODO: FIXME:
    async fn list_objects_v2(
        self: Arc<Self>,
        bucket: &str,
        prefix: &str,
        continuation_token: Option<String>,
        delimiter: Option<String>,
        max_keys: i32,
        fetch_owner: bool,
        start_after: Option<String>,
        incl_deleted: bool,
    ) -> Result<ListObjectsV2Info>;
    // ListObjectVersions TODO: FIXME:
    async fn list_object_versions(
        self: Arc<Self>,
        bucket: &str,
        prefix: &str,
        marker: Option<String>,
        version_marker: Option<String>,
        delimiter: Option<String>,
        max_keys: i32,
    ) -> Result<ListObjectVersionsInfo>;

    async fn walk(
        self: Arc<Self>,
        rx: CancellationToken,
        bucket: &str,
        prefix: &str,
        result: tokio::sync::mpsc::Sender<ObjectInfoOrErr>,
        opts: WalkOptions,
    ) -> Result<()>;

    async fn get_object_info(&self, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<ObjectInfo>;
    async fn verify_object_integrity(&self, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<()>;
    async fn copy_object(
        &self,
        src_bucket: &str,
        src_object: &str,
        dst_bucket: &str,
        dst_object: &str,
        src_info: &mut ObjectInfo,
        src_opts: &ObjectOptions,
        dst_opts: &ObjectOptions,
    ) -> Result<ObjectInfo>;
    async fn delete_object_version(&self, bucket: &str, object: &str, fi: &FileInfo, force_del_marker: bool) -> Result<()>;
    async fn delete_object(&self, bucket: &str, object: &str, opts: ObjectOptions) -> Result<ObjectInfo>;
    async fn delete_objects(
        &self,
        bucket: &str,
        objects: Vec<ObjectToDelete>,
        opts: ObjectOptions,
    ) -> (Vec<DeletedObject>, Vec<Option<Error>>);

    // TransitionObject TODO:
    // RestoreTransitionedObject TODO:

    async fn list_multipart_uploads(
        &self,
        bucket: &str,
        prefix: &str,
        key_marker: Option<String>,
        upload_id_marker: Option<String>,
        delimiter: Option<String>,
        max_uploads: usize,
    ) -> Result<ListMultipartsInfo>;
    async fn new_multipart_upload(&self, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<MultipartUploadResult>;
    async fn copy_object_part(
        &self,
        src_bucket: &str,
        src_object: &str,
        dst_bucket: &str,
        dst_object: &str,
        upload_id: &str,
        part_id: usize,
        start_offset: i64,
        length: i64,
        src_info: &ObjectInfo,
        src_opts: &ObjectOptions,
        dst_opts: &ObjectOptions,
    ) -> Result<()>;
    async fn put_object_part(
        &self,
        bucket: &str,
        object: &str,
        upload_id: &str,
        part_id: usize,
        data: &mut PutObjReader,
        opts: &ObjectOptions,
    ) -> Result<PartInfo>;
    async fn get_multipart_info(
        &self,
        bucket: &str,
        object: &str,
        upload_id: &str,
        opts: &ObjectOptions,
    ) -> Result<MultipartInfo>;
    async fn list_object_parts(
        &self,
        bucket: &str,
        object: &str,
        upload_id: &str,
        part_number_marker: Option<usize>,
        max_parts: usize,
        opts: &ObjectOptions,
    ) -> Result<ListPartsInfo>;
    async fn abort_multipart_upload(&self, bucket: &str, object: &str, upload_id: &str, opts: &ObjectOptions) -> Result<()>;
    async fn complete_multipart_upload(
        self: Arc<Self>,
        bucket: &str,
        object: &str,
        upload_id: &str,
        uploaded_parts: Vec<CompletePart>,
        opts: &ObjectOptions,
    ) -> Result<ObjectInfo>;
    async fn get_disks(&self, pool_idx: usize, set_idx: usize) -> Result<Vec<Option<DiskStore>>>;
    fn set_drive_counts(&self) -> Vec<usize>;

    // Health TODO:
    async fn put_object_metadata(&self, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<ObjectInfo>;
    // DecomTieredObject
    async fn get_object_tags(&self, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<String>;
    async fn add_partial(&self, bucket: &str, object: &str, version_id: &str) -> Result<()>;
    async fn transition_object(&self, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<()>;
    async fn restore_transitioned_object(self: Arc<Self>, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<()>;
    async fn put_object_tags(&self, bucket: &str, object: &str, tags: &str, opts: &ObjectOptions) -> Result<ObjectInfo>;
    async fn delete_object_tags(&self, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<ObjectInfo>;

    async fn heal_format(&self, dry_run: bool) -> Result<(HealResultItem, Option<Error>)>;
    async fn heal_bucket(&self, bucket: &str, opts: &HealOpts) -> Result<HealResultItem>;
    async fn heal_object(
        &self,
        bucket: &str,
        object: &str,
        version_id: &str,
        opts: &HealOpts,
    ) -> Result<(HealResultItem, Option<Error>)>;
    // async fn heal_objects(&self, bucket: &str, prefix: &str, opts: &HealOpts, hs: Arc<HealSequence>, is_meta: bool)
    // -> Result<()>;
    async fn get_pool_and_set(&self, id: &str) -> Result<(Option<usize>, Option<usize>, Option<usize>)>;
    async fn check_abandoned_parts(&self, bucket: &str, object: &str, opts: &HealOpts) -> Result<()>;
}
