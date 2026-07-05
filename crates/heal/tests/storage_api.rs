#[allow(unused_imports)]
pub(crate) mod bug_fixes {
    pub(crate) use rustfs_ecstore::api::disk::DiskStore;
    pub(crate) use rustfs_ecstore::api::disk::endpoint::Endpoint;
    pub(crate) use rustfs_storage_api::BucketInfo;
}

#[allow(unused_imports)]
pub(crate) mod endpoint_index {
    pub(crate) use rustfs_ecstore::api::disk::endpoint::Endpoint;
    pub(crate) use rustfs_ecstore::api::layout::EndpointServerPools;
    pub(crate) use rustfs_ecstore::api::layout::Endpoints;
    pub(crate) use rustfs_ecstore::api::layout::PoolEndpoints;
    pub(crate) use rustfs_ecstore::api::storage::ECStore;
    pub(crate) use rustfs_ecstore::api::storage::init_local_disks;
}

#[allow(unused_imports)]
pub(crate) mod integration {
    pub(crate) use rustfs_ecstore::api::bucket::metadata_sys::init_bucket_metadata_sys;
    pub(crate) use rustfs_ecstore::api::disk::endpoint::Endpoint;
    pub(crate) use rustfs_ecstore::api::layout::EndpointServerPools;
    pub(crate) use rustfs_ecstore::api::layout::Endpoints;
    pub(crate) use rustfs_ecstore::api::layout::PoolEndpoints;
    pub(crate) use rustfs_ecstore::api::storage::ECStore;
    pub(crate) use rustfs_ecstore::api::storage::init_local_disks;
    pub(crate) use rustfs_storage_api::BucketOperations;
    pub(crate) use rustfs_storage_api::BucketOptions;
    pub(crate) use rustfs_storage_api::ObjectIO;
    pub(crate) use rustfs_storage_api::ObjectOperations;
}
