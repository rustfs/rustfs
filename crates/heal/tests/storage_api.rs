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

// The temp-disk ECStore bootstrap itself lives in rustfs-test-utils
// (backlog#1153 infra-1); this surface keeps only what the heal test bodies
// still touch directly.
#[allow(unused_imports)]
pub(crate) mod integration {
    pub(crate) use rustfs_ecstore::api::storage::ECStore;
    pub(crate) use rustfs_storage_api::BucketOperations;
    pub(crate) use rustfs_storage_api::MakeBucketOptions;
    pub(crate) use rustfs_storage_api::ObjectIO;
    pub(crate) use rustfs_storage_api::ObjectOperations;
}
