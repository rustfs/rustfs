#[cfg(test)]
pub(crate) use rustfs_ecstore::api::bucket::bucket_target_sys::BucketTargetSys as E2eBucketTargetSys;
#[cfg(test)]
pub(crate) use rustfs_ecstore::api::disk::{VolumeInfo as E2eVolumeInfo, WalkDirOptions as E2eWalkDirOptions};
pub(crate) use rustfs_ecstore::api::rpc::{
    TonicInterceptor as E2eTonicInterceptor, node_service_time_out_client_no_auth as e2e_node_service_time_out_client_no_auth,
};
#[cfg(test)]
pub(crate) use rustfs_ecstore::api::rpc::{
    gen_tonic_signature_interceptor as gen_e2e_tonic_signature_interceptor,
    node_service_time_out_client as e2e_node_service_time_out_client,
};
