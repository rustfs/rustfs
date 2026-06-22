#![allow(dead_code)]

use rustfs_ecstore::api::bucket as ecstore_bucket;
use rustfs_ecstore::api::error as ecstore_error;

pub(crate) fn check_bucket_and_object_names(bucket: &str, object: &str) -> ecstore_error::Result<()> {
    ecstore_bucket::utils::check_bucket_and_object_names(bucket, object)
}

pub(crate) fn check_list_objs_args(bucket: &str, prefix: &str, marker: &Option<String>) -> ecstore_error::Result<()> {
    ecstore_bucket::utils::check_list_objs_args(bucket, prefix, marker)
}

pub(crate) fn check_object_name_for_length_and_slash(bucket: &str, object: &str) -> ecstore_error::Result<()> {
    ecstore_bucket::utils::check_object_name_for_length_and_slash(bucket, object)
}

pub(crate) fn check_valid_bucket_name_strict(bucket: &str) -> ecstore_error::Result<()> {
    ecstore_bucket::utils::check_valid_bucket_name_strict(bucket)
}

pub(crate) fn has_bad_path_component(object: &str) -> bool {
    ecstore_bucket::utils::has_bad_path_component(object)
}

pub(crate) fn is_meta_bucketname(bucket: &str) -> bool {
    ecstore_bucket::utils::is_meta_bucketname(bucket)
}

pub(crate) fn is_valid_object_prefix(object: &str) -> bool {
    ecstore_bucket::utils::is_valid_object_prefix(object)
}
