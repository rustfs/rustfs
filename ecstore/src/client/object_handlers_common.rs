use crate::StorageAPI;
use crate::bucket::lifecycle::lifecycle;
use crate::bucket::versioning::VersioningApi;
use crate::bucket::versioning_sys::BucketVersioningSys;
use crate::store::ECStore;
use crate::store_api::{ObjectOptions, ObjectToDelete};
use lock::local_locker::MAX_DELETE_LIST;

pub async fn delete_object_versions(api: ECStore, bucket: &str, to_del: &[ObjectToDelete], _lc_event: lifecycle::Event) {
    let mut remaining = to_del;
    loop {
        let mut to_del = remaining;
        if to_del.len() > MAX_DELETE_LIST {
            remaining = &to_del[MAX_DELETE_LIST..];
            to_del = &to_del[..MAX_DELETE_LIST];
        } else {
            remaining = &[];
        }
        let vc = BucketVersioningSys::get(bucket).await.expect("err!");
        let _deleted_objs = api.delete_objects(
            bucket,
            to_del.to_vec(),
            ObjectOptions {
                //prefix_enabled_fn:  vc.prefix_enabled(""),
                version_suspended: vc.suspended(),
                ..Default::default()
            },
        );
    }
}
