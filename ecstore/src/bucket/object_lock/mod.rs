use s3s::dto::{ObjectLockConfiguration, ObjectLockEnabled};

pub trait ObjectLockApi {
    fn enabled(&self) -> bool;
}

impl ObjectLockApi for ObjectLockConfiguration {
    fn enabled(&self) -> bool {
        self.object_lock_enabled
            .as_ref()
            .is_some_and(|v| v.as_str() == ObjectLockEnabled::ENABLED)
    }
}
