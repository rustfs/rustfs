use auth::{Credentials, UserIdentity};
use ecstore::store::ECStore;
use log::debug;
use manager::IamCache;
use policy::{Args, Policy};
use std::sync::{Arc, OnceLock};
use store::object::ObjectStore;
use time::OffsetDateTime;

mod cache;
mod format;
mod handler;

pub mod arn;
pub mod auth;
pub mod error;
pub mod manager;
pub mod policy;
pub mod service_type;
pub mod store;
pub mod utils;

pub use error::{Error, Result};

static IAM_SYS: OnceLock<Arc<IamCache<ObjectStore>>> = OnceLock::new();

pub async fn init_iam_sys(ecstore: Arc<ECStore>) -> crate::Result<()> {
    debug!("init iam system");
    let s = IamCache::new(ObjectStore::new(ecstore)).await;
    IAM_SYS.get_or_init(move || s);
    Ok(())
}

#[inline]
pub fn get() -> crate::Result<Arc<IamCache<ObjectStore>>> {
    IAM_SYS.get().map(|x| Arc::clone(x)).ok_or(Error::IamSysNotInitialized)
}

pub async fn is_allowed<'a>(args: Args<'a>) -> crate::Result<bool> {
    Ok(get()?.is_allowed(args).await)
}

pub async fn get_service_account(ak: &str) -> crate::Result<(Credentials, Option<Policy>)> {
    let (mut sa, policy) = get()?.get_service_account(ak).await?;

    sa.credentials.secret_key.clear();
    sa.credentials.access_key.clear();

    Ok((sa.credentials, policy))
}

pub async fn add_service_account(cred: Credentials) -> crate::Result<OffsetDateTime> {
    get()?.add_service_account(cred).await
}

pub async fn check_key(ak: &str) -> crate::Result<Option<UserIdentity>> {
    get()?.check_key(ak).await
}
