use auth::{Credentials, UserIdentity};
use ecstore::store::ECStore;
use log::debug;
use manager::IamCache;
use policy::{Args, Policy};
use std::{
    collections::HashMap,
    sync::{Arc, OnceLock},
};
use store::object::ObjectStore;
use time::OffsetDateTime;

pub mod cache;
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

static GLOBAL_ACTIVE_CRED: OnceLock<Credentials> = OnceLock::new();

pub fn init_global_action_cred(ak: Option<String>, sk: Option<String>) -> Result<()> {
    let ak = {
        if let Some(k) = ak {
            k
        } else {
            utils::gen_access_key(20).unwrap_or_default()
        }
    };

    let sk = {
        if let Some(k) = sk {
            k
        } else {
            utils::gen_secret_key(32).unwrap_or_default()
        }
    };

    GLOBAL_ACTIVE_CRED
        .set(Credentials {
            access_key: ak,
            secret_key: sk,
            ..Default::default()
        })
        .map_err(|_e| Error::CredNotInitialized)
}

pub fn get_global_action_cred() -> Option<Credentials> {
    GLOBAL_ACTIVE_CRED.get().cloned()
}

pub async fn init_iam_sys(ecstore: Arc<ECStore>) -> crate::Result<()> {
    debug!("init iam system");
    let s = IamCache::new(ObjectStore::new(ecstore)).await;
    IAM_SYS.get_or_init(move || s);
    Ok(())
}

#[inline]
pub fn get() -> crate::Result<Arc<IamCache<ObjectStore>>> {
    IAM_SYS.get().map(Arc::clone).ok_or(Error::IamSysNotInitialized)
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

pub async fn check_key(ak: &str) -> crate::Result<(Option<UserIdentity>, bool)> {
    if let Some(sys_cred) = get_global_action_cred() {
        if sys_cred.access_key == ak {
            return Ok((Some(UserIdentity::new(sys_cred)), true));
        }
    }
    get()?.check_key(ak).await
}

pub async fn list_users() -> crate::Result<HashMap<String, madmin::UserInfo>> {
    get()?.get_users().await
}

pub async fn get_user(ak: &str) -> crate::Result<(Option<UserIdentity>, bool)> {
    get()?.check_key(ak).await
}
