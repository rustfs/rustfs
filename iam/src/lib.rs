use common::error::{Error, Result};
use ecstore::store::ECStore;
use error::Error as IamError;
use manager::IamCache;
use policy::auth::Credentials;
use std::sync::{Arc, OnceLock};
use store::object::ObjectStore;
use sys::IamSys;
use tracing::{debug, instrument};

pub mod cache;
pub mod error;
pub mod manager;
pub mod store;
pub mod utils;

pub mod sys;

static IAM_SYS: OnceLock<Arc<IamSys<ObjectStore>>> = OnceLock::new();

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
        .unwrap();
    Ok(())
}

pub fn get_global_action_cred() -> Option<Credentials> {
    GLOBAL_ACTIVE_CRED.get().cloned()
}

#[instrument]
pub async fn init_iam_sys(ecstore: Arc<ECStore>) -> Result<()> {
    debug!("init iam system");
    let s = IamCache::new(ObjectStore::new(ecstore)).await;

    IAM_SYS.get_or_init(move || IamSys::new(s).into());
    Ok(())
}

#[inline]
pub fn get() -> Result<Arc<IamSys<ObjectStore>>> {
    IAM_SYS
        .get()
        .map(Arc::clone)
        .ok_or(Error::new(IamError::IamSysNotInitialized))
}
