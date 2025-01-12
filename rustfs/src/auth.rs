use iam::cache::CacheInner;
use log::warn;
use s3s::auth::S3Auth;
use s3s::auth::SecretKey;
use s3s::auth::SimpleAuth;
use s3s::s3_error;
use s3s::S3Result;

pub struct IAMAuth {
    simple_auth: SimpleAuth,
}

impl IAMAuth {
    pub fn new(ak: impl Into<String>, sk: impl Into<SecretKey>) -> Self {
        let simple_auth = SimpleAuth::from_single(ak, sk);
        Self { simple_auth }
    }
}

#[async_trait::async_trait]
impl S3Auth for IAMAuth {
    async fn get_secret_key(&self, access_key: &str) -> S3Result<SecretKey> {
        if access_key.is_empty() {
            return Err(s3_error!(NotSignedUp, "Your account is not signed up"));
        }

        if let Ok(key) = self.simple_auth.get_secret_key(access_key).await {
            return Ok(key);
        }

        warn!("Failed to get secret key from simple auth");

        if let Ok(iam_store) = iam::get() {
            let c = CacheInner::from(&iam_store.cache);
            warn!("Failed to get secret key from simple auth, try cache {}", access_key);
            warn!("users {:?}", c.users.values());
            warn!("sts_accounts {:?}", c.sts_accounts.values());
            if let Some(id) = c.get_user(access_key) {
                warn!("get cred {:?}", id.credentials);
                return Ok(SecretKey::from(id.credentials.secret_key.clone()));
            }
        }

        Err(s3_error!(NotSignedUp, "Your account is not signed up2"))
    }
}
