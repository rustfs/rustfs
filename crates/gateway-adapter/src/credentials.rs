// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! The gateway's [`CredentialProvider`], answered by RustFS IAM.
//!
//! Responsible for: resolving one access key id to the secret the gateway verifies a signature
//! with, and classifying every other outcome as either a business result
//! ([`CredentialLookup::NotFound`], eligible for the gateway's negative cache) or an operational
//! fault ([`ProviderError`], counted and never cached).
//! NOT responsible for: authorization of any kind, session-token claim validation, or deciding
//! what the client is answered with. The gateway answers every unusable credential identically.
//!
//! # Secret handling
//!
//! Secrets travel only inside the gateway's [`Credentials`], whose `Debug` redacts them. No type
//! in this module derives or implements `Debug`, and no error produced here carries text: the
//! IAM error is classified and dropped.

use std::sync::Arc;

use rustfs_gateway::{
    BoxFuture, CredentialLookup, CredentialProvider, Credentials, CredentialsError, ProviderError, SessionBinding,
};
use rustfs_iam::error::Error as IamError;
use rustfs_iam::store::Store;
use rustfs_iam::sys::IamSys;
use rustfs_policy::auth::{ACCOUNT_OFF, UserIdentity};

/// The opaque issuer recorded in the [`rustfs_gateway::SessionBinding`] of every RustFS STS
/// credential.
pub const STS_SESSION_ISSUER: &str = "rustfs-sts";

/// Access keys with this prefix belong to Keystone token authentication, which never verifies an
/// AWS signature.
const KEYSTONE_ACCESS_KEY_PREFIX: &str = "keystone:";

/// The narrow slice of IAM that a credential lookup needs.
///
/// Exists so the adapter can be tested against a fake: IAM exports no in-memory backend.
/// Implemented for [`IamSys`], whose `check_key` loads a user from storage on a cache miss.
pub trait IamKeyLookup: Send + Sync + 'static {
    /// Whether the IAM store has finished loading.
    fn is_ready(&self) -> bool;

    /// Resolves an access key, with [`IamSys::check_key`]'s contract: `Ok((None, _))` for an
    /// unknown key, `Ok((Some(identity), valid))` otherwise, where `valid` is false for a
    /// disabled, expired, or malformed credential.
    fn check_key<'a>(&'a self, access_key: &'a str) -> BoxFuture<'a, Result<(Option<UserIdentity>, bool), IamError>>;
}

impl<T: Store> IamKeyLookup for IamSys<T> {
    fn is_ready(&self) -> bool {
        IamSys::is_ready(self)
    }

    fn check_key<'a>(&'a self, access_key: &'a str) -> BoxFuture<'a, Result<(Option<UserIdentity>, bool), IamError>> {
        Box::pin(IamSys::check_key(self, access_key))
    }
}

/// A [`CredentialProvider`] over RustFS IAM, with the root credentials injected at assembly.
pub struct IamCredentials {
    iam: Arc<dyn IamKeyLookup>,
    root: Option<Credentials>,
}

impl IamCredentials {
    /// A provider that resolves every access key through `iam`.
    pub fn new(iam: Arc<dyn IamKeyLookup>) -> Self {
        Self { iam, root: None }
    }

    /// Adds the root credentials, which resolve even before IAM is ready.
    ///
    /// # Errors
    ///
    /// [`CredentialsError::Malformed`] when the access key id is not one the gateway can hold.
    pub fn with_root(mut self, access_key: &str, secret_key: &[u8]) -> Result<Self, CredentialsError> {
        self.root = Some(Credentials::new(access_key, secret_key)?);
        Ok(self)
    }

    async fn resolve(&self, access_key: &str) -> Result<CredentialLookup, ProviderError> {
        // Deliberate change from the legacy s3s path (`rustfs/src/auth.rs` `get_secret_key`),
        // which answered a `keystone:` key with an empty secret. Keystone authenticates by token,
        // so there is no AWS secret to verify a signature against: the key is unknown here.
        if access_key.starts_with(KEYSTONE_ACCESS_KEY_PREFIX) {
            return Ok(CredentialLookup::NotFound);
        }

        // Byte-exact, as the trait requires: the access key id is signed material.
        if let Some(root) = &self.root
            && root.identity().access_key_id() == access_key
        {
            return Ok(CredentialLookup::Found(root.clone_credentials()));
        }

        if !self.iam.is_ready() {
            return Err(ProviderError::Unavailable);
        }

        match self.iam.check_key(access_key).await {
            Ok((Some(identity), valid)) => to_gateway_credentials(access_key, &identity, valid).map(CredentialLookup::Found),
            Ok((None, _)) => Ok(CredentialLookup::NotFound),
            Err(error) => classify_iam_error(&error),
        }
    }
}

/// Classifies an IAM failure without keeping any of its text.
fn classify_iam_error(error: &IamError) -> Result<CredentialLookup, ProviderError> {
    match error {
        IamError::NoSuchUser(_)
        | IamError::NoSuchServiceAccount(_)
        | IamError::NoSuchTempAccount(_)
        | IamError::NoSuchAccount(_) => Ok(CredentialLookup::NotFound),
        IamError::IamSysNotInitialized => Err(ProviderError::Unavailable),
        _ => Err(ProviderError::Backend),
    }
}

/// Builds the gateway credential for one IAM identity. No authorization happens here: a
/// disabled or invalid principal still resolves, marked disabled, and the gateway refuses it
/// exactly as it refuses an unknown key.
fn to_gateway_credentials(access_key: &str, identity: &UserIdentity, valid: bool) -> Result<Credentials, ProviderError> {
    let cred = &identity.credentials;
    let mut credentials = Credentials::new(access_key, cred.secret_key.as_bytes()).map_err(|_| ProviderError::Backend)?;

    // A service account carries a claims token internally, but its clients never present it, so
    // it authenticates as a long-term credential (as `check_session_token` treats it in legacy).
    if !cred.session_token.is_empty() && !cred.is_service_account() {
        // A temporary credential without an expiry cannot become a gateway session, which always
        // ends. Refusing it as a fault is the fail-closed answer; a never-ending session is not.
        let expiration = cred.expiration.ok_or(ProviderError::Backend)?;
        let binding = SessionBinding::new(STS_SESSION_ISSUER, expiration.unix_timestamp()).map_err(|_| ProviderError::Backend)?;
        credentials = credentials
            .with_session(&cred.session_token, binding)
            .map_err(|_| ProviderError::Backend)?;
    }

    if !valid || cred.status == ACCOUNT_OFF {
        credentials = credentials.disable();
    }
    Ok(credentials)
}

impl CredentialProvider for IamCredentials {
    fn lookup<'a>(&'a self, access_key_id: &'a str) -> BoxFuture<'a, Result<CredentialLookup, ProviderError>> {
        Box::pin(self.resolve(access_key_id))
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]
mod tests {
    use super::*;
    use rustfs_credentials::Credentials as IamCredential;
    use rustfs_gateway::RequestNow;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use time::OffsetDateTime;

    const ACCESS_KEY: &str = "AKIDUSER0001";
    const SECRET: &str = "user-secret-key-0001";

    type Answer = Box<dyn Fn() -> Result<(Option<UserIdentity>, bool), IamError> + Send + Sync>;

    struct FakeIam {
        ready: bool,
        answer: Answer,
        calls: AtomicUsize,
    }

    impl IamKeyLookup for FakeIam {
        fn is_ready(&self) -> bool {
            self.ready
        }

        fn check_key<'a>(&'a self, _access_key: &'a str) -> BoxFuture<'a, Result<(Option<UserIdentity>, bool), IamError>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let answer = (self.answer)();
            Box::pin(async move { answer })
        }
    }

    fn fake(
        ready: bool,
        answer: impl Fn() -> Result<(Option<UserIdentity>, bool), IamError> + Send + Sync + 'static,
    ) -> Arc<FakeIam> {
        Arc::new(FakeIam {
            ready,
            answer: Box::new(answer),
            calls: AtomicUsize::new(0),
        })
    }

    fn user(status: &str) -> IamCredential {
        IamCredential {
            access_key: ACCESS_KEY.to_owned(),
            secret_key: SECRET.to_owned(),
            status: status.to_owned(),
            ..Default::default()
        }
    }

    fn found(cred: IamCredential, valid: bool) -> Result<(Option<UserIdentity>, bool), IamError> {
        Ok((Some(UserIdentity::new(cred)), valid))
    }

    async fn lookup(iam: Arc<FakeIam>, access_key: &str) -> Result<CredentialLookup, ProviderError> {
        IamCredentials::new(iam).lookup(access_key).await
    }

    fn expect_found(result: Result<CredentialLookup, ProviderError>) -> Credentials {
        match result {
            Ok(CredentialLookup::Found(credentials)) => credentials,
            Ok(CredentialLookup::NotFound) => panic!("expected Found, got NotFound"),
            Err(error) => panic!("expected Found, got {error:?}"),
        }
    }

    fn is_not_found(result: &Result<CredentialLookup, ProviderError>) -> bool {
        matches!(result, Ok(CredentialLookup::NotFound))
    }

    /// Positive — an enabled user resolves to exactly its secret, as a long-term credential.
    #[tokio::test]
    async fn credentials_found_user_resolves_to_its_secret() {
        let iam = fake(true, || found(user("on"), true));
        let credentials = expect_found(lookup(iam, ACCESS_KEY).await);
        assert_eq!(credentials.secret().expose(), SECRET.as_bytes());
        assert_eq!(credentials.identity().access_key_id(), ACCESS_KEY);
        assert!(!credentials.is_disabled());
        assert!(credentials.session_binding().is_none());
    }

    /// Negative — a key IAM does not know is a business result, not a fault.
    #[tokio::test]
    async fn credentials_unknown_key_is_not_found() {
        let iam = fake(true, || Ok((None, false)));
        assert!(is_not_found(&lookup(iam, "AKIDUNKNOWN").await));
    }

    /// Negative — every "no such principal" error IAM can surface is `NotFound`. Answering these
    /// as `Backend` would keep them out of the negative cache and count every guessed key as a
    /// backend fault.
    #[tokio::test]
    async fn credentials_no_such_principal_errors_are_not_found() {
        let errors: [fn() -> IamError; 4] = [
            || IamError::NoSuchUser(ACCESS_KEY.to_owned()),
            || IamError::NoSuchServiceAccount(ACCESS_KEY.to_owned()),
            || IamError::NoSuchTempAccount(ACCESS_KEY.to_owned()),
            || IamError::NoSuchAccount(ACCESS_KEY.to_owned()),
        ];
        for error in errors {
            let iam = fake(true, move || Err(error()));
            let result = lookup(iam, ACCESS_KEY).await;
            assert!(is_not_found(&result), "{:?} must be NotFound", error());
        }
    }

    /// Negative — any other IAM failure is a backend fault, never `NotFound` and never `Found`.
    #[tokio::test]
    async fn credentials_backend_error_is_backend() {
        let iam = fake(true, || Err(IamError::StringError("storage read failed".to_owned())));
        assert!(matches!(lookup(iam, ACCESS_KEY).await, Err(ProviderError::Backend)));
    }

    /// Negative — a `keystone:` key never reaches IAM and never resolves to a secret.
    ///
    /// Deliberate change from the legacy s3s path, which answered such a key with an empty
    /// secret: Keystone authenticates by token, so there is no AWS secret to verify against.
    #[tokio::test]
    async fn credentials_keystone_prefix_is_not_found_without_iam() {
        let iam = fake(true, || found(user("on"), true));
        let result = lookup(iam.clone(), "keystone:project-user").await;
        assert!(is_not_found(&result));
        assert_eq!(iam.calls.load(Ordering::SeqCst), 0);
    }

    /// Negative — a switched-off or otherwise invalid user resolves, but disabled, and the same
    /// user enabled does not.
    #[tokio::test]
    async fn credentials_disabled_user_is_disabled() {
        let off = expect_found(lookup(fake(true, || found(user("off"), false)), ACCESS_KEY).await);
        assert!(off.is_disabled());
        assert!(off.admit(None, RequestNow::from_unix_seconds(0)).is_err());

        let invalid = expect_found(lookup(fake(true, || found(user("on"), false)), ACCESS_KEY).await);
        assert!(invalid.is_disabled());

        let on = expect_found(lookup(fake(true, || found(user("on"), true)), ACCESS_KEY).await);
        assert!(!on.is_disabled());
    }

    /// Positive — an STS credential carries its token and its lifetime as one session binding.
    #[tokio::test]
    async fn credentials_sts_carries_session_binding() {
        let expires_at = 4_000_000_000_i64;
        let iam = fake(true, move || {
            let mut cred = user("on");
            cred.session_token = "sts-session-token".to_owned();
            cred.parent_user = "parent".to_owned();
            cred.expiration = Some(OffsetDateTime::from_unix_timestamp(expires_at).expect("valid timestamp"));
            found(cred, true)
        });
        let credentials = expect_found(lookup(iam, ACCESS_KEY).await);
        let binding = credentials.session_binding().expect("an STS credential has a session");
        assert_eq!(binding.expires_at_unix_seconds(), expires_at);
        assert_eq!(binding.issuer(), STS_SESSION_ISSUER);
        let now = RequestNow::from_unix_seconds(expires_at - 1);
        assert!(credentials.admit(Some(b"sts-session-token"), now).is_ok());
        assert!(credentials.admit(None, now).is_err());
    }

    /// Negative — an STS credential with no expiry cannot be expressed as a session the gateway
    /// would ever expire, so it is a backend fault rather than a session that never ends.
    #[tokio::test]
    async fn credentials_sts_without_expiry_is_backend() {
        let iam = fake(true, || {
            let mut cred = user("on");
            cred.session_token = "sts-session-token".to_owned();
            found(cred, true)
        });
        assert!(matches!(lookup(iam, ACCESS_KEY).await, Err(ProviderError::Backend)));
    }

    /// Positive — a service account carries a token internally but authenticates as a long-term
    /// credential: clients never present that token.
    #[tokio::test]
    async fn credentials_service_account_is_long_term() {
        let iam = fake(true, || {
            let mut cred = user("on");
            cred.session_token = "service-account-claims-token".to_owned();
            cred.parent_user = "parent".to_owned();
            cred.claims = Some([(rustfs_credentials::IAM_POLICY_CLAIM_NAME_SA.to_owned(), "inherited-policy".into())].into());
            found(cred, true)
        });
        let credentials = expect_found(lookup(iam, ACCESS_KEY).await);
        assert!(credentials.session_binding().is_none());
        assert!(credentials.admit(None, RequestNow::from_unix_seconds(0)).is_ok());
    }

    /// Negative — before IAM is ready a lookup is `Unavailable`, not `NotFound`: a key must not
    /// be negatively cached because the store had not loaded yet.
    #[tokio::test]
    async fn credentials_iam_not_ready_is_unavailable() {
        let iam = fake(false, || found(user("on"), true));
        assert!(matches!(lookup(iam.clone(), ACCESS_KEY).await, Err(ProviderError::Unavailable)));
        assert_eq!(iam.calls.load(Ordering::SeqCst), 0);

        let uninitialized = fake(true, || Err(IamError::IamSysNotInitialized));
        assert!(matches!(lookup(uninitialized, ACCESS_KEY).await, Err(ProviderError::Unavailable)));
    }

    /// Positive — the injected root credentials resolve without IAM, even before it is ready, and
    /// nothing else is answered with the root secret.
    #[tokio::test]
    async fn credentials_root_resolves_without_iam() {
        let iam = fake(false, || found(user("on"), true));
        let provider = IamCredentials::new(iam.clone())
            .with_root("rootadmin", b"root-secret-key")
            .expect("valid root credentials");
        let root = expect_found(provider.lookup("rootadmin").await);
        assert_eq!(root.secret().expose(), b"root-secret-key");
        assert!(matches!(provider.lookup("ROOTADMIN").await, Err(ProviderError::Unavailable)));
        assert_eq!(iam.calls.load(Ordering::SeqCst), 0);
    }
}
