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

//! The gateway's [`PolicySource`], answered by RustFS IAM.
//!
//! Responsible for: the one reading of the caller's IAM facts a request is judged against — which
//! principal kind it is, the user it acts for, its groups, its token claims, and whether it owns
//! the deployment — packed as a [`RustfsPrincipal`] into the gateway's [`PolicySnapshot`].
//! NOT responsible for: evaluating any policy, choosing which policy documents apply (that stays
//! in `IamSys::is_allowed`, which owns the STS, OIDC, session-policy and service-account rules), or
//! verifying the signature or the session token (the gateway did both before calling this).
//! Upstream: the gateway's verified [`Identity`]. Downstream: an authorizer that reads the payload
//! back with `PolicySnapshot::get::<RustfsPrincipal>()`. The RustFS gateway assembly installs no
//! policy source: its handlers resolve the caller through the legacy access phase.
//!
//! # Failure classes
//!
//! Every refusal is a [`PolicyError`] with a constant label and nothing from IAM or the request:
//! IAM not ready, not initialized, or failing, or root credentials not initialized, is
//! [`PolicyError::unavailable`]; a principal IAM no longer knows is [`UNKNOWN_PRINCIPAL`]; a
//! disabled one, or one whose secret fails the effective-length rule, is
//! [`DISABLED_PRINCIPAL`]; an identity
//! whose credential kind disagrees with IAM's record is [`SESSION_MISMATCH`]; a token whose claims
//! cannot be read is [`PolicyError::malformed`]. None of them is ever an empty snapshot: an empty
//! snapshot is what an anonymous caller gets, and answering a fault with it would judge an
//! authenticated caller as anonymous.
//!
//! # Secret handling
//!
//! [`RustfsPrincipal`] holds no secret key and no session token, and implements no `Debug`, so a
//! snapshot formatted into a log cannot print its claims either. The root credentials and the STS
//! token-signing key are read per request from the shared [`RootSource`] and never stored here.

use std::collections::HashMap;
use std::sync::Arc;

use rustfs_credentials::Credentials as IamCredential;
use rustfs_gateway::{BoxFuture, Identity, PolicyError, PolicySnapshot, PolicySource};
use rustfs_iam::error::Error as IamError;
use rustfs_iam::store::Store;
use rustfs_iam::sys::{
    IamSys, SESSION_POLICY_NAME, get_claims_from_token_with_secret, get_claims_from_token_with_secret_allow_missing_exp,
    is_rustfs_oidc_claims,
};
use rustfs_policy::auth::ACCOUNT_OFF;
use serde_json::Value;

use crate::credentials::IamKeyLookup;
use crate::root::{RootCredentials, RootSource, is_usable_secret};

/// [`PolicyError`] label: IAM does not know the authenticated principal (any longer).
pub const UNKNOWN_PRINCIPAL: &str = "unknown-principal";
/// [`PolicyError`] label: IAM holds the principal disabled, expired, or otherwise invalid.
pub const DISABLED_PRINCIPAL: &str = "disabled-principal";
/// [`PolicyError`] label: the identity's credential kind (session or long-term) disagrees with
/// what IAM holds for its access key.
pub const SESSION_MISMATCH: &str = "session-mismatch";

/// The narrow slice of IAM a policy reading needs, on top of [`IamKeyLookup`].
///
/// Implemented for [`IamSys`]; exists so the source can be tested against a fake.
pub trait IamPrincipalLookup: IamKeyLookup {
    /// The built-in IAM groups `user` belongs to now, as `IamSys::user_group_memberships`
    /// answers. These may include groups granted after a credential was issued.
    fn group_memberships(&self, user: &str) -> Vec<String>;
}

impl<T: Store> IamPrincipalLookup for IamSys<T> {
    fn group_memberships(&self, user: &str) -> Vec<String> {
        IamSys::user_group_memberships(self, user)
    }
}

/// Which kind of credential the principal authenticated with.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum PrincipalKind {
    /// The deployment's root credentials.
    Root,
    /// A long-term IAM user.
    User,
    /// A service account, acting for its parent user.
    ServiceAccount,
    /// An STS temporary credential, acting for its parent user.
    Temporary,
}

/// The caller's IAM facts: the payload of every [`PolicySnapshot`] [`IamPolicySource`] produces.
///
/// Carries no secret key and no session token, and deliberately implements no `Debug`.
pub struct RustfsPrincipal {
    kind: PrincipalKind,
    account: String,
    username: String,
    parent_user: String,
    credential_groups: Option<Vec<String>>,
    group_memberships: Vec<String>,
    claims: Option<HashMap<String, Value>>,
    is_owner: bool,
}

impl RustfsPrincipal {
    /// The kind of credential the caller authenticated with.
    #[must_use]
    pub fn kind(&self) -> PrincipalKind {
        self.kind
    }

    /// The access key the caller signed with (IAM `Args::account`).
    #[must_use]
    pub fn account(&self) -> &str {
        &self.account
    }

    /// The user the caller acts as: the parent user for an STS credential or a service account,
    /// the access key otherwise.
    #[must_use]
    pub fn username(&self) -> &str {
        &self.username
    }

    /// The parent user IAM records for the credential; empty for root and long-term users.
    #[must_use]
    pub fn parent_user(&self) -> &str {
        &self.parent_user
    }

    /// The groups recorded on the credential itself (IAM `Args::groups`).
    #[must_use]
    pub fn credential_groups(&self) -> &Option<Vec<String>> {
        &self.credential_groups
    }

    /// The groups IAM currently lists for [`Self::username`], read once with the snapshot.
    #[must_use]
    pub fn group_memberships(&self) -> &[String] {
        &self.group_memberships
    }

    /// The claims the credential's token carries; `None` when it has none.
    #[must_use]
    pub fn claims(&self) -> Option<&HashMap<String, Value>> {
        self.claims.as_ref()
    }

    /// Whether the caller owns the deployment (IAM `Args::is_owner`): root, or a credential of
    /// root that no session policy or external OIDC identity narrows.
    #[must_use]
    pub fn is_owner(&self) -> bool {
        self.is_owner
    }
}

/// A [`PolicySource`] over RustFS IAM and the live root credentials.
///
/// Built only by [`crate::IamAdapters::new`], so it always reads the same [`RootSource`] as the
/// credential provider it was assembled with.
pub struct IamPolicySource {
    iam: Arc<dyn IamPrincipalLookup>,
    root: Arc<dyn RootSource>,
}

impl IamPolicySource {
    pub(crate) fn new(iam: Arc<dyn IamPrincipalLookup>, root: Arc<dyn RootSource>) -> Self {
        Self { iam, root }
    }

    async fn read(&self, identity: Option<&Identity>) -> Result<PolicySnapshot, PolicyError> {
        let Some(identity) = identity else {
            return Ok(PolicySnapshot::empty());
        };
        let principal = self.principal(identity).await?;
        Ok(PolicySnapshot::of(Arc::new(principal)))
    }

    async fn principal(&self, identity: &Identity) -> Result<RustfsPrincipal, PolicyError> {
        let access_key = identity.access_key_id();
        let has_session = identity.session().is_some();
        // Without root credentials neither ownership nor STS claims can be decided.
        let Some(root) = self.root.current() else {
            return Err(PolicyError::unavailable());
        };
        let root_access_key = root.access_key();

        if access_key == root_access_key {
            // Root has no session token, so a session identity for its key is not one IAM issued.
            if has_session {
                return Err(PolicyError::because(SESSION_MISMATCH));
            }
            return Ok(RustfsPrincipal {
                kind: PrincipalKind::Root,
                account: access_key.to_owned(),
                username: access_key.to_owned(),
                parent_user: String::new(),
                credential_groups: None,
                group_memberships: Vec::new(),
                claims: None,
                is_owner: true,
            });
        }

        if !self.iam.is_ready() {
            return Err(PolicyError::unavailable());
        }
        let cred = match self.iam.check_key(access_key).await {
            Ok((Some(identity), true)) => identity.credentials,
            Ok((Some(_), false)) => return Err(PolicyError::because(DISABLED_PRINCIPAL)),
            Ok((None, _)) => return Err(PolicyError::because(UNKNOWN_PRINCIPAL)),
            Err(error) => return Err(classify_iam_error(&error)),
        };
        if cred.status == ACCOUNT_OFF || !is_usable_secret(cred.secret_key.as_bytes()) {
            return Err(PolicyError::because(DISABLED_PRINCIPAL));
        }

        let kind = if cred.is_service_account() {
            PrincipalKind::ServiceAccount
        } else if !cred.session_token.is_empty() {
            PrincipalKind::Temporary
        } else {
            PrincipalKind::User
        };
        // The gateway admitted this identity against the credential IAM returned, so the kinds
        // agree unless IAM's record changed in between; refuse rather than judge a stale reading.
        if has_session != (kind == PrincipalKind::Temporary) {
            return Err(PolicyError::because(SESSION_MISMATCH));
        }

        let claims = read_claims(kind, &cred, &root)?;
        let username = match kind {
            PrincipalKind::Temporary | PrincipalKind::ServiceAccount => cred.parent_user.clone(),
            PrincipalKind::Root | PrincipalKind::User => cred.access_key.clone(),
        };
        let membership_subject = if username.is_empty() { access_key } else { username.as_str() };
        let group_memberships = self.iam.group_memberships(membership_subject);
        let is_owner = acts_for_root(&cred, claims.as_ref(), root_access_key);

        Ok(RustfsPrincipal {
            kind,
            account: access_key.to_owned(),
            username,
            parent_user: cred.parent_user,
            credential_groups: cred.groups,
            group_memberships,
            claims,
            is_owner,
        })
    }
}

/// The claims of the credential's own token. The gateway admitted the presented token only when it
/// equals the stored one, so reading the stored token reads the presented one.
fn read_claims(
    kind: PrincipalKind,
    cred: &IamCredential,
    root: &RootCredentials,
) -> Result<Option<HashMap<String, Value>>, PolicyError> {
    // As in legacy `check_claims_from_token_with_context`: no token, no claims.
    if cred.session_token.is_empty() {
        return Ok(None);
    }
    let claims = match kind {
        PrincipalKind::ServiceAccount => {
            get_claims_from_token_with_secret_allow_missing_exp(&cred.session_token, &cred.secret_key)
                .map_err(|_| PolicyError::malformed())?
        }
        PrincipalKind::Temporary => get_claims_from_token_with_secret(&cred.session_token, root.sts_signing_key())
            .map_err(|_| PolicyError::malformed())?,
        PrincipalKind::Root | PrincipalKind::User => return Ok(None),
    };
    Ok(if claims.is_empty() { None } else { Some(claims) })
}

/// Classifies an IAM failure without keeping any of its text.
fn classify_iam_error(error: &IamError) -> PolicyError {
    match error {
        IamError::NoSuchUser(_)
        | IamError::NoSuchServiceAccount(_)
        | IamError::NoSuchTempAccount(_)
        | IamError::NoSuchAccount(_) => PolicyError::because(UNKNOWN_PRINCIPAL),
        _ => PolicyError::unavailable(),
    }
}

/// Root ownership as the legacy edge decides it (`rustfs/src/auth.rs` `has_root_access`): the
/// credential is root's or acts for root, and neither a session policy nor an external OIDC
/// identity narrows it.
fn acts_for_root(cred: &IamCredential, claims: Option<&HashMap<String, Value>>, root_access_key: &str) -> bool {
    (cred.access_key == root_access_key || cred.parent_user == root_access_key)
        && !claims.is_some_and(|claims| claims.contains_key(SESSION_POLICY_NAME) || is_rustfs_oidc_claims(claims))
}

impl PolicySource for IamPolicySource {
    fn snapshot<'a>(&'a self, identity: Option<&'a Identity>) -> BoxFuture<'a, Result<PolicySnapshot, PolicyError>> {
        Box::pin(self.read(identity))
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::root::RootCell;
    use rustfs_credentials::{Credentials as IamCredential, IAM_POLICY_CLAIM_NAME_SA};
    use rustfs_gateway::SessionBinding;
    use rustfs_iam::error::Error as IamError;
    use rustfs_iam::sys::SESSION_POLICY_NAME;
    use rustfs_iam::utils::generate_jwt;
    use rustfs_policy::auth::UserIdentity;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use time::OffsetDateTime;

    const ROOT_ACCESS_KEY: &str = "rootadmin";
    const ROOT_SECRET: &str = "root-secret-key-0001";
    /// Deliberately not the root secret, so every STS test proves which key claims are read with.
    const SIGNING_KEY: &str = "sts-signing-key-0001";
    const ACCESS_KEY: &str = "AKIDUSER0001";
    const SECRET: &str = "user-secret-key-0001";
    const PARENT: &str = "alice";
    const EXPIRES_AT: i64 = 4_000_000_000;

    type KeyAnswer = Result<(Option<UserIdentity>, bool), IamError>;
    type Answer = Box<dyn Fn() -> KeyAnswer + Send + Sync>;

    struct FakeIam {
        ready: bool,
        answer: Answer,
        calls: AtomicUsize,
    }

    impl IamKeyLookup for FakeIam {
        fn is_ready(&self) -> bool {
            self.ready
        }

        fn check_key<'a>(&'a self, _access_key: &'a str) -> BoxFuture<'a, KeyAnswer> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let answer = (self.answer)();
            Box::pin(async move { answer })
        }
    }

    impl IamPrincipalLookup for FakeIam {
        /// Distinct groups per name, so a test can tell which name the source asked about.
        fn group_memberships(&self, user: &str) -> Vec<String> {
            match user {
                PARENT => vec!["parent-group".to_owned()],
                ACCESS_KEY => vec!["key-group".to_owned()],
                ROOT_ACCESS_KEY => vec!["root-group".to_owned()],
                _ => Vec::new(),
            }
        }
    }

    fn fake(ready: bool, answer: impl Fn() -> KeyAnswer + Send + Sync + 'static) -> Arc<FakeIam> {
        Arc::new(FakeIam {
            ready,
            answer: Box::new(answer),
            calls: AtomicUsize::new(0),
        })
    }

    fn root() -> RootCredentials {
        RootCredentials::new(ROOT_ACCESS_KEY, ROOT_SECRET, SIGNING_KEY).expect("valid root credentials")
    }

    fn source(iam: Arc<FakeIam>) -> IamPolicySource {
        IamPolicySource::new(iam, Arc::new(RootCell::new(Some(root()))))
    }

    fn user() -> IamCredential {
        IamCredential {
            access_key: ACCESS_KEY.to_owned(),
            secret_key: SECRET.to_owned(),
            status: "on".to_owned(),
            groups: Some(vec!["cred-group".to_owned()]),
            ..Default::default()
        }
    }

    fn sts_token(claims: &[(&str, Value)], secret: &str) -> String {
        let mut map: HashMap<String, Value> = claims.iter().map(|(k, v)| ((*k).to_owned(), v.clone())).collect();
        map.insert("exp".to_owned(), Value::from(EXPIRES_AT));
        generate_jwt(&map, secret).expect("a token")
    }

    fn sts(parent: &str, token: String) -> IamCredential {
        IamCredential {
            session_token: token,
            parent_user: parent.to_owned(),
            expiration: Some(OffsetDateTime::from_unix_timestamp(EXPIRES_AT).expect("valid timestamp")),
            ..user()
        }
    }

    fn found(cred: IamCredential) -> KeyAnswer {
        Ok((Some(UserIdentity::new(cred)), true))
    }

    fn long_term(access_key: &str) -> Identity {
        Identity::new(access_key).expect("valid access key")
    }

    fn session(access_key: &str) -> Identity {
        let binding = SessionBinding::new(crate::STS_SESSION_ISSUER, EXPIRES_AT).expect("valid binding");
        long_term(access_key).with_session_binding(binding)
    }

    async fn principal_of(source: &IamPolicySource, identity: &Identity) -> PolicySnapshot {
        source.snapshot(Some(identity)).await.expect("a snapshot")
    }

    fn payload(snapshot: &PolicySnapshot) -> &RustfsPrincipal {
        snapshot.get::<RustfsPrincipal>().expect("the payload is a RustfsPrincipal")
    }

    fn refusal(result: Result<PolicySnapshot, PolicyError>) -> PolicyError {
        match result {
            Ok(snapshot) => panic!("expected a refusal, got a snapshot (empty: {})", snapshot.is_empty()),
            Err(error) => error,
        }
    }

    /// Compile-time: `RustfsPrincipal` has no `Debug`, so no formatting of a snapshot payload can
    /// reach its claims. The two blanket impls make the path ambiguous, and so a compile error, as
    /// soon as `RustfsPrincipal: Debug` holds.
    #[test]
    fn policy_principal_has_no_debug() {
        trait AmbiguousIfDebug<A> {
            fn some_item() {}
        }
        impl<T: ?Sized> AmbiguousIfDebug<()> for T {}
        impl<T: ?Sized + core::fmt::Debug> AmbiguousIfDebug<u8> for T {}
        let _ = <RustfsPrincipal as AmbiguousIfDebug<_>>::some_item;
    }

    /// Positive — a long-term user's facts: itself as username, both group sources, no claims,
    /// not the owner.
    #[tokio::test]
    async fn policy_user_snapshot_carries_iam_facts() {
        let iam = fake(true, || found(user()));
        let snapshot = principal_of(&source(iam), &long_term(ACCESS_KEY)).await;
        let principal = payload(&snapshot);
        assert!(principal.kind() == PrincipalKind::User);
        assert_eq!(principal.account(), ACCESS_KEY);
        assert_eq!(principal.username(), ACCESS_KEY);
        assert_eq!(principal.parent_user(), "");
        assert_eq!(principal.credential_groups(), &Some(vec!["cred-group".to_owned()]));
        assert_eq!(principal.group_memberships(), ["key-group".to_owned()]);
        assert!(principal.claims().is_none());
        assert!(!principal.is_owner());
    }

    /// Positive — an STS credential acts for its parent user, and its claims are the ones its
    /// token carries, read with the root secret that signed it.
    #[tokio::test]
    async fn policy_sts_snapshot_carries_parent_user_and_claims() {
        let token = sts_token(&[("sub", Value::from("alice-session"))], SIGNING_KEY);
        let iam = fake(true, move || found(sts(PARENT, token.clone())));
        let snapshot = principal_of(&source(iam), &session(ACCESS_KEY)).await;
        let principal = payload(&snapshot);
        assert!(principal.kind() == PrincipalKind::Temporary);
        assert_eq!(principal.account(), ACCESS_KEY);
        assert_eq!(principal.parent_user(), PARENT);
        assert_eq!(principal.username(), PARENT);
        let claims = principal.claims().expect("an STS principal has claims");
        assert_eq!(claims.get("sub"), Some(&Value::from("alice-session")));
        assert_eq!(principal.group_memberships(), ["parent-group".to_owned()]);
        assert!(!principal.is_owner());
    }

    /// Positive — a service account's claims are read with its own secret, and it acts for its
    /// parent.
    #[tokio::test]
    async fn policy_service_account_snapshot_reads_its_own_token() {
        let mut token_claims: HashMap<String, Value> = HashMap::new();
        token_claims.insert(IAM_POLICY_CLAIM_NAME_SA.to_owned(), Value::from("inherited-policy"));
        token_claims.insert("parent".to_owned(), Value::from(PARENT));
        let token = generate_jwt(&token_claims, SECRET).expect("a token");
        let iam = fake(true, move || {
            let mut cred = user();
            cred.session_token = token.clone();
            cred.parent_user = PARENT.to_owned();
            cred.claims = Some([(IAM_POLICY_CLAIM_NAME_SA.to_owned(), Value::from("inherited-policy"))].into());
            found(cred)
        });
        let snapshot = principal_of(&source(iam), &long_term(ACCESS_KEY)).await;
        let principal = payload(&snapshot);
        assert!(principal.kind() == PrincipalKind::ServiceAccount);
        assert_eq!(principal.username(), PARENT);
        assert_eq!(principal.claims().and_then(|c| c.get("parent")), Some(&Value::from(PARENT)));
        assert_eq!(principal.group_memberships(), ["parent-group".to_owned()]);
        assert!(!principal.is_owner());
    }

    /// Positive and negative — root is the owner without asking IAM, even before IAM is ready;
    /// an STS credential of root is the owner too, but not once a session policy narrows it.
    #[tokio::test]
    async fn policy_owner_is_root_or_unrestricted_root_session() {
        let not_ready = fake(false, || found(user()));
        let snapshot = principal_of(&source(not_ready.clone()), &long_term(ROOT_ACCESS_KEY)).await;
        let root = payload(&snapshot);
        assert!(root.kind() == PrincipalKind::Root);
        assert!(root.is_owner());
        assert_eq!(not_ready.calls.load(Ordering::SeqCst), 0);

        let plain = sts_token(&[], SIGNING_KEY);
        let iam = fake(true, move || found(sts(ROOT_ACCESS_KEY, plain.clone())));
        assert!(payload(&principal_of(&source(iam), &session(ACCESS_KEY)).await).is_owner());

        let narrowed = sts_token(&[(SESSION_POLICY_NAME, Value::from("e30"))], SIGNING_KEY);
        let iam = fake(true, move || found(sts(ROOT_ACCESS_KEY, narrowed.clone())));
        assert!(!payload(&principal_of(&source(iam), &session(ACCESS_KEY)).await).is_owner());
    }

    /// Negative — an anonymous caller is an empty snapshot, and IAM is not asked.
    #[tokio::test]
    async fn policy_anonymous_is_empty_snapshot() {
        let iam = fake(true, || found(user()));
        let snapshot = source(iam.clone()).snapshot(None).await.expect("an anonymous snapshot");
        assert!(snapshot.is_empty());
        assert_eq!(iam.calls.load(Ordering::SeqCst), 0);
    }

    /// Negative — IAM that is not ready, not initialized, or failing is `unavailable`: never an
    /// empty snapshot, which an authorizer would judge as an anonymous caller.
    #[tokio::test]
    async fn policy_iam_unavailable_is_unavailable() {
        let not_ready = fake(false, || found(user()));
        let error = refusal(source(not_ready.clone()).snapshot(Some(&long_term(ACCESS_KEY))).await);
        assert_eq!(error, PolicyError::unavailable());
        assert_eq!(not_ready.calls.load(Ordering::SeqCst), 0);

        let failures: [fn() -> IamError; 2] = [
            || IamError::IamSysNotInitialized,
            || IamError::StringError("storage read failed".to_owned()),
        ];
        for failure in failures {
            let iam = fake(true, move || Err(failure()));
            let error = refusal(source(iam).snapshot(Some(&long_term(ACCESS_KEY))).await);
            assert_eq!(error, PolicyError::unavailable(), "{:?}", failure());
        }
    }

    /// Negative — a principal IAM no longer knows is refused as unknown, not as unavailable and
    /// not as an empty snapshot.
    #[tokio::test]
    async fn policy_unknown_principal_is_refused() {
        let answers: [fn() -> KeyAnswer; 2] = [|| Ok((None, false)), || Err(IamError::NoSuchUser(ACCESS_KEY.to_owned()))];
        for answer in answers {
            let error = refusal(source(fake(true, answer)).snapshot(Some(&long_term(ACCESS_KEY))).await);
            assert_eq!(error, PolicyError::because(UNKNOWN_PRINCIPAL));
        }
    }

    /// Negative — a disabled or invalid principal is refused rather than given a snapshot.
    #[tokio::test]
    async fn policy_disabled_principal_is_refused() {
        let invalid = fake(true, || Ok((Some(UserIdentity::new(user())), false)));
        let error = refusal(source(invalid).snapshot(Some(&long_term(ACCESS_KEY))).await);
        assert_eq!(error, PolicyError::because(DISABLED_PRINCIPAL));

        let off = fake(true, || {
            let mut cred = user();
            cred.status = "off".to_owned();
            found(cred)
        });
        let error = refusal(source(off).snapshot(Some(&long_term(ACCESS_KEY))).await);
        assert_eq!(error, PolicyError::because(DISABLED_PRINCIPAL));
    }

    /// Negative — an STS token not signed by root yields no claims at all: it is `malformed`.
    #[tokio::test]
    async fn policy_sts_token_with_foreign_signature_is_malformed() {
        let forged = sts_token(&[("sub", Value::from("alice-session"))], "not-the-root-secret");
        let iam = fake(true, move || found(sts(PARENT, forged.clone())));
        let error = refusal(source(iam).snapshot(Some(&session(ACCESS_KEY))).await);
        assert_eq!(error, PolicyError::malformed());
    }

    /// Negative — the identity's credential kind must match what IAM holds: a session identity for
    /// a long-term key, or a long-term identity for an STS key, is refused.
    #[tokio::test]
    async fn policy_session_kind_mismatch_is_refused() {
        let token = sts_token(&[], SIGNING_KEY);
        let iam = fake(true, move || found(sts(PARENT, token.clone())));
        let error = refusal(source(iam).snapshot(Some(&long_term(ACCESS_KEY))).await);
        assert_eq!(error, PolicyError::because(SESSION_MISMATCH));

        let iam = fake(true, || found(user()));
        let error = refusal(source(iam).snapshot(Some(&session(ACCESS_KEY))).await);
        assert_eq!(error, PolicyError::because(SESSION_MISMATCH));

        let iam = fake(false, || found(user()));
        let error = refusal(source(iam).snapshot(Some(&session(ROOT_ACCESS_KEY))).await);
        assert_eq!(error, PolicyError::because(SESSION_MISMATCH));
    }

    fn service_account(parent: &str, extra_claims: &[(&str, Value)]) -> KeyAnswer {
        let mut token_claims: HashMap<String, Value> = extra_claims.iter().map(|(k, v)| ((*k).to_owned(), v.clone())).collect();
        token_claims.insert(IAM_POLICY_CLAIM_NAME_SA.to_owned(), Value::from("inherited-policy"));
        let mut cred = user();
        cred.session_token = generate_jwt(&token_claims, SECRET).expect("a token");
        cred.parent_user = parent.to_owned();
        cred.claims = Some([(IAM_POLICY_CLAIM_NAME_SA.to_owned(), Value::from("inherited-policy"))].into());
        found(cred)
    }

    /// Negative — a valid-flagged identity whose secret is empty or short gets no snapshot.
    #[tokio::test]
    async fn policy_short_secret_principal_is_refused() {
        for weak in ["", "1234567", "\0\0\0\0\0\0\0\0", "abc\0\0\0\0\0"] {
            let iam = fake(true, move || {
                let mut cred = user();
                cred.secret_key = weak.to_owned();
                found(cred)
            });
            let error = refusal(source(iam).snapshot(Some(&long_term(ACCESS_KEY))).await);
            assert_eq!(error, PolicyError::because(DISABLED_PRINCIPAL), "{weak:?}");
        }
        for usable in ["12345678", "abc\0defg"] {
            let iam = fake(true, move || {
                let mut cred = user();
                cred.secret_key = usable.to_owned();
                found(cred)
            });
            assert!(payload(&principal_of(&source(iam), &long_term(ACCESS_KEY)).await).kind() == PrincipalKind::User);
        }
    }

    /// Negative — a root session issued through external OIDC is not the owner, though its parent
    /// is root; the same session without the OIDC issuer is.
    #[tokio::test]
    async fn policy_oidc_root_session_is_not_owner() {
        let oidc = sts_token(
            &[
                ("iss", Value::from("rustfs-oidc")),
                ("oidc_provider", Value::from("corp-idp")),
                ("sub", Value::from("alice@corp")),
            ],
            SIGNING_KEY,
        );
        let iam = fake(true, move || found(sts(ROOT_ACCESS_KEY, oidc.clone())));
        assert!(!payload(&principal_of(&source(iam), &session(ACCESS_KEY)).await).is_owner());

        let local = sts_token(
            &[
                ("iss", Value::from("rustfs-sts")),
                ("oidc_provider", Value::from("corp-idp")),
                ("sub", Value::from("alice@corp")),
            ],
            SIGNING_KEY,
        );
        let iam = fake(true, move || found(sts(ROOT_ACCESS_KEY, local.clone())));
        assert!(payload(&principal_of(&source(iam), &session(ACCESS_KEY)).await).is_owner());
    }

    /// Positive and negative — a service account of root is the owner; one narrowed by a session
    /// policy is not, and neither is a service account of an ordinary user.
    #[tokio::test]
    async fn policy_service_account_of_root_is_owner() {
        let iam = fake(true, || service_account(ROOT_ACCESS_KEY, &[]));
        let principal = principal_of(&source(iam), &long_term(ACCESS_KEY)).await;
        assert!(payload(&principal).kind() == PrincipalKind::ServiceAccount);
        assert!(payload(&principal).is_owner());

        let iam = fake(true, || service_account(ROOT_ACCESS_KEY, &[(SESSION_POLICY_NAME, Value::from("e30"))]));
        assert!(!payload(&principal_of(&source(iam), &long_term(ACCESS_KEY)).await).is_owner());

        let iam = fake(true, || service_account(PARENT, &[]));
        assert!(!payload(&principal_of(&source(iam), &long_term(ACCESS_KEY)).await).is_owner());
    }

    /// Negative — STS claims are read with the STS signing key, not with the root secret: a token
    /// signed with the root secret while the signing key differs is `malformed`.
    #[tokio::test]
    async fn policy_sts_claims_use_the_signing_key() {
        let root_signed = sts_token(&[("sub", Value::from("alice-session"))], ROOT_SECRET);
        let iam = fake(true, move || found(sts(PARENT, root_signed.clone())));
        let error = refusal(source(iam).snapshot(Some(&session(ACCESS_KEY))).await);
        assert_eq!(error, PolicyError::malformed());
    }

    /// Negative — without root credentials no principal can be judged: `unavailable`, never an
    /// empty snapshot, and IAM is not asked.
    #[tokio::test]
    async fn policy_without_root_is_unavailable() {
        let iam = fake(true, || found(user()));
        let source = IamPolicySource::new(iam.clone(), Arc::new(RootCell::new(None)));
        for identity in [long_term(ACCESS_KEY), long_term(ROOT_ACCESS_KEY)] {
            assert_eq!(refusal(source.snapshot(Some(&identity)).await), PolicyError::unavailable());
        }
        assert_eq!(iam.calls.load(Ordering::SeqCst), 0);
    }

    type LiveRoot = Arc<std::sync::Mutex<Option<(String, String)>>>;

    /// The primary path: a source that reads a live accessor on every call, as the binary's does,
    /// with nothing cached in between.
    fn live_root_source(live: &LiveRoot) -> Arc<dyn RootSource> {
        let accessor = live.clone();
        Arc::new(move || {
            let guard = accessor.lock().ok()?;
            let (access_key, secret_key) = guard.as_ref()?;
            RootCredentials::new(access_key, secret_key, SIGNING_KEY).ok().map(Arc::new)
        })
    }

    fn set_live(live: &LiveRoot, value: Option<(&str, &str)>) {
        *live.lock().expect("live root lock") = value.map(|(ak, sk)| (ak.to_owned(), sk.to_owned()));
    }

    /// Positive and negative — the two adapters read one live root accessor: a root change is seen
    /// by both on the next request with no second step, the old root key stops being root to
    /// both, and an uninitialized or unusable root makes both unavailable.
    #[tokio::test]
    async fn adapters_share_one_live_root() {
        use rustfs_gateway::{CredentialLookup, CredentialProvider, ProviderError};

        let live: LiveRoot = Arc::default();
        set_live(&live, Some((ROOT_ACCESS_KEY, ROOT_SECRET)));
        let iam = fake(true, || Ok((None, false)));
        let adapters = crate::IamAdapters::new(iam, live_root_source(&live));

        let found = adapters.credentials.lookup(ROOT_ACCESS_KEY).await;
        assert!(matches!(found, Ok(CredentialLookup::Found(_))));
        let snapshot = principal_of(&adapters.policy, &long_term(ROOT_ACCESS_KEY)).await;
        assert!(payload(&snapshot).is_owner());

        set_live(&live, Some(("newroot", "new-root-secret-01")));
        match adapters.credentials.lookup("newroot").await {
            Ok(CredentialLookup::Found(credentials)) => assert_eq!(credentials.secret().expose(), b"new-root-secret-01"),
            _ => panic!("the new root key must resolve to the new secret"),
        }
        assert!(matches!(
            adapters.credentials.lookup(ROOT_ACCESS_KEY).await,
            Ok(CredentialLookup::NotFound)
        ));
        assert!(payload(&principal_of(&adapters.policy, &long_term("newroot")).await).is_owner());
        let old = refusal(adapters.policy.snapshot(Some(&long_term(ROOT_ACCESS_KEY))).await);
        assert_eq!(old, PolicyError::because(UNKNOWN_PRINCIPAL));

        for unusable in [None, Some(("newroot", "\0\0\0\0\0\0\0\0"))] {
            set_live(&live, unusable);
            assert!(matches!(adapters.credentials.lookup("newroot").await, Err(ProviderError::Unavailable)));
            assert_eq!(
                refusal(adapters.policy.snapshot(Some(&long_term("newroot"))).await),
                PolicyError::unavailable()
            );
        }
    }

    /// Positive — `install` builds the authenticator exactly once, over a provider that reads the
    /// pair's root source.
    #[tokio::test]
    async fn adapters_install_hands_over_the_paired_provider_once() {
        use rustfs_gateway::{CredentialLookup, CredentialProvider, RegionSet, ServiceBuilder, SigV4Authenticator};

        let live: LiveRoot = Arc::default();
        set_live(&live, Some((ROOT_ACCESS_KEY, ROOT_SECRET)));
        let adapters = crate::IamAdapters::new(fake(true, || Ok((None, false))), live_root_source(&live));
        let mut handed = Vec::new();
        let _builder = adapters.install(ServiceBuilder::new(), |provider| {
            handed.push(provider.clone());
            SigV4Authenticator::new(provider, RegionSet::new(["us-east-1"]).expect("a region"))
        });
        assert_eq!(handed.len(), 1);
        let found = handed[0].lookup(ROOT_ACCESS_KEY).await;
        assert!(matches!(found, Ok(CredentialLookup::Found(_))));
    }
}
