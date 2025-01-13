use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, AtomicI64, Ordering},
        Arc,
    },
    time::Duration,
};

use ecstore::store_err::is_err_object_not_found;
use log::{debug, warn};
use madmin::AccountStatus;
use time::OffsetDateTime;
use tokio::{
    select,
    sync::{
        mpsc,
        mpsc::{Receiver, Sender},
    },
};

use crate::{
    arn::ARN,
    auth::{self, Credentials, UserIdentity},
    cache::Cache,
    format::Format,
    handler::Handler,
    policy::{Args, Policy, UserType},
    store::Store,
    Error,
};

pub struct IamCache<T> {
    pub cache: Cache,
    pub api: T,
    pub loading: Arc<AtomicBool>,
    pub roles: HashMap<ARN, Vec<String>>,
    pub send_chan: Sender<i64>,
    pub last_timestamp: AtomicI64,
}

impl<T> IamCache<T>
where
    T: Store,
{
    pub(crate) async fn new(api: T) -> Arc<Self> {
        let (sender, reciver) = mpsc::channel::<i64>(100);

        let sys = Arc::new(Self {
            api,
            cache: Cache::default(),
            loading: Arc::new(AtomicBool::new(false)),
            send_chan: sender,
            roles: HashMap::new(),
            last_timestamp: AtomicI64::new(0),
        });

        sys.clone().init(reciver).await.unwrap();
        sys
    }

    async fn init(self: Arc<Self>, reciver: Receiver<i64>) -> crate::Result<()> {
        self.clone().save_iam_formatter().await?;
        self.clone().load().await?;

        // 后台线程开启定时更新或者接收到信号更新
        tokio::spawn({
            let s = Arc::clone(&self);
            async move {
                let ticker = tokio::time::interval(Duration::from_secs(120));
                tokio::pin!(ticker, reciver);
                loop {
                    select! {
                        _ = ticker.tick() => {
                            s.clone().load().await.unwrap();
                        },
                        i = reciver.recv() => {
                            match i {
                                Some(t) => {
                                    let last = s.last_timestamp.load(Ordering::Relaxed);
                                    if last <= t {
                                        s.clone().load().await.unwrap();
                                        ticker.reset();
                                    }
                                },
                                None => return,
                            }
                        }
                    }
                }
            }
        });

        Ok(())
    }

    async fn notify(&self) {
        self.send_chan.send(OffsetDateTime::now_utc().unix_timestamp()).await.unwrap();
    }

    async fn load(self: Arc<Self>) -> crate::Result<()> {
        debug!("load iam to cache");
        self.api.load_all(&self.cache).await?;
        self.last_timestamp
            .store(OffsetDateTime::now_utc().unix_timestamp(), Ordering::Relaxed);
        Ok(())
    }

    pub async fn list_all_iam_config_items(&self) -> crate::Result<HashMap<String, Vec<String>>> {
        todo!()
    }

    // todo, 判断是否存在，是否可以重试
    #[tracing::instrument(level = "debug", skip(self))]
    async fn save_iam_formatter(self: Arc<Self>) -> crate::Result<()> {
        match self.api.load_iam_config::<Format>(Format::PATH).await {
            Ok((format, _)) if format.version >= 1 => return Ok(()),
            Err(Error::EcstoreError(e)) if !is_err_object_not_found(&e) => {
                return Err(Error::EcstoreError(e));
            }
            _ => {}
        }

        self.api.save_iam_config(Format::new(), Format::PATH).await?;
        Ok(())
    }

    pub async fn list_service_accounts(&self, access_key: &str) -> crate::Result<Vec<Credentials>> {
        let users = self.cache.users.load();
        Ok(users
            .values()
            .filter_map(|x| {
                if !access_key.is_empty()
                    && x.credentials.parent_user.as_str() == access_key
                    && x.credentials.is_service_account()
                {
                    let mut c = x.credentials.clone();
                    c.secret_key = String::new();
                    c.session_token = String::new();
                    return Some(c);
                }

                None
            })
            .collect())
    }

    /// create a service account and update cache
    pub async fn add_service_account(&self, cred: Credentials) -> crate::Result<OffsetDateTime> {
        if cred.parent_user.is_empty() {
            return Err(Error::InvalidArgument);
        }

        if (cred.access_key.is_empty() && !cred.secret_key.is_empty())
            || (!cred.access_key.is_empty() && cred.secret_key.is_empty())
        {
            return Err(Error::StringError("Either ak or sk is empty".into()));
        }

        let users = self.cache.users.load();
        if let Some(x) = users.get(&cred.access_key) {
            if x.credentials.parent_user.as_str() != cred.parent_user.as_str() {
                return Err(crate::Error::StringError("access key is taken by another user".into()));
            }
            return Err(crate::Error::StringError("access key already taken".into()));
        }

        if let Some(x) = users.get(&cred.parent_user) {
            if x.credentials.is_service_account() {
                return Err(crate::Error::StringError(
                    "unable to create a service account for another service account".into(),
                ));
            }
        }

        let user_entiry = UserIdentity::from(cred);
        let path = format!(
            "config/iam/{}{}/identity.json",
            UserType::Svc.prefix(),
            user_entiry.credentials.access_key
        );
        debug!("save object: {path:?}");
        self.api.save_iam_config(&user_entiry, path).await?;

        Cache::add_or_update(
            &self.cache.users,
            &user_entiry.credentials.access_key,
            &user_entiry,
            OffsetDateTime::now_utc(),
        );

        Ok(user_entiry.update_at.unwrap_or(OffsetDateTime::now_utc()))
    }

    pub async fn is_allowed<'a>(&self, args: Args<'a>) -> bool {
        let handler = Handler::new((&self.cache).into(), &self.api, &self.roles);
        handler.is_allowed(args).await
    }

    pub async fn get_service_account(&self, ak: &str) -> crate::Result<(UserIdentity, Option<Policy>)> {
        let user = self.cache.users.load();
        let Some(u) = user.get(ak) else {
            return Err(Error::StringError("no service account".into()));
        };

        // if !u.credentials.is_service_account() {
        //     return Err(Error::StringError("it is not service account".into()));
        // }

        Ok((u.clone(), None))
    }

    pub async fn check_key(&self, ak: &str) -> crate::Result<(Option<UserIdentity>, bool)> {
        let user = self
            .cache
            .users
            .load()
            .get(ak)
            .cloned()
            .or_else(|| self.cache.sts_accounts.load().get(ak).cloned());

        match user {
            Some(u) => {
                if u.credentials.is_valid() {
                    Ok((Some(u), true))
                } else {
                    Ok((Some(u), false))
                }
            }
            _ => Ok((None, false)),
        }
    }
    pub async fn policy_db_get(&self, name: &str, _groups: Option<Vec<String>>) -> crate::Result<Vec<String>> {
        // let user = self.cache.users.load();
        // let Some(u) = user.get(name) else {
        //     return Err(Error::StringError("no service account".into()));
        // };

        let policies = self.cache.user_policies.load();

        let user_policies = {
            if let Some(p) = policies.get(name) {
                p.to_slice()
            } else {
                Vec::new()
            }
        };

        // TODO: groups

        Ok(user_policies)
    }

    pub async fn set_temp_user(&self, _access_key: &str, cred: &Credentials, _policy_name: &str) -> crate::Result<()> {
        let user_entiry = UserIdentity::from(cred.clone());
        let path = format!(
            "config/iam/{}{}/identity.json",
            UserType::Sts.prefix(),
            user_entiry.credentials.access_key
        );
        debug!("save object: {path:?}");
        self.api.save_iam_config(&user_entiry, path).await?;

        Cache::add_or_update(
            &self.cache.sts_accounts,
            &user_entiry.credentials.access_key,
            &user_entiry,
            OffsetDateTime::now_utc(),
        );

        Ok(())
    }

    // returns all users (not STS or service accounts)
    pub async fn get_users(&self) -> crate::Result<HashMap<String, madmin::UserInfo>> {
        let mut m = HashMap::new();

        let users = self.cache.users.load();
        let policies = self.cache.user_policies.load();
        let group_members = self.cache.user_group_memeberships.load();

        for (k, v) in users.iter() {
            warn!("k: {}, v: {:?}", k, v.credentials);

            if v.credentials.is_temp() || v.credentials.is_service_account() {
                continue;
            }

            let mut u = madmin::UserInfo {
                status: if v.credentials.is_valid() {
                    madmin::AccountStatus::Enabled
                } else {
                    madmin::AccountStatus::Disabled
                },
                updated_at: v.update_at,
                ..Default::default()
            };

            warn!("uinfo {:?}", u);

            if let Some(p) = policies.get(k) {
                u.policy_name = Some(p.policies.clone());
                u.updated_at = Some(p.update_at);
            }

            if let Some(members) = group_members.get(k) {
                u.member_of = Some(members.iter().cloned().collect());
            }

            m.insert(k.clone(), u);
        }

        Ok(m)
    }

    pub async fn add_user(&self, access_key: &str, secret_key: &str, status: &str) -> crate::Result<OffsetDateTime> {
        let status = {
            match status {
                val if val == AccountStatus::Enabled.as_ref() => auth::ACCOUNT_ON,
                auth::ACCOUNT_ON => auth::ACCOUNT_ON,
                _ => auth::ACCOUNT_OFF,
            }
        };

        let users = self.cache.users.load();
        if let Some(x) = users.get(access_key) {
            warn!("user already exists: {:?}", x);
            if x.credentials.is_temp() {
                return Err(crate::Error::IAMActionNotAllowed);
            }
        }

        let user_entiry = UserIdentity::from(Credentials {
            access_key: access_key.to_string(),
            secret_key: secret_key.to_string(),
            status: status.to_owned(),
            ..Default::default()
        });
        let path = format!(
            "config/iam/{}{}/identity.json",
            UserType::Reg.prefix(),
            user_entiry.credentials.access_key
        );
        debug!("save object: {path:?}");
        self.api.save_iam_config(&user_entiry, path).await?;

        Cache::add_or_update(
            &self.cache.users,
            &user_entiry.credentials.access_key,
            &user_entiry,
            OffsetDateTime::now_utc(),
        );

        Ok(user_entiry.update_at.unwrap_or(OffsetDateTime::now_utc()))
    }

    pub async fn delete_user(&self, access_key: &str, utype: UserType) -> crate::Result<()> {
        let users = self.cache.users.load();
        if let Some(x) = users.get(access_key) {
            if x.credentials.is_temp() {
                return Err(crate::Error::IAMActionNotAllowed);
            }
        }

        // if utype == UserType::Reg {}

        let path = format!("config/iam/{}{}/identity.json", UserType::Reg.prefix(), access_key);
        debug!("delete object: {path:?}");
        self.api.delete_iam_config(path).await?;

        // delete cache
        Cache::delete(&self.cache.users, access_key, OffsetDateTime::now_utc());

        Ok(())
    }

    pub async fn get_user(&self, access_key: &str) -> crate::Result<Option<UserIdentity>> {
        let u = self
            .cache
            .users
            .load()
            .get(access_key)
            .cloned()
            .or_else(|| self.cache.sts_accounts.load().get(access_key).cloned());

        Ok(u)
    }

    pub async fn get_user_info(&self, access_key: &str) -> crate::Result<madmin::UserInfo> {
        let users = self.cache.users.load();
        let policies = self.cache.user_policies.load();
        let group_members = self.cache.user_group_memeberships.load();

        let u = match users.get(access_key) {
            Some(u) => u,
            None => return Err(Error::NoSuchUser(access_key.to_string())),
        };

        if u.credentials.is_temp() || u.credentials.is_service_account() {
            return Err(Error::IAMActionNotAllowed);
        }

        let mut uinfo = madmin::UserInfo {
            status: if u.credentials.is_valid() {
                madmin::AccountStatus::Enabled
            } else {
                madmin::AccountStatus::Disabled
            },
            updated_at: u.update_at,
            ..Default::default()
        };

        if let Some(p) = policies.get(access_key) {
            uinfo.policy_name = Some(p.policies.clone());
            uinfo.updated_at = Some(p.update_at);
        }

        if let Some(members) = group_members.get(access_key) {
            uinfo.member_of = Some(members.iter().cloned().collect());
        }

        Ok(uinfo)
    }

    pub async fn set_user_status(&self, access_key: &str, status: AccountStatus) -> crate::Result<OffsetDateTime> {
        if access_key.is_empty() {
            return Err(Error::InvalidArgument);
        }

        let users = self.cache.users.load();
        let u = match users.get(access_key) {
            Some(u) => u,
            None => return Err(Error::NoSuchUser(access_key.to_string())),
        };

        if u.credentials.is_temp() || u.credentials.is_service_account() {
            return Err(Error::IAMActionNotAllowed);
        }

        let status = {
            match status {
                AccountStatus::Enabled => auth::ACCOUNT_ON,
                _ => auth::ACCOUNT_OFF,
            }
        };

        let user_entiry = UserIdentity::from(Credentials {
            access_key: access_key.to_string(),
            secret_key: u.credentials.secret_key.clone(),
            status: status.to_owned(),
            ..Default::default()
        });
        let path = format!(
            "config/iam/{}{}/identity.json",
            UserType::Reg.prefix(),
            user_entiry.credentials.access_key
        );
        debug!("save object: {path:?}");
        self.api.save_iam_config(&user_entiry, path).await?;

        Cache::add_or_update(
            &self.cache.users,
            &user_entiry.credentials.access_key,
            &user_entiry,
            OffsetDateTime::now_utc(),
        );

        Ok(user_entiry.update_at.unwrap_or(OffsetDateTime::now_utc()))
    }
}
