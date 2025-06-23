use bytes::Bytes;
use http::status::StatusCode;
use lazy_static::lazy_static;
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, hash_map::Entry},
    io::Cursor,
    sync::Arc,
    time::Duration,
};
use time::OffsetDateTime;
use tokio::io::BufReader;
use tokio::{select, sync::RwLock, time::interval};
use tracing::{debug, error, info, warn};

use crate::client::admin_handler_utils::AdminError;
use crate::error::{Error, Result, StorageError};
use crate::new_object_layer_fn;
use crate::tier::{
    tier_admin::TierCreds,
    tier_config::{TierConfig, TierType},
    tier_handlers::{ERR_TIER_ALREADY_EXISTS, ERR_TIER_NAME_NOT_UPPERCASE, ERR_TIER_NOT_FOUND},
    warm_backend::{check_warm_backend, new_warm_backend},
};
use crate::{
    StorageAPI,
    config::com::{CONFIG_PREFIX, read_config},
    disk::RUSTFS_META_BUCKET,
    store::ECStore,
    store_api::{ObjectOptions, PutObjReader},
};
use rustfs_rio::HashReader;
use rustfs_utils::path::{SLASH_SEPARATOR, path_join};
use s3s::S3ErrorCode;

use super::{
    tier_handlers::{ERR_TIER_BUCKET_NOT_FOUND, ERR_TIER_CONNECT_ERR, ERR_TIER_INVALID_CREDENTIALS, ERR_TIER_PERM_ERR},
    warm_backend::WarmBackendImpl,
};

const TIER_CFG_REFRESH: Duration = Duration::from_secs(15 * 60);

pub const TIER_CONFIG_FILE: &str = "tier-config.json";
pub const TIER_CONFIG_FORMAT: u16 = 1;
pub const TIER_CONFIG_V1: u16 = 1;
pub const TIER_CONFIG_VERSION: u16 = 1;

lazy_static! {
    //pub static ref TIER_CONFIG_PATH: PathBuf = path_join(&[PathBuf::from(RUSTFS_CONFIG_PREFIX), PathBuf::from(TIER_CONFIG_FILE)]);
}

const TIER_CFG_REFRESH_AT_HDR: &str = "X-RustFS-TierCfg-RefreshedAt";

pub const ERR_TIER_MISSING_CREDENTIALS: AdminError = AdminError {
    code: "XRustFSAdminTierMissingCredentials",
    message: "Specified remote credentials are empty",
    status_code: StatusCode::FORBIDDEN,
};

pub const ERR_TIER_BACKEND_IN_USE: AdminError = AdminError {
    code: "XRustFSAdminTierBackendInUse",
    message: "Specified remote tier is already in use",
    status_code: StatusCode::CONFLICT,
};

pub const ERR_TIER_TYPE_UNSUPPORTED: AdminError = AdminError {
    code: "XRustFSAdminTierTypeUnsupported",
    message: "Specified tier type is unsupported",
    status_code: StatusCode::BAD_REQUEST,
};

pub const ERR_TIER_BACKEND_NOT_EMPTY: AdminError = AdminError {
    code: "XRustFSAdminTierBackendNotEmpty",
    message: "Specified remote backend is not empty",
    status_code: StatusCode::BAD_REQUEST,
};

pub const ERR_TIER_INVALID_CONFIG: AdminError = AdminError {
    code: "XRustFSAdminTierInvalidConfig",
    message: "Unable to setup remote tier, check tier configuration",
    status_code: StatusCode::BAD_REQUEST,
};

#[derive(Serialize, Deserialize)]
pub struct TierConfigMgr {
    #[serde(skip)]
    pub driver_cache: HashMap<String, WarmBackendImpl>,
    pub tiers: HashMap<String, TierConfig>,
    pub last_refreshed_at: OffsetDateTime,
}

impl TierConfigMgr {
    pub fn new() -> Arc<RwLock<Self>> {
        Arc::new(RwLock::new(Self {
            driver_cache: HashMap::new(),
            tiers: HashMap::new(),
            last_refreshed_at: OffsetDateTime::now_utc(),
        }))
    }

    pub fn unmarshal(data: &[u8]) -> std::result::Result<TierConfigMgr, std::io::Error> {
        let cfg: TierConfigMgr = serde_json::from_slice(data)?;
        //let mut cfg = TierConfigMgr(m);
        //let mut cfg = m;
        Ok(cfg)
    }

    pub fn marshal(&self) -> std::result::Result<Bytes, std::io::Error> {
        let data = serde_json::to_vec(&self)?;

        //let mut data = Vec<u8>::with_capacity(self.msg_size()+4);
        let mut data = Bytes::from(data);

        //LittleEndian::write_u16(&mut data[0..2], TIER_CONFIG_FORMAT);
        //LittleEndian::write_u16(&mut data[2..4], TIER_CONFIG_VERSION);

        Ok(data)
    }

    pub fn refreshed_at(&self) -> OffsetDateTime {
        self.last_refreshed_at
    }

    pub fn is_tier_valid(&self, tier_name: &str) -> bool {
        let (_, valid) = self.is_tier_name_in_use(tier_name);
        valid
    }

    pub fn is_tier_name_in_use(&self, tier_name: &str) -> (TierType, bool) {
        if let Some(t) = self.tiers.get(tier_name) {
            return (t.tier_type.clone(), true);
        }
        (TierType::Unsupported, false)
    }

    pub async fn add(&mut self, tier: TierConfig, force: bool) -> std::result::Result<(), AdminError> {
        let tier_name = &tier.name;
        if tier_name != tier_name.to_uppercase().as_str() {
            return Err(ERR_TIER_NAME_NOT_UPPERCASE);
        }

        let (_, b) = self.is_tier_name_in_use(tier_name);
        if b {
            return Err(ERR_TIER_ALREADY_EXISTS);
        }

        let d = new_warm_backend(&tier, true).await?;

        if !force {
            let in_use = d.in_use().await;
            match in_use {
                Ok(b) => {
                    if b {
                        return Err(ERR_TIER_BACKEND_IN_USE);
                    }
                }
                Err(err) => {
                    warn!("tier add failed, err: {:?}", err);
                    if err.to_string().contains("connect") {
                        return Err(ERR_TIER_CONNECT_ERR);
                    } else if err.to_string().contains("authorization") {
                        return Err(ERR_TIER_INVALID_CREDENTIALS);
                    } else if err.to_string().contains("bucket") {
                        return Err(ERR_TIER_BUCKET_NOT_FOUND);
                    }
                    return Err(ERR_TIER_PERM_ERR);
                }
            }
        }

        self.driver_cache.insert(tier_name.to_string(), d);
        self.tiers.insert(tier_name.to_string(), tier);

        Ok(())
    }

    pub async fn remove(&mut self, tier_name: &str, force: bool) -> std::result::Result<(), AdminError> {
        let d = self.get_driver(tier_name).await;
        if let Err(err) = d {
            match err {
                ERR_TIER_NOT_FOUND => {
                    return Ok(());
                }
                _ => {
                    return Err(err);
                }
            }
        }
        if !force {
            let inuse = d.expect("err").in_use().await;
            if let Err(err) = inuse {
                return Err(ERR_TIER_PERM_ERR);
            } else if inuse.expect("err") {
                return Err(ERR_TIER_BACKEND_NOT_EMPTY);
            }
        }
        self.tiers.remove(tier_name);
        self.driver_cache.remove(tier_name);
        Ok(())
    }

    pub async fn verify(&mut self, tier_name: &str) -> std::result::Result<(), std::io::Error> {
        let d = match self.get_driver(tier_name).await {
            Ok(d) => d,
            Err(err) => {
                return Err(std::io::Error::other(err));
            }
        };
        if let Err(err) = check_warm_backend(Some(d)).await {
            return Err(std::io::Error::other(err));
        } else {
            return Ok(());
        }
    }

    pub fn empty(&self) -> bool {
        self.list_tiers().len() == 0
    }

    pub fn tier_type(&self, tier_name: &str) -> String {
        let cfg = self.tiers.get(tier_name);
        if cfg.is_none() {
            return "internal".to_string();
        }
        cfg.expect("err").tier_type.to_string()
    }

    pub fn list_tiers(&self) -> Vec<TierConfig> {
        let mut tier_cfgs = Vec::<TierConfig>::new();
        for (_, tier) in self.tiers.iter() {
            let tier = tier.clone();
            tier_cfgs.push(tier);
        }
        tier_cfgs
    }

    pub fn get(&self, tier_name: &str) -> Option<TierConfig> {
        for (tier_name2, tier) in self.tiers.iter() {
            if tier_name == tier_name2 {
                return Some(tier.clone());
            }
        }
        None
    }

    pub async fn edit(&mut self, tier_name: &str, creds: TierCreds) -> std::result::Result<(), AdminError> {
        let (tier_type, exists) = self.is_tier_name_in_use(tier_name);
        if !exists {
            return Err(ERR_TIER_NOT_FOUND);
        }

        let mut cfg = self.tiers[tier_name].clone();
        match tier_type {
            TierType::S3 => {
                let mut s3 = cfg.s3.as_mut().expect("err");
                if creds.aws_role {
                    s3.aws_role = true
                }
                if creds.aws_role_web_identity_token_file != "" && creds.aws_role_arn != "" {
                    s3.aws_role_arn = creds.aws_role_arn;
                    s3.aws_role_web_identity_token_file = creds.aws_role_web_identity_token_file;
                }
                if creds.access_key != "" && creds.secret_key != "" {
                    s3.access_key = creds.access_key;
                    s3.secret_key = creds.secret_key;
                }
            }
            TierType::RustFS => {
                let mut rustfs = cfg.rustfs.as_mut().expect("err");
                if creds.access_key == "" || creds.secret_key == "" {
                    return Err(ERR_TIER_MISSING_CREDENTIALS);
                }
                rustfs.access_key = creds.access_key;
                rustfs.secret_key = creds.secret_key;
            }
            TierType::MinIO => {
                let mut minio = cfg.minio.as_mut().expect("err");
                if creds.access_key == "" || creds.secret_key == "" {
                    return Err(ERR_TIER_MISSING_CREDENTIALS);
                }
                minio.access_key = creds.access_key;
                minio.secret_key = creds.secret_key;
            }
            _ => (),
        }

        let d = new_warm_backend(&cfg, true).await?;
        self.tiers.insert(tier_name.to_string(), cfg);
        self.driver_cache.insert(tier_name.to_string(), d);
        Ok(())
    }

    pub async fn get_driver<'a>(&'a mut self, tier_name: &str) -> std::result::Result<&'a WarmBackendImpl, AdminError> {
        Ok(match self.driver_cache.entry(tier_name.to_string()) {
            Entry::Occupied(e) => e.into_mut(),
            Entry::Vacant(e) => {
                let t = self.tiers.get(tier_name);
                if t.is_none() {
                    return Err(ERR_TIER_NOT_FOUND);
                }
                let d = new_warm_backend(t.expect("err"), false).await?;
                e.insert(d)
            }
        })
    }

    pub async fn reload(&mut self, api: Arc<ECStore>) -> std::result::Result<(), std::io::Error> {
        //let Some(api) = new_object_layer_fn() else { return Err(Error::msg("errServerNotInitialized")) };
        let new_config = load_tier_config(api).await;

        match &new_config {
            Ok(_c) => {}
            Err(err) => {
                return Err(std::io::Error::other(err.to_string()));
            }
        }
        self.driver_cache.clear();
        self.tiers.clear();
        let new_config = new_config.expect("err");
        for (tier, cfg) in new_config.tiers {
            self.tiers.insert(tier, cfg);
        }
        self.last_refreshed_at = OffsetDateTime::now_utc();
        Ok(())
    }

    #[tracing::instrument(level = "debug", name = "tier_save", skip(self))]
    pub async fn save(&self) -> std::result::Result<(), std::io::Error> {
        let Some(api) = new_object_layer_fn() else {
            return Err(std::io::Error::other("errServerNotInitialized"));
        };
        //let (pr, opts) = GLOBAL_TierConfigMgr.write().config_reader()?;

        self.save_tiering_config(api).await
    }

    pub async fn save_tiering_config<S: StorageAPI>(&self, api: Arc<S>) -> std::result::Result<(), std::io::Error> {
        let data = self.marshal()?;

        let config_file = format!("{}{}{}", CONFIG_PREFIX, SLASH_SEPARATOR, TIER_CONFIG_FILE);

        self.save_config(api, &config_file, data).await
    }

    pub async fn save_config<S: StorageAPI>(
        &self,
        api: Arc<S>,
        file: &str,
        data: Bytes,
    ) -> std::result::Result<(), std::io::Error> {
        self.save_config_with_opts(
            api,
            file,
            data,
            &ObjectOptions {
                max_parity: true,
                ..Default::default()
            },
        )
        .await
    }

    pub async fn save_config_with_opts<S: StorageAPI>(
        &self,
        api: Arc<S>,
        file: &str,
        data: Bytes,
        opts: &ObjectOptions,
    ) -> std::result::Result<(), std::io::Error> {
        debug!("save tier config:{}", file);
        let _ = api
            .put_object(RUSTFS_META_BUCKET, file, &mut PutObjReader::from_vec(data.to_vec()), opts)
            .await?;
        Ok(())
    }

    pub async fn refresh_tier_config(&mut self, api: Arc<ECStore>) {
        //let r = rand.New(rand.NewSource(time.Now().UnixNano()));
        let mut rng = rand::rng();
        let r = rng.random_range(0.0..1.0);
        let rand_interval = || Duration::from_secs((r * 60_f64).round() as u64);

        let mut t = interval(TIER_CFG_REFRESH + rand_interval());
        loop {
            select! {
                _ = t.tick() => {
                    if let Err(err) = self.reload(api.clone()).await {
                      info!("{}", err);
                    }
                }
                else => ()
            }
            t.reset();
        }
    }

    pub async fn init(&mut self, api: Arc<ECStore>) -> Result<()> {
        self.reload(api).await?;
        //if globalIsDistErasure {
        //    self.refresh_tier_config(api).await;
        //}
        Ok(())
    }
}

async fn new_and_save_tiering_config<S: StorageAPI>(api: Arc<S>) -> Result<TierConfigMgr> {
    let mut cfg = TierConfigMgr {
        driver_cache: HashMap::new(),
        tiers: HashMap::new(),
        last_refreshed_at: OffsetDateTime::now_utc(),
    };
    //lookup_configs(&mut cfg, api.clone()).await;
    cfg.save_tiering_config(api).await?;

    Ok(cfg)
}

#[tracing::instrument(level = "debug")]
async fn load_tier_config(api: Arc<ECStore>) -> std::result::Result<TierConfigMgr, std::io::Error> {
    let config_file = format!("{}{}{}", CONFIG_PREFIX, SLASH_SEPARATOR, TIER_CONFIG_FILE);
    let data = read_config(api.clone(), config_file.as_str()).await;
    if let Err(err) = data {
        if is_err_config_not_found(&err) {
            warn!("config not found, start to init");
            let cfg = new_and_save_tiering_config(api).await?;
            return Ok(cfg);
        } else {
            error!("read config err {:?}", &err);
            return Err(std::io::Error::other(err));
        }
    }

    let cfg;
    let version = 1; //LittleEndian::read_u16(&data[2..4]);
    match version {
        TIER_CONFIG_V1/*  | TIER_CONFIG_VERSION */ => {
            cfg = match TierConfigMgr::unmarshal(&data.unwrap()) {
                Ok(cfg) => cfg,
                Err(err) => {
                    return Err(std::io::Error::other(err.to_string()));
                }
            };
        }
        _ => {
            return Err(std::io::Error::other(format!("tierConfigInit: unknown version: {}", version)));
        }
    }

    Ok(cfg)
}

pub fn is_err_config_not_found(err: &StorageError) -> bool {
    matches!(err, StorageError::ObjectNotFound(_, _))
}
