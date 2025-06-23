use serde::{Deserialize, Serialize};
use std::fmt::Display;
use tracing::info;

const C_TierConfigVer: &str = "v1";

const ERR_TIER_NAME_EMPTY: &str = "remote tier name empty";
const ERR_TIER_INVALID_CONFIG: &str = "invalid tier config";
const ERR_TIER_INVALID_CONFIG_VERSION: &str = "invalid tier config version";
const ERR_TIER_TYPE_UNSUPPORTED: &str = "unsupported tier type";

#[derive(Serialize, Deserialize, Default, Debug, Clone)]
pub enum TierType {
    #[default]
    Unsupported,
    #[serde(rename = "s3")]
    S3,
    #[serde(rename = "azure")]
    Azure,
    #[serde(rename = "gcs")]
    GCS,
    #[serde(rename = "rustfs")]
    RustFS,
    #[serde(rename = "minio")]
    MinIO,
}

impl Display for TierType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TierType::S3 => {
                write!(f, "S3")
            }
            TierType::RustFS => {
                write!(f, "RustFS")
            }
            TierType::MinIO => {
                write!(f, "MinIO")
            }
            _ => {
                write!(f, "Unsupported")
            }
        }
    }
}

impl TierType {
    pub fn new(sc_type: &str) -> Self {
        match sc_type {
            "S3" => TierType::S3,
            "RustFS" => TierType::RustFS,
            "MinIO" => TierType::MinIO,
            _ => TierType::Unsupported,
        }
    }

    pub fn to_string(&self) -> String {
        match self {
            TierType::S3 => "s3".to_string(),
            TierType::RustFS => "rustfs".to_string(),
            TierType::MinIO => "minio".to_string(),
            _ => "unsupported".to_string(),
        }
    }
}

#[derive(Default, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct TierConfig {
    #[serde(skip)]
    pub version: String,
    #[serde(rename = "type")]
    pub tier_type: TierType,
    #[serde(skip)]
    pub name: String,
    #[serde(rename = "s3", skip_serializing_if = "Option::is_none")]
    pub s3: Option<TierS3>,
    //TODO: azure: Option<TierAzure>,
    //TODO: gcs: Option<TierGCS>,
    #[serde(rename = "rustfs", skip_serializing_if = "Option::is_none")]
    pub rustfs: Option<TierRustFS>,
    #[serde(rename = "minio", skip_serializing_if = "Option::is_none")]
    pub minio: Option<TierMinIO>,
}

impl Clone for TierConfig {
    fn clone(&self) -> TierConfig {
        let mut s3 = None;
        //az  TierAzure
        //gcs TierGCS
        let mut r = None;
        let mut m = None;
        match self.tier_type {
            TierType::S3 => {
                let mut s3_ = self.s3.as_ref().expect("err").clone();
                s3_.secret_key = "REDACTED".to_string();
                s3 = Some(s3_);
            }
            TierType::RustFS => {
                let mut r_ = self.rustfs.as_ref().expect("err").clone();
                r_.secret_key = "REDACTED".to_string();
                r = Some(r_);
            }
            TierType::MinIO => {
                let mut m_ = self.minio.as_ref().expect("err").clone();
                m_.secret_key = "REDACTED".to_string();
                m = Some(m_);
            }
            _ => (),
        }
        TierConfig {
            version: self.version.clone(),
            tier_type: self.tier_type.clone(),
            name: self.name.clone(),
            s3: s3,
            //azure:   az,
            //gcs:     gcs,
            rustfs: r,
            minio: m,
        }
    }
}

impl TierConfig {
    pub fn unmarshal(data: &[u8]) -> Result<TierConfig, std::io::Error> {
        /*let m: HashMap<String, HashMap<String, KVS>> = serde_json::from_slice(data)?;
        let mut cfg = TierConfig(m);
        cfg.set_defaults();
        Ok(cfg)*/
        todo!();
    }

    pub fn marshal(&self) -> Result<Vec<u8>, std::io::Error> {
        let data = serde_json::to_vec(&self)?;
        Ok(data)
    }

    fn endpoint(&self) -> String {
        match self.tier_type {
            TierType::S3 => self.s3.as_ref().expect("err").endpoint.clone(),
            TierType::RustFS => self.rustfs.as_ref().expect("err").endpoint.clone(),
            TierType::MinIO => self.minio.as_ref().expect("err").endpoint.clone(),
            _ => {
                info!("unexpected tier type {}", self.tier_type);
                "".to_string()
            }
        }
    }

    fn bucket(&self) -> String {
        match self.tier_type {
            TierType::S3 => self.s3.as_ref().expect("err").bucket.clone(),
            TierType::RustFS => self.rustfs.as_ref().expect("err").bucket.clone(),
            TierType::MinIO => self.minio.as_ref().expect("err").bucket.clone(),
            _ => {
                info!("unexpected tier type {}", self.tier_type);
                "".to_string()
            }
        }
    }

    fn prefix(&self) -> String {
        match self.tier_type {
            TierType::S3 => self.s3.as_ref().expect("err").prefix.clone(),
            TierType::RustFS => self.rustfs.as_ref().expect("err").prefix.clone(),
            TierType::MinIO => self.minio.as_ref().expect("err").prefix.clone(),
            _ => {
                info!("unexpected tier type {}", self.tier_type);
                "".to_string()
            }
        }
    }

    fn region(&self) -> String {
        match self.tier_type {
            TierType::S3 => self.s3.as_ref().expect("err").region.clone(),
            TierType::RustFS => self.rustfs.as_ref().expect("err").region.clone(),
            TierType::MinIO => self.minio.as_ref().expect("err").region.clone(),
            _ => {
                info!("unexpected tier type {}", self.tier_type);
                "".to_string()
            }
        }
    }
}

//type S3Options = impl Fn(TierS3) -> Pin<Box<Result<()>>> + Send + Sync + 'static;

#[derive(Serialize, Deserialize, Default, Debug, Clone)]
#[serde(default)]
pub struct TierS3 {
    pub name: String,
    pub endpoint: String,
    #[serde(rename = "accesskey")]
    pub access_key: String,
    #[serde(rename = "secretkey")]
    pub secret_key: String,
    pub bucket: String,
    pub prefix: String,
    pub region: String,
    #[serde(rename = "storageclass")]
    pub storage_class: String,
    #[serde(skip)]
    pub aws_role: bool,
    #[serde(skip)]
    pub aws_role_web_identity_token_file: String,
    #[serde(skip)]
    pub aws_role_arn: String,
    #[serde(skip)]
    pub aws_role_session_name: String,
    #[serde(skip)]
    pub aws_role_duration_seconds: i32,
}

impl TierS3 {
    fn new<F>(name: &str, access_key: &str, secret_key: &str, bucket: &str, options: Vec<F>) -> Result<TierConfig, std::io::Error>
    where
        F: Fn(TierS3) -> Box<Result<(), std::io::Error>> + Send + Sync + 'static,
    {
        if name == "" {
            return Err(std::io::Error::other(ERR_TIER_NAME_EMPTY));
        }
        let sc = TierS3 {
            access_key: access_key.to_string(),
            secret_key: secret_key.to_string(),
            bucket: bucket.to_string(),
            endpoint: "https://s3.amazonaws.com".to_string(),
            region: "".to_string(),
            storage_class: "".to_string(),
            ..Default::default()
        };

        for option in options {
            let option = option(sc.clone());
            let option = *option;
            option?;
        }

        Ok(TierConfig {
            version: C_TierConfigVer.to_string(),
            tier_type: TierType::S3,
            name: name.to_string(),
            s3: Some(sc),
            ..Default::default()
        })
    }
}

#[derive(Serialize, Deserialize, Default, Debug, Clone)]
#[serde(default)]
pub struct TierRustFS {
    pub name: String,
    pub endpoint: String,
    #[serde(rename = "accesskey")]
    pub access_key: String,
    #[serde(rename = "secretkey")]
    pub secret_key: String,
    pub bucket: String,
    pub prefix: String,
    pub region: String,
    #[serde(rename = "storageclass")]
    pub storage_class: String,
}

#[derive(Serialize, Deserialize, Default, Debug, Clone)]
#[serde(default)]
pub struct TierMinIO {
    pub name: String,
    pub endpoint: String,
    #[serde(rename = "accesskey")]
    pub access_key: String,
    #[serde(rename = "secretkey")]
    pub secret_key: String,
    pub bucket: String,
    pub prefix: String,
    pub region: String,
}

impl TierMinIO {
    fn new<F>(
        name: &str,
        endpoint: &str,
        access_key: &str,
        secret_key: &str,
        bucket: &str,
        options: Vec<F>,
    ) -> Result<TierConfig, std::io::Error>
    where
        F: Fn(TierMinIO) -> Box<Result<(), std::io::Error>> + Send + Sync + 'static,
    {
        if name == "" {
            return Err(std::io::Error::other(ERR_TIER_NAME_EMPTY));
        }
        let m = TierMinIO {
            access_key: access_key.to_string(),
            secret_key: secret_key.to_string(),
            bucket: bucket.to_string(),
            endpoint: endpoint.to_string(),
            ..Default::default()
        };

        for option in options {
            let option = option(m.clone());
            let option = *option;
            option?;
        }

        Ok(TierConfig {
            version: C_TierConfigVer.to_string(),
            tier_type: TierType::MinIO,
            name: name.to_string(),
            minio: Some(m),
            ..Default::default()
        })
    }
}
