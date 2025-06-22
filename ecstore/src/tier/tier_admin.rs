use std::{
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use rand::Rng;
use tracing::warn;
use http::status::StatusCode;
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize)]
#[derive(Default, Debug, Clone)]
#[serde(default)]
pub struct TierCreds {
    #[serde(rename = "accessKey")]
    pub access_key: String,
    #[serde(rename = "secretKey")]
    pub secret_key: String,

    #[serde(rename = "awsRole")]
    pub aws_role: bool,
    #[serde(rename = "awsRoleWebIdentityTokenFile")]
    pub aws_role_web_identity_token_file: String,
    #[serde(rename = "awsRoleArn")]
    pub aws_role_arn: String,

    //azsp: ServicePrincipalAuth,

    //#[serde(rename = "credsJson")]
    pub creds_json: Vec<u8>,
}