use crate::router::Operation;
use ecstore::utils::xml;
use hyper::StatusCode;
use matchit::Params;
use s3s::{
    dto::{AssumeRoleOutput, Credentials, Timestamp},
    s3_error, Body, S3Request, S3Response, S3Result,
};
use serde::Deserialize;
use serde_urlencoded::from_bytes;
use time::{Duration, OffsetDateTime};
use tracing::warn;

#[derive(Deserialize, Debug, Default)]
#[serde(rename_all = "PascalCase", default)]
pub struct AssumeRoleRequest {
    pub action: String,
    pub duration_seconds: usize,
    pub version: String,
    pub role_arn: String,
    pub role_session_name: String,
    pub policy: String,
    pub external_id: String,
}

// #[derive(Debug, Serialize, Default)]
// #[serde(rename_all = "PascalCase", default)]
// pub struct AssumeRoleResponse {
//     #[serde(rename = "AssumeRoleResult")]
//     pub result: AssumeRoleResult,
// }

// #[derive(Debug, Serialize, Default)]
// #[serde(rename_all = "PascalCase", default)]
// pub struct AssumeRoleResult {
//     pub credentials: Credentials,
// }

// #[derive(Debug, Serialize, Default)]
// #[serde(rename_all = "PascalCase", default)]
// pub struct Credentials {
//     #[serde(rename = "AccessKeyId")]
//     pub access_key: String,
//     #[serde(rename = "SecretAccessKey")]
//     pub secret_key: String,
//     pub status: String,
//     pub expiration: usize,
//     pub session_token: String,
//     pub parent_user: String,
// }

pub struct AssumeRoleHandle {}
#[async_trait::async_trait]
impl Operation for AssumeRoleHandle {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        warn!("handle AssumeRoleHandle");

        let Some(cred) = req.credentials else { return Err(s3_error!(InvalidRequest, "get cred failed")) };

        let mut input = req.input;

        let Some(bytes) = input.take_bytes() else {
            return Err(s3_error!(InvalidRequest, "get body failed"));
        };
        let body: AssumeRoleRequest = from_bytes(&bytes).map_err(|_e| s3_error!(InvalidRequest, "get body failed"))?;

        warn!("AssumeRole get body {:?}", body);

        let exp = OffsetDateTime::now_utc().saturating_add(Duration::days(1));

        // TODO: create tmp access_key
        let resp = AssumeRoleOutput {
            credentials: Some(Credentials {
                access_key_id: cred.access_key,
                expiration: Timestamp::from(exp),
                secret_access_key: cred.secret_key.expose().to_string(),
                session_token: "sdfsdf".to_owned(),
            }),
            ..Default::default()
        };

        // getAssumeRoleCredentials
        let output = xml::serialize::<AssumeRoleOutput>(&resp).unwrap();

        Ok(S3Response::new((StatusCode::OK, Body::from(output))))

        // return Err(s3_error!(NotImplemented));
    }
}

pub struct AccountInfoHandler {}
#[async_trait::async_trait]
impl Operation for AccountInfoHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        warn!("handle AccountInfoHandler");

        let Some(cred) = req.credentials else { return Err(s3_error!(InvalidRequest, "get cred failed")) };

        warn!("AccountInfoHandler cread {:?}", &cred);

        return Err(s3_error!(NotImplemented));
    }
}

pub struct ServiceHandle {}
#[async_trait::async_trait]
impl Operation for ServiceHandle {
    async fn call(&self, _req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        warn!("handle ServiceHandle");

        return Err(s3_error!(NotImplemented));
    }
}

pub struct ServerInfoHandler {}

#[async_trait::async_trait]
impl Operation for ServerInfoHandler {
    async fn call(&self, _req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        warn!("handle ServerInfoHandler");

        return Err(s3_error!(NotImplemented));
    }
}

pub struct InspectDataHandler {}

#[async_trait::async_trait]
impl Operation for InspectDataHandler {
    async fn call(&self, _req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        warn!("handle InspectDataHandler");

        return Err(s3_error!(NotImplemented));
    }
}

pub struct StorageInfoHandler {}

#[async_trait::async_trait]
impl Operation for StorageInfoHandler {
    async fn call(&self, _req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        warn!("handle StorageInfoHandler");

        return Err(s3_error!(NotImplemented));
    }
}

pub struct DataUsageInfoHandler {}

#[async_trait::async_trait]
impl Operation for DataUsageInfoHandler {
    async fn call(&self, _req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        warn!("handle DataUsageInfoHandler");

        return Err(s3_error!(NotImplemented));
    }
}

pub struct MetricsHandler {}

#[async_trait::async_trait]
impl Operation for MetricsHandler {
    async fn call(&self, _req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        warn!("handle MetricsHandler");

        return Err(s3_error!(NotImplemented));
    }
}

pub struct HealHandler {}

#[async_trait::async_trait]
impl Operation for HealHandler {
    async fn call(&self, _req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        warn!("handle HealHandler");

        return Err(s3_error!(NotImplemented));
    }
}

pub struct BackgroundHealStatusHandler {}

#[async_trait::async_trait]
impl Operation for BackgroundHealStatusHandler {
    async fn call(&self, _req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        warn!("handle BackgroundHealStatusHandler");

        return Err(s3_error!(NotImplemented));
    }
}

pub struct ListPools {}

#[async_trait::async_trait]
impl Operation for ListPools {
    async fn call(&self, _req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        warn!("handle ListPools");

        return Err(s3_error!(NotImplemented));
    }
}

pub struct StatusPool {}

#[async_trait::async_trait]
impl Operation for StatusPool {
    async fn call(&self, _req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        warn!("handle StatusPool");

        return Err(s3_error!(NotImplemented));
    }
}

pub struct StartDecommission {}

#[async_trait::async_trait]
impl Operation for StartDecommission {
    async fn call(&self, _req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        warn!("handle StartDecommission");

        return Err(s3_error!(NotImplemented));
    }
}

pub struct CancelDecommission {}

#[async_trait::async_trait]
impl Operation for CancelDecommission {
    async fn call(&self, _req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        warn!("handle CancelDecommission");

        return Err(s3_error!(NotImplemented));
    }
}

pub struct RebalanceStart {}

#[async_trait::async_trait]
impl Operation for RebalanceStart {
    async fn call(&self, _req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        warn!("handle RebalanceStart");

        return Err(s3_error!(NotImplemented));
    }
}

// RebalanceStatus
pub struct RebalanceStatus {}

#[async_trait::async_trait]
impl Operation for RebalanceStatus {
    async fn call(&self, _req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        warn!("handle RebalanceStatus");

        return Err(s3_error!(NotImplemented));
    }
}
// RebalanceStop
pub struct RebalanceStop {}

#[async_trait::async_trait]
impl Operation for RebalanceStop {
    async fn call(&self, _req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        warn!("handle RebalanceStop");

        return Err(s3_error!(NotImplemented));
    }
}
