use std::collections::HashMap;

use hyper::StatusCode;
use iam::{
    auth::CredentialsBuilder,
    policy::{
        action::{Action, AdminAction::ListServiceAccountsAdminAction},
        Args,
    },
};
use matchit::Params;
use s3s::{s3_error, Body, S3Request, S3Response, S3Result};
use tracing::{debug, warn};

use crate::admin::models::service_account::{AddServiceAccountReq, AddServiceAccountResp, Credentials, InfoServiceAccountResp};
use crate::admin::router::Operation;

pub struct AddServiceAccount {}
#[async_trait::async_trait]
impl Operation for AddServiceAccount {
    async fn call(&self, mut req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        warn!("handle AddServiceAccount, req: {req:?}");

        let Some(cred) = req.credentials else { return Err(s3_error!(InvalidRequest, "get cred failed")) };
        let _is_owner = true; // 先按true处理，后期根据请求决定。
        let body = req.input.store_all_unlimited().await.unwrap();
        let body = crypto::decrypt_data(cred.secret_key.expose().as_bytes(), &body[..])
            .map_err(|_| s3_error!(InternalError, "encrypt data failed"))?;

        debug!("body: {:?}", String::from_utf8_lossy(&body));

        let mut create_req: AddServiceAccountReq =
            serde_json::from_slice(&body[..]).map_err(|e| s3_error!(InvalidRequest, "unmarshal body failed, e: {:?}", e))?;

        create_req.expiration = create_req.expiration.and_then(|expire| expire.replace_millisecond(0).ok());

        if create_req.access_key.trim().len() != create_req.access_key.len() {
            return Err(s3_error!(InvalidRequest, "access key has spaces"));
        }

        // 校验合法性, Name, Expiration, Description
        let _target_user = create_req.target_user.as_ref().unwrap_or(&cred.access_key);
        let _deny_only = true;

        // todo 校验权限

        // if !iam::is_allowed(Args {
        //     account: &cred.access_key,
        //     groups: &[],
        //     action: Action::AdminAction(AdminAction::CreateServiceAccountAdminAction),
        //     bucket: "",
        //     conditions: &HashMap::new(),
        //     is_owner,
        //     object: "",
        //     claims: &HashMap::new(),
        //     deny_only,
        // })
        // .await
        // .unwrap_or(false)
        // {
        //     return Err(s3_error!(AccessDenied));
        // }
        //

        let cred = CredentialsBuilder::new()
            .parent_user(match create_req.target_user {
                Some(target_user) => target_user,
                _ => cred.access_key,
            })
            .access_key(create_req.access_key)
            .secret_key(create_req.secret_key)
            .description(create_req.description)
            .expiration(create_req.expiration)
            .session_policy({
                match create_req.policy {
                    Some(p) if !p.is_empty() => {
                        Some(serde_json::from_slice(&p).map_err(|_| s3_error!(InvalidRequest, "invalid policy"))?)
                    }
                    _ => None,
                }
            })
            .name(create_req.name)
            .try_build()
            .map_err(|e| s3_error!(InvalidRequest, "build cred failed, err: {:?}", e))?;

        let resp = serde_json::to_vec(&AddServiceAccountResp {
            credentials: Credentials {
                access_key: &cred.access_key,
                secret_key: &cred.secret_key,
                session_token: None,
                expiration: cred.expiration,
            },
        })
        .unwrap()
        .into();

        iam::add_service_account(cred).await.map_err(|e| {
            debug!("add cred failed: {e:?}");
            s3_error!(InternalError, "add cred failed")
        })?;

        Ok(S3Response::new((StatusCode::OK, resp)))
    }
}

pub struct UpdateServiceAccount {}
#[async_trait::async_trait]
impl Operation for UpdateServiceAccount {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        warn!("handle UpdateServiceAccount");

        let Some(_cred) = req.credentials else { return Err(s3_error!(InvalidRequest, "get cred failed")) };

        // return Err(s3_error!(NotImplemented));
        //

        todo!()
    }
}

pub struct InfoServiceAccount {}
#[async_trait::async_trait]
impl Operation for InfoServiceAccount {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        warn!("handle InfoServiceAccount");

        let Some(cred) = req.credentials else { return Err(s3_error!(InvalidRequest, "get cred failed")) };

        //accessKey
        let Some(ak) = req.uri.query().and_then(|x| {
            for mut x in x.split('&').map(|x| x.split('=')) {
                let Some(key) = x.next() else {
                    continue;
                };

                if key != "accessKey" {
                    continue;
                }

                let Some(value) = x.next() else {
                    continue;
                };

                return Some(value);
            }

            None
        }) else {
            return Err(s3_error!(InvalidRequest, "access key is not exist"));
        };

        let (sa, _sp) = iam::get_service_account(ak).await.map_err(|e| {
            debug!("get service account failed, err: {e:?}");
            s3_error!(InternalError)
        })?;

        if !iam::is_allowed(Args {
            account: &sa.access_key,
            groups: &sa.groups.unwrap_or_default()[..],
            action: Action::AdminAction(ListServiceAccountsAdminAction),
            bucket: "",
            conditions: &HashMap::new(),
            is_owner: true,
            object: "",
            claims: &HashMap::new(),
            deny_only: false,
        })
        .await
        .map_err(|_| s3_error!(InternalError))?
        {
            let req_user = &cred.access_key;
            if req_user != &sa.parent_user {
                return Err(s3_error!(AccessDenied));
            }
        }

        // let implied_policy = sp.version.is_empty() && sp.statements.is_empty();
        // let sva = if implied_policy {
        //     sp
        // } else {
        //     // 这里使用
        //     todo!();
        // };

        let body = serde_json::to_vec(&InfoServiceAccountResp {
            parent_user: sa.parent_user,
            account_status: sa.status,
            implied_policy: true,
            // policy: serde_json::to_string_pretty(&sva).map_err(|_| s3_error!(InternalError, "json marshal failed"))?,
            policy: "".into(),
            name: sa.name.unwrap_or_default(),
            description: sa.description.unwrap_or_default(),
            expiration: sa.expiration,
        })
        .map_err(|_| s3_error!(InternalError, "json marshal failed"))?;

        Ok(S3Response::new((
            StatusCode::OK,
            crypto::encrypt_data(cred.access_key.as_bytes(), &body[..])
                .map_err(|_| s3_error!(InternalError, "encrypt data failed"))?
                .into(),
        )))
    }
}

pub struct ListServiceAccount {}
#[async_trait::async_trait]
impl Operation for ListServiceAccount {
    async fn call(&self, _req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        warn!("handle ListServiceAccount");
        todo!()
    }
}

pub struct DeleteServiceAccount {}
#[async_trait::async_trait]
impl Operation for DeleteServiceAccount {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        warn!("handle DeleteServiceAccount");

        let Some(_cred) = req.credentials else { return Err(s3_error!(InvalidRequest, "get cred failed")) };

        let Some(_service_account) = params.get("accessKey") else {
            return Err(s3_error!(InvalidRequest, "Invalid arguments specified."));
        };

        todo!()
    }
}
