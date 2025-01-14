use std::collections::HashMap;

use http::HeaderMap;
use hyper::StatusCode;
use iam::{
    auth::CredentialsBuilder,
    policy::{
        action::{Action, AdminAction::ListServiceAccountsAdminAction},
        Args,
    },
};
use madmin::{AddServiceAccountReq, ListServiceAccountsResp, ServiceAccountInfo};
use matchit::Params;
use s3s::{header::CONTENT_TYPE, s3_error, Body, S3Error, S3ErrorCode, S3Request, S3Response, S3Result};
use serde_urlencoded::from_bytes;
use tracing::{debug, warn};

use crate::admin::router::Operation;
use crate::admin::{
    handlers::check_key_valid,
    models::service_account::{AddServiceAccountResp, Credentials, InfoServiceAccountResp},
};

pub struct AddServiceAccount {}
#[async_trait::async_trait]
impl Operation for AddServiceAccount {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        warn!("handle AddServiceAccount ");

        let Some(input_cred) = req.credentials else {
            return Err(s3_error!(InvalidRequest, "get cred failed"));
        };
        let _is_owner = true; // 先按true处理，后期根据请求决定。

        let Some(body) = req.input.bytes() else {
            return Err(s3_error!(InvalidRequest, "get body failed"));
        };

        let mut create_req: AddServiceAccountReq =
            serde_json::from_slice(&body[..]).map_err(|e| s3_error!(InvalidRequest, "unmarshal body failed, e: {:?}", e))?;

        create_req.expiration = create_req.expiration.and_then(|expire| expire.replace_millisecond(0).ok());

        if create_req.access_key.trim().len() != create_req.access_key.len() {
            return Err(s3_error!(InvalidRequest, "access key has spaces"));
        }

        let (cred, _) = check_key_valid(None, &input_cred.access_key).await.map_err(|e| {
            debug!("check key failed: {e:?}");
            s3_error!(InternalError, "check key failed")
        })?;

        // TODO check create_req validity

        // 校验合法性, Name, Expiration, Description
        let target_user = if let Some(u) = create_req.target_user {
            u
        } else {
            cred.access_key
        };
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
            .parent_user(target_user)
            .access_key(create_req.access_key)
            .secret_key(create_req.secret_key)
            .description(create_req.description.unwrap_or_default())
            .expiration(create_req.expiration)
            .session_policy({
                match create_req.policy {
                    Some(p) if !p.is_empty() => {
                        Some(serde_json::from_slice(p.as_bytes()).map_err(|_| s3_error!(InvalidRequest, "invalid policy"))?)
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

#[derive(Debug, Default, serde::Deserialize)]
pub struct ListServiceAccountQuery {
    pub user: Option<String>,
}

pub struct ListServiceAccount {}
#[async_trait::async_trait]
impl Operation for ListServiceAccount {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        warn!("handle ListServiceAccount");
        let query = {
            if let Some(query) = req.uri.query() {
                let input: ListServiceAccountQuery =
                    from_bytes(query.as_bytes()).map_err(|_e| s3_error!(InvalidArgument, "get body failed"))?;
                input
            } else {
                ListServiceAccountQuery::default()
            }
        };

        let target_account = if let Some(user) = query.user {
            user
        } else {
            let Some(input_cred) = req.credentials else {
                return Err(s3_error!(InvalidRequest, "get cred failed"));
            };

            let (cred, _owner) = check_key_valid(None, &input_cred.access_key).await.map_err(|e| {
                debug!("check key failed: {e:?}");
                s3_error!(InternalError, "check key failed")
            })?;

            if cred.parent_user.is_empty() {
                input_cred.access_key
            } else {
                cred.parent_user
            }
        };

        let service_accounts = iam::list_service_accounts(&target_account).await.map_err(|e| {
            debug!("list service account failed: {e:?}");
            s3_error!(InternalError, "list service account failed")
        })?;

        let accounts: Vec<ServiceAccountInfo> = service_accounts
            .into_iter()
            .map(|sa| ServiceAccountInfo {
                parent_user: sa.parent_user,
                account_status: sa.status,
                implied_policy: true, // or set according to your logic
                access_key: sa.access_key,
                name: sa.name,
                description: sa.description,
                expiration: sa.expiration,
            })
            .collect();

        let data = serde_json::to_vec(&ListServiceAccountsResp { accounts })
            .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("marshal users err {}", e)))?;

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, "application/json".parse().unwrap());

        Ok(S3Response::with_headers((StatusCode::OK, Body::from(data)), header))
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
