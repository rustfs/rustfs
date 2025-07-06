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
#![allow(unused_variables, unused_mut, unused_must_use)]

use http::{HeaderMap, StatusCode};
//use iam::get_global_action_cred;
use matchit::Params;
use s3s::{Body, S3Error, S3ErrorCode, S3Request, S3Response, S3Result, header::CONTENT_TYPE, s3_error};
use serde_urlencoded::from_bytes;
use time::OffsetDateTime;
use tracing::{debug, warn};

use crate::{
    admin::router::Operation,
    auth::{check_key_valid, get_session_token},
};

use rustfs_ecstore::{
    config::storageclass,
    global::GLOBAL_TierConfigMgr,
    tier::{
        tier::{ERR_TIER_BACKEND_IN_USE, ERR_TIER_BACKEND_NOT_EMPTY, ERR_TIER_MISSING_CREDENTIALS},
        tier_admin::TierCreds,
        tier_config::{TierConfig, TierType},
        tier_handlers::{
            ERR_TIER_ALREADY_EXISTS, ERR_TIER_CONNECT_ERR, ERR_TIER_INVALID_CREDENTIALS, ERR_TIER_NAME_NOT_UPPERCASE,
            ERR_TIER_NOT_FOUND,
        },
    },
};

#[derive(Debug, Clone, serde::Deserialize, Default)]
pub struct AddTierQuery {
    #[serde(rename = "accessKey")]
    #[allow(dead_code)]
    pub access_key: Option<String>,
    #[allow(dead_code)]
    pub status: Option<String>,
    #[serde(rename = "secretKey")]
    #[allow(dead_code)]
    pub secret_key: Option<String>,
    #[serde(rename = "serviceName")]
    #[allow(dead_code)]
    pub service_name: Option<String>,
    #[serde(rename = "sessionToken")]
    #[allow(dead_code)]
    pub session_token: Option<String>,
    pub tier: Option<String>,
    #[serde(rename = "tierName")]
    #[allow(dead_code)]
    pub tier_name: Option<String>,
    #[serde(rename = "tierType")]
    #[allow(dead_code)]
    pub tier_type: Option<String>,
    pub force: Option<String>,
}

pub struct AddTier {}
#[async_trait::async_trait]
impl Operation for AddTier {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let query = {
            if let Some(query) = req.uri.query() {
                let input: AddTierQuery =
                    from_bytes(query.as_bytes()).map_err(|_e| s3_error!(InvalidArgument, "get query failed"))?;
                input
            } else {
                AddTierQuery::default()
            }
        };

        let Some(input_cred) = req.credentials else {
            return Err(s3_error!(InvalidRequest, "get cred failed"));
        };

        let (cred, _owner) =
            check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &input_cred.access_key).await?;

        let mut input = req.input;
        let body = match input.store_all_unlimited().await {
            Ok(b) => b,
            Err(e) => {
                warn!("get body failed, e: {:?}", e);
                return Err(s3_error!(InvalidRequest, "get body failed"));
            }
        };

        let mut args: TierConfig = serde_json::from_slice(&body)
            .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("unmarshal body err {e}")))?;

        match args.tier_type {
            TierType::S3 => {
                args.name = args.s3.clone().unwrap().name;
            }
            TierType::RustFS => {
                args.name = args.rustfs.clone().unwrap().name;
            }
            TierType::MinIO => {
                args.name = args.minio.clone().unwrap().name;
            }
            _ => (),
        }
        debug!("add tier args {:?}", args);

        let mut force: bool = false;
        let force_str = query.force.clone().unwrap_or_default();
        if !force_str.is_empty() {
            force = force_str.parse().map_err(|e| {
                warn!("parse force failed, e: {:?}", e);
                s3_error!(InvalidRequest, "parse force failed")
            })?;
        }
        match args.name.as_str() {
            storageclass::STANDARD | storageclass::RRS => {
                warn!("tier reserved name, args.name: {}", args.name);
                return Err(s3_error!(InvalidRequest, "Cannot use reserved tier name"));
            }
            &_ => (),
        }

        let mut tier_config_mgr = GLOBAL_TierConfigMgr.write().await;
        //tier_config_mgr.reload(api);
        if let Err(err) = tier_config_mgr.add(args, force).await {
            if err.code == ERR_TIER_ALREADY_EXISTS.code {
                return Err(S3Error::with_message(
                    S3ErrorCode::Custom("TierNameAlreadyExist".into()),
                    "tier name already exists!",
                ));
            } else if err.code == ERR_TIER_NAME_NOT_UPPERCASE.code {
                return Err(S3Error::with_message(
                    S3ErrorCode::Custom("TierNameNotUppercase".into()),
                    "tier name not uppercase!",
                ));
            } else if err.code == ERR_TIER_BACKEND_IN_USE.code {
                return Err(S3Error::with_message(
                    S3ErrorCode::Custom("TierNameBackendInUse!".into()),
                    "tier name backend in use!",
                ));
            } else if err.code == ERR_TIER_CONNECT_ERR.code {
                return Err(S3Error::with_message(
                    S3ErrorCode::Custom("TierConnectError".into()),
                    "tier connect error!",
                ));
            } else if err.code == ERR_TIER_INVALID_CREDENTIALS.code {
                return Err(S3Error::with_message(S3ErrorCode::Custom(err.code.clone().into()), err.message.clone()));
            } else {
                warn!("tier_config_mgr add failed, e: {:?}", err);
                return Err(S3Error::with_message(
                    S3ErrorCode::Custom("TierAddFailed".into()),
                    format!("tier add failed. {err}"),
                ));
            }
        }
        if let Err(e) = tier_config_mgr.save().await {
            warn!("tier_config_mgr save failed, e: {:?}", e);
            return Err(S3Error::with_message(S3ErrorCode::Custom("TierAddFailed".into()), "tier save failed"));
        }

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, "application/json".parse().unwrap());

        Ok(S3Response::with_headers((StatusCode::OK, Body::empty()), header))
    }
}

pub struct EditTier {}
#[async_trait::async_trait]
impl Operation for EditTier {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let query = {
            if let Some(query) = req.uri.query() {
                let input: AddTierQuery =
                    from_bytes(query.as_bytes()).map_err(|_e| s3_error!(InvalidArgument, "get query failed"))?;
                input
            } else {
                AddTierQuery::default()
            }
        };

        let Some(input_cred) = req.credentials else {
            return Err(s3_error!(InvalidRequest, "get cred failed"));
        };

        let (cred, _owner) =
            check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &input_cred.access_key).await?;

        let mut input = req.input;
        let body = match input.store_all_unlimited().await {
            Ok(b) => b,
            Err(e) => {
                warn!("get body failed, e: {:?}", e);
                return Err(s3_error!(InvalidRequest, "get body failed"));
            }
        };

        let creds: TierCreds = serde_json::from_slice(&body)
            .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("unmarshal body err {e}")))?;

        debug!("edit tier args {:?}", creds);

        let tier_name = params.get("tiername").map(|s| s.to_string()).unwrap_or_default();

        let mut tier_config_mgr = GLOBAL_TierConfigMgr.write().await;
        //tier_config_mgr.reload(api);
        if let Err(err) = tier_config_mgr.edit(&tier_name, creds).await {
            if err.code == ERR_TIER_NOT_FOUND.code {
                return Err(S3Error::with_message(S3ErrorCode::Custom("TierNotFound".into()), "tier not found!"));
            } else if err.code == ERR_TIER_MISSING_CREDENTIALS.code {
                return Err(S3Error::with_message(
                    S3ErrorCode::Custom("TierMissingCredentials".into()),
                    "tier missing credentials!",
                ));
            } else {
                warn!("tier_config_mgr edit failed, e: {:?}", err);
                return Err(S3Error::with_message(
                    S3ErrorCode::Custom("TierEditFailed".into()),
                    format!("tier edit failed. {err}"),
                ));
            }
        }
        if let Err(e) = tier_config_mgr.save().await {
            warn!("tier_config_mgr save failed, e: {:?}", e);
            return Err(S3Error::with_message(S3ErrorCode::Custom("TierEditFailed".into()), "tier save failed"));
        }

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, "application/json".parse().unwrap());

        Ok(S3Response::with_headers((StatusCode::OK, Body::empty()), header))
    }
}

#[derive(Debug, Clone, serde::Deserialize, Default)]
pub struct BucketQuery {
    #[serde(rename = "bucket")]
    #[allow(dead_code)]
    pub bucket: String,
}
pub struct ListTiers {}
#[async_trait::async_trait]
impl Operation for ListTiers {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let query = {
            if let Some(query) = req.uri.query() {
                let input: BucketQuery =
                    from_bytes(query.as_bytes()).map_err(|_e| s3_error!(InvalidArgument, "get query failed"))?;
                input
            } else {
                BucketQuery::default()
            }
        };

        let mut tier_config_mgr = GLOBAL_TierConfigMgr.read().await;
        let tiers = tier_config_mgr.list_tiers();

        let data = serde_json::to_vec(&tiers)
            .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("marshal tiers err {e}")))?;

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, "application/json".parse().unwrap());

        Ok(S3Response::with_headers((StatusCode::OK, Body::from(data)), header))
    }
}

pub struct RemoveTier {}
#[async_trait::async_trait]
impl Operation for RemoveTier {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let query = {
            if let Some(query) = req.uri.query() {
                let input: AddTierQuery =
                    from_bytes(query.as_bytes()).map_err(|_e| s3_error!(InvalidArgument, "get query failed"))?;
                input
            } else {
                AddTierQuery::default()
            }
        };

        let Some(input_cred) = req.credentials else {
            return Err(s3_error!(InvalidRequest, "get cred failed"));
        };

        let (cred, _owner) =
            check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &input_cred.access_key).await?;

        //let sys_cred = get_global_action_cred()
        //    .ok_or_else(|| S3Error::with_message(S3ErrorCode::InternalError, "get_global_action_cred failed"))?;

        let mut force: bool = false;
        let force_str = query.force.clone().unwrap_or_default();
        if !force_str.is_empty() {
            force = force_str.parse().map_err(|e| {
                warn!("parse force failed, e: {:?}", e);
                s3_error!(InvalidRequest, "parse force failed")
            })?;
        }

        let tier_name = params.get("tiername").map(|s| s.to_string()).unwrap_or_default();

        let mut tier_config_mgr = GLOBAL_TierConfigMgr.write().await;
        //tier_config_mgr.reload(api);
        if let Err(err) = tier_config_mgr.remove(&tier_name, force).await {
            if err.code == ERR_TIER_NOT_FOUND.code {
                return Err(S3Error::with_message(S3ErrorCode::Custom("TierNotFound".into()), "tier not found."));
            } else if err.code == ERR_TIER_BACKEND_NOT_EMPTY.code {
                return Err(S3Error::with_message(S3ErrorCode::Custom("TierNameBackendInUse".into()), "tier is used."));
            } else {
                warn!("tier_config_mgr remove failed, e: {:?}", err);
                return Err(S3Error::with_message(
                    S3ErrorCode::Custom("TierRemoveFailed".into()),
                    format!("tier remove failed. {err}"),
                ));
            }
        }

        if let Err(e) = tier_config_mgr.save().await {
            warn!("tier_config_mgr save failed, e: {:?}", e);
            return Err(S3Error::with_message(S3ErrorCode::Custom("TierRemoveFailed".into()), "tier save failed"));
        }

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, "application/json".parse().unwrap());

        Ok(S3Response::with_headers((StatusCode::OK, Body::empty()), header))
    }
}

pub struct VerifyTier {}
#[async_trait::async_trait]
impl Operation for VerifyTier {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let query = {
            if let Some(query) = req.uri.query() {
                let input: AddTierQuery =
                    from_bytes(query.as_bytes()).map_err(|_e| s3_error!(InvalidArgument, "get query failed"))?;
                input
            } else {
                AddTierQuery::default()
            }
        };

        let Some(input_cred) = req.credentials else {
            return Err(s3_error!(InvalidRequest, "get cred failed"));
        };

        let (cred, _owner) =
            check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &input_cred.access_key).await?;

        //let sys_cred = get_global_action_cred()
        //    .ok_or_else(|| S3Error::with_message(S3ErrorCode::InternalError, "get_global_action_cred failed"))?;

        let mut tier_config_mgr = GLOBAL_TierConfigMgr.write().await;
        tier_config_mgr.verify(&query.tier.unwrap()).await;

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, "application/json".parse().unwrap());

        Ok(S3Response::with_headers((StatusCode::OK, Body::empty()), header))
    }
}

pub struct GetTierInfo {}
#[async_trait::async_trait]
impl Operation for GetTierInfo {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let query = {
            if let Some(query) = req.uri.query() {
                let input: AddTierQuery =
                    from_bytes(query.as_bytes()).map_err(|_e| s3_error!(InvalidArgument, "get query failed"))?;
                input
            } else {
                AddTierQuery::default()
            }
        };

        let tier_config_mgr = GLOBAL_TierConfigMgr.read().await;
        let info = tier_config_mgr.get(&query.tier.unwrap());

        let data = serde_json::to_vec(&info)
            .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("marshal tier err {e}")))?;

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, "application/json".parse().unwrap());

        Ok(S3Response::with_headers((StatusCode::OK, Body::from(data)), header))
    }
}

#[derive(Debug, serde::Deserialize, Default)]
pub struct ClearTierQuery {
    pub rand: Option<String>,
    pub force: String,
}

pub struct ClearTier {}
#[async_trait::async_trait]
impl Operation for ClearTier {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let query = {
            if let Some(query) = req.uri.query() {
                let input: ClearTierQuery =
                    from_bytes(query.as_bytes()).map_err(|_e| s3_error!(InvalidArgument, "get query failed"))?;
                input
            } else {
                ClearTierQuery::default()
            }
        };

        //let Some(input_cred) = req.credentials else {
        //    return Err(s3_error!(InvalidRequest, "get cred failed"));
        //};

        //let (cred, _owner) =
        //    check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &input_cred.access_key).await?;

        let mut force: bool = false;
        let force_str = query.force;
        if !force_str.is_empty() {
            force = force_str.parse().unwrap();
        }

        let t = OffsetDateTime::now_utc();
        let mut rand = "AGD1R25GI3I1GJGUGJFD7FBS4DFAASDF".to_string();
        rand.insert_str(3, &t.day().to_string());
        rand.insert_str(17, &t.month().to_string());
        rand.insert_str(23, &t.year().to_string());
        warn!("tier_config_mgr rand: {}", rand);
        if query.rand != Some(rand) {
            return Err(s3_error!(InvalidRequest, "get rand failed"));
        };

        let mut tier_config_mgr = GLOBAL_TierConfigMgr.write().await;
        //tier_config_mgr.reload(api);
        if let Err(err) = tier_config_mgr.clear_tier(force).await {
            warn!("tier_config_mgr clear failed, e: {:?}", err);
            return Err(S3Error::with_message(
                S3ErrorCode::Custom("TierClearFailed".into()),
                format!("tier clear failed. {err}"),
            ));
        }
        if let Err(e) = tier_config_mgr.save().await {
            warn!("tier_config_mgr save failed, e: {:?}", e);
            return Err(S3Error::with_message(S3ErrorCode::Custom("TierEditFailed".into()), "tier save failed"));
        }

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, "application/json".parse().unwrap());

        Ok(S3Response::with_headers((StatusCode::OK, Body::empty()), header))
    }
}

/*pub struct PostRestoreObject {}
#[async_trait::async_trait]
impl Operation for PostRestoreObject {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let query = {
            if let Some(query) = req.uri.query() {
                let input: PostRestoreObject =
                    from_bytes(query.as_bytes()).map_err(|_e| s3_error!(InvalidArgument, "get query failed"))?;
                input
            } else {
                PostRestoreObject::default()
            }
        };

        let bucket = params.bucket;
        if let Err(e) = un_escape_path(params.object) {
            warn!("post restore object failed, e: {:?}", e);
            return Err(S3Error::with_message(S3ErrorCode::Custom("PostRestoreObjectFailed".into()), "post restore object failed"));
        }

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        let get_object_info = store.get_object_info();

        if Err(err) = check_request_auth_type(req, policy::RestoreObjectAction, bucket, object) {
            return Err(S3Error::with_message(S3ErrorCode::Custom("PostRestoreObjectFailed".into()), "post restore object failed"));
        }

        if req.content_length <= 0 {
            return Err(S3Error::with_message(S3ErrorCode::Custom("ErrEmptyRequestBody".into()), "post restore object failed"));
        }
        let Some(opts) = post_restore_opts(req, bucket, object) else {
            return Err(S3Error::with_message(S3ErrorCode::Custom("ErrEmptyRequestBody".into()), "post restore object failed"));
        };

        let Some(obj_info) = getObjectInfo(ctx, bucket, object, opts) else {
            return Err(S3Error::with_message(S3ErrorCode::Custom("ErrEmptyRequestBody".into()), "post restore object failed"));
        };

        if obj_info.transitioned_object.status != lifecycle::TRANSITION_COMPLETE {
            return Err(S3Error::with_message(S3ErrorCode::Custom("ErrEmptyRequestBody".into()), "post restore object failed"));
        }

        let mut api_err;
        let Some(rreq) = parsere_store_request(req.body(), req.content_length) else {
            let api_err = errorCodes.ToAPIErr(ErrMalformedXML);
            api_err.description = err.Error()
            return Err(S3Error::with_message(S3ErrorCode::Custom("ErrEmptyRequestBody".into()), "post restore object failed"));
        };
        let mut status_code = http::StatusCode::OK;
        let mut already_restored = false;
        if Err(err) = rreq.validate(store) {
            api_err = errorCodes.ToAPIErr(ErrMalformedXML)
            api_err.description = err.Error()
            return Err(S3Error::with_message(S3ErrorCode::Custom("ErrEmptyRequestBody".into()), "post restore object failed"));
        } else {
            if obj_info.restore_ongoing && rreq.Type != "SELECT" {
                return Err(S3Error::with_message(S3ErrorCode::Custom("ErrObjectRestoreAlreadyInProgress".into()), "post restore object failed"));
            }
            if !obj_info.restore_ongoing && !obj_info.restore_expires.unix_timestamp() == 0 {
                status_code = http::StatusCode::Accepted;
                already_restored = true;
            }
        }
        let restore_expiry = lifecycle::expected_expiry_time(OffsetDateTime::now_utc(), rreq.days);
        let mut metadata = clone_mss(obj_info.user_defined);

        if rreq.type != "SELECT" {
            obj_info.metadataOnly = true;
            metadata[xhttp.AmzRestoreExpiryDays] = rreq.days;
            metadata[xhttp.AmzRestoreRequestDate] = OffsetDateTime::now_utc().format(http::TimeFormat);
            if already_restored {
                metadata[xhttp.AmzRestore] = completedRestoreObj(restore_expiry).String()
            } else {
                metadata[xhttp.AmzRestore] = ongoingRestoreObj().String()
            }
            obj_info.user_defined = metadata;
            if let Err(err) = store.copy_object(bucket, object, bucket, object, obj_info, ObjectOptions {
                version_id: obj_info.version_id,
            }, ObjectOptions {
                version_id: obj_info.version_id,
                m_time:     obj_info.mod_time,
            }) {
                return Err(S3Error::with_message(S3ErrorCode::Custom("ErrInvalidObjectState".into()), "post restore object failed"));
            }
            if already_restored {
                return Ok(());
            }
        }

        let restore_object = must_get_uuid();
        if rreq.output_location.s3.bucket_name != "" {
            w.Header()[xhttp.AmzRestoreOutputPath] = []string{pathJoin(rreq.OutputLocation.S3.BucketName, rreq.OutputLocation.S3.Prefix, restoreObject)}
        }
        w.WriteHeader(status_code)
        send_event(EventArgs {
            event_name:  event::ObjectRestorePost,
            bucket_name: bucket,
            object:      obj_info,
            req_params:  extract_req_params(r),
            user_agent:  req.user_agent(),
            host:        handlers::get_source_ip(r),
        });
        tokio::spawn(async move {
            if !rreq.SelectParameters.IsEmpty() {
                let actual_size = obj_info.get_actual_size();
                if actual_size.is_err() {
                    return Err(S3Error::with_message(S3ErrorCode::Custom("ErrInvalidObjectState".into()), "post restore object failed"));
                }

                let object_rsc = s3select.NewObjectReadSeekCloser(
                    |offset int64| -> (io.ReadCloser, error) {
                        rs := &HTTPRangeSpec{
                          IsSuffixLength: false,
                          Start:          offset,
                          End:            -1,
                        }
                        return getTransitionedObjectReader(bucket, object, rs, r.Header,
                          obj_info, ObjectOptions {version_id: obj_info.version_id});
                    },
                    actual_size.unwrap(),
                );
                if err = rreq.SelectParameters.Open(objectRSC); err != nil {
                    if serr, ok := err.(s3select.SelectError); ok {
                        let encoded_error_response = encodeResponse(APIErrorResponse {
                            code:       serr.ErrorCode(),
                            message:    serr.ErrorMessage(),
                            bucket_name: bucket,
                            key:        object,
                            resource:   r.URL.Path,
                            request_id:  w.Header().Get(xhttp.AmzRequestID),
                            host_id:     globalDeploymentID(),
                        });
                        //writeResponse(w, serr.HTTPStatusCode(), encodedErrorResponse, mimeXML)
                        Ok(S3Response::with_headers((StatusCode::OK, Body::empty()), header));
                    } else {
                        return Err(S3Error::with_message(S3ErrorCode::Custom("ErrInvalidObjectState".into()), "post restore object failed"));
                    }
                    return Ok(());
                }
                let nr = httptest.NewRecorder();
                let rw = xhttp.NewResponseRecorder(nr);
                rw.log_err_body = true;
                rw.log_all_body = true;
                rreq.select_parameters.evaluate(rw);
                rreq.select_parameters.Close();
                return Ok(S3Response::with_headers((StatusCode::OK, Body::empty()), header));
            }
            let opts = ObjectOptions {
                transition: TransitionOptions {
                    restore_request: rreq,
                    restore_expiry:  restore_expiry,
                },
                version_id: objInfo.version_id,
            }
            if Err(err) = store.restore_transitioned_object(bucket, object, opts) {
                format!(format!("unable to restore transitioned bucket/object {}/{}: {}", bucket, object, err.to_string()));
                return Ok(S3Response::with_headers((StatusCode::OK, Body::empty()), header));
            }

            send_event(EventArgs {
                EventName:  event.ObjectRestoreCompleted,
                BucketName: bucket,
                Object:     objInfo,
                ReqParams:  extractReqParams(r),
                UserAgent:  r.UserAgent(),
                Host:       handlers.GetSourceIP(r),
            });
        });

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, "application/json".parse().unwrap());

        Ok(S3Response::with_headers((StatusCode::OK, Body::empty()), header))
    }
}*/
