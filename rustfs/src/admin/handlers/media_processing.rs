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

//! Admin surface for the per-bucket media processing profile.
//!
//! `PUT` / `GET` / `DELETE` on one route manage the profile that decides which
//! objects get image and video renditions, and `GET .../capabilities` reports
//! what the local ffmpeg can actually encode so an operator does not configure
//! a format their toolchain will refuse at render time.

use crate::admin::handlers::admin_json_response;
use crate::admin::handlers::on_demand_migration::{
    authorize_for_bucket, bucket_from_params, format_updated_at, notify_peers_reload,
};
use crate::admin::router::{AdminOperation, Operation, S3Router};
use crate::admin::runtime_sources::{app_context_from_req, object_store_from_req};
use crate::admin::storage_api::bucket::metadata_sys;
use crate::admin::storage_api::s3::error as admin_s3_error;
use crate::admin::storage_api::s3::{Body, S3Error, S3ErrorCode, S3Request, S3Response, S3Result};
use crate::admin::utils::read_compatible_admin_body;
use crate::server::ADMIN_PREFIX;
use hyper::{Method, StatusCode};
use matchit::Params;
use rustfs_config::MAX_ADMIN_REQUEST_BODY_SIZE;
use rustfs_media_processing::plan::{KNOWN_PIXEL_FORMATS, MediaProfile, is_known_pixel_format};
use rustfs_policy::policy::action::AdminAction;
use serde::Serialize;
use tracing::info;

use crate::storage_api::media_processing::BUCKET_MEDIA_PROCESSING_CONFIG;

const ROUTE_PATH: &str = "/v3/media-processing/{bucket}";
const CAPABILITIES_ROUTE_PATH: &str = "/v3/media-processing/{bucket}/capabilities";
const ERR_CODE_NO_SUCH_CONFIGURATION: &str = "NoSuchMediaProcessingConfiguration";

pub struct SetBucketMediaProcessingHandler;
pub struct GetBucketMediaProcessingHandler;
pub struct DeleteBucketMediaProcessingHandler;
pub struct GetBucketMediaProcessingCapabilitiesHandler;

pub fn register_media_processing_route(r: &mut S3Router<AdminOperation>) -> std::io::Result<()> {
    r.insert(
        Method::PUT,
        format!("{ADMIN_PREFIX}{ROUTE_PATH}").as_str(),
        AdminOperation(&SetBucketMediaProcessingHandler {}),
    )?;
    r.insert(
        Method::GET,
        format!("{ADMIN_PREFIX}{ROUTE_PATH}").as_str(),
        AdminOperation(&GetBucketMediaProcessingHandler {}),
    )?;
    r.insert(
        Method::DELETE,
        format!("{ADMIN_PREFIX}{ROUTE_PATH}").as_str(),
        AdminOperation(&DeleteBucketMediaProcessingHandler {}),
    )?;
    r.insert(
        Method::GET,
        format!("{ADMIN_PREFIX}{CAPABILITIES_ROUTE_PATH}").as_str(),
        AdminOperation(&GetBucketMediaProcessingCapabilitiesHandler {}),
    )?;
    Ok(())
}

#[derive(Serialize)]
struct GetBucketMediaProcessingResponse {
    bucket: String,
    config: MediaProfile,
    updated_at: String,
}

#[derive(Serialize)]
struct SetBucketMediaProcessingResponse {
    bucket: String,
    config: MediaProfile,
    updated_at: String,
}

/// What the local toolchain can encode, so a profile can be written against it.
#[derive(Serialize)]
struct MediaProcessingCapabilitiesResponse {
    /// False when no ffmpeg with libx264 was reachable; rendition will not run.
    available: bool,
    /// Whether the worker pool is started. It is off whenever `available` is.
    running: bool,
    encoders: Vec<String>,
    /// Pixel formats a rendition may name in `pixelFormat`.
    pixel_formats: Vec<String>,
}

/// The error `GET` answers when a bucket has never had a profile.
///
/// The status has to be set explicitly: a custom error code otherwise falls
/// back to 500, and a bucket without a profile is the normal first-run state,
/// not a server fault. The console reads the status to tell the two apart.
fn no_such_configuration_error(bucket: &str) -> S3Error {
    let mut error = S3Error::with_message(
        S3ErrorCode::Custom(ERR_CODE_NO_SUCH_CONFIGURATION.into()),
        format!("media processing is not configured for bucket {bucket}"),
    );
    error.set_status_code(StatusCode::NOT_FOUND);
    error
}

/// Parses a profile, treating "nothing configured" as "use the defaults".
///
/// An empty body is the quickest way to turn a bucket into an image/video host,
/// and so is `{}`. Without the second case `{}` deserialises to a profile with
/// no rules at all — silently processing nothing — which reads as a successful
/// configuration but does nothing.
fn parse_profile(body: &[u8]) -> S3Result<MediaProfile> {
    if body.iter().all(u8::is_ascii_whitespace) {
        return Ok(MediaProfile::default());
    }
    let profile = serde_json::from_slice::<serde_json::Value>(body)
        .map_err(|err| admin_s3_error(S3ErrorCode::InvalidArgument, format!("invalid media processing config: {err}")))?;
    if profile.as_object().is_some_and(serde_json::Map::is_empty) {
        return Ok(MediaProfile::default());
    }
    serde_json::from_value(profile)
        .map_err(|err| admin_s3_error(S3ErrorCode::InvalidArgument, format!("invalid media processing config: {err}")))
}

/// Rejects a profile that could never produce anything, so a typo fails at
/// configuration time rather than silently on the first upload.
fn validate(profile: &MediaProfile) -> S3Result<()> {
    if profile.variant_prefix.trim_matches('/').is_empty() {
        return Err(admin_s3_error(
            S3ErrorCode::InvalidArgument,
            "variantPrefix must not be empty: derived objects would collide with their sources",
        ));
    }
    if profile
        .rules
        .iter()
        .all(|rule| rule.images.is_empty() && rule.videos.is_empty())
    {
        return Err(admin_s3_error(
            S3ErrorCode::InvalidArgument,
            "at least one rule must define an image or video rendition",
        ));
    }
    if profile.min_source_bytes >= profile.max_source_bytes {
        return Err(admin_s3_error(
            S3ErrorCode::InvalidArgument,
            "minSourceBytes must be smaller than maxSourceBytes",
        ));
    }
    if !(0.0..=1.0).contains(&profile.min_variant_ratio) {
        return Err(admin_s3_error(S3ErrorCode::InvalidArgument, "minVariantRatio must be between 0 and 1"));
    }

    for rule in &profile.rules {
        for variant in &rule.images {
            check_pixel_format(&rule.name, &variant.name, variant.pixel_format.as_deref())?;
        }
        for variant in &rule.videos {
            check_pixel_format(&rule.name, &variant.name, variant.pixel_format.as_deref())?;
            if variant.max_fps.is_some_and(|fps| !fps.is_finite() || fps <= 0.0) {
                return Err(admin_s3_error(
                    S3ErrorCode::InvalidArgument,
                    format!("rule {} rendition {}: maxFps must be a positive number", rule.name, variant.name),
                ));
            }
            if variant.video_codec.supports_crf() && variant.crf > 63 {
                return Err(admin_s3_error(
                    S3ErrorCode::InvalidArgument,
                    format!(
                        "rule {} rendition {}: crf {} is outside the 0-63 range every supported encoder accepts",
                        rule.name, variant.name, variant.crf
                    ),
                ));
            }
            // A frame-rate bound below ~10fps is almost certainly a typo, and
            // rendering it would cost minutes per object to produce something
            // unusable.
            if variant.max_fps.is_some_and(|fps| fps < 1.0) {
                return Err(admin_s3_error(
                    S3ErrorCode::InvalidArgument,
                    format!(
                        "rule {} rendition {}: maxFps below 1 would produce an unusable clip",
                        rule.name, variant.name
                    ),
                ));
            }
        }
    }
    Ok(())
}

/// Rejects a pixel format ffmpeg will not recognise.
///
/// A misspelled format is otherwise invisible: the profile is accepted, the
/// bucket reports itself configured, and every rendition of every upload fails
/// at render time with a message only the operator will ever read.
fn check_pixel_format(rule: &str, rendition: &str, pixel_format: Option<&str>) -> S3Result<()> {
    let Some(pixel_format) = pixel_format.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(());
    };
    if is_known_pixel_format(&pixel_format.to_ascii_lowercase()) {
        return Ok(());
    }
    Err(admin_s3_error(
        S3ErrorCode::InvalidArgument,
        format!(
            "rule {rule} rendition {rendition}: {pixel_format} is not a known pixel format; supported: {}",
            KNOWN_PIXEL_FORMATS.join(", ")
        ),
    ))
}

#[async_trait::async_trait]
impl Operation for SetBucketMediaProcessingHandler {
    #[tracing::instrument(skip_all)]
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let bucket = bucket_from_params(&params)?;
        let cred = authorize_for_bucket(&req, AdminAction::SetBucketMediaProcessingAction, &bucket).await?;

        if object_store_from_req(&req).is_none() {
            return Err(admin_s3_error(S3ErrorCode::InternalError, "object store is not initialized"));
        }
        let path = req.uri.path().to_owned();
        let context = app_context_from_req(&req);
        // Reading the body consumes `req.input`, so everything still needed
        // afterwards is captured here.
        let body = read_compatible_admin_body(req.input, MAX_ADMIN_REQUEST_BODY_SIZE, &path, &cred.secret_key).await?;
        let profile = parse_profile(&body)?;
        validate(&profile)?;

        let expected_incarnation_id = metadata_sys::capture_bucket_metadata_incarnation(&bucket)
            .await
            .map_err(|err| admin_s3_error(S3ErrorCode::InternalError, format!("failed to capture bucket incarnation: {err}")))?;

        let json = serde_json::to_vec(&profile)
            .map_err(|err| admin_s3_error(S3ErrorCode::InternalError, format!("failed to encode config: {err}")))?;
        let updated_at =
            metadata_sys::update_if_incarnation(&bucket, BUCKET_MEDIA_PROCESSING_CONFIG, json, expected_incarnation_id)
                .await
                .map_err(|err| {
                    admin_s3_error(S3ErrorCode::InternalError, format!("failed to save media processing config: {err}"))
                })?;

        info!(
            event = "admin_media_processing_config_set",
            component = "admin",
            subsystem = "media_processing",
            bucket = %bucket,
            enabled = profile.enabled,
            rules = profile.rules.len(),
            "media processing config set"
        );
        notify_peers_reload(context, bucket.clone(), "set media processing config");

        let response = SetBucketMediaProcessingResponse {
            bucket,
            config: profile,
            updated_at: format_updated_at(updated_at)?,
        };
        admin_json_response(req.uri.path(), &cred.secret_key, StatusCode::OK, &response)
    }
}

#[async_trait::async_trait]
impl Operation for GetBucketMediaProcessingHandler {
    #[tracing::instrument(skip_all)]
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let bucket = bucket_from_params(&params)?;
        let cred = authorize_for_bucket(&req, AdminAction::GetBucketMediaProcessingAction, &bucket).await?;

        if object_store_from_req(&req).is_none() {
            return Err(admin_s3_error(S3ErrorCode::InternalError, "object store is not initialized"));
        }
        let Some((raw, updated_at)) = crate::storage_api::media_processing::get_media_processing_config(&bucket)
            .await
            .map_err(|err| {
                admin_s3_error(S3ErrorCode::InternalError, format!("failed to read media processing config: {err}"))
            })?
        else {
            return Err(no_such_configuration_error(&bucket));
        };

        let config: MediaProfile = serde_json::from_slice(&raw).map_err(|err| {
            admin_s3_error(S3ErrorCode::InternalError, format!("stored media processing config is unreadable: {err}"))
        })?;

        let response = GetBucketMediaProcessingResponse {
            bucket,
            config,
            updated_at: format_updated_at(updated_at)?,
        };
        admin_json_response(req.uri.path(), &cred.secret_key, StatusCode::OK, &response)
    }
}

#[async_trait::async_trait]
impl Operation for DeleteBucketMediaProcessingHandler {
    #[tracing::instrument(skip_all)]
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let bucket = bucket_from_params(&params)?;
        authorize_for_bucket(&req, AdminAction::SetBucketMediaProcessingAction, &bucket).await?;

        let expected_incarnation_id = metadata_sys::capture_bucket_metadata_incarnation(&bucket)
            .await
            .map_err(|err| admin_s3_error(S3ErrorCode::InternalError, format!("failed to capture bucket incarnation: {err}")))?;

        // Idempotent: clearing an absent config still answers 204. Already
        // derived objects stay in place.
        metadata_sys::delete_if_incarnation(&bucket, BUCKET_MEDIA_PROCESSING_CONFIG, expected_incarnation_id)
            .await
            .map_err(|err| {
                admin_s3_error(S3ErrorCode::InternalError, format!("failed to clear media processing config: {err}"))
            })?;

        info!(
            event = "admin_media_processing_config_cleared",
            component = "admin",
            subsystem = "media_processing",
            bucket = %bucket,
            "media processing config cleared"
        );
        notify_peers_reload(app_context_from_req(&req), bucket.clone(), "clear media processing config");

        Ok(S3Response::new((StatusCode::NO_CONTENT, Body::empty())))
    }
}

#[async_trait::async_trait]
impl Operation for GetBucketMediaProcessingCapabilitiesHandler {
    #[tracing::instrument(skip_all)]
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let bucket = bucket_from_params(&params)?;
        let cred = authorize_for_bucket(&req, AdminAction::GetBucketMediaProcessingAction, &bucket).await?;

        let engine = crate::app::media_processing::engine_from_env();
        // The pool only runs with a working ffmpeg that has libx264; report the
        // same gate the startup probe uses rather than a second, divergent
        // definition.
        let available = engine.is_available().await && engine.supports_encoder("libx264").await;
        let encoders = if available {
            engine.encoders().await.unwrap_or_default()
        } else {
            Vec::new()
        };

        let response = MediaProcessingCapabilitiesResponse {
            available,
            running: crate::app::media_processing::is_running(),
            encoders,
            pixel_formats: KNOWN_PIXEL_FORMATS.iter().map(|format| (*format).to_owned()).collect(),
        };
        if !available {
            // The body still carries the reason so a client can show it rather
            // than only seeing a status code.
            let payload = serde_json::to_vec(&response).unwrap_or_default();
            return Ok(S3Response::new((StatusCode::SERVICE_UNAVAILABLE, Body::from(payload))));
        }
        admin_json_response(req.uri.path(), &cred.secret_key, StatusCode::OK, &response)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn profile(json: &str) -> S3Result<MediaProfile> {
        parse_profile(json.as_bytes())
    }

    #[test]
    fn an_empty_body_yields_the_default_profile() {
        let parsed = profile("   \n").expect("defaults");
        assert!(parsed.enabled());
        assert!(!parsed.rules.is_empty());

        // `{}` says nothing either, and must not quietly configure a profile
        // with no rules — that processes nothing while looking configured.
        let empty_object = profile("{}").expect("defaults");
        assert!(empty_object.enabled());
        assert!(!empty_object.rules.is_empty());
    }

    #[test]
    fn malformed_json_is_rejected_with_the_reason() {
        assert!(profile("{not json").is_err());
    }

    #[test]
    fn unknown_fields_are_tolerated_so_operators_can_edit_json_by_hand() {
        let parsed = profile(r#"{"enabled":true,"futureKnob":1,"rules":[]}"#).expect("tolerated");
        assert!(parsed.enabled());
    }

    #[test]
    fn a_profile_that_could_never_produce_anything_is_refused() {
        let empty_prefix =
            profile(r#"{"variantPrefix":"","rules":[{"name":"r","videos":[{"name":"720p","maxHeight":720}]}]}"#).expect("parses");
        assert!(validate(&empty_prefix).is_err(), "an empty prefix would collide with sources");

        let no_renditions = profile(r#"{"rules":[{"name":"r"}]}"#).expect("parses");
        assert!(validate(&no_renditions).is_err(), "a rule with no renditions does nothing");

        let inverted_bounds = profile(
            r#"{"minSourceBytes":100,"maxSourceBytes":10,
                "rules":[{"name":"r","videos":[{"name":"720p","maxHeight":720}]}]}"#,
        )
        .expect("parses");
        assert!(validate(&inverted_bounds).is_err(), "the size window must not be empty");
    }

    #[test]
    fn a_bucket_without_a_profile_answers_not_found() {
        // A 500 here makes the console report a broken server for a bucket that
        // simply has not been configured yet.
        let error = no_such_configuration_error("media");
        assert_eq!(error.status_code(), Some(StatusCode::NOT_FOUND));
        assert!(
            error
                .message()
                .is_some_and(|message| message.contains("media processing is not configured")),
            "the message must name the condition: {:?}",
            error.message()
        );
    }

    #[test]
    fn a_usable_profile_passes_validation() {
        let good = profile(
            r#"{"enabled":true,"variantPrefix":"derived",
                "rules":[{"name":"clips","includeSuffixes":["mkv"],
                          "videos":[{"name":"720p","maxHeight":720}],"deleteSource":false}]}"#,
        )
        .expect("parses");
        validate(&good).expect("valid");

        let rule = good.rule_for("clips/a.mkv").expect("rule claims the clip");
        assert_eq!(rule.videos[0].max_height, Some(720));
        assert!(!rule.delete_source);
    }

    #[test]
    fn a_mistyped_pixel_format_is_refused_at_configuration_time() {
        let bad = profile(
            r#"{"rules":[{"name":"clips",
                "videos":[{"name":"720p","pixelFormat":"yuv420"}]}]}"#,
        )
        .expect("parses");
        let error = validate(&bad).expect_err("yuv420 is not a pixel format");
        assert!(
            error.message().is_some_and(|message| message.contains("yuv420p")),
            "the error must name a valid alternative: {:?}",
            error.message()
        );

        let also_bad = profile(r#"{"rules":[{"name":"r","images":[{"name":"t","pixelFormat":"rgb99"}]}]}"#).expect("parses");
        assert!(validate(&also_bad).is_err());

        let good = profile(
            r#"{"rules":[{"name":"clips",
                "videos":[{"name":"720p","pixelFormat":"yuv420p10le","maxFps":30,"crf":20}],
                "images":[{"name":"gray","pixelFormat":"gray"}]}]}"#,
        )
        .expect("parses");
        validate(&good).expect("valid");
    }

    #[test]
    fn nonsensical_encoding_bounds_are_refused() {
        let bad_fps = profile(r#"{"rules":[{"name":"r","videos":[{"name":"v","maxFps":0.0}]}]}"#).expect("parses");
        assert!(validate(&bad_fps).is_err(), "a zero frame rate cannot be encoded");

        let bad_crf = profile(r#"{"rules":[{"name":"r","videos":[{"name":"v","crf":200}]}]}"#).expect("parses");
        assert!(validate(&bad_crf).is_err(), "crf 200 is outside every encoder's range");

        let bad_ratio = profile(
            r#"{"minVariantRatio":1.5,
                "rules":[{"name":"r","videos":[{"name":"v"}]}]}"#,
        )
        .expect("parses");
        assert!(validate(&bad_ratio).is_err());
    }
}
