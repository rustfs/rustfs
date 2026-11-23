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

use crate::app::bucket_usecase::DefaultBucketUsecase;
use crate::app::storage_api::s3::{ListObjectsV2Input, ListObjectsV2Output, S3Request, S3Response, S3Result};

/// ListObjectsV2 through `usecase` as the legacy stack answers it.
///
/// Owned by the ListObjectsV2 batch (B2). Tests outside that batch's files call this seam instead of the use-case
/// method; when batch B2 changes the use-case signature it changes only this body.
pub(crate) async fn list_objects_v2(
    usecase: &DefaultBucketUsecase,
    req: S3Request<ListObjectsV2Input>,
) -> S3Result<S3Response<ListObjectsV2Output>> {
    use rustfs_gateway_types::compat::s3s_5761ddfe::generated::ops::list_objects_v2 as ops;
    // Same facts `RequestEnvelope::from_s3s` copies: method, URI, headers, extensions, region, service, and credentials.
    let credentials = req.credentials.as_ref().map(|credentials| {
        let session_token = crate::auth::get_session_token(&req.uri, &req.headers).map(str::to_owned);
        crate::app::RequestCredentials::new(
            credentials.access_key.clone(),
            credentials.secret_key.expose().to_owned(),
            session_token.clone(),
        )
        .unwrap_or_else(|_| crate::app::RequestCredentials::without_secret_key(credentials.access_key.clone(), session_token))
    });
    let envelope = crate::app::RequestEnvelope::from_parts(
        crate::app::RequestHead {
            method: req.method.clone(),
            uri: req.uri.clone(),
            headers: req.headers.clone(),
            credentials,
            region: req.region.as_ref().map(|region| region.as_str().to_owned()),
            service: req.service.clone(),
        },
        req.extensions.clone(),
    );
    let input = ops::input_from_s3s(req.input).map_err(crate::app::dto_values::seam_error)?;
    let mut output = list_objects_v2_owned(usecase, envelope, input).await?;
    let migration_list = output.rustfs_on_demand_migration_list.take();
    let s3s_output = ops::output_to_s3s(output).map_err(crate::app::dto_values::seam_error)?;
    let mut response = S3Response::new(s3s_output);
    if let Some(value) = migration_list {
        response.headers.insert(
            "x-rustfs-on-demand-migration-list",
            http::HeaderValue::from_str(&value).expect("valid migration list header"),
        );
    }
    Ok(response)
}

/// [`list_objects_v2`] for a fixture that sets nothing but `GET /`.
///
/// `build_request` leaves the URI, headers, and extensions empty, so this envelope matches that request.
#[cfg(test)]
pub(crate) async fn list_objects_v2_method_only(
    usecase: &DefaultBucketUsecase,
    input: ListObjectsV2Input,
) -> S3Result<S3Response<ListObjectsV2Output>> {
    use rustfs_gateway_types::compat::s3s_5761ddfe::generated::ops::list_objects_v2 as ops;
    let envelope = crate::app::RequestEnvelope::from_parts(
        crate::app::RequestHead {
            method: http::Method::GET,
            uri: http::Uri::from_static("/"),
            headers: http::HeaderMap::new(),
            credentials: None,
            region: None,
            service: None,
        },
        http::Extensions::new(),
    );
    let input = ops::input_from_s3s(input).map_err(crate::app::dto_values::seam_error)?;
    let mut output = list_objects_v2_owned(usecase, envelope, input).await?;
    let migration_list = output.rustfs_on_demand_migration_list.take();
    let s3s_output = ops::output_to_s3s(output).map_err(crate::app::dto_values::seam_error)?;
    let mut response = S3Response::new(s3s_output);
    if let Some(value) = migration_list {
        response.headers.insert(
            "x-rustfs-on-demand-migration-list",
            http::HeaderValue::from_str(&value).expect("valid migration list header"),
        );
    }
    Ok(response)
}

/// Calls `DefaultBucketUsecase::execute_list_objects_v2` with owned inputs.
pub(crate) async fn list_objects_v2_owned(
    usecase: &DefaultBucketUsecase,
    envelope: crate::app::RequestEnvelope,
    input: rustfs_gateway::dto::ListObjectsV2Input,
) -> rustfs_s3_types::S3Result<rustfs_gateway::dto::ListObjectsV2Output> {
    usecase.execute_list_objects_v2(envelope, input).await
}
