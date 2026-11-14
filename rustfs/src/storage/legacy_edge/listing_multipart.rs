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

//! Legacy s3s edge of the object and multipart-upload listings, CreateMultipartUpload and
//! AbortMultipartUpload: batch B2 of the gateway migration (rustfs/backlog#2765).
//!
//! `impl S3 for FS` in `ecfs.rs` calls the `*_edge` fn of each operation here. A migrated
//! operation converts its request into the gateway's input, runs the owned use case and converts
//! the answer back, then applies what the legacy encoder needs and the gateway's encoder does
//! itself: `encoding-type=url` (legacy RustFS's members and per-segment rule, the one the gateway
//! applies under its RustFS profile) and XML escaping of entity tags. The batch that moves these
//! operations onto the gateway owns this file and is the only one that edits it.

use crate::storage::ecfs::*;
use crate::storage::helper::audit_only_operation;
use crate::storage::s3_api::bucket::encode_list_output_value;
use crate::storage::s3_api::{self, mark_on_demand_migration_list_local_only};
use rustfs_gateway_types::compat::s3s_5761ddfe::generated::ops::{
    list_multipart_uploads, list_object_versions, list_objects, list_objects_v2,
};
use rustfs_io_metrics::record_s3_op;
use rustfs_s3_ops::S3Operation;
use tracing::instrument;

impl FS {
    #[instrument(name = "abort_multipart_upload", level = "debug", skip(self, req))]
    pub(in crate::storage::ecfs) async fn abort_multipart_upload_edge(
        &self,
        req: S3Request<AbortMultipartUploadInput>,
    ) -> S3Result<S3Response<AbortMultipartUploadOutput>> {
        let usecase = s3_api::multipart_usecase_for(self);
        audit_only_operation(req, S3Operation::AbortMultipartUpload, |req| usecase.execute_abort_multipart_upload(req)).await
    }

    #[instrument(name = "create_multipart_upload", level = "debug", skip(self, req))]
    pub(in crate::storage::ecfs) async fn create_multipart_upload_edge(
        &self,
        req: S3Request<CreateMultipartUploadInput>,
    ) -> S3Result<S3Response<CreateMultipartUploadOutput>> {
        crate::hp_guard!("S3::create_multipart_upload");
        let usecase = s3_api::multipart_usecase_for(self);
        usecase.execute_create_multipart_upload(req).await
    }

    pub(in crate::storage::ecfs) async fn list_multipart_uploads_edge(
        &self,
        req: S3Request<ListMultipartUploadsInput>,
    ) -> S3Result<S3Response<ListMultipartUploadsOutput>> {
        record_s3_op(S3Operation::ListMultipartUploads);
        let usecase = s3_api::multipart_usecase_for(self);
        let encoding = req.input.encoding_type.clone();
        let mut response = through_gateway_seam(
            req,
            list_multipart_uploads::input_from_s3s,
            list_multipart_uploads::output_to_s3s,
            |envelope, input| usecase.execute_list_multipart_uploads(envelope, input),
        )
        .await?;
        legacy_list_multipart_uploads(&mut response.output, encoding.as_ref());
        Ok(response)
    }

    pub(in crate::storage::ecfs) async fn list_object_versions_edge(
        &self,
        req: S3Request<ListObjectVersionsInput>,
    ) -> S3Result<S3Response<ListObjectVersionsOutput>> {
        record_s3_op(S3Operation::ListObjectVersions);
        let usecase = s3_api::bucket_usecase_for(self);
        let encoding = req.input.encoding_type.clone();
        let mut response = through_gateway_seam(
            req,
            list_object_versions::input_from_s3s,
            list_object_versions::output_to_s3s,
            |envelope, input| usecase.execute_list_object_versions(envelope, input),
        )
        .await?;
        legacy_list_object_versions(&mut response.output, encoding.as_ref());
        Ok(response)
    }

    #[instrument(name = "list_objects", level = "debug", skip(self, req))]
    pub(in crate::storage::ecfs) async fn list_objects_edge(
        &self,
        req: S3Request<ListObjectsInput>,
    ) -> S3Result<S3Response<ListObjectsOutput>> {
        record_s3_op(S3Operation::ListObjects);
        let usecase = s3_api::bucket_usecase_for(self);
        let encoding = req.input.encoding_type.clone();
        let mut response =
            through_gateway_seam(req, list_objects::input_from_s3s, list_objects::output_to_s3s, |envelope, input| {
                usecase.execute_list_objects(envelope, input)
            })
            .await?;
        let output = &mut response.output;
        legacy_listing_entries(&mut output.contents, &mut output.common_prefixes, encoding.as_ref());
        output.next_marker = output
            .next_marker
            .take()
            .map(|marker| encode_list_output_value(marker, encoding.as_ref()));
        Ok(response)
    }

    #[instrument(name = "list_objects_v2", level = "debug", skip(self, req))]
    pub(in crate::storage::ecfs) async fn list_objects_v2_edge(
        &self,
        req: S3Request<ListObjectsV2Input>,
    ) -> S3Result<S3Response<ListObjectsV2Output>> {
        crate::hp_guard!("S3::list_objects_v2");
        record_s3_op(S3Operation::ListObjectsV2);
        let usecase = s3_api::bucket_usecase_for(self);
        list_objects_v2_through_legacy_edge(req, |envelope, input| usecase.execute_list_objects_v2(envelope, input)).await
    }

    #[instrument(name = "list_parts", level = "debug", skip(self, req))]
    pub(in crate::storage::ecfs) async fn list_parts_edge(
        &self,
        req: S3Request<ListPartsInput>,
    ) -> S3Result<S3Response<ListPartsOutput>> {
        record_s3_op(S3Operation::ListParts);
        let usecase = s3_api::multipart_usecase_for(self);
        usecase.execute_list_parts(req).await
    }
}

/// The listing entries as the legacy encoder must receive them: each key and rolled-up prefix
/// percent-encoded segment by segment when the request asked for exactly `url`, and each entity tag
/// XML-escaped, because the legacy encoder writes the tag's text unescaped.
pub(super) fn legacy_listing_entries(
    contents: &mut Option<Vec<Object>>,
    common_prefixes: &mut Option<Vec<CommonPrefix>>,
    encoding: Option<&EncodingType>,
) {
    let encode = |value: &mut String| *value = encode_list_output_value(std::mem::take(value), encoding);
    for object in contents.iter_mut().flatten() {
        if let Some(key) = object.key.as_mut() {
            encode(key);
        }
        escape_entity_tag(&mut object.e_tag);
    }
    for prefix in common_prefixes
        .iter_mut()
        .flatten()
        .filter_map(|prefix| prefix.prefix.as_mut())
    {
        encode(prefix);
    }
}

/// ListObjectsV2 as the legacy stack answers it, without the count; the test seam
/// (`app/test_seams/list_objects_v2.rs`) runs the same conversion.
pub(crate) async fn list_objects_v2_through_legacy_edge<F>(
    req: S3Request<ListObjectsV2Input>,
    usecase: impl FnOnce(RequestEnvelope, rustfs_gateway_types::dto::ListObjectsV2Input) -> F,
) -> S3Result<S3Response<ListObjectsV2Output>>
where
    F: Future<Output = rustfs_s3_types::S3Result<rustfs_gateway_types::dto::ListObjectsV2Output>>,
{
    let encoding = req.input.encoding_type.clone();
    let mut source_left_out = false;
    let mut response = through_gateway_seam(
        req,
        list_objects_v2::input_from_s3s,
        list_objects_v2::output_to_s3s,
        |envelope, input| async {
            let mut output = usecase(envelope, input).await?;
            // A RustFS member the s3s output has no field for: the legacy edge writes it as a header.
            source_left_out = output.rustfs_on_demand_migration_list.take().is_some();
            Ok(output)
        },
    )
    .await?;
    let output = &mut response.output;
    legacy_listing_entries(&mut output.contents, &mut output.common_prefixes, encoding.as_ref());
    if source_left_out {
        mark_on_demand_migration_list_local_only(&mut response.headers);
    }
    Ok(response)
}

/// The version listing as the legacy encoder must receive it: every key-shaped member
/// percent-encoded as in [`legacy_listing_entries`], and each entity tag XML-escaped.
pub(super) fn legacy_list_object_versions(output: &mut ListObjectVersionsOutput, encoding: Option<&EncodingType>) {
    let encode = |value: &mut String| *value = encode_list_output_value(std::mem::take(value), encoding);
    for value in [
        &mut output.prefix,
        &mut output.delimiter,
        &mut output.key_marker,
        &mut output.next_key_marker,
    ]
    .into_iter()
    .flatten()
    {
        encode(value);
    }
    for version in output.versions.iter_mut().flatten() {
        if let Some(key) = version.key.as_mut() {
            encode(key);
        }
        escape_entity_tag(&mut version.e_tag);
    }
    for marker in output
        .delete_markers
        .iter_mut()
        .flatten()
        .filter_map(|marker| marker.key.as_mut())
    {
        encode(marker);
    }
    for prefix in output
        .common_prefixes
        .iter_mut()
        .flatten()
        .filter_map(|prefix| prefix.prefix.as_mut())
    {
        encode(prefix);
    }
}

/// The legacy encoder writes an entity tag's text as given; the gateway's writer escapes it.
fn escape_entity_tag(e_tag: &mut Option<ETag>) {
    if let Some(ETag::Strong(tag) | ETag::Weak(tag)) = e_tag.as_mut() {
        *tag = quick_xml::escape::escape(tag.as_str()).into_owned();
    }
}

/// The upload listing as the legacy encoder must receive it: every key-shaped member
/// percent-encoded as in [`legacy_listing_entries`]; upload ids never are.
pub(super) fn legacy_list_multipart_uploads(output: &mut ListMultipartUploadsOutput, encoding: Option<&EncodingType>) {
    let encode = |value: &mut String| *value = encode_list_output_value(std::mem::take(value), encoding);
    for value in [
        &mut output.prefix,
        &mut output.delimiter,
        &mut output.key_marker,
        &mut output.next_key_marker,
    ]
    .into_iter()
    .flatten()
    {
        encode(value);
    }
    for key in output.uploads.iter_mut().flatten().filter_map(|upload| upload.key.as_mut()) {
        encode(key);
    }
    for prefix in output
        .common_prefixes
        .iter_mut()
        .flatten()
        .filter_map(|prefix| prefix.prefix.as_mut())
    {
        encode(prefix);
    }
}
