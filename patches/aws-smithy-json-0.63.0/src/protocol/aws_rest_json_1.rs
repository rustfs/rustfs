/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! AWS REST JSON 1.0 protocol implementation.
//!
//! This module provides [`AwsRestJsonProtocol`], which constructs an
//! [`HttpBindingProtocol`] with a [`JsonCodec`] configured for the
//! `aws.protocols#restJson1` protocol:
//!
//! - Uses `@jsonName` trait for JSON property names
//! - Default timestamp format: `epoch-seconds`
//! - Content-Type: `application/json`

use crate::codec::{JsonCodec, JsonCodecSettings};
use aws_smithy_schema::http_protocol::HttpBindingProtocol;
use aws_smithy_schema::{Schema, ShapeId};
use aws_smithy_types::config_bag::ConfigBag;

static PROTOCOL_ID: ShapeId = ShapeId::from_static("aws.protocols", "restJson1", "");

/// AWS REST JSON 1.0 protocol (`aws.protocols#restJson1`).
///
/// This is a thin configuration wrapper that constructs an [`HttpBindingProtocol`]
/// with a [`JsonCodec`] using REST JSON settings. The `HttpBindingProtocol` handles
/// splitting members between HTTP bindings and the JSON payload.
#[derive(Debug)]
pub struct AwsRestJsonProtocol {
    inner: HttpBindingProtocol<JsonCodec>,
}

impl AwsRestJsonProtocol {
    /// Creates a new REST JSON protocol with default settings.
    pub fn new() -> Self {
        let codec = JsonCodec::new(
            JsonCodecSettings::builder()
                .use_json_name(true)
                .default_timestamp_format(aws_smithy_types::date_time::Format::EpochSeconds)
                .build(),
        );
        Self {
            inner: HttpBindingProtocol::new(PROTOCOL_ID, codec, "application/json"),
        }
    }

    /// Returns a reference to the inner `HttpBindingProtocol`.
    pub fn inner(&self) -> &HttpBindingProtocol<JsonCodec> {
        &self.inner
    }
}

impl Default for AwsRestJsonProtocol {
    fn default() -> Self {
        Self::new()
    }
}

impl aws_smithy_schema::protocol::ClientProtocolInner for AwsRestJsonProtocol {
    type Request = aws_smithy_runtime_api::http::Request;
    type Response = aws_smithy_runtime_api::http::Response;

    fn protocol_id(&self) -> &ShapeId {
        self.inner.protocol_id()
    }

    fn serialize_request(
        &self,
        input: &dyn aws_smithy_schema::serde::SerializableStruct,
        input_schema: &Schema,
        endpoint: &str,
        cfg: &ConfigBag,
    ) -> Result<aws_smithy_runtime_api::http::Request, aws_smithy_schema::serde::SerdeError> {
        self.inner
            .serialize_request(input, input_schema, endpoint, cfg)
    }

    fn deserialize_response<'a>(
        &self,
        response: &'a aws_smithy_runtime_api::http::Response,
        output_schema: &Schema,
        cfg: &ConfigBag,
    ) -> Result<
        Box<dyn aws_smithy_schema::serde::ShapeDeserializer + 'a>,
        aws_smithy_schema::serde::SerdeError,
    > {
        self.inner
            .deserialize_response(response, output_schema, cfg)
    }

    fn payload_codec(&self) -> Option<&dyn aws_smithy_schema::codec::DynCodec> {
        self.inner.payload_codec()
    }

    fn update_endpoint(
        &self,
        request: &mut aws_smithy_runtime_api::http::Request,
        endpoint: &aws_smithy_types::endpoint::Endpoint,
        cfg: &ConfigBag,
    ) -> Result<(), aws_smithy_schema::serde::SerdeError> {
        self.inner.update_endpoint(request, endpoint, cfg)
    }
}
