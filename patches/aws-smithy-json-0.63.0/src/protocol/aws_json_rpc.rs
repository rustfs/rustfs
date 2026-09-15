/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! AWS JSON RPC protocol implementation (`awsJson1_0` and `awsJson1_1`).
//!
//! # Protocol behaviors
//!
//! - HTTP method: always POST, path: always `/`
//! - `X-Amz-Target`: `{ServiceName}.{OperationName}` (required)
//! - Does **not** use `@jsonName` trait
//! - Default timestamp format: `epoch-seconds`
//! - Ignores HTTP binding traits
//!
//! # Differences between 1.0 and 1.1
//!
//! - Content-Type: `application/x-amz-json-1.0` vs `application/x-amz-json-1.1`
//! - Error `__type` serialization differs on the server side, but clients MUST
//!   accept either format for both versions.

use crate::codec::{JsonCodec, JsonCodecSettings};
use aws_smithy_runtime_api::client::orchestrator::Metadata;
use aws_smithy_schema::http_protocol::HttpRpcProtocol;
use aws_smithy_schema::{shape_id, Schema, ShapeId};
use aws_smithy_types::config_bag::ConfigBag;

/// AWS JSON RPC protocol (`awsJson1_0` / `awsJson1_1`).
#[derive(Debug)]
pub struct AwsJsonRpcProtocol {
    inner: HttpRpcProtocol<JsonCodec>,
    target_prefix: String,
}

impl AwsJsonRpcProtocol {
    /// Creates an AWS JSON 1.0 protocol instance.
    ///
    /// `target_prefix` is the Smithy service shape name used in the `X-Amz-Target` header
    /// (e.g., `"TrentService"` for KMS, `"DynamoDB_20120810"` for DynamoDB).
    pub fn aws_json_1_0(target_prefix: impl Into<String>) -> Self {
        Self::new(
            shape_id!("aws.protocols", "awsJson1_0"),
            "application/x-amz-json-1.0",
            target_prefix.into(),
        )
    }

    /// Creates an AWS JSON 1.1 protocol instance.
    ///
    /// `target_prefix` is the Smithy service shape name used in the `X-Amz-Target` header.
    pub fn aws_json_1_1(target_prefix: impl Into<String>) -> Self {
        Self::new(
            shape_id!("aws.protocols", "awsJson1_1"),
            "application/x-amz-json-1.1",
            target_prefix.into(),
        )
    }

    fn new(protocol_id: ShapeId, content_type: &'static str, target_prefix: String) -> Self {
        let codec = JsonCodec::new(
            JsonCodecSettings::builder()
                .use_json_name(false)
                .default_timestamp_format(aws_smithy_types::date_time::Format::EpochSeconds)
                .build(),
        );
        Self {
            inner: HttpRpcProtocol::new(protocol_id, codec, content_type),
            target_prefix,
        }
    }
}

impl aws_smithy_schema::protocol::ClientProtocolInner for AwsJsonRpcProtocol {
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
        let mut request = self
            .inner
            .serialize_request(input, input_schema, endpoint, cfg)?;
        if let Some(metadata) = cfg.load::<Metadata>() {
            request.headers_mut().insert(
                "X-Amz-Target",
                format!("{}.{}", self.target_prefix, metadata.name()),
            );
        }
        Ok(request)
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

#[cfg(test)]
mod tests {
    use super::*;
    use aws_smithy_schema::protocol::ClientProtocolInner;
    use aws_smithy_schema::serde::{SerdeError, SerializableStruct, ShapeSerializer};
    use aws_smithy_schema::ShapeType;
    use aws_smithy_types::config_bag::Layer;

    struct EmptyStruct;
    impl SerializableStruct for EmptyStruct {
        fn serialize_members(&self, _: &mut dyn ShapeSerializer) -> Result<(), SerdeError> {
            Ok(())
        }
    }

    static TEST_SCHEMA: aws_smithy_schema::Schema =
        aws_smithy_schema::Schema::new(shape_id!("test", "Input"), ShapeType::Structure);

    fn cfg_with_metadata(service: &str, operation: &str) -> ConfigBag {
        let mut layer = Layer::new("test");
        layer.store_put(Metadata::new(operation.to_string(), service.to_string()));
        ConfigBag::of_layers(vec![layer])
    }

    #[test]
    fn json_1_0_content_type() {
        let request = AwsJsonRpcProtocol::aws_json_1_0("TestService")
            .serialize_request(
                &EmptyStruct,
                &TEST_SCHEMA,
                "https://example.com",
                &ConfigBag::base(),
            )
            .unwrap();
        assert_eq!(
            request.headers().get("Content-Type").unwrap(),
            "application/x-amz-json-1.0"
        );
    }

    #[test]
    fn json_1_1_content_type() {
        let request = AwsJsonRpcProtocol::aws_json_1_1("TestService")
            .serialize_request(
                &EmptyStruct,
                &TEST_SCHEMA,
                "https://example.com",
                &ConfigBag::base(),
            )
            .unwrap();
        assert_eq!(
            request.headers().get("Content-Type").unwrap(),
            "application/x-amz-json-1.1"
        );
    }

    #[test]
    fn sets_x_amz_target() {
        let cfg = cfg_with_metadata("MyService", "DoThing");
        let request = AwsJsonRpcProtocol::aws_json_1_0("MyService")
            .serialize_request(&EmptyStruct, &TEST_SCHEMA, "https://example.com", &cfg)
            .unwrap();
        assert_eq!(
            request.headers().get("X-Amz-Target").unwrap(),
            "MyService.DoThing"
        );
    }

    #[test]
    fn json_1_0_protocol_id() {
        assert_eq!(
            AwsJsonRpcProtocol::aws_json_1_0("Svc")
                .protocol_id()
                .as_str(),
            "aws.protocols#awsJson1_0"
        );
    }

    #[test]
    fn json_1_1_protocol_id() {
        assert_eq!(
            AwsJsonRpcProtocol::aws_json_1_1("Svc")
                .protocol_id()
                .as_str(),
            "aws.protocols#awsJson1_1"
        );
    }
}
