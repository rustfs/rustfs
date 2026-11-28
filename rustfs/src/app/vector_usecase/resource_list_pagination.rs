// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

//! Scoped resource-list positions. Every page still requires current authorization.

use super::*;
use crate::table_catalog::VectorBucketBindingV1;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ResourceListCursor {
    version: u8,
    context: String,
    pub(super) after: String,
    identity: String,
}

pub(super) struct ResourceListPagination {
    operation: Operation,
    context: String,
    prefix: Option<String>,
}

fn invalid_token() -> ProtocolError {
    ProtocolError::validation("nextToken", "Invalid continuation token")
}

fn hash(value: &impl Serialize) -> Result<String, ProtocolError> {
    crate::vector::pagination::digest(value)
        .map(|bytes| hex_simd::encode_to_string(bytes, hex_simd::AsciiCase::Lower))
        .map_err(|_| unavailable())
}

impl ResourceListPagination {
    pub(super) fn for_request(
        service: &DefaultVectorUsecase,
        authorization: &AuthorizedVectorRequest,
        request: &Request,
        parent: Option<&BucketHandle>,
    ) -> Result<Self, ProtocolError> {
        let app = service.server.installed_app_context().ok_or_else(unavailable)?;
        let deployment = app.object_store().id;
        let prefix = match request {
            Request::ListVectorBuckets(input) => input.prefix.as_deref(),
            Request::ListIndexes(input) => input.prefix.as_deref(),
            _ => return Err(unavailable()),
        };
        Self::new(
            request.operation(),
            deployment,
            &service.region,
            authorization.verified.pagination_principal()?,
            prefix,
            parent,
        )
    }

    fn new(
        operation: Operation,
        deployment: Uuid,
        region: &str,
        principal: [u8; 32],
        prefix: Option<&str>,
        parent: Option<&BucketHandle>,
    ) -> Result<Self, ProtocolError> {
        if deployment.is_nil()
            || !matches!(
                (operation, parent),
                (Operation::ListVectorBuckets, None) | (Operation::ListIndexes, Some(_))
            )
        {
            return Err(unavailable());
        }
        let parent = parent
            .map(VectorBucketBindingV1::from_handle)
            .transpose()
            .map_err(catalog_error)?;
        let context = hash(&(
            "rustfs.s3vectors.resource-list.context.v1",
            operation.name(),
            deployment,
            region,
            "",
            principal,
            prefix,
            parent,
        ))?;
        Ok(Self {
            operation,
            context,
            prefix: prefix.map(str::to_owned),
        })
    }

    fn validate_name(&self, name: &str) -> Result<(), ProtocolError> {
        let alphanumeric = |byte: u8| byte.is_ascii_lowercase() || byte.is_ascii_digit();
        let bytes = name.as_bytes();
        if !(3..=63).contains(&bytes.len())
            || !bytes.first().copied().is_some_and(alphanumeric)
            || !bytes.last().copied().is_some_and(alphanumeric)
            || !bytes
                .iter()
                .all(|byte| alphanumeric(*byte) || *byte == b'-' || (self.operation == Operation::ListIndexes && *byte == b'.'))
            || self.prefix.as_ref().is_some_and(|prefix| !name.starts_with(prefix))
        {
            return Err(invalid_token());
        }
        Ok(())
    }

    pub(super) fn decode(&self, token: Option<&str>) -> Result<Option<ResourceListCursor>, ProtocolError> {
        let Some(token) = token else { return Ok(None) };
        if token.is_empty() || token.len() > 512 {
            return Err(invalid_token());
        }
        let bytes = base64_simd::URL_SAFE_NO_PAD
            .decode_to_vec(token.as_bytes())
            .map_err(|_| invalid_token())?;
        if base64_simd::URL_SAFE_NO_PAD.encode_to_string(&bytes) != token {
            return Err(invalid_token());
        }
        let cursor: ResourceListCursor = serde_json::from_slice(&bytes).map_err(|_| invalid_token())?;
        if cursor.version != 1
            || cursor.context != self.context
            || cursor.identity.len() != 64
            || !cursor
                .identity
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(invalid_token());
        }
        self.validate_name(&cursor.after)?;
        Ok(Some(cursor))
    }

    fn identity(&self, bucket: &BucketHandle, table_id: Option<&str>) -> Result<String, ProtocolError> {
        if !matches!(
            (self.operation, table_id),
            (Operation::ListVectorBuckets, None) | (Operation::ListIndexes, Some(_))
        ) {
            return Err(unavailable());
        }
        hash(&(
            "rustfs.s3vectors.resource-list.position.v1",
            self.operation.name(),
            VectorBucketBindingV1::from_handle(bucket).map_err(catalog_error)?,
            table_id,
        ))
    }

    pub(super) fn check_identity(
        &self,
        cursor: &ResourceListCursor,
        bucket: &BucketHandle,
        table_id: Option<&str>,
    ) -> Result<(), ProtocolError> {
        if cursor.identity != self.identity(bucket, table_id)? {
            return Err(invalid_token());
        }
        Ok(())
    }

    pub(super) fn encode(&self, after: &str, bucket: &BucketHandle, table_id: Option<&str>) -> Result<String, ProtocolError> {
        self.validate_name(after).map_err(|_| unavailable())?;
        let cursor = ResourceListCursor {
            version: 1,
            context: self.context.clone(),
            after: after.to_owned(),
            identity: self.identity(bucket, table_id)?,
        };
        let bytes = serde_json::to_vec(&cursor).map_err(|_| unavailable())?;
        let token = base64_simd::URL_SAFE_NO_PAD.encode_to_string(bytes);
        if token.len() > 512 {
            return Err(unavailable());
        }
        Ok(token)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn bucket(incarnation: u128) -> BucketHandle {
        VectorBucketBindingV1 {
            version: 1,
            storage_bucket_id: "vector-storage".into(),
            bucket_incarnation: Uuid::from_u128(incarnation),
            account_namespace_id: String::new(),
            region: "us-east-1".into(),
            public_name: "page-bucket".into(),
        }
        .to_handle()
        .unwrap()
    }

    fn pagination(operation: Operation, parent: Option<&BucketHandle>) -> ResourceListPagination {
        ResourceListPagination::new(operation, Uuid::from_u128(1), "us-east-1", [1; 32], None, parent).unwrap()
    }

    #[test]
    fn resource_list_cursor_rejects_malformed_and_out_of_scope_tokens() {
        let bucket = bucket(2);
        let owner = pagination(Operation::ListVectorBuckets, None);
        let token = owner.encode("page-bucket", &bucket, None).unwrap();
        let cursor = owner.decode(Some(&token)).unwrap().unwrap();
        owner.check_identity(&cursor, &bucket, None).unwrap();
        let json = String::from_utf8(base64_simd::URL_SAFE_NO_PAD.decode_to_vec(token.as_bytes()).unwrap()).unwrap();
        let mut malformed = vec![String::new(), "!".into(), "a".repeat(513), format!("{token}=")];
        for body in [
            json.replace("\"version\":1", "\"version\":2"),
            json.replace("\"version\":1", "\"version\":1,\"version\":1"),
            json.replace("\"version\":1,", ""),
            json.replace("\"version\":1", "\"version\":\"1\""),
            json.replace("\"version\":1", "\"version\":1,\"extra\":true"),
            json.replace("page-bucket", "../page-bucket"),
            json.replace("page-bucket", "page.bucket"),
            json.replace(&cursor.identity, "abcd"),
        ] {
            malformed.push(base64_simd::URL_SAFE_NO_PAD.encode_to_string(body.as_bytes()));
        }
        for token in malformed {
            assert_eq!(owner.decode(Some(&token)).err().unwrap().code(), ErrorCode::Validation);
        }
        for changed in [
            ResourceListPagination::new(Operation::ListVectorBuckets, Uuid::from_u128(3), "us-east-1", [1; 32], None, None)
                .unwrap(),
            ResourceListPagination::new(Operation::ListVectorBuckets, Uuid::from_u128(1), "us-west-2", [1; 32], None, None)
                .unwrap(),
            ResourceListPagination::new(Operation::ListVectorBuckets, Uuid::from_u128(1), "us-east-1", [2; 32], None, None)
                .unwrap(),
            ResourceListPagination::new(
                Operation::ListVectorBuckets,
                Uuid::from_u128(1),
                "us-east-1",
                [1; 32],
                Some("page"),
                None,
            )
            .unwrap(),
            pagination(Operation::ListIndexes, Some(&bucket)),
        ] {
            assert_eq!(changed.decode(Some(&token)).err().unwrap().code(), ErrorCode::Validation);
        }
    }

    #[test]
    fn resource_list_cursor_binds_parent_incarnation_and_anchor_identity() {
        let original = bucket(2);
        let recreated = bucket(3);
        let owner = pagination(Operation::ListIndexes, Some(&original));
        let first = Uuid::from_u128(4).to_string();
        let replacement = Uuid::from_u128(5).to_string();
        let token = owner.encode("page.index", &original, Some(&first)).unwrap();
        let position = owner.decode(Some(&token)).unwrap().unwrap();
        owner.check_identity(&position, &original, Some(&first)).unwrap();
        assert_eq!(
            owner
                .check_identity(&position, &original, Some(&replacement))
                .unwrap_err()
                .code(),
            ErrorCode::Validation
        );
        let changed_parent = pagination(Operation::ListIndexes, Some(&recreated));
        assert_eq!(changed_parent.decode(Some(&token)).err().unwrap().code(), ErrorCode::Validation);
        let bucket_owner = pagination(Operation::ListVectorBuckets, None);
        let token = bucket_owner.encode("page-bucket", &original, None).unwrap();
        let position = bucket_owner.decode(Some(&token)).unwrap().unwrap();
        assert_eq!(
            bucket_owner.check_identity(&position, &recreated, None).unwrap_err().code(),
            ErrorCode::Validation
        );
    }

    #[test]
    fn resource_list_cursor_bounds_names_and_requires_an_instance_deployment() {
        let bucket = bucket(2);
        let owner = pagination(Operation::ListVectorBuckets, None);
        let name = "a".repeat(63);
        let token = owner.encode(&name, &bucket, None).unwrap();
        assert_eq!(token.len(), 323);
        assert_eq!(owner.decode(Some(&token)).unwrap().unwrap().after, name);
        assert!(owner.encode(&"a".repeat(64), &bucket, None).is_err());
        assert!(
            ResourceListPagination::new(Operation::ListVectorBuckets, Uuid::nil(), "us-east-1", [1; 32], None, None).is_err()
        );
        let filtered = ResourceListPagination::new(
            Operation::ListVectorBuckets,
            Uuid::from_u128(1),
            "us-east-1",
            [1; 32],
            Some("page"),
            None,
        )
        .unwrap();
        let token = filtered.encode("page-bucket", &bucket, None).unwrap();
        let mut value: serde_json::Value =
            serde_json::from_slice(&base64_simd::URL_SAFE_NO_PAD.decode_to_vec(token.as_bytes()).unwrap()).unwrap();
        value["after"] = json!("another-bucket");
        let token = base64_simd::URL_SAFE_NO_PAD.encode_to_string(serde_json::to_vec(&value).unwrap());
        assert_eq!(filtered.decode(Some(&token)).err().unwrap().code(), ErrorCode::Validation);
    }
}
