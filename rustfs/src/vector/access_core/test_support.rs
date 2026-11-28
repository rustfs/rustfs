// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

//! Pure request and resource fixtures shared by Vector unit and app integration tests.

use super::{BucketAuthorizationFacts, VectorTarget};
use crate::{
    storage_api::vector::{
        AccountNamespaceId, BucketHandle, BucketIncarnation, BucketKind, BucketPublicName, ManagedTableId, StorageBucketId,
    },
    table_catalog::{CatalogFormat, CatalogName, CatalogResourceIdentity, VectorEffectiveEncryptionV1, VectorIndexDefinitionV1},
    vector::protocol::{Operation, Request, Route, dto},
};
use http::Method;
use serde_json::{Value, json};
use std::collections::BTreeMap;
use uuid::Uuid;

pub(crate) const REGION: &str = "us-east-1";
pub(crate) const BUCKET: &str = "s6-vector-bucket";
pub(crate) const INDEX: &str = "s6-index";

pub(crate) fn parse(operation: Operation, body: Value) -> Request {
    let route = Route::recognize(&Method::POST, &format!("/{}", operation.name())).unwrap();
    route.parse(None, &serde_json::to_vec(&body).unwrap(), REGION).unwrap()
}

pub(crate) fn request_for(operation: Operation) -> Request {
    let bucket = json!({"vectorBucketName":BUCKET});
    let mut index = json!({"vectorBucketName":BUCKET,"indexName":INDEX});
    match operation {
        Operation::CreateVectorBucket => parse(operation, bucket),
        Operation::CreateIndex => parse(
            operation,
            json!({
                "vectorBucketName":BUCKET,"indexName":INDEX,"dataType":"float32",
                "dimension":3,"distanceMetric":"cosine"
            }),
        ),
        Operation::DeleteVectorBucket
        | Operation::DeleteVectorBucketPolicy
        | Operation::GetVectorBucket
        | Operation::GetVectorBucketPolicy
        | Operation::ListIndexes => parse(operation, bucket),
        Operation::PutVectorBucketDefaultIndexMode => {
            parse(operation, json!({"vectorBucketName":BUCKET,"defaultIndexMode":"ENHANCED"}))
        }
        Operation::PutVectorBucketPolicy => parse(operation, json!({"vectorBucketName":BUCKET,"policy":"{}"})),
        Operation::ListVectorBuckets => parse(operation, json!({})),
        Operation::GetVectors | Operation::DeleteVectors => {
            index["keys"] = json!(["key-1"]);
            parse(operation, index)
        }
        Operation::PutVectors => {
            index["vectors"] = json!([{"key":"key-1","data":{"float32":[1.0,0.0,0.0]}}]);
            parse(operation, index)
        }
        Operation::QueryVectors => {
            index["queryVector"] = json!({"float32":[1.0,0.0,0.0]});
            index["topK"] = json!(1);
            parse(operation, index)
        }
        Operation::UpdateIndexMode => {
            index["indexMode"] = json!("ENHANCED");
            parse(operation, index)
        }
        Operation::TagResource => Request::TagResource(dto::TagResourceInput {
            resource_arn: resource_arn(REGION, BUCKET, INDEX),
            tags: BTreeMap::from([("project".into(), "search".into())]),
        }),
        Operation::UntagResource => Request::UntagResource(dto::UntagResourceInput {
            resource_arn: resource_arn(REGION, BUCKET, INDEX),
            tag_keys: vec!["project".into()],
        }),
        Operation::ListTagsForResource => Request::ListTagsForResource(dto::ListTagsForResourceInput {
            resource_arn: resource_arn(REGION, BUCKET, INDEX),
        }),
        Operation::GetIndex | Operation::DeleteIndex | Operation::ListVectors => parse(operation, index),
    }
}

pub(crate) struct ResourceFixture {
    pub(crate) bucket: BucketAuthorizationFacts,
    pub(crate) index_name: CatalogName,
    pub(crate) resource: CatalogResourceIdentity,
    pub(crate) definition: VectorIndexDefinitionV1,
}

impl ResourceFixture {
    pub(crate) fn new() -> Self {
        let handle = BucketHandle::try_from_parts(
            BucketKind::Vector,
            AccountNamespaceId::empty(),
            REGION.into(),
            BucketPublicName::try_new(BUCKET.into()).unwrap(),
            StorageBucketId::try_new("s6-test-storage".into()).unwrap(),
            BucketIncarnation::try_new(Uuid::new_v4()).unwrap(),
        )
        .unwrap();
        let resource = CatalogResourceIdentity::try_new(
            handle.clone(),
            ManagedTableId::try_new(Uuid::new_v4().to_string()).unwrap(),
            CatalogFormat::Lance,
        )
        .unwrap();
        Self {
            bucket: BucketAuthorizationFacts {
                handle,
                tags: BTreeMap::from([("department".into(), "research".into())]),
                policy: None,
            },
            index_name: CatalogName::parse(INDEX).unwrap(),
            resource,
            definition: VectorIndexDefinitionV1 {
                version: 1,
                effective_encryption: VectorEffectiveEncryptionV1::Aes256,
                tags: BTreeMap::from([("project".into(), "search".into())]),
            },
        }
    }

    pub(crate) fn target(&self, operation: Operation) -> VectorTarget<'_> {
        match operation {
            Operation::CreateVectorBucket => VectorTarget::NewBucket { public_name: BUCKET },
            Operation::ListVectorBuckets => VectorTarget::Service,
            Operation::CreateIndex => VectorTarget::NewIndex {
                bucket: &self.bucket,
                index_name: &self.index_name,
            },
            Operation::DeleteVectorBucket
            | Operation::DeleteVectorBucketPolicy
            | Operation::GetVectorBucket
            | Operation::GetVectorBucketPolicy
            | Operation::ListIndexes
            | Operation::PutVectorBucketDefaultIndexMode
            | Operation::PutVectorBucketPolicy => VectorTarget::Bucket(&self.bucket),
            _ => VectorTarget::Index {
                bucket: &self.bucket,
                index_name: &self.index_name,
                resource: &self.resource,
                definition: &self.definition,
            },
        }
    }
}

pub(crate) fn resource_arn(region: &str, bucket: &str, index: &str) -> String {
    super::resource_arn(region, bucket, index)
}

pub(crate) fn target_names<'a>(target: &'a VectorTarget<'_>) -> (&'a str, &'a str) {
    target.names()
}
