// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[derive(Debug)]
pub(super) enum RequestTarget {
    Service,
    NewBucket(String),
    Bucket(String),
    NewIndex { bucket: String, index: CatalogName },
    Index { bucket: String, index: CatalogName },
}

impl RequestTarget {
    pub(super) fn resolve(request: &Request, region: &str) -> Result<Self, ProtocolError> {
        macro_rules! bucket {
            ($input:expr) => {
                Self::Bucket(bucket_selector(&$input.vector_bucket_name, &$input.vector_bucket_arn, region)?)
            };
        }
        macro_rules! index {
            ($input:expr) => {{
                let (bucket, index) = index_selector(&$input.vector_bucket_name, &$input.index_name, &$input.index_arn, region)?;
                Self::Index { bucket, index }
            }};
        }
        Ok(match request {
            Request::CreateVectorBucket(input) => Self::NewBucket(input.vector_bucket_name.clone()),
            Request::ListVectorBuckets(_) => Self::Service,
            Request::CreateIndex(input) => Self::NewIndex {
                bucket: bucket_selector(&input.vector_bucket_name, &input.vector_bucket_arn, region)?,
                index: name(&input.index_name)?,
            },
            Request::DeleteVectorBucket(input) => bucket!(input),
            Request::GetVectorBucket(input) => bucket!(input),
            Request::GetVectorBucketPolicy(input) => bucket!(input),
            Request::PutVectorBucketPolicy(input) => bucket!(input),
            Request::DeleteVectorBucketPolicy(input) => bucket!(input),
            Request::PutVectorBucketDefaultIndexMode(input) => bucket!(input),
            Request::ListIndexes(input) => bucket!(input),
            Request::DeleteIndex(input) => index!(input),
            Request::GetIndex(input) => index!(input),
            Request::PutVectors(input) => index!(input),
            Request::GetVectors(input) => index!(input),
            Request::DeleteVectors(input) => index!(input),
            Request::ListVectors(input) => index!(input),
            Request::QueryVectors(input) => index!(input),
            Request::UpdateIndexMode(input) => index!(input),
            Request::ListTagsForResource(input) => arn_target(&input.resource_arn, region)?,
            Request::TagResource(input) => arn_target(&input.resource_arn, region)?,
            Request::UntagResource(input) => arn_target(&input.resource_arn, region)?,
        })
    }
}

fn name(value: &str) -> Result<CatalogName, ProtocolError> {
    CatalogName::parse(value).map_err(|_| ProtocolError::validation("indexName", "invalid Index name"))
}

fn arn_target(arn: &str, region: &str) -> Result<RequestTarget, ProtocolError> {
    let resource = arn
        .strip_prefix(&format!("arn:aws:s3vectors:{region}::bucket/"))
        .ok_or_else(|| ProtocolError::validation("resourceArn", "resource belongs to another service or region"))?;
    if let Some((bucket, index)) = resource.split_once("/index/") {
        BucketPublicName::try_new(bucket.to_owned()).map_err(|_| denied())?;
        Ok(RequestTarget::Index {
            bucket: bucket.to_owned(),
            index: name(index)?,
        })
    } else {
        BucketPublicName::try_new(resource.to_owned()).map_err(|_| denied())?;
        Ok(RequestTarget::Bucket(resource.to_owned()))
    }
}

fn bucket_selector(name: &Option<String>, arn: &Option<String>, region: &str) -> Result<String, ProtocolError> {
    match (name, arn) {
        (Some(name), None) => Ok(name.clone()),
        (None, Some(arn)) => match arn_target(arn, region)? {
            RequestTarget::Bucket(bucket) => Ok(bucket),
            _ => Err(denied()),
        },
        _ => Err(ProtocolError::validation("vectorBucketName", "select exactly one Vector bucket")),
    }
}

fn index_selector(
    bucket: &Option<String>,
    index: &Option<String>,
    arn: &Option<String>,
    region: &str,
) -> Result<(String, CatalogName), ProtocolError> {
    match (bucket, index, arn) {
        (Some(bucket), Some(index), None) => Ok((bucket.clone(), name(index)?)),
        (None, None, Some(arn)) => match arn_target(arn, region)? {
            RequestTarget::Index { bucket, index } => Ok((bucket, index)),
            _ => Err(denied()),
        },
        _ => Err(ProtocolError::validation("indexName", "select exactly one Vector Index")),
    }
}
