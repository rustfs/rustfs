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

//! MinIO-compatible metadata listing extension routes and the website host, on the legacy stack.
//!
//! Responsible for: [`MetadataRoute`], the legacy custom route that claims a website host's
//! requests and the two metadata listings ahead of the admin router, and hands each to the owned
//! rules both stacks share (`app/website.rs`, `app/metadata_listing.rs`).
//! NOT responsible for: the website or listing rules themselves, or the gateway stack's
//! selection of the same requests (`server/stack.rs` and the gateway's RustFS dialect).
//! Upstream: `server/http.rs`. Downstream: the website host, the metadata listings, the admin route.

use super::RequestEnvelope;
use super::metadata_listing::{self, query_has, query_value};
use super::website::WebsiteHost;
use crate::app::runtime_sources::ServerContextSlot;
use crate::auth::{check_key_valid, get_session_token};
use crate::storage::access::req_info_mut;
use crate::storage::req_info::ReqInfo;
use async_trait::async_trait;
use http::header::{CONTENT_TYPE, HOST};
use http::{Extensions, HeaderMap, HeaderValue, Method, Uri};
use s3s::host::{MultiDomain, S3Host};
use s3s::route::S3Route;
use s3s::{Body, S3Error, S3Request, S3Response, S3Result, s3_error};
use std::sync::Arc;

use super::metadata_listing::MetadataListing as MetadataOperation;

pub(crate) struct MetadataRoute<A> {
    admin: A,
    host: Option<MultiDomain>,
    website: Option<WebsiteHost>,
}

pub(crate) fn with_metadata_route<A>(
    admin: A,
    host: Option<MultiDomain>,
    website_domains: Vec<String>,
    website_scheme: &'static str,
    server_ctx: Arc<ServerContextSlot>,
) -> MetadataRoute<A> {
    MetadataRoute {
        admin,
        host,
        website: WebsiteHost::new(website_domains, website_scheme, server_ctx),
    }
}

impl<A> MetadataRoute<A> {
    fn website(&self, uri: &Uri, headers: &HeaderMap) -> Option<&WebsiteHost> {
        self.website.as_ref().filter(|website| website.claims(uri, headers))
    }
}

#[async_trait]
impl<A> S3Route for MetadataRoute<A>
where
    A: S3Route,
{
    fn is_match(&self, method: &Method, uri: &Uri, headers: &HeaderMap, extensions: &mut Extensions) -> bool {
        self.website(uri, headers).is_some()
            || metadata_operation(method, uri, headers, self.host.as_ref()).is_some()
            || self.admin.is_match(method, uri, headers, extensions)
    }

    async fn check_access(&self, req: &mut S3Request<Body>) -> S3Result<()> {
        // A website read is anonymous whatever the request carries; the host decides it.
        if self.website(&req.uri, &req.headers).is_some() {
            Ok(())
        } else if let Some(target) = metadata_operation(&req.method, &req.uri, &req.headers, self.host.as_ref()) {
            check_metadata_access(req, target).await
        } else {
            self.admin.check_access(req).await
        }
    }

    async fn call(&self, req: S3Request<Body>) -> S3Result<S3Response<Body>> {
        if let Some(website) = self.website(&req.uri, &req.headers) {
            let (parts, body) = website
                .answer(req.method, req.uri, req.headers, req.extensions)
                .await
                .into_parts();
            return Ok(S3Response {
                output: Body::http_body_unsync(body),
                status: Some(parts.status),
                headers: parts.headers,
                extensions: parts.extensions,
            });
        }
        match metadata_operation(&req.method, &req.uri, &req.headers, self.host.as_ref()) {
            Some(target) => call_metadata_listing(req, target).await,
            None => self.admin.call(req).await,
        }
    }
}

struct BucketTarget {
    operation: MetadataOperation,
    bucket: String,
}

fn metadata_operation(method: &Method, uri: &Uri, headers: &HeaderMap, host: Option<&MultiDomain>) -> Option<BucketTarget> {
    if method != Method::GET {
        return None;
    }
    let bucket = bucket_from_request(uri, headers, host).ok()?;

    let query = uri.query()?;
    if query_value(query, "metadata").as_deref() != Some("true") {
        return None;
    }
    let operation = if query_has(query, "versions") {
        MetadataOperation::ListObjectVersions
    } else if query_value(query, "list-type").as_deref() == Some("2") {
        MetadataOperation::ListObjectsV2
    } else {
        return None;
    };

    Some(BucketTarget { operation, bucket })
}

async fn check_metadata_access(req: &mut S3Request<Body>, target: BucketTarget) -> S3Result<()> {
    // Custom routes bypass the S3Access::check hook that normally resolves the
    // SigV4-verified credentials into ReqInfo, so resolve them here the same
    // way; otherwise a signed request is evaluated as anonymous and denied
    // (rustfs#4845). Anonymous requests keep cred=None and stay subject to the
    // bucket-policy-only evaluation in authorize_request.
    let (cred, is_owner) = if let Some(input_cred) = &req.credentials {
        let (cred, is_owner) =
            check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &input_cred.access_key).await?;
        (Some(cred), is_owner)
    } else {
        (None, false)
    };

    {
        if req_info_mut(req).is_err() {
            req.extensions.insert(ReqInfo::default());
        }
        let req_info = req_info_mut(req)?;
        req_info.cred = cred;
        req_info.is_owner = is_owner;
        req_info.bucket = Some(target.bucket.clone());
    }

    Ok(metadata_listing::authorize(req, target.operation, &target.bucket).await?)
}

async fn call_metadata_listing(req: S3Request<Body>, target: BucketTarget) -> S3Result<S3Response<Body>> {
    let envelope = RequestEnvelope::from_s3s(&req);
    let body = metadata_listing::answer(&envelope, target.operation, target.bucket).await?;
    let mut response = S3Response::new(Body::from(body));
    response
        .headers
        .insert(CONTENT_TYPE, HeaderValue::from_static("application/xml"));
    Ok(response)
}

fn bucket_from_request(uri: &Uri, headers: &HeaderMap, host: Option<&MultiDomain>) -> S3Result<String> {
    if let Some(host) = host
        && let Some(host_header) = headers.get(HOST).and_then(|value| value.to_str().ok())
    {
        let virtual_host = host.parse_host_header(host_header)?;
        if let Some(bucket) = virtual_host.bucket() {
            return if uri.path() == "/" {
                Ok(bucket.to_owned())
            } else {
                Err(s3_error!(InvalidRequest, "bucket-level metadata route requires a bucket path"))
            };
        }
    }

    path_style_bucket(uri)?.ok_or_else(|| s3_error!(InvalidRequest, "bucket name is required"))
}

fn path_style_bucket(uri: &Uri) -> S3Result<Option<String>> {
    let path = uri.path().trim_matches('/');
    if path.is_empty() {
        return Ok(None);
    }
    if path.contains('/') {
        return Err(s3_error!(InvalidRequest, "bucket-level metadata route does not accept an object path"));
    }
    urlencoding::decode(path)
        .map(|bucket| Some(bucket.into_owned()))
        .map_err(S3Error::internal_error)
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn metadata_route_prepares_and_applies_table_listing_isolation() {
        use crate::app::runtime_sources::{AppContext, IamInterface, KmsInterface, ServerContextSlot};
        use crate::app::storage_api::bucket_usecase::contract::bucket::{BucketOperations as _, MakeBucketOptions};
        use crate::app::storage_api::object_usecase::access::TableDataPlaneListAccess;
        use crate::app::storage_api::object_usecase::contract::object::ObjectIO as _;
        use crate::app::storage_api::object_usecase::{StorageObjectOptions, StoragePutObjReader};
        use crate::app::table_list_isolation;
        use crate::table_catalog::{
            ConfiguredTableCatalogStore, EcStoreTableCatalogObjectBackend, NamespaceEntry, TABLE_BUCKET_CATALOG_TYPE,
            TABLE_BUCKET_MARKER_CONFIG, TABLE_CATALOG_ENTRY_VERSION, TableBucketEntry, TableCatalogBackingMode,
            TableCatalogEntryState, TableCatalogStore, TableEntry,
        };
        use std::{collections::BTreeMap, sync::Arc};

        struct UnreadyIam;
        impl IamInterface for UnreadyIam {
            fn handle(&self) -> Arc<rustfs_iam::sys::IamSys<rustfs_iam::store::object::ObjectStore>> {
                panic!("anonymous listing must not resolve IAM credentials")
            }
            fn is_ready(&self) -> bool {
                false
            }
        }
        struct TestKms;
        impl KmsInterface for TestKms {
            fn handle(&self) -> Arc<rustfs_kms::KmsServiceManager> {
                Arc::new(rustfs_kms::KmsServiceManager::new())
            }
        }

        let store = crate::app::gating_test_env::shared_gating_ecstore().await;
        let bucket = format!("metadata-isolation-{}", uuid::Uuid::new_v4());
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("create table bucket");
        store
            .update_bucket_metadata_config(
                &bucket,
                TABLE_BUCKET_MARKER_CONFIG,
                crate::table_catalog::table_bucket_marker_json().expect("table bucket marker"),
            )
            .await
            .expect("enable table bucket");
        let policy = serde_json::json!({"Version": "2012-10-17", "Statement": [{
            "Effect": "Allow", "Principal": {"AWS": "*"},
            "Action": ["s3:ListBucket", "s3:ListBucketVersions"], "Resource": format!("arn:aws:s3:::{bucket}")
        }]});
        store
            .update_bucket_metadata_config(
                &bucket,
                crate::storage::storage_api::ecstore_bucket::metadata::BUCKET_POLICY_CONFIG,
                serde_json::to_vec(&policy).expect("listing policy"),
            )
            .await
            .expect("allow anonymous object listings");
        let context = Arc::new(AppContext::new(Arc::clone(&store), Arc::new(UnreadyIam), Arc::new(TestKms)));
        assert!(context.publish_action_credentials(rustfs_credentials::Credentials {
            access_key: "table-list-root".to_owned(),
            secret_key: "table-list-root-secret".to_owned(),
            status: "on".to_owned(),
            ..Default::default()
        }));
        let backend = EcStoreTableCatalogObjectBackend::new(Arc::clone(&store));
        let catalog =
            ConfiguredTableCatalogStore::new_for_test(backend, TableCatalogBackingMode::from_env().expect("catalog mode"));
        catalog
            .put_table_bucket(TableBucketEntry {
                version: TABLE_CATALOG_ENTRY_VERSION,
                table_bucket: bucket.clone(),
                catalog_type: TABLE_BUCKET_CATALOG_TYPE.to_owned(),
                warehouse_root: format!("s3://{bucket}/"),
                state: TableCatalogEntryState::Active,
                properties: BTreeMap::new(),
                active_rename_id: None,
                created_at: None,
                updated_at: None,
            })
            .await
            .expect("seed catalog bucket");
        catalog
            .create_namespace(NamespaceEntry {
                version: TABLE_CATALOG_ENTRY_VERSION,
                table_bucket: bucket.clone(),
                namespace: "sales".to_owned(),
                namespace_id: "sales".to_owned(),
                state: TableCatalogEntryState::Active,
                properties: BTreeMap::new(),
                created_at: None,
                updated_at: None,
            })
            .await
            .expect("seed catalog namespace");
        catalog
            .create_table(TableEntry {
                version: TABLE_CATALOG_ENTRY_VERSION,
                table_bucket: bucket.clone(),
                namespace: "sales".to_owned(),
                table: "orders".to_owned(),
                table_id: "table-id".to_owned(),
                table_uuid: uuid::Uuid::new_v4().to_string(),
                format: "ICEBERG".to_owned(),
                format_version: 2,
                warehouse_location: format!("s3://{bucket}/tables/table-id"),
                metadata_location: format!("s3://{bucket}/tables/table-id/metadata.json"),
                version_token: "v1".to_owned(),
                generation: 1,
                state: TableCatalogEntryState::Active,
                properties: BTreeMap::new(),
                created_at: None,
                updated_at: None,
            })
            .await
            .expect("seed catalog table");
        for key in ["ordinary.txt", "tables/table-id/data.parquet", ".rustfs-table/private.json"] {
            store
                .put_object(
                    &bucket,
                    key,
                    &mut StoragePutObjReader::from_vec(b"private metadata".to_vec()),
                    &StorageObjectOptions::default(),
                )
                .await
                .expect("seed objects for listing");
        }
        let slot = ServerContextSlot::new();
        assert!(slot.install(context));
        for operation in [MetadataOperation::ListObjectsV2, MetadataOperation::ListObjectVersions] {
            let mut req = website_request(&format!("/{bucket}?list-type=2&metadata=true"));
            req.extensions.insert(Arc::clone(&slot));
            super::check_metadata_access(
                &mut req,
                super::BucketTarget {
                    operation,
                    bucket: bucket.clone(),
                },
            )
            .await
            .expect("bucket listing is authorized");
            let access = req
                .extensions
                .get::<TableDataPlaneListAccess>()
                .expect("metadata-route authorization must attach table isolation");
            assert!(access.allows_object("ordinary.txt"));
            assert!(!access.allows_object("tables/table-id/data.parquet"));
            assert!(!access.allows_object(".rustfs-table/private.json"));
            let objects = match operation {
                MetadataOperation::ListObjectsV2 => {
                    table_list_isolation::list_objects_v2(
                        Arc::clone(&store),
                        access,
                        table_list_isolation::ListObjectsV2Request {
                            bucket: &bucket,
                            prefix: "",
                            continuation_token: None,
                            delimiter: None,
                            max_keys: 1000,
                            start_after: None,
                            incl_deleted: false,
                            opaque_cursor_supported: true,
                        },
                    )
                    .await
                    .expect("protected object listing succeeds")
                    .objects
                }
                MetadataOperation::ListObjectVersions => {
                    table_list_isolation::list_object_versions(
                        Arc::clone(&store),
                        access,
                        table_list_isolation::ListObjectVersionsRequest {
                            bucket: &bucket,
                            prefix: "",
                            key_marker: None,
                            version_id_marker: None,
                            delimiter: None,
                            max_keys: 1000,
                        },
                    )
                    .await
                    .expect("protected version listing succeeds")
                    .objects
                }
            };
            let keys: Vec<_> = objects.into_iter().map(|object| object.name).collect();
            assert_eq!(keys, vec!["ordinary.txt"], "catalog and unauthorized warehouse objects must be hidden");
        }
    }

    use super::{MetadataOperation, metadata_operation};
    use http::header::HOST;
    use http::{Extensions, HeaderMap, Method, Uri};
    use s3s::host::MultiDomain;
    use s3s::{Body, S3Request};

    fn uri(value: &str) -> Uri {
        value.parse().expect("test URI should parse")
    }

    fn website_request(value: &str) -> S3Request<Body> {
        let mut headers = HeaderMap::new();
        headers.insert(HOST, "site.example.com".parse().expect("valid host"));
        S3Request {
            input: Body::empty(),
            method: Method::GET,
            uri: uri(value),
            headers,
            extensions: Extensions::new(),
            credentials: None,
            region: None,
            service: None,
            trailing_headers: None,
        }
    }

    #[test]
    fn metadata_operation_matches_path_style_extensions_only() {
        assert_eq!(
            metadata_operation(&Method::GET, &uri("/bucket?versions&metadata=true"), &HeaderMap::new(), None)
                .map(|target| target.operation),
            Some(MetadataOperation::ListObjectVersions)
        );
        assert_eq!(
            metadata_operation(&Method::GET, &uri("/bucket?list-type=2&metadata=true"), &HeaderMap::new(), None)
                .map(|target| target.operation),
            Some(MetadataOperation::ListObjectsV2)
        );
        assert_eq!(
            metadata_operation(&Method::GET, &uri("/?list-type=2&metadata=true"), &HeaderMap::new(), None)
                .map(|target| target.operation),
            None
        );
        assert_eq!(
            metadata_operation(&Method::GET, &uri("/bucket/key?list-type=2&metadata=true"), &HeaderMap::new(), None)
                .map(|target| target.operation),
            None
        );
        assert_eq!(
            metadata_operation(&Method::GET, &uri("/bucket?list-type=2"), &HeaderMap::new(), None).map(|target| target.operation),
            None
        );
        assert_eq!(
            metadata_operation(&Method::PUT, &uri("/bucket?list-type=2&metadata=true"), &HeaderMap::new(), None)
                .map(|target| target.operation),
            None
        );
    }

    #[test]
    fn metadata_operation_matches_virtual_hosted_bucket_root() {
        let host = MultiDomain::new(["example.com:9000"]).expect("valid test host domain");
        let mut headers = HeaderMap::new();
        headers.insert(HOST, "demo-bucket.example.com:9000".parse().expect("valid host header"));

        let target = metadata_operation(&Method::GET, &uri("/?list-type=2&metadata=true"), &headers, Some(&host))
            .expect("virtual-hosted bucket root should match metadata route");
        assert_eq!(target.operation, MetadataOperation::ListObjectsV2);
        assert_eq!(target.bucket, "demo-bucket");

        assert_eq!(
            metadata_operation(&Method::GET, &uri("/object.txt?list-type=2&metadata=true"), &headers, Some(&host))
                .map(|target| target.operation),
            None
        );
    }

    #[test]
    fn metadata_operation_matches_unconfigured_host_fallbacks() {
        let host = MultiDomain::new(["s3.example.com:9000"]).expect("valid test host domain");

        let mut path_style_headers = HeaderMap::new();
        path_style_headers.insert(HOST, "localhost:9000".parse().expect("valid host header"));
        let path_style = metadata_operation(
            &Method::GET,
            &uri("/path-bucket?list-type=2&metadata=true"),
            &path_style_headers,
            Some(&host),
        )
        .expect("unmatched host with a port should use path-style routing");
        assert_eq!(path_style.operation, MetadataOperation::ListObjectsV2);
        assert_eq!(path_style.bucket, "path-bucket");

        let mut cname_headers = HeaderMap::new();
        cname_headers.insert(HOST, "cdn.example.org".parse().expect("valid host header"));
        let cname = metadata_operation(&Method::GET, &uri("/?list-type=2&metadata=true"), &cname_headers, Some(&host))
            .expect("unmatched valid bucket host should use CNAME routing");
        assert_eq!(cname.operation, MetadataOperation::ListObjectsV2);
        assert_eq!(cname.bucket, "cdn.example.org");
    }
}
