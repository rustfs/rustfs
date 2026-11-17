// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

//! Signed ordinary S3 requests against real Vector objects and multipart sessions.

use super::*;
use crate::app::object::{DefaultObjectUsecase, ManagedEncryption, ManagedObjectContext};
use crate::app::storage_api::{
    error::is_err_object_not_found,
    managed::{BucketKind, ManagedObjectAccess, ManagedObjectOperation, get_bucket_metadata},
    multipart_usecase::{ECStore, StorageObjectOptions},
    request_context::RequestContext,
    test::contract::{
        bucket::{BucketOperations as _, MakeBucketOptions},
        multipart::MultipartOperations as _,
        object::ObjectOperations as _,
    },
};
use crate::server::{
    Signed,
    gateway_stack::{GatewayAssembly, GatewayStack},
    signed_request_with_headers,
};
use bytes::Bytes;
use http::{HeaderMap, HeaderValue, StatusCode};
use http_body_util::{BodyExt as _, Full};
use serde::Deserialize;
use std::time::Duration;

const VECTOR_BYTES: &[u8] = b"original-vector-file";
const VECTOR_PART: &[u8] = b"original-vector-part";
const GENERAL_BYTES: &[u8] = b"ordinary-general-object";

struct Answer {
    status: StatusCode,
    body: Bytes,
}

impl Answer {
    fn success(self, case: &str) -> Bytes {
        assert!(self.status.is_success(), "{case}: {} {:?}", self.status, self.body);
        self.body
    }

    fn denied(self, case: &str) {
        #[derive(Deserialize)]
        struct Error {
            #[serde(rename = "Code")]
            code: String,
        }
        assert_eq!(self.status, StatusCode::FORBIDDEN, "{case}: {:?}", self.body);
        let error: Error = quick_xml::de::from_reader(self.body.as_ref()).expect("S3 XML refusal");
        assert_eq!(error.code, "AccessDenied", "{case}");
    }
}

struct OrdinaryS3<'a> {
    gateway: &'a GatewayStack,
    owner: &'a Credentials,
}

impl OrdinaryS3<'_> {
    async fn send(&self, method: Method, path: &str, query: &str, copy_source: Option<&str>, body: Bytes) -> Answer {
        let mut headers = HeaderMap::new();
        let content_length = (method == Method::PUT || method == Method::POST).then(|| body.len().to_string().parse().unwrap());
        if !body.is_empty() && method == Method::POST {
            headers.insert(http::header::CONTENT_TYPE, HeaderValue::from_static("application/xml"));
        }
        if let Some(source) = copy_source {
            headers.insert("x-amz-copy-source", source.parse().unwrap());
        }
        let mut request = signed_request_with_headers(
            Signed::Header,
            method,
            path,
            query,
            &self.owner.access_key,
            &self.owner.secret_key,
            headers,
        )
        .map(|_| Full::new(body));
        // Content-Length is framing added after the shared signer chooses SignedHeaders.
        if let Some(content_length) = content_length {
            request.headers_mut().insert(http::header::CONTENT_LENGTH, content_length);
        }
        request.extensions_mut().insert(RequestContext::fallback());
        let response = tokio::time::timeout(Duration::from_secs(30), self.gateway.call(request))
            .await
            .expect("ordinary S3 request completes");
        let status = response.status();
        let body = tokio::time::timeout(Duration::from_secs(30), response.into_body().collect())
            .await
            .expect("ordinary S3 body completes")
            .expect("ordinary S3 body")
            .to_bytes();
        Answer { status, body }
    }
}

fn multipart_query(upload_id: &str, part_number: bool) -> String {
    let mut query = url::form_urlencoded::Serializer::new(String::new());
    query.append_pair("uploadId", upload_id);
    if part_number {
        query.append_pair("partNumber", "1");
    }
    query.finish()
}

fn completed_part(etag: &str) -> Bytes {
    Bytes::from(format!(
        "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{}</ETag></Part></CompleteMultipartUpload>",
        quick_xml::escape::escape(etag)
    ))
}

async fn upload_snapshot(store: &ECStore, bucket: &str) -> Value {
    let uploads = store.list_multipart_uploads(bucket, "", None, None, None, 100).await.unwrap();
    assert!(!uploads.is_truncated);
    let mut rows = Vec::new();
    for upload in uploads.uploads {
        let parts = store
            .list_object_parts(bucket, &upload.object, &upload.upload_id, None, 100, &StorageObjectOptions::default())
            .await
            .unwrap();
        assert!(!parts.is_truncated);
        rows.push(json!({
            "bucket":upload.bucket, "object":upload.object, "id":upload.upload_id,
            "initiated":upload.initiated.map(|time|time.unix_timestamp_nanos().to_string()),
            "upload_metadata":upload.user_defined, "part_metadata":parts.user_defined,
            "parts":parts.parts.into_iter().map(|part|json!({
                "number":part.part_num, "etag":part.etag, "size":part.size,
                "actual_size":part.actual_size,
                "modified":part.last_mod.map(|time|time.unix_timestamp_nanos().to_string()),
            })).collect::<Vec<_>>()
        }));
    }
    json!(rows)
}

struct ProtectedState<'a> {
    store: &'a ECStore,
    usecase: &'a DefaultObjectUsecase,
    managed: &'a ManagedObjectContext,
    general_bucket: &'a str,
    vector_key: &'a str,
    vector_pending: &'a str,
    revision: Uuid,
}

impl ProtectedState<'_> {
    async fn snapshot(&self) -> Value {
        let vector_bucket = self.managed.access().bucket().storage_bucket_id().as_str();
        let metadata = get_bucket_metadata(self.store, vector_bucket).await.unwrap();
        metadata.validate_bucket_handle(self.managed.access().bucket()).unwrap();
        assert_eq!(self.managed.access().bucket().kind(), BucketKind::Vector);
        let (info, bytes) = self
            .usecase
            .managed_get_object(self.managed, self.vector_key, Some(self.revision), None, VECTOR_BYTES.len())
            .await
            .unwrap();
        assert_eq!(bytes.as_ref(), VECTOR_BYTES);
        assert_eq!(info.data_dir, Some(self.revision));
        let general = self
            .store
            .get_object_info(self.general_bucket, self.vector_key, &StorageObjectOptions::default())
            .await
            .unwrap();
        for (bucket, key) in [
            (vector_bucket, self.vector_pending),
            (self.general_bucket, self.vector_pending),
            (self.general_bucket, "escaped"),
        ] {
            let error = self
                .store
                .get_object_info(bucket, key, &StorageObjectOptions::default())
                .await
                .expect_err("denied requests must not publish a target");
            assert!(is_err_object_not_found(&error), "{bucket}/{key}: {error:?}");
        }
        json!({
            "vector_object":{
                "revision":info.data_dir, "etag":info.etag, "size":info.size,
                "version":info.version_id, "metadata":info.user_defined,
            },
            "general_object":{
                "revision":general.data_dir, "etag":general.etag, "size":general.size,
                "version":general.version_id, "metadata":general.user_defined,
            },
            "vector_uploads":upload_snapshot(self.store, vector_bucket).await,
            "general_uploads":upload_snapshot(self.store, self.general_bucket).await,
        })
    }
}

#[test]
#[serial_test::serial]
fn ordinary_s3_gateway_copy_and_multipart_refuse_vector_storage_without_mutation() {
    run_large_stack_test("vector-s3-copy-multipart-isolation", || async {
        let (app, credentials, _) = install_real_iam_user(allow_all(), false).await;
        let (source, original_access, store, _reset) =
            crate::app::object::managed_request_fixture_with_encryption(ManagedEncryption::Unencrypted).await;
        let managed_app = source.server_context().unwrap().app_context().unwrap();
        assert!(Arc::ptr_eq(&app.iam().handle(), &managed_app.iam().handle()));
        assert!(Arc::ptr_eq(&store, &managed_app.object_store()));
        let (verified, owner) = crate::auth::check_key_valid_with_context("", &credentials.access_key, Some(&managed_app))
            .await
            .unwrap();
        assert!(!owner);
        let bucket = original_access.bucket().storage_bucket_id().as_str().to_owned();
        let bucket_facts = BucketAuthorizationFacts {
            handle: original_access.bucket().clone(),
            tags: BTreeMap::new(),
            policy: None,
        };
        let resource = CatalogResourceIdentity::try_new(
            bucket_facts.handle.clone(),
            original_access.table_id().clone(),
            CatalogFormat::Lance,
        )
        .unwrap();
        let index = CatalogName::parse(INDEX).unwrap();
        let definition = VectorIndexDefinitionV1 {
            version: 1,
            effective_encryption: VectorEffectiveEncryptionV1::Unencrypted,
            tags: BTreeMap::new(),
        };
        let target = VectorTarget::Index {
            bucket: &bucket_facts,
            index_name: &index,
            resource: &resource,
            definition: &definition,
        };
        for (operation, input) in [
            (
                Operation::PutVectors,
                json!({
                    "vectorBucketName":bucket_facts.handle.public_name().as_str(), "indexName":INDEX,
                    "vectors":[{"key":"state", "data":{"float32":[1.0, 0.0, 0.0]}}],
                }),
            ),
            (
                Operation::GetVectors,
                json!({
                    "vectorBucketName":bucket_facts.handle.public_name().as_str(), "indexName":INDEX,
                    "keys":["state"], "returnData":true,
                }),
            ),
        ] {
            let request = parse(operation, input);
            authorize_with_iam(
                managed_app.iam().handle().as_ref(),
                &verified,
                false,
                &request,
                &target,
                REGION,
                HashMap::new(),
            )
            .await
            .unwrap();
        }
        let access = ManagedObjectAccess::try_from_parts(
            original_access.bucket().clone(),
            original_access.table_id().clone(),
            original_access.allowed_prefix().to_owned(),
            [
                ManagedObjectOperation::CreateOnly,
                ManagedObjectOperation::Get,
                ManagedObjectOperation::Head,
                ManagedObjectOperation::CreateMultipart,
                ManagedObjectOperation::UploadPart,
                ManagedObjectOperation::CompleteMultipart,
                ManagedObjectOperation::AbortMultipart,
            ]
            .into_iter()
            .collect(),
        )
        .unwrap();
        let vector_key = format!("{}original.bin", access.allowed_prefix());
        let vector_pending = format!("{}pending.bin", access.allowed_prefix());
        let envelope = super::permission_model::envelope_for_verified_identity(
            &source,
            &verified,
            false,
            &bucket,
            &vector_key,
            "/PutVectors",
            "s3vectors",
        );
        let managed =
            ManagedObjectContext::from_authorized_request(access, Arc::new(envelope), ManagedEncryption::Unencrypted).unwrap();
        let usecase = DefaultObjectUsecase::with_context(Some(managed_app.clone()));
        let original = usecase
            .managed_create_object(
                &managed,
                &vector_key,
                VECTOR_BYTES.len() as u64,
                futures::stream::iter([Ok(Bytes::from_static(VECTOR_BYTES))]),
            )
            .await
            .unwrap();
        let vector_upload = usecase
            .managed_create_multipart_upload(&managed, &vector_pending)
            .await
            .unwrap();
        let vector_part = usecase
            .managed_upload_part(
                &vector_upload,
                1,
                VECTOR_PART.len() as u64,
                futures::stream::iter([Ok(Bytes::from_static(VECTOR_PART))]),
            )
            .await
            .unwrap();
        let vector_uploads = store.list_multipart_uploads(&bucket, "", None, None, None, 10).await.unwrap();
        assert!(!vector_uploads.is_truncated);
        assert_eq!(vector_uploads.uploads.len(), 1);
        assert_eq!(vector_uploads.uploads[0].object, vector_pending);
        let vector_upload_id = &vector_uploads.uploads[0].upload_id;

        let root = managed_app.action_credentials().get().unwrap();
        let (_, is_owner) = crate::auth::check_key_valid_with_context("", &root.access_key, Some(&managed_app))
            .await
            .unwrap();
        assert!(is_owner);
        let gateway = GatewayStack::assemble(GatewayAssembly {
            server_ctx: source.server_context().unwrap().clone(),
            root_access_key: root.access_key.clone(),
            root_secret_key: root.secret_key.clone(),
            region: Some(REGION.into()),
            server_domains: Vec::new(),
            body_read_timeout: Duration::from_secs(10),
            keystone_auth: None,
        })
        .unwrap();
        let s3 = OrdinaryS3 {
            gateway: &gateway,
            owner: &root,
        };
        let general_bucket = format!("s3-vector-control-{}", Uuid::new_v4().simple());
        store
            .make_bucket(&general_bucket, &MakeBucketOptions::default())
            .await
            .unwrap();
        assert_eq!(
            get_bucket_metadata(&store, &general_bucket)
                .await
                .unwrap()
                .bucket_kind()
                .unwrap(),
            BucketKind::General
        );
        let general_source = format!("/{general_bucket}/{vector_key}");
        let general_copy = format!("/{general_bucket}/{}control-copy", managed.access().allowed_prefix());
        let general_pending = format!("/{general_bucket}/{vector_pending}");
        let vector_source = format!("/{bucket}/{vector_key}");
        let vector_pending_path = format!("/{bucket}/{vector_pending}");
        s3.send(Method::PUT, &general_source, "", None, Bytes::from_static(GENERAL_BYTES))
            .await
            .success("General PutObject");
        assert_eq!(
            s3.send(Method::GET, &general_source, "", None, Bytes::new())
                .await
                .success("General GetObject"),
            GENERAL_BYTES
        );
        s3.send(Method::PUT, &general_copy, "", Some(&general_source), Bytes::new())
            .await
            .success("General CopyObject");
        assert_eq!(
            s3.send(Method::GET, &general_copy, "", None, Bytes::new())
                .await
                .success("General copied bytes"),
            GENERAL_BYTES
        );
        #[derive(Deserialize)]
        struct CreatedUpload {
            #[serde(rename = "UploadId")]
            upload_id: String,
        }
        let created = s3
            .send(Method::POST, &general_pending, "uploads", None, Bytes::new())
            .await
            .success("General CreateMultipartUpload");
        let general_upload: CreatedUpload = quick_xml::de::from_reader(created.as_ref()).unwrap();
        let general_part_query = multipart_query(&general_upload.upload_id, true);
        let general_upload_query = multipart_query(&general_upload.upload_id, false);
        s3.send(
            Method::PUT,
            &general_pending,
            &general_part_query,
            None,
            Bytes::from_static(GENERAL_BYTES),
        )
        .await
        .success("General UploadPart");
        let copied = s3
            .send(Method::PUT, &general_pending, &general_part_query, Some(&general_source), Bytes::new())
            .await
            .success("General UploadPartCopy");
        #[derive(Deserialize)]
        struct CopiedPart {
            #[serde(rename = "ETag")]
            etag: String,
        }
        let copied: CopiedPart = quick_xml::de::from_reader(copied.as_ref()).unwrap();
        s3.send(Method::GET, &general_pending, &general_upload_query, None, Bytes::new())
            .await
            .success("General ListParts");
        let listed = s3
            .send(Method::GET, &format!("/{general_bucket}"), "uploads", None, Bytes::new())
            .await
            .success("General ListMultipartUploads");
        assert!(std::str::from_utf8(&listed).unwrap().contains(&general_upload.upload_id));

        let protected = ProtectedState {
            store: &store,
            usecase: &usecase,
            managed: &managed,
            general_bucket: &general_bucket,
            vector_key: &vector_key,
            vector_pending: &vector_pending,
            revision: original.data_dir.unwrap(),
        };
        let before = protected.snapshot().await;
        let vector_upload_query = multipart_query(vector_upload_id, false);
        let vector_part_query = multipart_query(vector_upload_id, true);
        let vector_complete = completed_part(vector_part.etag.as_deref().unwrap());
        let cases = [
            (
                "CopyObject source",
                Method::PUT,
                format!("/{general_bucket}/escaped"),
                String::new(),
                Some(vector_source.clone()),
                Bytes::new(),
            ),
            (
                "CopyObject destination",
                Method::PUT,
                vector_source.clone(),
                String::new(),
                Some(general_source.clone()),
                Bytes::new(),
            ),
            (
                "UploadPartCopy source",
                Method::PUT,
                general_pending.clone(),
                general_part_query,
                Some(vector_source.clone()),
                Bytes::new(),
            ),
            (
                "UploadPartCopy destination",
                Method::PUT,
                vector_pending_path.clone(),
                vector_part_query.clone(),
                Some(general_source.clone()),
                Bytes::new(),
            ),
            (
                "CreateMultipartUpload",
                Method::POST,
                vector_pending_path.clone(),
                "uploads".into(),
                None,
                Bytes::new(),
            ),
            (
                "UploadPart",
                Method::PUT,
                vector_pending_path.clone(),
                vector_part_query,
                None,
                Bytes::from_static(b"forbidden-replacement"),
            ),
            (
                "ListMultipartUploads",
                Method::GET,
                format!("/{bucket}"),
                "uploads".into(),
                None,
                Bytes::new(),
            ),
            (
                "ListParts",
                Method::GET,
                vector_pending_path.clone(),
                vector_upload_query.clone(),
                None,
                Bytes::new(),
            ),
            (
                "CompleteMultipartUpload",
                Method::POST,
                vector_pending_path.clone(),
                vector_upload_query.clone(),
                None,
                vector_complete,
            ),
            (
                "AbortMultipartUpload",
                Method::DELETE,
                vector_pending_path,
                vector_upload_query,
                None,
                Bytes::new(),
            ),
        ];
        for (case, method, path, query, copy_source, body) in cases {
            s3.send(method, &path, &query, copy_source.as_deref(), body)
                .await
                .denied(case);
            assert_eq!(protected.snapshot().await, before, "{case} must preserve objects and uploads");
            eprintln!("Vector ordinary S3 isolation: {case}: AccessDenied; objects and multipart state unchanged");
        }
        s3.send(Method::POST, &general_pending, &general_upload_query, None, completed_part(&copied.etag))
            .await
            .success("General CompleteMultipartUpload");
        assert_eq!(
            s3.send(Method::GET, &general_pending, "", None, Bytes::new())
                .await
                .success("General completed bytes"),
            GENERAL_BYTES
        );
        let completed = usecase
            .managed_complete_multipart_upload(&vector_upload, vec![vector_part])
            .await
            .unwrap();
        let (_, bytes) = usecase
            .managed_get_object(&managed, &vector_pending, completed.data_dir, None, VECTOR_PART.len())
            .await
            .unwrap();
        assert_eq!(bytes.as_ref(), VECTOR_PART);
        let (_, bytes) = usecase
            .managed_get_object(&managed, &vector_key, original.data_dir, None, VECTOR_BYTES.len())
            .await
            .unwrap();
        assert_eq!(bytes.as_ref(), VECTOR_BYTES);
        let abort_path = format!("/{general_bucket}/abort-control");
        let created = s3
            .send(Method::POST, &abort_path, "uploads", None, Bytes::new())
            .await
            .success("General abort session creation");
        let abort: CreatedUpload = quick_xml::de::from_reader(created.as_ref()).unwrap();
        s3.send(Method::DELETE, &abort_path, &multipart_query(&abort.upload_id, false), None, Bytes::new())
            .await
            .success("General AbortMultipartUpload");
        assert_eq!(upload_snapshot(&store, &general_bucket).await, json!([]));
        assert_eq!(upload_snapshot(&store, &bucket).await, json!([]));
    });
}
