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

//! The two request classes the retired legacy metadata route claimed, website hosts and
//! `metadata=true` listings, sent to the gateway stack over a real store, IAM and signatures.
//!
//! The case tables are the ones the legacy-versus-gateway comparison ran before the legacy stack
//! was removed; each expected status is the one both stacks agreed on then. Table isolation is
//! observed on the listing a caller receives, because the listing use case filters only when the
//! authorization attached the table snapshot, and it fails open when nothing was attached.

use super::*;
use crate::app::storage_api::bucket_usecase::contract::bucket::{BucketOperations as _, MakeBucketOptions};
use crate::app::storage_api::object_usecase::contract::object::ObjectIO as _;
use crate::app::storage_api::object_usecase::{StorageObjectOptions, StoragePutObjReader};
use crate::table_catalog::{
    ConfiguredTableCatalogStore, EcStoreTableCatalogObjectBackend, NamespaceEntry, TABLE_BUCKET_CATALOG_TYPE,
    TABLE_BUCKET_MARKER_CONFIG, TABLE_CATALOG_ENTRY_VERSION, TableBucketEntry, TableCatalogBackingMode, TableCatalogEntryState,
    TableCatalogStore, TableEntry,
};
use std::collections::BTreeMap;

const WEBSITE_DOMAIN: &str = "website.test";
const S3_HOST: &str = "localhost:9000";
const BUCKET: &str = "parity-site";
const EMPTY: &str = "parity-empty";
const TABLES: &str = "parity-tables";

/// The gateway stack over a store holding the website bucket, an empty bucket and a table bucket,
/// with two IAM users: `list-allowed` may list buckets and versions, `list-none` has no policy.
async fn stack() -> (rustfs_test_utils::TestECStoreEnv, S3EdgeService) {
    let env = rustfs_test_utils::TestECStoreEnv::builder()
        .prefix("metadata_route_gateway")
        .disk_count(1)
        .init_bucket_metadata(true)
        .build()
        .await;
    ObjectStore::new(env.ecstore.clone())
        .save_iam_config(serde_json::json!({"version":1}), format!("{}/format.json", *IAM_CONFIG_PREFIX))
        .await
        .unwrap();
    let iam = rustfs_iam::build_iam_sys(env.ecstore.clone()).await.unwrap();
    for (key, actions) in [
        ("list-allowed", Some(vec!["s3:ListBucket", "s3:ListBucketVersions"])),
        ("list-none", None),
    ] {
        iam.create_user(
            key,
            &AddOrUpdateUserReq {
                secret_key: USER_SECRET.to_owned(),
                policy: None,
                status: AccountStatus::Enabled,
            },
        )
        .await
        .unwrap();
        if let Some(actions) = actions {
            let document =
                serde_json::json!({"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":actions,"Resource":"*"}]});
            iam.set_policy(key, Policy::parse_config(&serde_json::to_vec(&document).unwrap()).unwrap())
                .await
                .unwrap();
            iam.policy_db_set(key, UserType::Reg, false, key).await.unwrap();
        }
    }
    let context = Arc::new(AppContext::with_default_interfaces(
        env.ecstore.clone(),
        iam,
        Arc::new(rustfs_kms::KmsServiceManager::new()),
    ));
    assert!(context.publish_action_credentials(rustfs_credentials::Credentials {
        access_key: ROOT.to_owned(),
        secret_key: ROOT_SECRET.to_owned(),
        status: "on".to_owned(),
        ..Default::default()
    }));
    publish_test_app_context(context.clone());
    let slot = ServerContextSlot::new();
    assert!(slot.install(context));

    let store = env.ecstore.clone();
    for bucket in [BUCKET, EMPTY] {
        store.make_bucket(bucket, &MakeBucketOptions::default()).await.unwrap();
    }
    for (key, body, redirect) in [
        ("index.html", "home", None),
        ("404.html", "error page", None),
        ("docs", "ordinary object", None),
        ("section/index.html", "section page", None),
        ("private.txt", "private", None),
        ("move", "ordinary redirect object", Some("/new-page")),
        ("a b/c", "encoded key", None),
    ] {
        let mut options = StorageObjectOptions::default();
        options
            .user_defined
            .insert("x-amz-meta-project".to_owned(), "alpha".to_owned());
        if let Some(location) = redirect {
            options
                .user_defined
                .insert(rustfs_utils::http::AMZ_WEBSITE_REDIRECT_LOCATION.to_owned(), location.to_owned());
        }
        store
            .put_object(BUCKET, key, &mut StoragePutObjReader::from_vec(body.as_bytes().to_vec()), &options)
            .await
            .unwrap();
    }
    let website = "<WebsiteConfiguration><IndexDocument><Suffix>index.html</Suffix></IndexDocument><ErrorDocument><Key>404.html</Key></ErrorDocument></WebsiteConfiguration>";
    store
        .update_bucket_metadata_config(
            BUCKET,
            crate::app::storage_api::bucket::metadata::BUCKET_WEBSITE_CONFIG,
            website.as_bytes().to_vec(),
        )
        .await
        .unwrap();
    let policy = serde_json::json!({"Version": "2012-10-17", "Statement": [
        {"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":format!("arn:aws:s3:::{BUCKET}/*")},
        {"Effect":"Deny","Principal":"*","Action":"s3:GetObject","Resource":format!("arn:aws:s3:::{BUCKET}/private.txt")}
    ]});
    store
        .update_bucket_metadata_config(
            BUCKET,
            crate::app::storage_api::bucket::metadata::BUCKET_POLICY_CONFIG,
            serde_json::to_vec(&policy).unwrap(),
        )
        .await
        .unwrap();
    seed_table_bucket(&env).await;

    let mut config = config::Config::new("127.0.0.1:9001", Vec::new());
    config.console_enable = false;
    let website = crate::app::website::WebsiteHost::new(vec![WEBSITE_DOMAIN.to_owned()], "http", slot.clone());
    let edge = assemble_s3_stack(&config, website, assembly(slot)).unwrap();
    (env, edge)
}

/// `TABLES`: a table bucket whose catalog owns the `tables/table-id/` warehouse prefix, with an
/// ordinary key, a warehouse key and a reserved catalog key, and a policy that lets anyone list.
async fn seed_table_bucket(env: &rustfs_test_utils::TestECStoreEnv) {
    let store = &env.ecstore;
    store.make_bucket(TABLES, &MakeBucketOptions::default()).await.unwrap();
    store
        .update_bucket_metadata_config(
            TABLES,
            TABLE_BUCKET_MARKER_CONFIG,
            crate::table_catalog::table_bucket_marker_json().unwrap(),
        )
        .await
        .unwrap();
    let policy = serde_json::json!({"Version": "2012-10-17", "Statement": [{
        "Effect": "Allow", "Principal": {"AWS": "*"},
        "Action": ["s3:ListBucket", "s3:ListBucketVersions"], "Resource": format!("arn:aws:s3:::{TABLES}")
    }]});
    store
        .update_bucket_metadata_config(
            TABLES,
            crate::app::storage_api::bucket::metadata::BUCKET_POLICY_CONFIG,
            serde_json::to_vec(&policy).unwrap(),
        )
        .await
        .unwrap();
    let catalog = ConfiguredTableCatalogStore::new_for_test(
        EcStoreTableCatalogObjectBackend::new(Arc::clone(store)),
        TableCatalogBackingMode::from_env().unwrap(),
    );
    catalog
        .put_table_bucket(TableBucketEntry {
            version: TABLE_CATALOG_ENTRY_VERSION,
            table_bucket: TABLES.to_owned(),
            catalog_type: TABLE_BUCKET_CATALOG_TYPE.to_owned(),
            warehouse_root: format!("s3://{TABLES}/"),
            state: TableCatalogEntryState::Active,
            properties: BTreeMap::new(),
            active_rename_id: None,
            created_at: None,
            updated_at: None,
        })
        .await
        .unwrap();
    catalog
        .create_namespace(NamespaceEntry {
            version: TABLE_CATALOG_ENTRY_VERSION,
            table_bucket: TABLES.to_owned(),
            namespace: "sales".to_owned(),
            namespace_id: "sales".to_owned(),
            state: TableCatalogEntryState::Active,
            properties: BTreeMap::new(),
            created_at: None,
            updated_at: None,
        })
        .await
        .unwrap();
    catalog
        .create_table(TableEntry {
            version: TABLE_CATALOG_ENTRY_VERSION,
            table_bucket: TABLES.to_owned(),
            namespace: "sales".to_owned(),
            table: "orders".to_owned(),
            table_id: "table-id".to_owned(),
            table_uuid: uuid::Uuid::new_v4().to_string(),
            format: "ICEBERG".to_owned(),
            format_version: 2,
            warehouse_location: format!("s3://{TABLES}/tables/table-id"),
            metadata_location: format!("s3://{TABLES}/tables/table-id/metadata.json"),
            version_token: "v1".to_owned(),
            generation: 1,
            state: TableCatalogEntryState::Active,
            properties: BTreeMap::new(),
            created_at: None,
            updated_at: None,
        })
        .await
        .unwrap();
    for key in ["ordinary.txt", "tables/table-id/data.parquet", ".rustfs-table/private.json"] {
        store
            .put_object(
                TABLES,
                key,
                &mut StoragePutObjReader::from_vec(b"private metadata".to_vec()),
                &StorageObjectOptions::default(),
            )
            .await
            .unwrap();
    }
}

fn website_request(
    method: Method,
    path: &str,
    host: &str,
    extra: Option<(&'static str, &'static str)>,
) -> HttpRequest<Empty<Bytes>> {
    let mut builder = HttpRequest::builder()
        .method(method)
        .uri(path)
        .header(http::header::HOST, host)
        .header("x-request-id", REQUEST_ID);
    if let Some((name, value)) = extra {
        builder = builder.header(name, value);
    }
    builder.body(Empty::new()).unwrap()
}

fn signed_listing(bucket: &str, query: &str, key: &str, secret: &str) -> HttpRequest<Empty<Bytes>> {
    signed(&format!("/{bucket}"), query, S3_HOST, key, secret, Signed::Header)
}

/// The `<Key>` of every entry a listing document names, in order.
fn keys(body: &[u8]) -> Vec<String> {
    let text = String::from_utf8_lossy(body);
    text.split("<Key>")
        .skip(1)
        .filter_map(|tail| tail.split_once("</Key>").map(|(key, _)| key.to_owned()))
        .collect()
}

/// One website request: method, path, host, an extra header, and the status the website answers.
type WebsiteCase<'a> = (Method, &'a str, String, Option<(&'static str, &'static str)>, StatusCode);

#[tokio::test]
#[serial_test::serial]
async fn website_host_answers_each_request_class_with_the_status_the_legacy_table_recorded() {
    let (_env, edge) = stack().await;
    let site = format!("{BUCKET}.{WEBSITE_DOMAIN}");
    let path_style = format!("/{BUCKET}/index.html");
    let cases: Vec<WebsiteCase<'_>> = vec![
        (Method::GET, "/", site.clone(), None, StatusCode::OK),
        (Method::HEAD, "/", site.clone(), None, StatusCode::OK),
        (Method::GET, "/docs", site.clone(), None, StatusCode::OK),
        (
            Method::GET,
            "/index.html",
            site.clone(),
            Some(("range", "bytes=1-2")),
            StatusCode::PARTIAL_CONTENT,
        ),
        (Method::GET, "/missing", site.clone(), None, StatusCode::NOT_FOUND),
        (Method::HEAD, "/missing", site.clone(), None, StatusCode::NOT_FOUND),
        // The bucket policy denies everyone this object: a private object is not served.
        (Method::GET, "/private.txt", site.clone(), None, StatusCode::FORBIDDEN),
        (Method::GET, "/section?x=1", site.clone(), None, StatusCode::FOUND),
        (Method::GET, "/move", site.clone(), None, StatusCode::MOVED_PERMANENTLY),
        (
            Method::GET,
            "/move",
            site.clone(),
            Some(("if-none-match", "*")),
            StatusCode::MOVED_PERMANENTLY,
        ),
        (
            Method::GET,
            "/index.html",
            site.clone(),
            Some(("range", "bytes=999999-")),
            StatusCode::RANGE_NOT_SATISFIABLE,
        ),
        (
            Method::GET,
            "/index.html",
            site.clone(),
            Some(("if-modified-since", "yesterday")),
            StatusCode::BAD_REQUEST,
        ),
        (
            Method::GET,
            "/index.html",
            site.clone(),
            Some(("if-none-match", "*")),
            StatusCode::NOT_MODIFIED,
        ),
        // A website host serves reads only.
        (Method::POST, "/", site.clone(), None, StatusCode::METHOD_NOT_ALLOWED),
        (Method::PUT, "/index.html", site.clone(), None, StatusCode::METHOD_NOT_ALLOWED),
        (Method::DELETE, "/index.html", site.clone(), None, StatusCode::METHOD_NOT_ALLOWED),
        (Method::GET, "/rustfs/admin/v3/info", site.clone(), None, StatusCode::NOT_FOUND),
        (Method::GET, "/", format!("{EMPTY}.{WEBSITE_DOMAIN}"), None, StatusCode::NOT_FOUND),
        (Method::GET, "/", format!("absent.{WEBSITE_DOMAIN}"), None, StatusCode::NOT_FOUND),
        (Method::GET, path_style.as_str(), WEBSITE_DOMAIN.to_owned(), None, StatusCode::NOT_FOUND),
    ];
    let mut failures = Vec::new();
    for (method, path, host, extra, status) in &cases {
        let (got, _, body) = answer(edge.clone(), website_request(method.clone(), path, host, *extra)).await;
        if got != *status {
            failures.push(format!(
                "website {method} {host}{path} {extra:?}: want {status}, got {got} {}",
                String::from_utf8_lossy(&body)
            ));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));

    // The website's own content, not just its status.
    let (_, _, home) = answer(edge.clone(), website_request(Method::GET, "/", &site, None)).await;
    assert_eq!(home, "home");
    let (_, _, missing) = answer(edge.clone(), website_request(Method::GET, "/missing", &site, None)).await;
    assert_eq!(missing, "error page");
    let (_, _, private) = answer(edge.clone(), website_request(Method::GET, "/private.txt", &site, None)).await;
    assert!(
        !String::from_utf8_lossy(&private).contains("private"),
        "the denied object's content was served: {private:?}"
    );
}

/// A malformed path and a SigV4 `Authorization` header sent to a website host.
///
/// `/%zz` is not an encoding the key decoder refuses, so it names the key `%zz`: the website
/// answers its `404` error document. A bad `Authorization` header neither refuses nor widens a
/// read: the website service never reads it (`200`; the legacy S3 service refused it with `400
/// InvalidRequest` before routing, the one difference the comparison recorded).
#[tokio::test]
#[serial_test::serial]
async fn website_host_answers_a_malformed_path_and_ignores_an_authorization_header() {
    let (_env, edge) = stack().await;
    let site = format!("{BUCKET}.{WEBSITE_DOMAIN}");
    let bad_auth = Some(("authorization", "AWS4-HMAC-SHA256 Credential=nobody/20200101/us-east-1/s3/aws4_request"));

    let (status, _, body) = answer(edge.clone(), website_request(Method::GET, "/%zz", &site, None)).await;
    assert_eq!((status, body.as_ref()), (StatusCode::NOT_FOUND, b"error page".as_slice()));

    let (status, _, body) = answer(edge.clone(), website_request(Method::GET, "/index.html", &site, bad_auth)).await;
    assert_eq!((status, body.as_ref()), (StatusCode::OK, b"home".as_slice()));

    // The header cannot widen a read either: the denied object stays denied under a bad header.
    let (status, _, _) = answer(edge.clone(), website_request(Method::GET, "/private.txt", &site, bad_auth)).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
}

#[tokio::test]
#[serial_test::serial]
async fn metadata_listings_follow_the_callers_iam_policy() {
    let (_env, edge) = stack().await;
    let anonymous = |query: &'static str| move || website_request(Method::GET, &format!("/{BUCKET}?{query}"), S3_HOST, None);
    let cases: Vec<(&str, HttpRequest<Empty<Bytes>>, StatusCode, Option<&str>)> = vec![
        (
            "root v2",
            signed_listing(BUCKET, "list-type=2&metadata=true", ROOT, ROOT_SECRET),
            StatusCode::OK,
            None,
        ),
        (
            "root versions",
            signed_listing(BUCKET, "metadata=true&versions=", ROOT, ROOT_SECRET),
            StatusCode::OK,
            None,
        ),
        (
            "empty bucket",
            signed_listing(EMPTY, "list-type=2&metadata=true", ROOT, ROOT_SECRET),
            StatusCode::OK,
            None,
        ),
        (
            "first page",
            signed_listing(BUCKET, "list-type=2&max-keys=2&metadata=true", ROOT, ROOT_SECRET),
            StatusCode::OK,
            None,
        ),
        (
            "url encoding",
            signed_listing(BUCKET, "encoding-type=url&list-type=2&metadata=true&prefix=a", ROOT, ROOT_SECRET),
            StatusCode::OK,
            None,
        ),
        // rustfs#4845: a signed user is resolved to its IAM policy, not evaluated as anonymous.
        (
            "listing user v2",
            signed_listing(BUCKET, "list-type=2&metadata=true", "list-allowed", USER_SECRET),
            StatusCode::OK,
            None,
        ),
        (
            "listing user versions",
            signed_listing(BUCKET, "metadata=true&versions=", "list-allowed", USER_SECRET),
            StatusCode::OK,
            None,
        ),
        (
            "bad encoding",
            signed_listing(BUCKET, "encoding-type=xml&list-type=2&metadata=true", ROOT, ROOT_SECRET),
            StatusCode::BAD_REQUEST,
            Some("InvalidArgument"),
        ),
        (
            "bad token",
            signed_listing(BUCKET, "continuation-token=%25%25&list-type=2&metadata=true", ROOT, ROOT_SECRET),
            StatusCode::BAD_REQUEST,
            None,
        ),
        (
            "bad max-keys",
            signed_listing(BUCKET, "list-type=2&max-keys=ten&metadata=true", ROOT, ROOT_SECRET),
            StatusCode::BAD_REQUEST,
            Some("InvalidArgument"),
        ),
        (
            "no policy user v2",
            signed_listing(BUCKET, "list-type=2&metadata=true", "list-none", USER_SECRET),
            StatusCode::FORBIDDEN,
            Some("AccessDenied"),
        ),
        (
            "no policy user versions",
            signed_listing(BUCKET, "metadata=true&versions=", "list-none", USER_SECRET),
            StatusCode::FORBIDDEN,
            Some("AccessDenied"),
        ),
        (
            "missing bucket",
            signed_listing("parity-absent", "list-type=2&metadata=true", ROOT, ROOT_SECRET),
            StatusCode::NOT_FOUND,
            None,
        ),
        (
            "anonymous v2",
            anonymous("list-type=2&metadata=true")(),
            StatusCode::FORBIDDEN,
            Some("AccessDenied"),
        ),
        (
            "anonymous versions",
            anonymous("versions&metadata=true")(),
            StatusCode::FORBIDDEN,
            Some("AccessDenied"),
        ),
    ];
    let mut failures = Vec::new();
    for (label, request, status, code) in cases {
        let (got, _, body) = answer(edge.clone(), request).await;
        if got != status {
            failures.push(format!("listing {label}: want {status}, got {got} {}", String::from_utf8_lossy(&body)));
        } else if let Some(code) = code
            && refusal(&body).0 != code
        {
            failures.push(format!("listing {label}: want code {code}, got {:?}", refusal(&body)));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[tokio::test]
#[serial_test::serial]
async fn metadata_listing_documents_carry_the_extension_and_the_callers_read_rights() {
    let (_env, edge) = stack().await;
    let (_, _, root) = answer(edge.clone(), signed_listing(BUCKET, "list-type=2&metadata=true", ROOT, ROOT_SECRET)).await;
    let text = String::from_utf8_lossy(&root);
    for part in [
        "<ListBucketResult",
        "<UserMetadata>",
        "<project>alpha</project>",
        "<Internal>",
    ] {
        assert!(text.contains(part), "the listing lacks {part}: {text}");
    }
    // The bucket policy denies everyone `private.txt`: its entry carries no metadata, while the
    // entries the policy lets everyone read do.
    let (_, _, user) = answer(
        edge.clone(),
        signed_listing(BUCKET, "list-type=2&metadata=true", "list-allowed", USER_SECRET),
    )
    .await;
    let listed = String::from_utf8_lossy(&user).into_owned();
    let entry = |key: &str| {
        listed
            .split("<Contents>")
            .find(|entry| entry.contains(&format!("<Key>{key}</Key>")))
            .unwrap_or_default()
            .to_owned()
    };
    assert!(
        !entry("private.txt").contains("<UserMetadata>") && entry("docs").contains("<UserMetadata>"),
        "the listing's metadata ignores what the caller may read: {listed}"
    );
    // A first page of two names a continuation token, and the second page accepts it.
    let (_, _, first) = answer(
        edge.clone(),
        signed_listing(BUCKET, "list-type=2&max-keys=2&metadata=true", ROOT, ROOT_SECRET),
    )
    .await;
    let first = String::from_utf8_lossy(&first).into_owned();
    let token = first
        .split_once("<NextContinuationToken>")
        .and_then(|(_, tail)| tail.split_once('<'))
        .map(|(token, _)| token.to_owned())
        .unwrap_or_else(|| panic!("the first page has no continuation token: {first}"));
    let query = format!("continuation-token={}&list-type=2&metadata=true", urlencoding::encode(&token));
    let (status, _, second) = answer(edge, signed_listing(BUCKET, &query, ROOT, ROOT_SECRET)).await;
    assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&second));
    assert!(String::from_utf8_lossy(&second).contains("<ContinuationToken>"));
}

/// The gateway handler's listing authorization attaches the table snapshot, so a caller without
/// table access never receives a warehouse or catalog key. Without the snapshot the use case lists
/// the bucket unfiltered, so the filtering is observed on what the caller receives.
#[tokio::test]
#[serial_test::serial]
async fn a_metadata_listing_of_a_table_bucket_hides_the_keys_the_caller_has_no_table_access_to() {
    let (_env, edge) = stack().await;
    for query in ["list-type=2&metadata=true", "metadata=true&versions="] {
        let anonymous = website_request(Method::GET, &format!("/{TABLES}?{query}"), S3_HOST, None);
        for (caller, request, visible) in [
            // No table-metadata right: the ordinary key only, never the warehouse or catalog keys.
            ("anonymous", anonymous, vec!["ordinary.txt"]),
            (
                "listing user",
                signed_listing(TABLES, query, "list-allowed", USER_SECRET),
                vec!["ordinary.txt"],
            ),
            // The owner may read table data, so the control shows the filter is selective: the
            // warehouse key is listed, the reserved catalog prefix still is not.
            (
                "root",
                signed_listing(TABLES, query, ROOT, ROOT_SECRET),
                vec!["ordinary.txt", "tables/table-id/data.parquet"],
            ),
        ] {
            let (status, _, body) = answer(edge.clone(), request).await;
            assert_eq!(status, StatusCode::OK, "{caller} {query}: {}", String::from_utf8_lossy(&body));
            assert_eq!(keys(&body), visible, "{caller} {query}: {}", String::from_utf8_lossy(&body));
        }
    }
}
