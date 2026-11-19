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

//! The two request classes the legacy metadata route claims — website hosts and `metadata=true`
//! listings — sent to the legacy stack and to the gateway stack over the same store, IAM and
//! signatures: each answers the same status, body and representation headers on both.

use super::*;
use crate::app::storage_api::bucket_usecase::contract::bucket::{BucketOperations as _, MakeBucketOptions};
use crate::app::storage_api::object_usecase::contract::object::ObjectIO as _;
use crate::app::storage_api::object_usecase::{StorageObjectOptions, StoragePutObjReader};

const WEBSITE_DOMAIN: &str = "website.test";
const S3_HOST: &str = "localhost:9000";
const BUCKET: &str = "parity-site";
const EMPTY: &str = "parity-empty";

/// Both stacks, each over the same store: the legacy one with the metadata route mounted as
/// `server/http.rs` mounts it.
async fn stacks() -> (rustfs_test_utils::TestECStoreEnv, crate::server::http::S3Service, S3EdgeService) {
    let env = rustfs_test_utils::TestECStoreEnv::builder()
        .prefix("metadata_route_parity")
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

    let website_domains = vec![WEBSITE_DOMAIN.to_owned()];
    let mut legacy = crate::server::http::S3ServiceBuilder::new(UnusedS3);
    legacy.set_auth(crate::auth::IAMAuth::with_server_context(ROOT, ROOT_SECRET, slot.clone()));
    legacy.set_route(crate::app::metadata_route::with_metadata_route(
        crate::admin::make_admin_route(false, slot.clone()).unwrap(),
        Some(crate::server::http::MultiDomain::new(["s3.example.test"]).unwrap()),
        website_domains.clone(),
        "http",
        slot.clone(),
    ));
    legacy.set_host(crate::server::http::MultiDomain::new(["s3.example.test"]).unwrap());
    let legacy = legacy.build();
    let mut config = config::Config::new("127.0.0.1:9001", Vec::new());
    config.s3_stack = config::S3Stack::Gateway;
    config.console_enable = false;
    let website = crate::app::website::WebsiteHost::new(website_domains, "http", slot.clone());
    let edge = select_s3_stack(&config, legacy.clone(), website, || Ok(assembly(slot.clone()))).unwrap();
    (env, legacy, edge)
}

/// What both stacks must agree on: the status, the body (an S3 error by its code), and the
/// representation and redirect headers.
///
/// An S3 error is compared by its status and code: its framing is the gateway's for every
/// operation, which states the error document's length where the legacy stack streams it.
fn observed(response: (StatusCode, HeaderMap, Bytes)) -> String {
    let (status, mut headers, body) = response;
    let body = if body.starts_with(b"<?xml") && body.windows(7).any(|window| window == b"<Error>") {
        headers.remove(http::header::CONTENT_LENGTH);
        format!("error {:?}", refusal(&body).0)
    } else {
        String::from_utf8_lossy(&body).into_owned()
    };
    let named = [
        "content-type",
        "content-length",
        "content-range",
        "etag",
        "last-modified",
        "location",
    ]
    .map(|name| format!("{name}={:?}", headers.get(name)));
    format!("{status} {} {body}", named.join(" "))
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

/// One website request: method, path, host, an extra header, and the status both stacks answer.
type WebsiteCase<'a> = (Method, &'a str, String, Option<(&'static str, &'static str)>, StatusCode);

/// One listing request: its label, how to build it, and the status both stacks answer.
type ListingCase = (&'static str, Box<dyn Fn() -> HttpRequest<Empty<Bytes>>>, StatusCode);

/// Sends `request()` to both stacks and returns each answer as [`observed`] reads it.
async fn both(
    legacy: &crate::server::http::S3Service,
    edge: &S3EdgeService,
    request: impl Fn() -> HttpRequest<Empty<Bytes>>,
) -> (String, String) {
    (
        observed(answer(legacy.clone(), request()).await),
        observed(answer(edge.clone(), request()).await),
    )
}

#[tokio::test]
async fn website_and_metadata_route_answers_match_the_legacy_stack() {
    let (_env, legacy, edge) = stacks().await;
    let mut failures = Vec::new();
    let site = format!("{BUCKET}.{WEBSITE_DOMAIN}");
    let path_style = format!("/{BUCKET}/index.html");
    let website_cases: Vec<WebsiteCase<'_>> = vec![
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
        (Method::POST, "/", site.clone(), None, StatusCode::METHOD_NOT_ALLOWED),
        (Method::GET, "/rustfs/admin/v3/info", site.clone(), None, StatusCode::NOT_FOUND),
        (Method::GET, "/", format!("{EMPTY}.{WEBSITE_DOMAIN}"), None, StatusCode::NOT_FOUND),
        (Method::GET, "/", format!("absent.{WEBSITE_DOMAIN}"), None, StatusCode::NOT_FOUND),
        (Method::GET, path_style.as_str(), WEBSITE_DOMAIN.to_owned(), None, StatusCode::NOT_FOUND),
    ];
    for (method, path, host, extra, status) in &website_cases {
        let (old, new) = both(&legacy, &edge, || website_request(method.clone(), path, host, *extra)).await;
        if old != new || !old.starts_with(status.as_str()) {
            failures.push(format!("website {method} {host}{path} {extra:?}\n  legacy  {old}\n  gateway {new}"));
        }
    }

    let signed_as = |path: String, query: &'static str, key: &'static str, secret: &'static str| {
        move || signed(&path, query, S3_HOST, key, secret, Signed::Header)
    };
    let listing_cases: Vec<ListingCase> = vec![
        (
            "root v2",
            Box::new(signed_as(format!("/{BUCKET}"), "list-type=2&metadata=true", ROOT, ROOT_SECRET)),
            StatusCode::OK,
        ),
        (
            "root versions",
            Box::new(signed_as(format!("/{BUCKET}"), "metadata=true&versions=", ROOT, ROOT_SECRET)),
            StatusCode::OK,
        ),
        (
            "empty bucket",
            Box::new(signed_as(format!("/{EMPTY}"), "list-type=2&metadata=true", ROOT, ROOT_SECRET)),
            StatusCode::OK,
        ),
        (
            "first page",
            Box::new(signed_as(format!("/{BUCKET}"), "list-type=2&max-keys=2&metadata=true", ROOT, ROOT_SECRET)),
            StatusCode::OK,
        ),
        (
            "url encoding",
            Box::new(signed_as(
                format!("/{BUCKET}"),
                "encoding-type=url&list-type=2&metadata=true&prefix=a",
                ROOT,
                ROOT_SECRET,
            )),
            StatusCode::OK,
        ),
        (
            "listing user",
            Box::new(signed_as(format!("/{BUCKET}"), "list-type=2&metadata=true", "list-allowed", USER_SECRET)),
            StatusCode::OK,
        ),
        (
            "bad encoding",
            Box::new(signed_as(
                format!("/{BUCKET}"),
                "encoding-type=xml&list-type=2&metadata=true",
                ROOT,
                ROOT_SECRET,
            )),
            StatusCode::BAD_REQUEST,
        ),
        (
            "bad token",
            Box::new(signed_as(
                format!("/{BUCKET}"),
                "continuation-token=%25%25&list-type=2&metadata=true",
                ROOT,
                ROOT_SECRET,
            )),
            StatusCode::BAD_REQUEST,
        ),
        (
            "bad max-keys",
            Box::new(signed_as(
                format!("/{BUCKET}"),
                "list-type=2&max-keys=ten&metadata=true",
                ROOT,
                ROOT_SECRET,
            )),
            StatusCode::BAD_REQUEST,
        ),
        (
            "no policy user",
            Box::new(signed_as(format!("/{BUCKET}"), "list-type=2&metadata=true", "list-none", USER_SECRET)),
            StatusCode::FORBIDDEN,
        ),
        (
            "no policy versions",
            Box::new(signed_as(format!("/{BUCKET}"), "metadata=true&versions=", "list-none", USER_SECRET)),
            StatusCode::FORBIDDEN,
        ),
        (
            "missing bucket",
            Box::new(signed_as("/parity-absent".to_owned(), "list-type=2&metadata=true", ROOT, ROOT_SECRET)),
            StatusCode::NOT_FOUND,
        ),
        (
            "anonymous",
            Box::new(move || website_request(Method::GET, &format!("/{BUCKET}?list-type=2&metadata=true"), S3_HOST, None)),
            StatusCode::FORBIDDEN,
        ),
        (
            "anonymous versions",
            Box::new(move || website_request(Method::GET, &format!("/{BUCKET}?versions&metadata=true"), S3_HOST, None)),
            StatusCode::FORBIDDEN,
        ),
    ];
    for (label, request, status) in &listing_cases {
        let (old, new) = both(&legacy, &edge, request).await;
        if old != new || !old.starts_with(status.as_str()) {
            failures.push(format!("listing {label}\n  legacy  {old}\n  gateway {new}"));
        }
    }

    // The listing documents carry what the extension adds, so equal bodies are not two empty ones.
    let root_v2 = answer(
        edge.clone(),
        signed(
            &format!("/{BUCKET}"),
            "list-type=2&metadata=true",
            S3_HOST,
            ROOT,
            ROOT_SECRET,
            Signed::Header,
        ),
    )
    .await;
    let text = String::from_utf8_lossy(&root_v2.2);
    for part in [
        "<ListBucketResult",
        "<UserMetadata>",
        "<project>alpha</project>",
        "<Internal>",
    ] {
        if !text.contains(part) {
            failures.push(format!("gateway listing lacks {part}: {text}"));
        }
    }
    let user = answer(
        edge.clone(),
        signed(
            &format!("/{BUCKET}"),
            "list-type=2&metadata=true",
            S3_HOST,
            "list-allowed",
            USER_SECRET,
            Signed::Header,
        ),
    )
    .await;
    // The bucket policy denies everyone `private.txt`, so its entry carries no metadata, while
    // the entries the policy lets everyone read do.
    let listed = String::from_utf8_lossy(&user.2).into_owned();
    let entry = |key: &str| {
        listed
            .split("<Contents>")
            .find(|entry| entry.contains(&format!("<Key>{key}</Key>")))
            .unwrap_or_default()
            .to_owned()
    };
    if entry("private.txt").contains("<UserMetadata>") || !entry("docs").contains("<UserMetadata>") {
        failures.push(format!("the listing's metadata ignores what the caller may read: {listed}"));
    }
    let first = String::from_utf8_lossy(
        &answer(
            edge.clone(),
            signed(
                &format!("/{BUCKET}"),
                "list-type=2&max-keys=2&metadata=true",
                S3_HOST,
                ROOT,
                ROOT_SECRET,
                Signed::Header,
            ),
        )
        .await
        .2,
    )
    .into_owned();
    match first
        .split_once("<NextContinuationToken>")
        .and_then(|(_, tail)| tail.split_once('<'))
    {
        Some((token, _)) => {
            let query: &'static str = Box::leak(
                format!("continuation-token={}&list-type=2&metadata=true", urlencoding::encode(token)).into_boxed_str(),
            );
            let (old, new) = both(&legacy, &edge, signed_as(format!("/{BUCKET}"), query, ROOT, ROOT_SECRET)).await;
            if old != new || !old.starts_with("200") || !old.contains("<ContinuationToken>") {
                failures.push(format!("listing second page\n  legacy  {old}\n  gateway {new}"));
            }
        }
        None => failures.push(format!("the first page has no continuation token: {first}")),
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// A malformed path and a SigV4 `Authorization` header sent to a website host.
///
/// `/%zz` is not a percent-encoding the key decoder refuses, so it names the key `%zz`: both
/// stacks answer the website's `404` error document. A bad `Authorization` header is the one
/// difference: the legacy stack's S3 service refuses the header before routing (`400
/// InvalidRequest`), while the gateway stack's website service never reads it, because a website
/// read is anonymous and the header can neither refuse nor widen it (`200`).
#[tokio::test]
async fn website_host_answers_a_malformed_path_and_ignores_an_authorization_header() {
    let (_env, legacy, edge) = stacks().await;
    let site = format!("{BUCKET}.{WEBSITE_DOMAIN}");
    let bad_auth = Some(("authorization", "AWS4-HMAC-SHA256 Credential=nobody/20200101/us-east-1/s3/aws4_request"));

    let request = || website_request(Method::GET, "/%zz", &site, None);
    let (old, new) = both(&legacy, &edge, request).await;
    assert_eq!(old, new);
    assert!(new.starts_with("404") && new.ends_with("error page"), "{new}");

    let request = || website_request(Method::GET, "/index.html", &site, bad_auth);
    let (old, new) = both(&legacy, &edge, request).await;
    assert!(old.starts_with("400") && old.ends_with("error \"InvalidRequest\""), "{old}");
    assert!(new.starts_with("200") && new.ends_with("home"), "{new}");
}
