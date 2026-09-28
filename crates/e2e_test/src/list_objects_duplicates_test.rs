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

#[cfg(test)]
mod tests {
    use crate::common::{RustFSTestEnvironment, init_logging};
    use aws_sdk_s3::Client;
    use aws_sdk_s3::primitives::ByteStream;
    use tracing::info;

    /// Helper function to create an S3 client for testing
    fn create_s3_client(env: &RustFSTestEnvironment) -> Client {
        env.create_s3_client()
    }

    /// Helper function to create a test bucket
    async fn create_bucket(client: &Client, bucket: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut retries = 20;
        loop {
            match client.create_bucket().bucket(bucket).send().await {
                Ok(_) => {
                    info!("Bucket {} created successfully", bucket);
                    return Ok(());
                }
                Err(e) => {
                    // Ignore if bucket already exists
                    if e.to_string().contains("BucketAlreadyOwnedByYou") || e.to_string().contains("BucketAlreadyExists") {
                        info!("Bucket {} already exists", bucket);
                        return Ok(());
                    }
                    if retries > 0 {
                        retries -= 1;
                        info!("Bucket creation failed, retrying... ({})", retries);
                        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                        continue;
                    }
                    return Err(Box::new(e));
                }
            }
        }
    }

    /// Test ensuring that ListObjectsV2 returns unique CommonPrefixes even if "folder" objects exist.
    ///
    /// Bug Reference: Issue #1797
    /// Veeam creates 0-byte objects ending in '/' (e.g. "folder/") to represent folders.
    /// If "folder/file.txt" also exists, "folder/" is a CommonPrefix.
    /// The bug was that "folder/" (the object) and "folder/" (derived prefix) were both added to CommonPrefixes
    /// when delimiter was "/" because the deduplication check was explicitly skipped for "/" delimiter.
    #[tokio::test]
    async fn test_list_objects_v2_unique_common_prefixes() {
        init_logging();
        info!("Starting test: ListObjectsV2 should return unique CommonPrefixes");

        let mut env = RustFSTestEnvironment::new().await.expect("Failed to create test environment");
        env.start_rustfs_server(vec![]).await.expect("Failed to start RustFS");

        let client = create_s3_client(&env);
        let bucket = "test-list-unique-prefixes";

        // Create bucket
        create_bucket(&client, bucket).await.expect("Failed to create bucket");

        // 1. Create a file inside a folder
        client
            .put_object()
            .bucket(bucket)
            .key("folder/file.txt")
            .body(ByteStream::from_static(b"content"))
            .send()
            .await
            .expect("Failed to create file inside folder");

        // 2. Create the "folder" object itself (Veeam behavior)
        client
            .put_object()
            .bucket(bucket)
            .key("folder/")
            .body(ByteStream::from_static(b""))
            .send()
            .await
            .expect("Failed to create folder object");

        // 3. List with delimiter="/"
        let result = client
            .list_objects_v2()
            .bucket(bucket)
            .delimiter("/")
            .send()
            .await
            .expect("Failed to list objects");

        // Verify prefixes
        let prefixes = result.common_prefixes();
        info!("CommonPrefixes: {:?}", prefixes);

        // Should contain "folder/" exactly once
        let folder_prefixes: Vec<_> = prefixes.iter().filter(|p| p.prefix() == Some("folder/")).collect();

        assert_eq!(
            folder_prefixes.len(),
            1,
            "Expected exactly 1 'folder/' prefix, found {}",
            folder_prefixes.len()
        );

        // Verify that "folder/" is NOT returned as an object in Contents.
        // For this regression test, we expect "folder/" to be represented only as a CommonPrefix
        // (rolled up from "folder/file.txt" and the explicit "folder/" object), and to appear there
        // exactly once. It must not appear in Contents at all.

        // Ensure "folder/" is NOT in contents (Contents)
        let folder_in_contents = result.contents().iter().any(|o| o.key() == Some("folder/"));
        assert!(
            !folder_in_contents,
            "Expected 'folder/' to be rolled up into CommonPrefixes, but found it in Contents"
        );

        // Stop the RustFS server to ensure proper cleanup
        env.stop_server();
    }

    /// Test ensuring that ListObjectsV2 returns unique keys when an explicit directory marker
    /// exists under the requested prefix and delimiter is not provided.
    ///
    /// Bug Reference: Issue #2439
    /// When both "marker/subdir/" and "marker/subdir/file.txt" exist, listing with
    /// Prefix="marker/" must not duplicate "marker/subdir/file.txt" in Contents.
    #[tokio::test]
    async fn test_list_objects_v2_unique_contents_with_explicit_directory_markers() {
        init_logging();
        info!("Starting test: ListObjectsV2 should return unique keys with explicit directory markers");

        let mut env = RustFSTestEnvironment::new().await.expect("Failed to create test environment");
        env.start_rustfs_server(vec![]).await.expect("Failed to start RustFS");

        let client = create_s3_client(&env);
        let bucket = "test-list-unique-contents";

        create_bucket(&client, bucket).await.expect("Failed to create bucket");

        for (key, body) in [
            ("marker/", ByteStream::from_static(b"")),
            ("marker/subdir/", ByteStream::from_static(b"")),
            ("marker/file.txt", ByteStream::from_static(b"content")),
            ("marker/subdir/file.txt", ByteStream::from_static(b"nested")),
        ] {
            client
                .put_object()
                .bucket(bucket)
                .key(key)
                .body(body)
                .send()
                .await
                .unwrap_or_else(|err| panic!("Failed to create test object {key}: {err}"));
        }

        let result = client
            .list_objects_v2()
            .bucket(bucket)
            .prefix("marker/")
            .send()
            .await
            .expect("Failed to list objects");

        let keys: Vec<String> = result
            .contents()
            .iter()
            .filter_map(|object| object.key().map(ToOwned::to_owned))
            .collect();

        info!("Contents: {:?}", keys);

        assert_eq!(
            keys,
            vec![
                "marker/".to_string(),
                "marker/file.txt".to_string(),
                "marker/subdir/".to_string(),
                "marker/subdir/file.txt".to_string(),
            ]
        );
        assert_eq!(result.key_count(), Some(4));

        env.stop_server();
    }

    /// Test ensuring that a plain object and a same-named prefix coexist in
    /// delimiter listings on a single-disk deployment.
    ///
    /// Bug Reference: backlog#880 / backlog#1042
    /// On a single disk, object `a` and its children `a/...` share one backing
    /// directory, so the non-recursive scan used to classify `a` as an object
    /// and never produce the prefix entry `a/`. Delimiter="/" listings then
    /// returned Contents `a` but silently dropped CommonPrefix `a/`.
    #[tokio::test]
    async fn test_list_objects_v2_object_and_same_named_prefix_coexist() {
        init_logging();
        info!("Starting test: ListObjectsV2 should return both object `a` and CommonPrefix `a/`");

        let mut env = RustFSTestEnvironment::new().await.expect("Failed to create test environment");
        env.start_rustfs_server_with_env(vec![], &[("RUSTFS_CONSOLE_ENABLE", "false")])
            .await
            .expect("Failed to start RustFS");

        let client = create_s3_client(&env);
        let bucket = "test-list-object-prefix-coexist";

        create_bucket(&client, bucket).await.expect("Failed to create bucket");

        for (key, body) in [
            ("a", ByteStream::from_static(b"object body")),
            ("a/b", ByteStream::from_static(b"child body")),
            ("plain", ByteStream::from_static(b"no children")),
        ] {
            client
                .put_object()
                .bucket(bucket)
                .key(key)
                .body(body)
                .send()
                .await
                .unwrap_or_else(|err| panic!("Failed to create test object {key}: {err}"));
        }

        let result = client
            .list_objects_v2()
            .bucket(bucket)
            .delimiter("/")
            .send()
            .await
            .expect("Failed to list objects");

        let keys: Vec<&str> = result.contents().iter().filter_map(|object| object.key()).collect();
        let prefixes: Vec<&str> = result.common_prefixes().iter().filter_map(|prefix| prefix.prefix()).collect();

        info!("Contents: {:?}, CommonPrefixes: {:?}", keys, prefixes);

        assert_eq!(keys, vec!["a", "plain"], "objects `a` and `plain` must both stay in Contents");
        assert_eq!(
            prefixes,
            vec!["a/"],
            "prefix `a/` must be listed and `plain/` must not appear as a phantom prefix"
        );

        // Children are still reachable under the prefix.
        let nested = client
            .list_objects_v2()
            .bucket(bucket)
            .prefix("a/")
            .delimiter("/")
            .send()
            .await
            .expect("Failed to list objects under prefix");
        let nested_keys: Vec<&str> = nested.contents().iter().filter_map(|object| object.key()).collect();
        assert_eq!(nested_keys, vec!["a/b"]);

        // Pagination must keep both entries across page boundaries: `a` sorts
        // before `a/`, so a one-key page splits them.
        let page1 = client
            .list_objects_v2()
            .bucket(bucket)
            .delimiter("/")
            .max_keys(1)
            .send()
            .await
            .expect("Failed to list first page");
        let page1_keys: Vec<&str> = page1.contents().iter().filter_map(|object| object.key()).collect();
        assert_eq!(page1_keys, vec!["a"]);
        assert_eq!(page1.is_truncated(), Some(true), "one-key first page must be truncated");

        let mut token = page1.next_continuation_token().map(ToOwned::to_owned);
        let mut remaining_keys = Vec::new();
        let mut remaining_prefixes = Vec::new();
        while let Some(continuation) = token {
            let page = client
                .list_objects_v2()
                .bucket(bucket)
                .delimiter("/")
                .max_keys(1)
                .continuation_token(continuation)
                .send()
                .await
                .expect("Failed to list continuation page");
            remaining_keys.extend(
                page.contents()
                    .iter()
                    .filter_map(|object| object.key().map(ToOwned::to_owned)),
            );
            remaining_prefixes.extend(
                page.common_prefixes()
                    .iter()
                    .filter_map(|prefix| prefix.prefix().map(ToOwned::to_owned)),
            );
            token = page.next_continuation_token().map(ToOwned::to_owned);
        }

        assert_eq!(remaining_prefixes, vec!["a/".to_string()], "prefix `a/` must survive pagination");
        assert_eq!(remaining_keys, vec!["plain".to_string()]);

        env.stop_server();
    }

    async fn collect_prefix_pages(
        client: &Client,
        bucket: &str,
        prefix: &str,
        delimiter: Option<&str>,
        max_keys: i32,
        start_after: Option<&str>,
    ) -> (Vec<aws_sdk_s3::types::Object>, Vec<String>) {
        let mut objects = Vec::new();
        let mut prefixes = Vec::new();
        let mut token = None;
        let mut seen_tokens = std::collections::HashSet::new();
        for _ in 0..64 {
            let page = client
                .list_objects_v2()
                .bucket(bucket)
                .prefix(prefix)
                .set_delimiter(delimiter.map(ToOwned::to_owned))
                .max_keys(max_keys)
                .set_start_after(start_after.map(ToOwned::to_owned))
                .set_continuation_token(token)
                .send()
                .await
                .expect("prefix pagination request should succeed");
            let count = page.contents().len() + page.common_prefixes().len();
            assert!(count <= usize::try_from(max_keys).expect("positive page size"));
            assert_eq!(page.key_count(), Some(i32::try_from(count).expect("small page")));
            objects.extend_from_slice(page.contents());
            prefixes.extend(
                page.common_prefixes()
                    .iter()
                    .map(|item| item.prefix().expect("CommonPrefix must contain a prefix").to_owned()),
            );
            if page.is_truncated() == Some(false) {
                assert!(page.next_continuation_token().is_none(), "final page must not have a next token");
                return (objects, prefixes);
            }
            assert_eq!(page.is_truncated(), Some(true));
            let next = page.next_continuation_token().expect("truncated page requires a token");
            assert!(!next.is_empty());
            assert!(seen_tokens.insert(next.to_owned()), "continuation token must advance");
            token = Some(next.to_owned());
        }
        panic!("prefix pagination exceeded its finite page budget");
    }

    fn object_keys(objects: &[aws_sdk_s3::types::Object]) -> Vec<&str> {
        objects
            .iter()
            .map(|object| object.key().expect("listed object must have a key"))
            .collect()
    }

    /// Issue #8175: the prefix object must occur once, including across page boundaries.
    #[tokio::test]
    async fn test_list_objects_v2_prefix_marker_pagination() {
        init_logging();
        let mut env = RustFSTestEnvironment::new().await.expect("create test environment");
        env.start_rustfs_server(vec![]).await.expect("start RustFS");
        let client = create_s3_client(&env);
        let bucket = "test-prefix-marker-pagination";
        create_bucket(&client, bucket).await.expect("create bucket");
        let mut expected = vec!["content/".to_owned()];
        expected.extend((0..25).map(|index| format!("content/0123456789abcdef0123456789abcdef/{index:03}")));
        for key in &expected {
            client
                .put_object()
                .bucket(bucket)
                .key(key)
                .body(ByteStream::from_static(if key == "content/" { b"" } else { b"x" }))
                .send()
                .await
                .expect("create prefix fixture");
        }
        for max_keys in [1000, 1, 2] {
            let (objects, prefixes) = collect_prefix_pages(&client, bucket, "content/", None, max_keys, None).await;
            assert_eq!(object_keys(&objects), expected, "all keys must appear once at MaxKeys={max_keys}");
            assert!(prefixes.is_empty());
        }
        let (objects, prefixes) = collect_prefix_pages(&client, bucket, "content/", None, 1, Some("content/")).await;
        assert_eq!(object_keys(&objects), expected[1..]);
        assert!(prefixes.is_empty());

        // V1 shares the storage listing path but resumes with a key marker.
        let mut marker = Some("content/".to_owned());
        let mut v1_keys = Vec::new();
        let mut finished = false;
        for _ in 0..32 {
            let page = client
                .list_objects()
                .bucket(bucket)
                .prefix("content/")
                .max_keys(1)
                .set_marker(marker.clone())
                .send()
                .await
                .expect("list V1 continuation page");
            assert!(page.contents().len() <= 1);
            let keys = object_keys(page.contents());
            v1_keys.extend(keys.iter().map(|key| (*key).to_owned()));
            if page.is_truncated() == Some(false) {
                finished = true;
                break;
            }
            assert_eq!(page.is_truncated(), Some(true));
            let next = page
                .next_marker()
                .or_else(|| keys.last().copied())
                .expect("V1 page must advance");
            assert!(marker.as_deref().is_none_or(|previous| next > previous));
            marker = Some(next.to_owned());
        }
        assert!(finished, "V1 pagination must terminate");
        assert_eq!(v1_keys, expected[1..]);
        env.stop_server();
    }

    /// An exact prefix match does not establish EOF, including for ordinary keys.
    #[tokio::test]
    async fn test_list_objects_v2_exact_prefix_pagination_boundaries() {
        init_logging();
        let mut env = RustFSTestEnvironment::new().await.expect("create test environment");
        env.start_rustfs_server(vec![]).await.expect("start RustFS");
        let client = create_s3_client(&env);
        let bucket = "test-exact-prefix-boundaries";
        create_bucket(&client, bucket).await.expect("create bucket");
        for key in [
            "a",
            "ab",
            "solo/",
            "marker/",
            "marker/file",
            "marker/subdir/",
            "marker/subdir/file",
        ] {
            client
                .put_object()
                .bucket(bucket)
                .key(key)
                .body(ByteStream::from_static(b""))
                .send()
                .await
                .expect("create boundary fixture");
        }
        let (objects, prefixes) = collect_prefix_pages(&client, bucket, "a", None, 1, None).await;
        assert_eq!(object_keys(&objects), vec!["a", "ab"]);
        assert!(prefixes.is_empty());
        let first = client
            .list_objects()
            .bucket(bucket)
            .prefix("a")
            .max_keys(1)
            .send()
            .await
            .expect("list V1 exact-prefix first page");
        assert_eq!(object_keys(first.contents()), vec!["a"]);
        assert_eq!(first.is_truncated(), Some(true));
        let last = client
            .list_objects()
            .bucket(bucket)
            .prefix("a")
            .max_keys(1)
            .marker(first.next_marker().unwrap_or("a"))
            .send()
            .await
            .expect("list V1 exact-prefix final page");
        assert_eq!(object_keys(last.contents()), vec!["ab"]);
        assert_eq!(last.is_truncated(), Some(false));

        let solo = client
            .list_objects_v2()
            .bucket(bucket)
            .prefix("solo/")
            .max_keys(1)
            .send()
            .await
            .expect("list isolated marker");
        assert_eq!(object_keys(solo.contents()), vec!["solo/"]);
        assert_eq!(solo.is_truncated(), Some(false));
        assert!(solo.next_continuation_token().is_none());
        let (objects, prefixes) = collect_prefix_pages(&client, bucket, "marker/", Some("/"), 1, None).await;
        assert_eq!(object_keys(&objects), vec!["marker/", "marker/file"]);
        assert_eq!(prefixes, vec!["marker/subdir/"]);
        env.stop_server();
    }

    /// A directory marker must retain its own metadata when a same-named object exists.
    #[tokio::test]
    async fn test_list_objects_v2_prefix_marker_preserves_metadata() {
        init_logging();
        let mut env = RustFSTestEnvironment::new().await.expect("create test environment");
        env.start_rustfs_server(vec![]).await.expect("start RustFS");
        let client = create_s3_client(&env);
        let bucket = "test-prefix-marker-metadata";
        create_bucket(&client, bucket).await.expect("create bucket");
        let plain = client
            .put_object()
            .bucket(bucket)
            .key("content")
            .body(ByteStream::from_static(b"plain object body"))
            .send()
            .await
            .expect("put plain object");
        let directory = client
            .put_object()
            .bucket(bucket)
            .key("content/")
            .body(ByteStream::from_static(b""))
            .send()
            .await
            .expect("put directory marker");
        assert_ne!(plain.e_tag(), directory.e_tag(), "fixture ETags must distinguish the objects");
        for max_keys in [1, 1000] {
            let (objects, prefixes) = collect_prefix_pages(&client, bucket, "content/", None, max_keys, None).await;
            assert_eq!(object_keys(&objects), vec!["content/"]);
            assert!(prefixes.is_empty());
            assert_eq!(objects[0].size(), Some(0));
            assert_eq!(objects[0].e_tag(), directory.e_tag());
            let (objects, prefixes) = collect_prefix_pages(&client, bucket, "content", None, max_keys, None).await;
            assert_eq!(object_keys(&objects), vec!["content", "content/"]);
            assert!(prefixes.is_empty());
            assert_eq!(objects[0].size(), Some(17));
            assert_eq!(objects[0].e_tag(), plain.e_tag());
            assert_eq!(objects[1].size(), Some(0));
            assert_eq!(objects[1].e_tag(), directory.e_tag());
        }
        env.stop_server();
    }
}
