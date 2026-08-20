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

//! Regression tests for object listing and metacache consistency.
//!
//! Covers the recurring pattern where ListObjectsV2 returns incomplete results,
//! silently truncates with IsTruncated=false, or corrupts the metadata cache.
//! This has regressed 8+ times.
//!
//! ## Regression Issues
//!
//! - rustfs#5166: Metacache listing quorum failed timeout after cluster startup
//! - rustfs#5156: Metacache producer failed
//! - rustfs#5051: ListObjectsV2 returns empty results for shallow prefixes
//! - rustfs#4810: walk_dir timeout silently truncates listings (200, IsTruncated=false)
//! - rustfs#4648: Object listing oscillates between complete, partial, and zero
//! - rustfs#3191: ListObjectsV2 timeout corrupts metadata cache → NoSuchBucket

#[cfg(test)]
mod tests {
    use crate::common::{RustFSTestEnvironment, init_logging};
    use aws_sdk_s3::primitives::ByteStream;
    use std::collections::HashSet;
    use std::error::Error;
    use tracing::info;

    type TestResult = Result<(), Box<dyn Error + Send + Sync>>;

    /// RT-06: Verify ListObjectsV2 pagination completeness for medium-sized bucket.
    ///
    /// Regression pattern: listing returns 200 with IsTruncated=false but
    /// misses objects (rustfs#4810: walk_dir timeout truncation).
    ///
    /// Steps:
    /// 1. Upload 100 objects with known keys
    /// 2. List all objects via pagination (max_keys=10)
    /// 3. Verify all 100 keys are returned exactly once
    /// 4. Verify no duplicates or skipped keys
    #[tokio::test]
    async fn test_list_objects_v2_completeness_100_objects() -> TestResult {
        init_logging();
        info!("RT-06: listing completeness with 100 objects");

        let mut env = RustFSTestEnvironment::new().await.expect("create test environment");
        env.start_rustfs_server_with_env(vec![], &[("RUSTFS_CONSOLE_ENABLE", "false")])
            .await
            .expect("start RustFS");

        let client = env.create_s3_client();
        let bucket = "rt06-list-completeness";

        client.create_bucket().bucket(bucket).send().await.expect("create bucket");

        // Upload 100 objects
        let expected_keys: Vec<String> = (0..100).map(|i| format!("obj-{i:04}.txt")).collect();
        for key in &expected_keys {
            client
                .put_object()
                .bucket(bucket)
                .key(key)
                .body(ByteStream::from_static(b"data"))
                .send()
                .await
                .expect("put object");
        }

        // Paginate through all objects (small page size to force multiple pages)
        let mut all_keys: Vec<String> = Vec::new();
        let mut continuation_token: Option<String> = None;

        loop {
            let mut req = client.list_objects_v2().bucket(bucket).max_keys(10);

            if let Some(ref token) = continuation_token {
                req = req.continuation_token(token);
            }

            let resp = req.send().await.expect("list objects page");

            for obj in resp.contents() {
                all_keys.push(obj.key().unwrap_or("").to_string());
            }

            if !resp.is_truncated().unwrap_or(false) {
                break;
            }
            continuation_token = resp.next_continuation_token().map(|s| s.to_string());
        }

        // Verify completeness and uniqueness
        let unique_keys: HashSet<&str> = all_keys.iter().map(|s| s.as_str()).collect();

        assert_eq!(
            all_keys.len(),
            100,
            "RT-06 FAIL: expected 100 objects, listed {} (regression: walk_dir truncation)",
            all_keys.len()
        );
        assert_eq!(
            unique_keys.len(),
            100,
            "RT-06 FAIL: found {} unique keys but listed {} total (duplicates!)",
            unique_keys.len(),
            all_keys.len()
        );

        for key in &expected_keys {
            assert!(
                unique_keys.contains(key.as_str()),
                "RT-06 FAIL: key '{key}' missing from listing (regression rustfs#4810)"
            );
        }

        info!("RT-06 PASS: all 100 objects listed completely and uniquely");
        Ok(())
    }

    /// RT-06b: Verify listing with prefix filter returns correct subset.
    ///
    /// Regression pattern: prefix filter returns empty or includes wrong keys
    /// (rustfs#5051: empty results for shallow prefixes).
    #[tokio::test]
    async fn test_list_objects_v2_prefix_filter_correctness() -> TestResult {
        init_logging();
        info!("RT-06b: prefix filter correctness");

        let mut env = RustFSTestEnvironment::new().await.expect("create test environment");
        env.start_rustfs_server_with_env(vec![], &[("RUSTFS_CONSOLE_ENABLE", "false")])
            .await
            .expect("start RustFS");

        let client = env.create_s3_client();
        let bucket = "rt06b-prefix-filter";

        client.create_bucket().bucket(bucket).send().await.expect("create bucket");

        // Upload objects with different prefixes
        for i in 0..5 {
            client
                .put_object()
                .bucket(bucket)
                .key(format!("logs/app-{i:04}.log"))
                .body(ByteStream::from_static(b"log data"))
                .send()
                .await
                .expect("put log object");

            client
                .put_object()
                .bucket(bucket)
                .key(format!("data/file-{i:04}.csv"))
                .body(ByteStream::from_static(b"csv data"))
                .send()
                .await
                .expect("put data object");
        }

        // List with prefix "logs/" — should return exactly 5
        let resp = client
            .list_objects_v2()
            .bucket(bucket)
            .prefix("logs/")
            .send()
            .await
            .expect("list with prefix");

        assert_eq!(
            resp.contents().len(),
            5,
            "RT-06b FAIL: expected 5 objects with prefix 'logs/', found {} (regression rustfs#5051)",
            resp.contents().len()
        );

        for obj in resp.contents() {
            assert!(
                obj.key().unwrap_or("").starts_with("logs/"),
                "RT-06b FAIL: object '{}' does not match prefix 'logs/'",
                obj.key().unwrap_or("?")
            );
        }

        // List with prefix "data/" — should return exactly 5
        let resp = client
            .list_objects_v2()
            .bucket(bucket)
            .prefix("data/")
            .send()
            .await
            .expect("list with data/ prefix");

        assert_eq!(
            resp.contents().len(),
            5,
            "RT-06b FAIL: expected 5 objects with prefix 'data/', found {}",
            resp.contents().len()
        );

        // List with prefix "nonexistent/" — should return 0
        let resp = client
            .list_objects_v2()
            .bucket(bucket)
            .prefix("nonexistent/")
            .send()
            .await
            .expect("list with nonexistent prefix");

        assert!(
            resp.contents().is_empty(),
            "RT-06b FAIL: expected 0 objects with prefix 'nonexistent/', found {}",
            resp.contents().len()
        );

        info!("RT-06b PASS: prefix filter returns correct subset");
        Ok(())
    }

    /// RT-06c: Verify listing with delimiter and CommonPrefixes.
    ///
    /// Regression pattern: delimiter handling produces incorrect CommonPrefixes
    /// or misses objects at the delimiter boundary.
    #[tokio::test]
    async fn test_list_objects_v2_delimiter_common_prefixes() -> TestResult {
        init_logging();
        info!("RT-06c: delimiter and CommonPrefixes");

        let mut env = RustFSTestEnvironment::new().await.expect("create test environment");
        env.start_rustfs_server_with_env(vec![], &[("RUSTFS_CONSOLE_ENABLE", "false")])
            .await
            .expect("start RustFS");

        let client = env.create_s3_client();
        let bucket = "rt06c-delimiter";

        client.create_bucket().bucket(bucket).send().await.expect("create bucket");

        // Create a hierarchical structure
        let keys = vec!["a.txt", "dir1/b.txt", "dir1/sub1/c.txt", "dir1/sub2/d.txt", "dir2/e.txt"];

        for key in &keys {
            client
                .put_object()
                .bucket(bucket)
                .key(*key)
                .body(ByteStream::from_static(b"content"))
                .send()
                .await
                .expect("put object");
        }

        // List with delimiter "/" at root level
        let resp = client
            .list_objects_v2()
            .bucket(bucket)
            .delimiter("/")
            .send()
            .await
            .expect("list with delimiter");

        // Should have 1 object (a.txt) and 2 common prefixes (dir1/, dir2/)
        let contents: Vec<_> = resp.contents().iter().map(|o| o.key().unwrap_or("")).collect();
        let prefixes: Vec<_> = resp.common_prefixes().iter().map(|p| p.prefix().unwrap_or("")).collect();

        assert!(contents.contains(&"a.txt"), "RT-06c FAIL: root object 'a.txt' missing from listing");
        assert_eq!(contents.len(), 1, "RT-06c FAIL: expected 1 root-level object, found {}", contents.len());
        assert_eq!(prefixes.len(), 2, "RT-06c FAIL: expected 2 common prefixes, found {:?}", prefixes);
        assert!(prefixes.contains(&"dir1/"), "RT-06c FAIL: 'dir1/' missing from CommonPrefixes");
        assert!(prefixes.contains(&"dir2/"), "RT-06c FAIL: 'dir2/' missing from CommonPrefixes");

        info!("RT-06c PASS: delimiter and CommonPrefixes correct");
        Ok(())
    }

    /// RT-06d: Verify listing returns correct IsTruncated flag.
    ///
    /// Regression pattern: IsTruncated=false when there are more objects
    /// (rustfs#4810: walk_dir timeout truncation with false IsTruncated).
    #[tokio::test]
    async fn test_list_objects_v2_is_truncated_correctness() -> TestResult {
        init_logging();
        info!("RT-06d: IsTruncated correctness");

        let mut env = RustFSTestEnvironment::new().await.expect("create test environment");
        env.start_rustfs_server_with_env(vec![], &[("RUSTFS_CONSOLE_ENABLE", "false")])
            .await
            .expect("start RustFS");

        let client = env.create_s3_client();
        let bucket = "rt06d-truncated";

        client.create_bucket().bucket(bucket).send().await.expect("create bucket");

        // Upload 15 objects
        for i in 0..15 {
            client
                .put_object()
                .bucket(bucket)
                .key(format!("item-{i:04}.txt"))
                .body(ByteStream::from_static(b"data"))
                .send()
                .await
                .expect("put object");
        }

        // List with max_keys=5 — should be truncated
        let resp = client
            .list_objects_v2()
            .bucket(bucket)
            .max_keys(5)
            .send()
            .await
            .expect("list with max_keys=5");

        assert!(
            resp.is_truncated().unwrap_or(false),
            "RT-06d FAIL: IsTruncated should be true with 15 objects and max_keys=5"
        );
        assert_eq!(resp.contents().len(), 5, "RT-06d FAIL: expected 5 objects in first page");
        assert!(
            resp.next_continuation_token().is_some(),
            "RT-06d FAIL: NextContinuationToken should be present when truncated"
        );

        // List with max_keys=100 — should NOT be truncated
        let resp = client
            .list_objects_v2()
            .bucket(bucket)
            .max_keys(100)
            .send()
            .await
            .expect("list with max_keys=100");

        assert!(
            !resp.is_truncated().unwrap_or(false),
            "RT-06d FAIL: IsTruncated should be false with 15 objects and max_keys=100"
        );
        assert_eq!(resp.contents().len(), 15, "RT-06d FAIL: expected 15 objects with max_keys=100");

        info!("RT-06d PASS: IsTruncated flag is correct");
        Ok(())
    }
}
