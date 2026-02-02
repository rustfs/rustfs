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

use rustfs_trusted_proxies::AwsMetadataFetcher;
use rustfs_trusted_proxies::CloudDetector;
use rustfs_trusted_proxies::CloudMetadataFetcher;
use std::time::Duration;

#[tokio::test]
async fn test_cloud_detector_disabled() {
    let detector = CloudDetector::new(false, Duration::from_secs(1), None);
    let provider = detector.detect_provider();
    assert!(provider.is_none());
}

#[tokio::test]
async fn test_aws_metadata_fetcher() {
    let fetcher = AwsMetadataFetcher::new(Duration::from_secs(5));
    assert_eq!(fetcher.provider_name(), "aws");
}
