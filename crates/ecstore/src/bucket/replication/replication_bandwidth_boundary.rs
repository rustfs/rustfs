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

use std::sync::Arc;

use tokio::io::AsyncRead;

use crate::bucket::bandwidth::monitor::Monitor;
use crate::bucket::bandwidth::reader::{BucketOptions, MonitorReaderOptions, MonitoredReader};

pub(crate) type ReplicationBucketMonitor = Monitor;

pub(crate) fn wrap_reader(
    stream: Box<dyn AsyncRead + Unpin + Send + Sync>,
    monitor: Arc<ReplicationBucketMonitor>,
    bucket: &str,
    arn: &str,
    header_size: usize,
) -> Box<dyn AsyncRead + Unpin + Send + Sync> {
    Box::new(MonitoredReader::new(
        monitor,
        stream,
        MonitorReaderOptions {
            bucket_options: BucketOptions {
                name: bucket.to_string(),
                replication_arn: arn.to_string(),
            },
            header_size,
        },
    ))
}
