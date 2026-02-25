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

use s3s::dto::{GetBucketReplicationOutput, PutBucketReplicationOutput, ReplicationConfiguration};

pub(crate) fn build_get_bucket_replication_output(
    replication_configuration: ReplicationConfiguration,
) -> GetBucketReplicationOutput {
    GetBucketReplicationOutput {
        replication_configuration: Some(replication_configuration),
    }
}

pub(crate) fn build_put_bucket_replication_output() -> PutBucketReplicationOutput {
    PutBucketReplicationOutput::default()
}

#[cfg(test)]
mod tests {
    use super::{build_get_bucket_replication_output, build_put_bucket_replication_output};
    use s3s::dto::ReplicationConfiguration;

    #[test]
    fn test_build_get_bucket_replication_output_sets_configuration() {
        let config = ReplicationConfiguration::default();
        let output = build_get_bucket_replication_output(config.clone());

        assert_eq!(output.replication_configuration, Some(config));
    }

    #[test]
    fn test_build_put_bucket_replication_output_is_default() {
        let output = build_put_bucket_replication_output();
        assert_eq!(output, Default::default());
    }
}
