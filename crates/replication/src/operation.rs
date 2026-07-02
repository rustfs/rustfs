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

use std::collections::HashMap;

use rustfs_utils::http::{
    AMZ_BUCKET_REPLICATION_STATUS, AMZ_OBJECT_TAGGING, SSEC_ALGORITHM_HEADER, SSEC_KEY_HEADER, SSEC_KEY_MD5_HEADER,
};

use crate::{ReplicationStatusType, ReplicationType};

#[derive(Debug, Clone, Default)]
pub struct MustReplicateOptions {
    meta: HashMap<String, String>,
    op_type: ReplicationType,
    replication_request: bool,
}

impl MustReplicateOptions {
    pub fn new(meta: &HashMap<String, String>, user_tags: String, op_type: ReplicationType, replication_request: bool) -> Self {
        let mut meta = meta.clone();
        if !user_tags.is_empty() {
            meta.insert(AMZ_OBJECT_TAGGING.to_string(), user_tags);
        }

        Self {
            meta,
            op_type,
            replication_request,
        }
    }

    pub fn replication_status(&self) -> ReplicationStatusType {
        if let Some(rs) = self.meta.get(AMZ_BUCKET_REPLICATION_STATUS) {
            return ReplicationStatusType::from(rs.as_str());
        }
        ReplicationStatusType::default()
    }

    pub fn is_existing_object_replication(&self) -> bool {
        self.op_type == ReplicationType::ExistingObject
    }

    pub fn is_metadata_replication(&self) -> bool {
        self.op_type == ReplicationType::Metadata
    }

    pub fn is_replication_request(&self) -> bool {
        self.replication_request
    }

    pub fn user_tags(&self) -> &str {
        self.meta.get(AMZ_OBJECT_TAGGING).map(String::as_str).unwrap_or_default()
    }
}

pub fn is_ssec_encrypted(user_defined: &HashMap<String, String>) -> bool {
    user_defined.contains_key(SSEC_ALGORITHM_HEADER)
        || user_defined.contains_key(SSEC_KEY_HEADER)
        || user_defined.contains_key(SSEC_KEY_MD5_HEADER)
}

#[cfg(test)]
mod tests {
    use super::{MustReplicateOptions, is_ssec_encrypted};
    use crate::{ReplicationStatusType, ReplicationType};
    use rustfs_utils::http::{AMZ_BUCKET_REPLICATION_STATUS, SSEC_ALGORITHM_HEADER};
    use std::collections::HashMap;

    #[test]
    fn must_replicate_options_preserves_user_tags_and_operation_type() {
        let options = MustReplicateOptions::new(&HashMap::new(), "env=prod".to_string(), ReplicationType::ExistingObject, true);

        assert_eq!(options.user_tags(), "env=prod");
        assert!(options.is_existing_object_replication());
        assert!(options.is_replication_request());
    }

    #[test]
    fn must_replicate_options_reads_replication_status_header() {
        let meta = HashMap::from([(AMZ_BUCKET_REPLICATION_STATUS.to_string(), "COMPLETED".to_string())]);
        let options = MustReplicateOptions::new(&meta, String::new(), ReplicationType::Object, false);

        assert_eq!(options.replication_status(), ReplicationStatusType::Completed);
    }

    #[test]
    fn ssec_detection_uses_existing_metadata_headers() {
        let meta = HashMap::from([(SSEC_ALGORITHM_HEADER.to_string(), "AES256".to_string())]);

        assert!(is_ssec_encrypted(&meta));
        assert!(!is_ssec_encrypted(&HashMap::new()));
    }
}
