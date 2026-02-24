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

use super::*;

impl SetDisks {
    pub async fn update_restore_metadata(
        &self,
        bucket: &str,
        object: &str,
        obj_info: &ObjectInfo,
        opts: &ObjectOptions,
    ) -> Result<()> {
        let mut oi = obj_info.clone();
        oi.metadata_only = true;

        oi.user_defined.remove(X_AMZ_RESTORE.as_str());

        let version_id = oi.version_id.map(|v| v.to_string());
        let _obj = self
            .copy_object(
                bucket,
                object,
                bucket,
                object,
                &mut oi,
                &ObjectOptions {
                    version_id: version_id.clone(),
                    ..Default::default()
                },
                &ObjectOptions {
                    version_id,
                    ..Default::default()
                },
            )
            .await?;
        Ok(())
    }
}
