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

use crate::storage::s3_api::common::rustfs_owner;
use s3s::dto::{GetBucketAclOutput, GetObjectAclOutput, Grant, Grantee, Owner, Permission, Type};

fn full_control_grants(owner: &Owner) -> Vec<Grant> {
    vec![Grant {
        grantee: Some(Grantee {
            type_: Type::from_static(Type::CANONICAL_USER),
            display_name: owner.display_name.clone(),
            email_address: None,
            id: owner.id.clone(),
            uri: None,
        }),
        permission: Some(Permission::from_static(Permission::FULL_CONTROL)),
    }]
}

pub(crate) fn build_get_bucket_acl_output() -> GetBucketAclOutput {
    let owner = rustfs_owner();
    GetBucketAclOutput {
        grants: Some(full_control_grants(&owner)),
        owner: Some(owner),
    }
}

pub(crate) fn build_get_object_acl_output() -> GetObjectAclOutput {
    let owner = rustfs_owner();
    GetObjectAclOutput {
        grants: Some(full_control_grants(&owner)),
        owner: Some(owner),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::{build_get_bucket_acl_output, build_get_object_acl_output};
    use crate::storage::s3_api::common::rustfs_owner;
    use s3s::dto::{Permission, Type};

    #[test]
    fn test_build_get_bucket_acl_output_contains_full_control_grant_and_owner() {
        let output = build_get_bucket_acl_output();
        let grants = output.grants.as_ref().expect("grants should be present");
        let owner = output.owner.as_ref().expect("owner should be present");

        assert_eq!(grants.len(), 1);
        assert_eq!(grants[0].permission.as_ref().map(Permission::as_str), Some(Permission::FULL_CONTROL));
        let grantee = grants[0].grantee.as_ref().expect("owner grant must identify its grantee");
        assert_eq!(grantee.type_.as_str(), Type::CANONICAL_USER);
        assert_eq!(grantee.id, owner.id);
        assert_eq!(grantee.display_name, owner.display_name);

        let expected_owner = rustfs_owner();
        assert_eq!(owner.display_name, expected_owner.display_name);
        assert_eq!(owner.id, expected_owner.id);
    }

    #[test]
    fn test_build_get_object_acl_output_contains_full_control_grant_and_owner() {
        let output = build_get_object_acl_output();
        let grants = output.grants.as_ref().expect("grants should be present");
        let owner = output.owner.as_ref().expect("owner should be present");

        assert_eq!(grants.len(), 1);
        assert_eq!(grants[0].permission.as_ref().map(Permission::as_str), Some(Permission::FULL_CONTROL));
        let grantee = grants[0].grantee.as_ref().expect("owner grant must identify its grantee");
        assert_eq!(grantee.type_.as_str(), Type::CANONICAL_USER);
        assert_eq!(grantee.id, owner.id);
        assert_eq!(grantee.display_name, owner.display_name);

        let expected_owner = rustfs_owner();
        assert_eq!(owner.display_name, expected_owner.display_name);
        assert_eq!(owner.id, expected_owner.id);
    }
}
