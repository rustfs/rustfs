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

use crate::error::{Error, Result};
use serde::{
    Deserialize, Deserializer, Serialize,
    de::{self, Error as DeError, Visitor},
};
use std::{collections::HashSet, fmt, ops::Deref};
use strum::{EnumString, IntoStaticStr};

use super::{Error as IamError, Validator, utils::wildcard};

/// A set of policy actions that always serializes as an array of strings,
/// conforming to the S3 policy specification for consistency and compatibility.
#[derive(Clone, Default, Debug)]
pub struct ActionSet(pub HashSet<Action>);

impl Serialize for ActionSet {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeSeq;

        // Always serialize as array, even for single action, to match S3 specification
        // and ensure compatibility with AWS SDK clients that expect array format
        let mut seq = serializer.serialize_seq(Some(self.0.len()))?;
        for action in &self.0 {
            let action_str: &str = action.into();
            seq.serialize_element(action_str)?;
        }
        seq.end()
    }
}

impl ActionSet {
    /// Returns true if the action set is empty.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn is_match(&self, action: &Action) -> bool {
        for act in self.0.iter() {
            if act.is_match(action) {
                return true;
            }

            if matches!(act, Action::S3Action(S3Action::GetObjectVersionAction))
                && matches!(action, Action::S3Action(S3Action::GetObjectAction))
            {
                return true;
            }
        }

        false
    }
}

impl Deref for ActionSet {
    type Target = HashSet<Action>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Validator for ActionSet {
    type Error = Error;
    fn is_valid(&self) -> Result<()> {
        Ok(())
    }
}

impl PartialEq for ActionSet {
    fn eq(&self, other: &Self) -> bool {
        self.len() == other.len() && self.0.iter().all(|x| other.0.contains(x))
    }
}

impl<'de> Deserialize<'de> for ActionSet {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct ActionOrVecVisitor;

        impl<'de> Visitor<'de> for ActionOrVecVisitor {
            type Value = ActionSet;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a string or an array of strings")
            }

            fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
            where
                E: de::Error,
            {
                let action = Action::try_from(value).map_err(|e| E::custom(format!("invalid action: {}", e)))?;
                let mut set = HashSet::new();
                set.insert(action);
                Ok(ActionSet(set))
            }

            fn visit_seq<A>(self, mut seq: A) -> std::result::Result<Self::Value, A::Error>
            where
                A: de::SeqAccess<'de>,
                A::Error: DeError,
            {
                let mut set = HashSet::with_capacity(seq.size_hint().unwrap_or(0));
                while let Some(value) = seq.next_element::<String>()? {
                    match Action::try_from(value.as_str()) {
                        Ok(action) => {
                            set.insert(action);
                        }
                        Err(e) => {
                            return Err(A::Error::custom(format!("invalid action: {}", e)));
                        }
                    }
                }
                Ok(ActionSet(set))
            }
        }

        deserializer.deserialize_any(ActionOrVecVisitor)
    }
}

#[derive(Serialize, Deserialize, Hash, PartialEq, Eq, Clone, Debug, Copy)]
#[serde(try_from = "&str", untagged)]
pub enum Action {
    S3Action(S3Action),
    AdminAction(AdminAction),
    StsAction(StsAction),
    KmsAction(KmsAction),
    None,
}

impl Action {
    pub fn is_match(&self, action: &Action) -> bool {
        wildcard::is_match::<&str, &str>(self.into(), action.into())
    }
}

impl From<&Action> for &str {
    fn from(value: &Action) -> &'static str {
        match value {
            Action::S3Action(s) => s.into(),
            Action::AdminAction(s) => s.into(),
            Action::StsAction(s) => s.into(),
            Action::KmsAction(s) => s.into(),
            Action::None => "",
        }
    }
}

impl Action {
    const S3_PREFIX: &'static str = "s3:";
    const ADMIN_PREFIX: &'static str = "admin:";
    const STS_PREFIX: &'static str = "sts:";
    const KMS_PREFIX: &'static str = "kms:";
}

impl TryFrom<&str> for Action {
    type Error = Error;
    fn try_from(value: &str) -> std::result::Result<Self, Self::Error> {
        // Support wildcard "*" which matches all S3 actions (AWS S3 standard)
        if value == "*" {
            return Ok(Self::S3Action(S3Action::AllActions));
        }
        if value.starts_with(Self::S3_PREFIX) {
            Ok(Self::S3Action(
                S3Action::try_from(value).map_err(|_| IamError::InvalidAction(value.into()))?,
            ))
        } else if value.starts_with(Self::ADMIN_PREFIX) {
            Ok(Self::AdminAction(
                AdminAction::try_from(value).map_err(|_| IamError::InvalidAction(value.into()))?,
            ))
        } else if value.starts_with(Self::STS_PREFIX) {
            Ok(Self::StsAction(
                StsAction::try_from(value).map_err(|_| IamError::InvalidAction(value.into()))?,
            ))
        } else if value.starts_with(Self::KMS_PREFIX) {
            Ok(Self::KmsAction(
                KmsAction::try_from(value).map_err(|_| IamError::InvalidAction(value.into()))?,
            ))
        } else {
            Err(IamError::InvalidAction(value.into()).into())
        }
    }
}

#[derive(Serialize, Deserialize, Hash, PartialEq, Eq, Clone, EnumString, IntoStaticStr, Debug, Copy)]
#[cfg_attr(test, derive(Default))]
#[serde(try_from = "&str", into = "&str")]
pub enum S3Action {
    #[cfg_attr(test, default)]
    #[strum(serialize = "s3:*")]
    AllActions,
    #[strum(serialize = "s3:AbortMultipartUpload")]
    AbortMultipartUploadAction,
    #[strum(serialize = "s3:CreateBucket")]
    CreateBucketAction,
    #[strum(serialize = "s3:DeleteBucket")]
    DeleteBucketAction,
    #[strum(serialize = "s3:ForceDeleteBucket")]
    ForceDeleteBucketAction,
    #[strum(serialize = "s3:DeleteBucketPolicy")]
    DeleteBucketPolicyAction,
    #[strum(serialize = "s3:DeleteBucketCors")]
    DeleteBucketCorsAction,
    #[strum(serialize = "s3:DeleteObject")]
    DeleteObjectAction,
    #[strum(serialize = "s3:GetBucketLocation")]
    GetBucketLocationAction,
    #[strum(serialize = "s3:GetBucketNotification")]
    GetBucketNotificationAction,
    #[strum(serialize = "s3:GetBucketPolicy")]
    GetBucketPolicyAction,
    #[strum(serialize = "s3:GetBucketCors")]
    GetBucketCorsAction,
    #[strum(serialize = "s3:GetObject")]
    GetObjectAction,
    #[strum(serialize = "s3:GetObjectAttributes")]
    GetObjectAttributesAction,
    #[strum(serialize = "s3:HeadBucket")]
    HeadBucketAction,
    #[strum(serialize = "s3:ListAllMyBuckets")]
    ListAllMyBucketsAction,
    #[strum(serialize = "s3:ListBucket")]
    ListBucketAction,
    #[strum(serialize = "s3:GetBucketPolicyStatus")]
    GetBucketPolicyStatusAction,
    #[strum(serialize = "s3:ListBucketVersions")]
    ListBucketVersionsAction,
    #[strum(serialize = "s3:ListBucketMultipartUploads")]
    ListBucketMultipartUploadsAction,
    #[strum(serialize = "s3:ListenNotification")]
    ListenNotificationAction,
    #[strum(serialize = "s3:ListenBucketNotification")]
    ListenBucketNotificationAction,
    #[strum(serialize = "s3:ListMultipartUploadParts")]
    ListMultipartUploadPartsAction,
    #[strum(serialize = "s3:PutBucketLifecycle")]
    PutBucketLifecycleAction,
    #[strum(serialize = "s3:GetBucketLifecycle")]
    GetBucketLifecycleAction,
    #[strum(serialize = "s3:PutBucketNotification")]
    PutBucketNotificationAction,
    #[strum(serialize = "s3:PutBucketPolicy")]
    PutBucketPolicyAction,
    #[strum(serialize = "s3:PutBucketCors")]
    PutBucketCorsAction,
    #[strum(serialize = "s3:PutObject")]
    PutObjectAction,
    #[strum(serialize = "s3:DeleteObjectVersion")]
    DeleteObjectVersionAction,
    #[strum(serialize = "s3:DeleteObjectVersionTagging")]
    DeleteObjectVersionTaggingAction,
    #[strum(serialize = "s3:GetObjectVersion")]
    GetObjectVersionAction,
    #[strum(serialize = "s3:GetObjectVersionAttributes")]
    GetObjectVersionAttributesAction,
    #[strum(serialize = "s3:GetObjectVersionTagging")]
    GetObjectVersionTaggingAction,
    #[strum(serialize = "s3:PutObjectVersionTagging")]
    PutObjectVersionTaggingAction,
    #[strum(serialize = "s3:BypassGovernanceRetention")]
    BypassGovernanceRetentionAction,
    #[strum(serialize = "s3:PutObjectRetention")]
    PutObjectRetentionAction,
    #[strum(serialize = "s3:GetObjectRetention")]
    GetObjectRetentionAction,
    #[strum(serialize = "s3:GetObjectLegalHold")]
    GetObjectLegalHoldAction,
    #[strum(serialize = "s3:PutObjectLegalHold")]
    PutObjectLegalHoldAction,
    #[strum(serialize = "s3:GetBucketObjectLockConfiguration")]
    GetBucketObjectLockConfigurationAction,
    #[strum(serialize = "s3:PutBucketObjectLockConfiguration")]
    PutBucketObjectLockConfigurationAction,
    #[strum(serialize = "s3:GetBucketTagging")]
    GetBucketTaggingAction,
    #[strum(serialize = "s3:PutBucketTagging")]
    PutBucketTaggingAction,
    #[strum(serialize = "s3:GetObjectTagging")]
    GetObjectTaggingAction,
    #[strum(serialize = "s3:PutObjectTagging")]
    PutObjectTaggingAction,
    #[strum(serialize = "s3:DeleteObjectTagging")]
    DeleteObjectTaggingAction,
    #[strum(serialize = "s3:PutBucketEncryption")]
    PutBucketEncryptionAction,
    #[strum(serialize = "s3:GetBucketEncryption")]
    GetBucketEncryptionAction,
    #[strum(serialize = "s3:PutBucketVersioning")]
    PutBucketVersioningAction,
    #[strum(serialize = "s3:GetBucketVersioning")]
    GetBucketVersioningAction,
    #[strum(serialize = "s3:GetReplicationConfiguration")]
    GetReplicationConfigurationAction,
    #[strum(serialize = "s3:PutReplicationConfiguration")]
    PutReplicationConfigurationAction,
    #[strum(serialize = "s3:ReplicateObject")]
    ReplicateObjectAction,
    #[strum(serialize = "s3:ReplicateDelete")]
    ReplicateDeleteAction,
    #[strum(serialize = "s3:ReplicateTags")]
    ReplicateTagsAction,
    #[strum(serialize = "s3:GetObjectVersionForReplication")]
    GetObjectVersionForReplicationAction,
    #[strum(serialize = "s3:RestoreObject")]
    RestoreObjectAction,
    #[strum(serialize = "s3:ResetBucketReplicationState")]
    ResetBucketReplicationStateAction,
    #[strum(serialize = "s3:PutObjectFanOut")]
    PutObjectFanOutAction,
}

// #[derive(Serialize, Deserialize, Hash, PartialEq, Eq, Clone, EnumString, IntoStaticStr, Debug, Copy)]
// #[serde(try_from = "&str", into = "&str")]
// pub enum AdminAction {
//     #[strum(serialize = "admin:*")]
//     AllActions,
//     #[strum(serialize = "admin:Profiling")]
//     ProfilingAdminAction,
//     #[strum(serialize = "admin:ServerTrace")]
//     TraceAdminAction,
//     #[strum(serialize = "admin:ConsoleLog")]
//     ConsoleLogAdminAction,
//     #[strum(serialize = "admin:ServerInfo")]
//     ServerInfoAdminAction,
//     #[strum(serialize = "admin:OBDInfo")]
//     HealthInfoAdminAction,
//     #[strum(serialize = "admin:TopLocksInfo")]
//     TopLocksAdminAction,
//     #[strum(serialize = "admin:LicenseInfo")]
//     LicenseInfoAdminAction,
//     #[strum(serialize = "admin:BandwidthMonitor")]
//     BandwidthMonitorAction,
//     #[strum(serialize = "admin:InspectData")]
//     InspectDataAction,
//     #[strum(serialize = "admin:Prometheus")]
//     PrometheusAdminAction,
//     #[strum(serialize = "admin:ListServiceAccounts")]
//     ListServiceAccountsAdminAction,
//     #[strum(serialize = "admin:CreateServiceAccount")]
//     CreateServiceAccountAdminAction,
// }

// AdminAction - admin policy action.
#[derive(Serialize, Deserialize, Hash, PartialEq, Eq, Clone, EnumString, IntoStaticStr, Debug, Copy)]
#[serde(try_from = "&str", into = "&str")]
pub enum AdminAction {
    #[strum(serialize = "admin:Heal")]
    HealAdminAction,
    #[strum(serialize = "admin:Decommission")]
    DecommissionAdminAction,
    #[strum(serialize = "admin:Rebalance")]
    RebalanceAdminAction,
    #[strum(serialize = "admin:StorageInfo")]
    StorageInfoAdminAction,
    #[strum(serialize = "admin:Prometheus")]
    PrometheusAdminAction,
    #[strum(serialize = "admin:DataUsageInfo")]
    DataUsageInfoAdminAction,
    #[strum(serialize = "admin:ForceUnlock")]
    ForceUnlockAdminAction,
    #[strum(serialize = "admin:TopLocksInfo")]
    TopLocksAdminAction,
    #[strum(serialize = "admin:Profiling")]
    ProfilingAdminAction,
    #[strum(serialize = "admin:ServerTrace")]
    TraceAdminAction,
    #[strum(serialize = "admin:ConsoleLog")]
    ConsoleLogAdminAction,
    #[strum(serialize = "admin:KMSCreateKey")]
    KMSCreateKeyAdminAction,
    #[strum(serialize = "admin:KMSKeyStatus")]
    KMSKeyStatusAdminAction,
    #[strum(serialize = "admin:ServerInfo")]
    ServerInfoAdminAction,
    #[strum(serialize = "admin:OBDInfo")]
    HealthInfoAdminAction,
    #[strum(serialize = "admin:LicenseInfo")]
    LicenseInfoAdminAction,
    #[strum(serialize = "admin:BandwidthMonitor")]
    BandwidthMonitorAction,
    #[strum(serialize = "admin:InspectData")]
    InspectDataAction,
    #[strum(serialize = "admin:ServerUpdate")]
    ServerUpdateAdminAction,
    #[strum(serialize = "admin:ServiceRestart")]
    ServiceRestartAdminAction,
    #[strum(serialize = "admin:ServiceStop")]
    ServiceStopAdminAction,
    #[strum(serialize = "admin:ServiceFreeze")]
    ServiceFreezeAdminAction,
    #[strum(serialize = "admin:ConfigUpdate")]
    ConfigUpdateAdminAction,
    #[strum(serialize = "admin:CreateUser")]
    CreateUserAdminAction,
    #[strum(serialize = "admin:DeleteUser")]
    DeleteUserAdminAction,
    #[strum(serialize = "admin:ListUsers")]
    ListUsersAdminAction,
    #[strum(serialize = "admin:EnableUser")]
    EnableUserAdminAction,
    #[strum(serialize = "admin:DisableUser")]
    DisableUserAdminAction,
    #[strum(serialize = "admin:GetUser")]
    GetUserAdminAction,
    #[strum(serialize = "admin:SiteReplicationAdd")]
    SiteReplicationAddAction,
    #[strum(serialize = "admin:SiteReplicationDisable")]
    SiteReplicationDisableAction,
    #[strum(serialize = "admin:SiteReplicationRemove")]
    SiteReplicationRemoveAction,
    #[strum(serialize = "admin:SiteReplicationResync")]
    SiteReplicationResyncAction,
    #[strum(serialize = "admin:SiteReplicationInfo")]
    SiteReplicationInfoAction,
    #[strum(serialize = "admin:SiteReplicationOperation")]
    SiteReplicationOperationAction,
    #[strum(serialize = "admin:CreateServiceAccount")]
    CreateServiceAccountAdminAction,
    #[strum(serialize = "admin:UpdateServiceAccount")]
    UpdateServiceAccountAdminAction,
    #[strum(serialize = "admin:RemoveServiceAccount")]
    RemoveServiceAccountAdminAction,
    #[strum(serialize = "admin:ListServiceAccounts")]
    ListServiceAccountsAdminAction,
    #[strum(serialize = "admin:ListTemporaryAccounts")]
    ListTemporaryAccountsAdminAction,
    #[strum(serialize = "admin:AddUserToGroup")]
    AddUserToGroupAdminAction,
    #[strum(serialize = "admin:RemoveUserFromGroup")]
    RemoveUserFromGroupAdminAction,
    #[strum(serialize = "admin:GetGroup")]
    GetGroupAdminAction,
    #[strum(serialize = "admin:ListGroups")]
    ListGroupsAdminAction,
    #[strum(serialize = "admin:EnableGroup")]
    EnableGroupAdminAction,
    #[strum(serialize = "admin:DisableGroup")]
    DisableGroupAdminAction,
    #[strum(serialize = "admin:CreatePolicy")]
    CreatePolicyAdminAction,
    #[strum(serialize = "admin:DeletePolicy")]
    DeletePolicyAdminAction,
    #[strum(serialize = "admin:GetPolicy")]
    GetPolicyAdminAction,
    #[strum(serialize = "admin:AttachUserOrGroupPolicy")]
    AttachPolicyAdminAction,
    #[strum(serialize = "admin:UpdatePolicyAssociation")]
    UpdatePolicyAssociationAction,
    #[strum(serialize = "admin:ListUserPolicies")]
    ListUserPoliciesAdminAction,
    #[strum(serialize = "admin:SetBucketQuota")]
    SetBucketQuotaAdminAction,
    #[strum(serialize = "admin:GetBucketQuota")]
    GetBucketQuotaAdminAction,
    #[strum(serialize = "admin:SetBucketTarget")]
    SetBucketTargetAction,
    #[strum(serialize = "admin:GetBucketTarget")]
    GetBucketTargetAction,
    #[strum(serialize = "admin:ReplicationDiff")]
    ReplicationDiff,
    #[strum(serialize = "admin:ImportBucketMetadata")]
    ImportBucketMetadataAction,
    #[strum(serialize = "admin:ExportBucketMetadata")]
    ExportBucketMetadataAction,
    #[strum(serialize = "admin:SetTier")]
    SetTierAction,
    #[strum(serialize = "admin:ListTier")]
    ListTierAction,
    #[strum(serialize = "admin:ExportIAM")]
    ExportIAMAction,
    #[strum(serialize = "admin:ImportIAM")]
    ImportIAMAction,
    #[strum(serialize = "admin:ListBatchJobs")]
    ListBatchJobsAction,
    #[strum(serialize = "admin:DescribeBatchJob")]
    DescribeBatchJobAction,
    #[strum(serialize = "admin:StartBatchJob")]
    StartBatchJobAction,
    #[strum(serialize = "admin:CancelBatchJob")]
    CancelBatchJobAction,
    #[strum(serialize = "admin:*")]
    AllAdminActions,
}

impl AdminAction {
    // IsValid - checks if action is valid or not.
    pub fn is_valid(&self) -> bool {
        matches!(
            self,
            AdminAction::HealAdminAction
                | AdminAction::DecommissionAdminAction
                | AdminAction::RebalanceAdminAction
                | AdminAction::StorageInfoAdminAction
                | AdminAction::PrometheusAdminAction
                | AdminAction::DataUsageInfoAdminAction
                | AdminAction::ForceUnlockAdminAction
                | AdminAction::TopLocksAdminAction
                | AdminAction::ProfilingAdminAction
                | AdminAction::TraceAdminAction
                | AdminAction::ConsoleLogAdminAction
                | AdminAction::KMSCreateKeyAdminAction
                | AdminAction::KMSKeyStatusAdminAction
                | AdminAction::ServerInfoAdminAction
                | AdminAction::HealthInfoAdminAction
                | AdminAction::LicenseInfoAdminAction
                | AdminAction::BandwidthMonitorAction
                | AdminAction::InspectDataAction
                | AdminAction::ServerUpdateAdminAction
                | AdminAction::ServiceRestartAdminAction
                | AdminAction::ServiceStopAdminAction
                | AdminAction::ServiceFreezeAdminAction
                | AdminAction::ConfigUpdateAdminAction
                | AdminAction::CreateUserAdminAction
                | AdminAction::DeleteUserAdminAction
                | AdminAction::ListUsersAdminAction
                | AdminAction::EnableUserAdminAction
                | AdminAction::DisableUserAdminAction
                | AdminAction::GetUserAdminAction
                | AdminAction::SiteReplicationAddAction
                | AdminAction::SiteReplicationDisableAction
                | AdminAction::SiteReplicationRemoveAction
                | AdminAction::SiteReplicationResyncAction
                | AdminAction::SiteReplicationInfoAction
                | AdminAction::SiteReplicationOperationAction
                | AdminAction::CreateServiceAccountAdminAction
                | AdminAction::UpdateServiceAccountAdminAction
                | AdminAction::RemoveServiceAccountAdminAction
                | AdminAction::ListServiceAccountsAdminAction
                | AdminAction::ListTemporaryAccountsAdminAction
                | AdminAction::AddUserToGroupAdminAction
                | AdminAction::RemoveUserFromGroupAdminAction
                | AdminAction::GetGroupAdminAction
                | AdminAction::ListGroupsAdminAction
                | AdminAction::EnableGroupAdminAction
                | AdminAction::DisableGroupAdminAction
                | AdminAction::CreatePolicyAdminAction
                | AdminAction::DeletePolicyAdminAction
                | AdminAction::GetPolicyAdminAction
                | AdminAction::AttachPolicyAdminAction
                | AdminAction::UpdatePolicyAssociationAction
                | AdminAction::ListUserPoliciesAdminAction
                | AdminAction::SetBucketQuotaAdminAction
                | AdminAction::GetBucketQuotaAdminAction
                | AdminAction::SetBucketTargetAction
                | AdminAction::GetBucketTargetAction
                | AdminAction::ReplicationDiff
                | AdminAction::ImportBucketMetadataAction
                | AdminAction::ExportBucketMetadataAction
                | AdminAction::SetTierAction
                | AdminAction::ListTierAction
                | AdminAction::ExportIAMAction
                | AdminAction::ImportIAMAction
                | AdminAction::ListBatchJobsAction
                | AdminAction::DescribeBatchJobAction
                | AdminAction::StartBatchJobAction
                | AdminAction::CancelBatchJobAction
                | AdminAction::AllAdminActions
        )
    }
}

#[derive(Serialize, Deserialize, Hash, PartialEq, Eq, Clone, EnumString, IntoStaticStr, Debug, Copy)]
#[serde(try_from = "&str", into = "&str")]
pub enum StsAction {}

#[derive(Serialize, Deserialize, Hash, PartialEq, Eq, Clone, EnumString, IntoStaticStr, Debug, Copy)]
#[serde(try_from = "&str", into = "&str")]
pub enum KmsAction {
    #[strum(serialize = "kms:*")]
    AllActions,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn test_action_wildcard_parsing() {
        // Test that "*" parses to S3Action::AllActions
        let action = Action::try_from("*").expect("Should parse wildcard");
        assert!(matches!(action, Action::S3Action(S3Action::AllActions)));
    }

    #[test]
    fn test_actionset_serialize_single_element() {
        // Single element should serialize as array for S3 specification compliance
        let mut set = HashSet::new();
        set.insert(Action::S3Action(S3Action::GetObjectAction));
        let actionset = ActionSet(set);

        let json = serde_json::to_string(&actionset).expect("Should serialize");
        let parsed: serde_json::Value = serde_json::from_str(&json).expect("Should parse");
        assert!(parsed.is_array(), "Should serialize as array");
        let arr = parsed.as_array().expect("Should be array");
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0].as_str().unwrap(), "s3:GetObject");
    }

    #[test]
    fn test_actionset_serialize_multiple_elements() {
        // Multiple elements should serialize as array
        let mut set = HashSet::new();
        set.insert(Action::S3Action(S3Action::GetObjectAction));
        set.insert(Action::S3Action(S3Action::PutObjectAction));
        let actionset = ActionSet(set);

        let json = serde_json::to_string(&actionset).expect("Should serialize");
        let parsed: serde_json::Value = serde_json::from_str(&json).expect("Should parse");
        assert!(parsed.is_array());
        let arr = parsed.as_array().expect("Should be array");
        assert_eq!(arr.len(), 2);
    }

    #[test]
    fn test_actionset_wildcard_serialization() {
        // Wildcard action should serialize as array for S3 specification compliance
        let mut set = HashSet::new();
        set.insert(Action::try_from("*").expect("Should parse wildcard"));
        let actionset = ActionSet(set);

        let json = serde_json::to_string(&actionset).expect("Should serialize");
        let parsed: serde_json::Value = serde_json::from_str(&json).expect("Should parse");
        assert!(parsed.is_array(), "Should serialize as array");
        let arr = parsed.as_array().expect("Should be array");
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0].as_str().unwrap(), "s3:*");
    }
}
