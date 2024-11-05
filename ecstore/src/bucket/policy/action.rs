use crate::{bucket::policy::condition::keyname::ALL_SUPPORT_KEYS, utils};
use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    str::FromStr,
    vec,
};

use super::condition::{
    key::{Key, KeySet},
    keyname::{KeyName, COMMOM_KEYS},
};

#[derive(Debug, Deserialize, Serialize, Default, Clone, PartialEq, Eq)]

pub struct ActionSet(HashSet<Action>);

impl ActionSet {
    pub fn is_match(&self, act: &Action) -> bool {
        for item in self.0.iter() {
            if item.is_match(act) {
                return true;
            }

            if item == &Action::GetObjectVersion && act == &Action::GetObjectVersion {
                return true;
            }
        }

        false
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl AsRef<HashSet<Action>> for ActionSet {
    fn as_ref(&self) -> &HashSet<Action> {
        &self.0
    }
}

// TODO:: 使用字符串
// 定义Action枚举类型
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, Default, Hash)]
pub enum Action {
    #[serde(rename = "s3:AbortMultipartUpload")]
    AbortMultipartUpload,
    #[serde(rename = "s3:CreateBucket")]
    CreateBucket,
    #[serde(rename = "s3:DeleteBucket")]
    DeleteBucket,
    #[serde(rename = "s3:ForceDeleteBucket")]
    ForceDeleteBucket,
    #[serde(rename = "s3:DeleteBucketPolicy")]
    DeleteBucketPolicy,
    #[serde(rename = "s3:DeleteBucketCors")]
    DeleteBucketCors,
    #[serde(rename = "s3:DeleteObject")]
    DeleteObject,
    #[serde(rename = "s3:GetBucketLocation")]
    GetBucketLocation,
    #[serde(rename = "s3:GetBucketNotification")]
    GetBucketNotification,
    #[serde(rename = "s3:GetBucketPolicy")]
    GetBucketPolicy,
    #[serde(rename = "s3:GetBucketCors")]
    GetBucketCors,
    #[serde(rename = "s3:GetObject")]
    GetObject,
    #[serde(rename = "s3:GetObjectAttributes")]
    GetObjectAttributes,
    #[serde(rename = "s3:HeadBucket")]
    HeadBucket,
    #[serde(rename = "s3:ListAllMyBuckets")]
    ListAllMyBuckets,
    #[serde(rename = "s3:ListBucket")]
    ListBucket,
    #[serde(rename = "s3:GetBucketPolicyStatus")]
    GetBucketPolicyStatus,
    #[serde(rename = "s3:ListBucketVersions")]
    ListBucketVersions,
    #[serde(rename = "s3:ListBucketMultipartUploads")]
    ListBucketMultipartUploads,
    #[serde(rename = "s3:ListenNotification")]
    ListenNotification,
    #[serde(rename = "s3:ListenBucketNotification")]
    ListenBucketNotification,
    #[serde(rename = "s3:ListMultipartUploadParts")]
    ListMultipartUploadParts,
    #[serde(rename = "s3:PutLifecycleConfiguration")]
    PutLifecycleConfiguration,
    #[serde(rename = "s3:GetLifecycleConfiguration")]
    GetLifecycleConfiguration,
    #[serde(rename = "s3:PutBucketNotification")]
    PutBucketNotification,
    #[serde(rename = "s3:PutBucketPolicy")]
    PutBucketPolicy,
    #[serde(rename = "s3:PutBucketCors")]
    PutBucketCors,
    #[serde(rename = "s3:PutObject")]
    PutObject,
    #[serde(rename = "s3:DeleteObjectVersion")]
    DeleteObjectVersion,
    #[serde(rename = "s3:DeleteObjectVersionTagging")]
    DeleteObjectVersionTagging,
    #[serde(rename = "s3:GetObjectVersion")]
    GetObjectVersion,
    #[serde(rename = "s3:GetObjectVersionAttributes")]
    GetObjectVersionAttributes,
    #[serde(rename = "s3:GetObjectVersionTagging")]
    GetObjectVersionTagging,
    #[serde(rename = "s3:PutObjectVersionTagging")]
    PutObjectVersionTagging,
    #[serde(rename = "s3:BypassGovernanceRetention")]
    BypassGovernanceRetention,
    #[serde(rename = "s3:PutObjectRetention")]
    PutObjectRetention,
    #[serde(rename = "s3:GetObjectRetention")]
    GetObjectRetention,
    #[serde(rename = "s3:GetObjectLegalHold")]
    GetObjectLegalHold,
    #[serde(rename = "s3:PutObjectLegalHold")]
    PutObjectLegalHold,
    #[serde(rename = "s3:GetBucketObjectLockConfiguration")]
    GetBucketObjectLockConfiguration,
    #[serde(rename = "s3:PutBucketObjectLockConfiguration")]
    PutBucketObjectLockConfiguration,
    #[serde(rename = "s3:GetBucketTagging")]
    GetBucketTagging,
    #[serde(rename = "s3:PutBucketTagging")]
    PutBucketTagging,
    #[serde(rename = "s3:GetObjectTagging")]
    GetObjectTagging,
    #[serde(rename = "s3:PutObjectTagging")]
    PutObjectTagging,
    #[serde(rename = "s3:DeleteObjectTagging")]
    DeleteObjectTagging,
    #[serde(rename = "s3:PutBucketEncryption")]
    PutBucketEncryption,
    #[serde(rename = "s3:GetBucketEncryption")]
    GetBucketEncryption,
    #[serde(rename = "s3:PutBucketVersioning")]
    PutBucketVersioning,
    #[serde(rename = "s3:GetBucketVersioning")]
    GetBucketVersioning,
    #[serde(rename = "s3:PutReplicationConfiguration")]
    PutReplicationConfiguration,
    #[serde(rename = "s3:GetReplicationConfiguration")]
    GetReplicationConfiguration,
    #[serde(rename = "s3:ReplicateObject")]
    ReplicateObject,
    #[serde(rename = "s3:ReplicateDelete")]
    ReplicateDelete,
    #[serde(rename = "s3:ReplicateTags")]
    ReplicateTags,
    #[serde(rename = "s3:GetObjectVersionForReplication")]
    GetObjectVersionForReplication,
    #[serde(rename = "s3:RestoreObject")]
    RestoreObject,
    #[serde(rename = "s3:ResetBucketReplicationState")]
    ResetBucketReplicationState,
    #[serde(rename = "s3:PutObjectFanOut")]
    PutObjectFanOut,
    #[default]
    #[serde(rename = "s3:*")]
    AllActions,
}

lazy_static! {
    #[derive(Debug)]
    static ref SUPPORT_OBJCET_ACTIONS: HashSet<Action> = {
        let mut h = HashSet::new();
        h.insert(Action::AllActions);
        h.insert(Action::AbortMultipartUpload);
        h.insert(Action::DeleteObject);
        h.insert(Action::GetObject);
        h.insert(Action::ListMultipartUploadParts);
        h.insert(Action::PutObject);
        h.insert(Action::BypassGovernanceRetention);
        h.insert(Action::PutObjectRetention);
        h.insert(Action::GetObjectRetention);
        h.insert(Action::PutObjectLegalHold);
        h.insert(Action::GetObjectLegalHold);
        h.insert(Action::GetObjectTagging);
        h.insert(Action::PutObjectTagging);
        h.insert(Action::DeleteObjectTagging);
        h.insert(Action::GetObjectVersion);
        h.insert(Action::GetObjectVersionTagging);
        h.insert(Action::DeleteObjectVersion);
        h.insert(Action::DeleteObjectVersionTagging);
        h.insert(Action::PutObjectVersionTagging);
        h.insert(Action::ReplicateObject);
        h.insert(Action::ReplicateDelete);
        h.insert(Action::ReplicateTags);
        h.insert(Action::GetObjectVersionForReplication);
        h.insert(Action::RestoreObject);
        h.insert(Action::ResetBucketReplicationState);
        h.insert(Action::PutObjectFanOut);
        h.insert(Action::GetObjectAttributes);
        h.insert(Action::GetObjectVersionAttributes);
        h
    };
}

impl Action {
    pub fn is_object_action(&self) -> bool {
        for act in SUPPORT_OBJCET_ACTIONS.iter() {
            if self.is_match(act) {
                return true;
            }
        }
        false
    }
    pub fn is_match(&self, a: &Action) -> bool {
        utils::wildcard::match_pattern(self.clone().as_str(), a.clone().as_str())
    }

    fn as_str(&self) -> &'static str {
        match self {
            Action::AbortMultipartUpload => "s3:AbortMultipartUpload",
            Action::CreateBucket => "s3:CreateBucket",
            Action::DeleteBucket => "s3:DeleteBucket",
            Action::ForceDeleteBucket => "s3:ForceDeleteBucket",
            Action::DeleteBucketPolicy => "s3:DeleteBucketPolicy",
            Action::DeleteBucketCors => "s3:DeleteBucketCors",
            Action::DeleteObject => "s3:DeleteObject",
            Action::GetBucketLocation => "s3:GetBucketLocation",
            Action::GetBucketNotification => "s3:GetBucketNotification",
            Action::GetBucketPolicy => "s3:GetBucketPolicy",
            Action::GetBucketCors => "s3:GetBucketCors",
            Action::GetObject => "s3:GetObject",
            Action::GetObjectAttributes => "s3:GetObjectAttributes",
            Action::HeadBucket => "s3:HeadBucket",
            Action::ListAllMyBuckets => "s3:ListAllMyBuckets",
            Action::ListBucket => "s3:ListBucket",
            Action::GetBucketPolicyStatus => "s3:GetBucketPolicyStatus",
            Action::ListBucketVersions => "s3:ListBucketVersions",
            Action::ListBucketMultipartUploads => "s3:ListBucketMultipartUploads",
            Action::ListenNotification => "s3:ListenNotification",
            Action::ListenBucketNotification => "s3:ListenBucketNotification",
            Action::ListMultipartUploadParts => "s3:ListMultipartUploadParts",
            Action::PutLifecycleConfiguration => "s3:PutLifecycleConfiguration",
            Action::GetLifecycleConfiguration => "s3:GetLifecycleConfiguration",
            Action::PutBucketNotification => "s3:PutBucketNotification",
            Action::PutBucketPolicy => "s3:PutBucketPolicy",
            Action::PutBucketCors => "s3:PutBucketCors",
            Action::PutObject => "s3:PutObject",
            Action::DeleteObjectVersion => "s3:DeleteObjectVersion",
            Action::DeleteObjectVersionTagging => "s3:DeleteObjectVersionTagging",
            Action::GetObjectVersion => "s3:GetObjectVersion",
            Action::GetObjectVersionAttributes => "s3:GetObjectVersionAttributes",
            Action::GetObjectVersionTagging => "s3:GetObjectVersionTagging",
            Action::PutObjectVersionTagging => "s3:PutObjectVersionTagging",
            Action::BypassGovernanceRetention => "s3:BypassGovernanceRetention",
            Action::PutObjectRetention => "s3:PutObjectRetention",
            Action::GetObjectRetention => "s3:GetObjectRetention",
            Action::GetObjectLegalHold => "s3:GetObjectLegalHold",
            Action::PutObjectLegalHold => "s3:PutObjectLegalHold",
            Action::GetBucketObjectLockConfiguration => "s3:GetBucketObjectLockConfiguration",
            Action::PutBucketObjectLockConfiguration => "s3:PutBucketObjectLockConfiguration",
            Action::GetBucketTagging => "s3:GetBucketTagging",
            Action::PutBucketTagging => "s3:PutBucketTagging",
            Action::GetObjectTagging => "s3:GetObjectTagging",
            Action::PutObjectTagging => "s3:PutObjectTagging",
            Action::DeleteObjectTagging => "s3:DeleteObjectTagging",
            Action::PutBucketEncryption => "s3:PutEncryptionConfiguration",
            Action::GetBucketEncryption => "s3:GetEncryptionConfiguration",
            Action::PutBucketVersioning => "s3:PutBucketVersioning",
            Action::GetBucketVersioning => "s3:GetBucketVersioning",
            Action::PutReplicationConfiguration => "s3:GetReplicationConfiguration",
            Action::GetReplicationConfiguration => "s3:PutReplicationConfiguration",
            Action::ReplicateObject => "s3:ReplicateObject",
            Action::ReplicateDelete => "s3:ReplicateDelete",
            Action::ReplicateTags => "s3:ReplicateTags",
            Action::GetObjectVersionForReplication => "s3:GetObjectVersionForReplication",
            Action::RestoreObject => "s3:RestoreObject",
            Action::ResetBucketReplicationState => "s3:ResetBucketReplicationState",
            Action::PutObjectFanOut => "s3:PutObjectFanOut",
            Action::AllActions => "s3:*",
        }
    }

    // pub fn from_str(s: &str) -> Option<Self> {
    //     match s {
    //         "s3:AbortMultipartUpload" => Some(Action::AbortMultipartUpload),
    //         "s3:CreateBucket" => Some(Action::CreateBucket),
    //         "s3:DeleteBucket" => Some(Action::DeleteBucket),
    //         "s3:ForceDeleteBucket" => Some(Action::ForceDeleteBucket),
    //         "s3:DeleteBucketPolicy" => Some(Action::DeleteBucketPolicy),
    //         "s3:DeleteBucketCors" => Some(Action::DeleteBucketCors),
    //         "s3:DeleteObject" => Some(Action::DeleteObject),
    //         "s3:GetBucketLocation" => Some(Action::GetBucketLocation),
    //         "s3:GetBucketNotification" => Some(Action::GetBucketNotification),
    //         "s3:GetBucketPolicy" => Some(Action::GetBucketPolicy),
    //         "s3:GetBucketCors" => Some(Action::GetBucketCors),
    //         "s3:GetObject" => Some(Action::GetObject),
    //         "s3:GetObjectAttributes" => Some(Action::GetObjectAttributes),
    //         "s3:HeadBucket" => Some(Action::HeadBucket),
    //         "s3:ListAllMyBuckets" => Some(Action::ListAllMyBuckets),
    //         "s3:ListBucket" => Some(Action::ListBucket),
    //         "s3:GetBucketPolicyStatus" => Some(Action::GetBucketPolicyStatus),
    //         "s3:ListBucketVersions" => Some(Action::ListBucketVersions),
    //         "s3:ListBucketMultipartUploads" => Some(Action::ListBucketMultipartUploads),
    //         "s3:ListenNotification" => Some(Action::ListenNotification),
    //         "s3:ListenBucketNotification" => Some(Action::ListenBucketNotification),
    //         "s3:ListMultipartUploadParts" => Some(Action::ListMultipartUploadParts),
    //         "s3:PutLifecycleConfiguration" => Some(Action::PutLifecycleConfiguration),
    //         "s3:GetLifecycleConfiguration" => Some(Action::GetLifecycleConfiguration),
    //         "s3:PutBucketNotification" => Some(Action::PutBucketNotification),
    //         "s3:PutBucketPolicy" => Some(Action::PutBucketPolicy),
    //         "s3:PutBucketCors" => Some(Action::PutBucketCors),
    //         "s3:PutObject" => Some(Action::PutObject),
    //         "s3:DeleteObjectVersion" => Some(Action::DeleteObjectVersion),
    //         "s3:DeleteObjectVersionTagging" => Some(Action::DeleteObjectVersionTagging),
    //         "s3:GetObjectVersion" => Some(Action::GetObjectVersion),
    //         "s3:GetObjectVersionAttributes" => Some(Action::GetObjectVersionAttributes),
    //         "s3:GetObjectVersionTagging" => Some(Action::GetObjectVersionTagging),
    //         "s3:PutObjectVersionTagging" => Some(Action::PutObjectVersionTagging),
    //         "s3:BypassGovernanceRetention" => Some(Action::BypassGovernanceRetention),
    //         "s3:PutObjectRetention" => Some(Action::PutObjectRetention),
    //         "s3:GetObjectRetention" => Some(Action::GetObjectRetention),
    //         "s3:GetObjectLegalHold" => Some(Action::GetObjectLegalHold),
    //         "s3:PutObjectLegalHold" => Some(Action::PutObjectLegalHold),
    //         "s3:GetBucketObjectLockConfiguration" => Some(Action::GetBucketObjectLockConfiguration),
    //         "s3:PutBucketObjectLockConfiguration" => Some(Action::PutBucketObjectLockConfiguration),
    //         "s3:GetBucketTagging" => Some(Action::GetBucketTagging),
    //         "s3:PutBucketTagging" => Some(Action::PutBucketTagging),
    //         "s3:GetObjectTagging" => Some(Action::GetObjectTagging),
    //         "s3:PutObjectTagging" => Some(Action::PutObjectTagging),
    //         "s3:DeleteObjectTagging" => Some(Action::DeleteObjectTagging),
    //         "s3:PutEncryptionConfiguration" => Some(Action::PutBucketEncryption),
    //         "s3:GetEncryptionConfiguration" => Some(Action::GetBucketEncryption),
    //         "s3:PutBucketVersioning" => Some(Action::PutBucketVersioning),
    //         "s3:GetBucketVersioning" => Some(Action::GetBucketVersioning),
    //         "s3:PutReplicationConfiguration" => Some(Action::PutReplicationConfiguration),
    //         "s3:GetReplicationConfiguration" => Some(Action::GetReplicationConfiguration),
    //         "s3:ReplicateObject" => Some(Action::ReplicateObject),
    //         "s3:ReplicateDelete" => Some(Action::ReplicateDelete),
    //         "s3:ReplicateTags" => Some(Action::ReplicateTags),
    //         "s3:GetObjectVersionForReplication" => Some(Action::GetObjectVersionForReplication),
    //         "s3:RestoreObject" => Some(Action::RestoreObject),
    //         "s3:ResetBucketReplicationState" => Some(Action::ResetBucketReplicationState),
    //         "s3:PutObjectFanOut" => Some(Action::PutObjectFanOut),
    //         "s3:*" => Some(Action::AllActions),
    //         _ => None,
    //     }
    // }
}

impl FromStr for Action {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "s3:AbortMultipartUpload" => Ok(Action::AbortMultipartUpload),
            "s3:CreateBucket" => Ok(Action::CreateBucket),
            "s3:DeleteBucket" => Ok(Action::DeleteBucket),
            "s3:ForceDeleteBucket" => Ok(Action::ForceDeleteBucket),
            "s3:DeleteBucketPolicy" => Ok(Action::DeleteBucketPolicy),
            "s3:DeleteBucketCors" => Ok(Action::DeleteBucketCors),
            "s3:DeleteObject" => Ok(Action::DeleteObject),
            "s3:GetBucketLocation" => Ok(Action::GetBucketLocation),
            "s3:GetBucketNotification" => Ok(Action::GetBucketNotification),
            "s3:GetBucketPolicy" => Ok(Action::GetBucketPolicy),
            "s3:GetBucketCors" => Ok(Action::GetBucketCors),
            "s3:GetObject" => Ok(Action::GetObject),
            "s3:GetObjectAttributes" => Ok(Action::GetObjectAttributes),
            "s3:HeadBucket" => Ok(Action::HeadBucket),
            "s3:ListAllMyBuckets" => Ok(Action::ListAllMyBuckets),
            "s3:ListBucket" => Ok(Action::ListBucket),
            "s3:GetBucketPolicyStatus" => Ok(Action::GetBucketPolicyStatus),
            "s3:ListBucketVersions" => Ok(Action::ListBucketVersions),
            "s3:ListBucketMultipartUploads" => Ok(Action::ListBucketMultipartUploads),
            "s3:ListenNotification" => Ok(Action::ListenNotification),
            "s3:ListenBucketNotification" => Ok(Action::ListenBucketNotification),
            "s3:ListMultipartUploadParts" => Ok(Action::ListMultipartUploadParts),
            "s3:PutLifecycleConfiguration" => Ok(Action::PutLifecycleConfiguration),
            "s3:GetLifecycleConfiguration" => Ok(Action::GetLifecycleConfiguration),
            "s3:PutBucketNotification" => Ok(Action::PutBucketNotification),
            "s3:PutBucketPolicy" => Ok(Action::PutBucketPolicy),
            "s3:PutBucketCors" => Ok(Action::PutBucketCors),
            "s3:PutObject" => Ok(Action::PutObject),
            "s3:DeleteObjectVersion" => Ok(Action::DeleteObjectVersion),
            "s3:DeleteObjectVersionTagging" => Ok(Action::DeleteObjectVersionTagging),
            "s3:GetObjectVersion" => Ok(Action::GetObjectVersion),
            "s3:GetObjectVersionAttributes" => Ok(Action::GetObjectVersionAttributes),
            "s3:GetObjectVersionTagging" => Ok(Action::GetObjectVersionTagging),
            "s3:PutObjectVersionTagging" => Ok(Action::PutObjectVersionTagging),
            "s3:BypassGovernanceRetention" => Ok(Action::BypassGovernanceRetention),
            "s3:PutObjectRetention" => Ok(Action::PutObjectRetention),
            "s3:GetObjectRetention" => Ok(Action::GetObjectRetention),
            "s3:GetObjectLegalHold" => Ok(Action::GetObjectLegalHold),
            "s3:PutObjectLegalHold" => Ok(Action::PutObjectLegalHold),
            "s3:GetBucketObjectLockConfiguration" => Ok(Action::GetBucketObjectLockConfiguration),
            "s3:PutBucketObjectLockConfiguration" => Ok(Action::PutBucketObjectLockConfiguration),
            "s3:GetBucketTagging" => Ok(Action::GetBucketTagging),
            "s3:PutBucketTagging" => Ok(Action::PutBucketTagging),
            "s3:GetObjectTagging" => Ok(Action::GetObjectTagging),
            "s3:PutObjectTagging" => Ok(Action::PutObjectTagging),
            "s3:DeleteObjectTagging" => Ok(Action::DeleteObjectTagging),
            "s3:PutEncryptionConfiguration" => Ok(Action::PutBucketEncryption),
            "s3:GetEncryptionConfiguration" => Ok(Action::GetBucketEncryption),
            "s3:PutBucketVersioning" => Ok(Action::PutBucketVersioning),
            "s3:GetBucketVersioning" => Ok(Action::GetBucketVersioning),
            "s3:PutReplicationConfiguration" => Ok(Action::PutReplicationConfiguration),
            "s3:GetReplicationConfiguration" => Ok(Action::GetReplicationConfiguration),
            "s3:ReplicateObject" => Ok(Action::ReplicateObject),
            "s3:ReplicateDelete" => Ok(Action::ReplicateDelete),
            "s3:ReplicateTags" => Ok(Action::ReplicateTags),
            "s3:GetObjectVersionForReplication" => Ok(Action::GetObjectVersionForReplication),
            "s3:RestoreObject" => Ok(Action::RestoreObject),
            "s3:ResetBucketReplicationState" => Ok(Action::ResetBucketReplicationState),
            "s3:PutObjectFanOut" => Ok(Action::PutObjectFanOut),
            "s3:*" => Ok(Action::AllActions),
            _ => Err(()),
        }
    }
}

pub struct ActionConditionKeyMap(HashMap<Action, KeySet>);

impl ActionConditionKeyMap {
    pub fn lookup(&self, action: &Action) -> KeySet {
        let common_keys: Vec<Key> = COMMOM_KEYS.iter().map(|v| v.to_key()).collect();

        let mut merged_keys = KeySet::from_keys(&common_keys);

        for (act, key) in self.0.iter() {
            if action.is_match(act) {
                merged_keys.merge(key);
            }
        }

        merged_keys
    }
}

lazy_static! {
    pub static ref IAMActionConditionKeyMap: ActionConditionKeyMap = create_action_condition_key_map();
}

fn create_action_condition_key_map() -> ActionConditionKeyMap {
    let common_keys: Vec<Key> = COMMOM_KEYS.iter().map(|v| v.to_key()).collect();
    let all_support_keys: Vec<Key> = ALL_SUPPORT_KEYS.iter().map(|v| v.to_key()).collect();

    let mut map = HashMap::new();

    map.insert(Action::AllActions, KeySet::from_keys(&all_support_keys));
    map.insert(Action::AbortMultipartUpload, KeySet::from_keys(&common_keys));
    map.insert(Action::CreateBucket, KeySet::from_keys(&common_keys));

    let mut delete_obj_keys = common_keys.clone();
    delete_obj_keys.push(KeyName::S3VersionID.to_key());
    map.insert(Action::DeleteObject, KeySet::from_keys(&delete_obj_keys));

    map.insert(Action::GetBucketLocation, KeySet::from_keys(&common_keys));
    map.insert(Action::GetBucketPolicyStatus, KeySet::from_keys(&common_keys));

    let mut get_obj_keys = common_keys.clone();
    get_obj_keys.extend(vec![
        KeyName::S3XAmzServerSideEncryption.to_key(),
        KeyName::S3XAmzServerSideEncryptionCustomerAlgorithm.to_key(),
        KeyName::S3XAmzServerSideEncryptionAwsKmsKeyID.to_key(),
        KeyName::S3VersionID.to_key(),
        KeyName::ExistingObjectTag.to_key(),
    ]);
    map.insert(Action::DeleteObject, KeySet::from_keys(&get_obj_keys));

    map.insert(Action::HeadBucket, KeySet::from_keys(&common_keys));

    let mut get_obj_attr_keys = common_keys.clone();
    get_obj_attr_keys.push(KeyName::ExistingObjectTag.to_key());
    map.insert(Action::DeleteObject, KeySet::from_keys(&get_obj_attr_keys));

    let mut get_obj_ver_attr_keys = common_keys.clone();
    get_obj_ver_attr_keys.extend(vec![KeyName::S3VersionID.to_key(), KeyName::ExistingObjectTag.to_key()]);
    map.insert(Action::DeleteObject, KeySet::from_keys(&get_obj_ver_attr_keys));

    map.insert(Action::ListAllMyBuckets, KeySet::from_keys(&common_keys));

    let mut list_bucket_keys = common_keys.clone();
    list_bucket_keys.extend(vec![
        KeyName::S3Prefix.to_key(),
        KeyName::S3Delimiter.to_key(),
        KeyName::S3MaxKeys.to_key(),
    ]);
    map.insert(Action::ListBucket, KeySet::from_keys(&list_bucket_keys));
    map.insert(Action::ListBucketVersions, KeySet::from_keys(&list_bucket_keys));

    map.insert(Action::ListBucketMultipartUploads, KeySet::from_keys(&common_keys));
    map.insert(Action::ListenNotification, KeySet::from_keys(&common_keys));
    map.insert(Action::ListenBucketNotification, KeySet::from_keys(&common_keys));
    map.insert(Action::ListMultipartUploadParts, KeySet::from_keys(&common_keys));

    let mut put_obj_keys = common_keys.clone();
    put_obj_keys.extend(vec![
        KeyName::S3XAmzCopySource.to_key(),
        KeyName::S3XAmzServerSideEncryption.to_key(),
        KeyName::S3XAmzServerSideEncryptionCustomerAlgorithm.to_key(),
        KeyName::S3XAmzServerSideEncryptionAwsKmsKeyID.to_key(),
        KeyName::S3XAmzMetadataDirective.to_key(),
        KeyName::S3XAmzStorageClass.to_key(),
        KeyName::S3VersionID.to_key(),
        KeyName::S3ObjectLockRetainUntilDate.to_key(),
        KeyName::S3ObjectLockMode.to_key(),
        KeyName::S3ObjectLockLegalHold.to_key(),
        KeyName::RequestObjectTagKeys.to_key(),
        KeyName::RequestObjectTag.to_key(),
    ]);
    map.insert(Action::PutObject, KeySet::from_keys(&put_obj_keys));

    let mut put_obj_retention_keys = common_keys.clone();
    put_obj_retention_keys.extend(vec![
        KeyName::S3XAmzServerSideEncryption.to_key(),
        KeyName::S3XAmzServerSideEncryptionCustomerAlgorithm.to_key(),
        KeyName::S3XAmzServerSideEncryptionAwsKmsKeyID.to_key(),
        KeyName::S3ObjectLockRemainingRetentionDays.to_key(),
        KeyName::S3ObjectLockRetainUntilDate.to_key(),
        KeyName::S3ObjectLockMode.to_key(),
        KeyName::S3VersionID.to_key(),
    ]);
    map.insert(Action::PutObjectRetention, KeySet::from_keys(&put_obj_retention_keys));

    let mut get_obj_retention_keys = common_keys.clone();
    get_obj_retention_keys.extend(vec![
        KeyName::S3XAmzServerSideEncryption.to_key(),
        KeyName::S3XAmzServerSideEncryptionCustomerAlgorithm.to_key(),
        KeyName::S3XAmzServerSideEncryptionAwsKmsKeyID.to_key(),
        KeyName::S3VersionID.to_key(),
    ]);
    map.insert(Action::GetObjectRetention, KeySet::from_keys(&get_obj_retention_keys));

    let mut put_obj_hold_keys = common_keys.clone();
    put_obj_hold_keys.extend(vec![
        KeyName::S3XAmzServerSideEncryption.to_key(),
        KeyName::S3XAmzServerSideEncryptionCustomerAlgorithm.to_key(),
        KeyName::S3XAmzServerSideEncryptionAwsKmsKeyID.to_key(),
        KeyName::S3ObjectLockLegalHold.to_key(),
        KeyName::S3VersionID.to_key(),
    ]);
    map.insert(Action::PutObjectLegalHold, KeySet::from_keys(&put_obj_hold_keys));

    map.insert(Action::GetObjectLegalHold, KeySet::from_keys(&common_keys));

    let mut bypass_governance_retention_keys = common_keys.clone();
    bypass_governance_retention_keys.extend(vec![
        KeyName::S3VersionID.to_key(),
        KeyName::S3ObjectLockRemainingRetentionDays.to_key(),
        KeyName::S3ObjectLockRetainUntilDate.to_key(),
        KeyName::S3ObjectLockMode.to_key(),
        KeyName::S3ObjectLockLegalHold.to_key(),
        KeyName::RequestObjectTagKeys.to_key(),
        KeyName::RequestObjectTag.to_key(),
    ]);
    map.insert(Action::BypassGovernanceRetention, KeySet::from_keys(&bypass_governance_retention_keys));

    map.insert(Action::GetBucketObjectLockConfiguration, KeySet::from_keys(&common_keys));
    map.insert(Action::PutBucketObjectLockConfiguration, KeySet::from_keys(&common_keys));
    map.insert(Action::GetBucketTagging, KeySet::from_keys(&common_keys));

    let mut put_bucket_tagging_keys = common_keys.clone();
    put_bucket_tagging_keys.extend(vec![KeyName::RequestObjectTagKeys.to_key(), KeyName::RequestObjectTag.to_key()]);
    map.insert(Action::PutBucketTagging, KeySet::from_keys(&put_bucket_tagging_keys));

    let mut put_object_tagging_keys = common_keys.clone();
    put_object_tagging_keys.extend(vec![
        KeyName::S3VersionID.to_key(),
        KeyName::ExistingObjectTag.to_key(),
        KeyName::RequestObjectTagKeys.to_key(),
        KeyName::RequestObjectTag.to_key(),
    ]);
    map.insert(Action::PutObjectTagging, KeySet::from_keys(&put_object_tagging_keys));

    let mut get_object_tagging_keys = common_keys.clone();
    get_object_tagging_keys.extend(vec![KeyName::S3VersionID.to_key(), KeyName::ExistingObjectTag.to_key()]);
    map.insert(Action::GetObjectTagging, KeySet::from_keys(&get_object_tagging_keys));
    map.insert(Action::DeleteObjectTagging, KeySet::from_keys(&get_object_tagging_keys));

    map.insert(Action::PutObjectVersionTagging, KeySet::from_keys(&put_object_tagging_keys));
    map.insert(Action::GetObjectVersionTagging, KeySet::from_keys(&get_object_tagging_keys));
    map.insert(Action::GetObjectVersion, KeySet::from_keys(&get_object_tagging_keys));

    let mut delete_object_version_keys = common_keys.clone();
    delete_object_version_keys.extend(vec![KeyName::S3VersionID.to_key()]);

    map.insert(Action::DeleteObjectVersion, KeySet::from_keys(&delete_object_version_keys));
    map.insert(Action::DeleteObjectVersionTagging, KeySet::from_keys(&get_object_tagging_keys));

    map.insert(Action::GetReplicationConfiguration, KeySet::from_keys(&common_keys));
    map.insert(Action::PutReplicationConfiguration, KeySet::from_keys(&common_keys));

    map.insert(Action::ReplicateObject, KeySet::from_keys(&get_object_tagging_keys));
    map.insert(Action::ReplicateDelete, KeySet::from_keys(&get_object_tagging_keys));
    map.insert(Action::ReplicateTags, KeySet::from_keys(&get_object_tagging_keys));
    map.insert(Action::GetObjectVersionForReplication, KeySet::from_keys(&get_object_tagging_keys));

    map.insert(Action::RestoreObject, KeySet::from_keys(&common_keys));
    map.insert(Action::ResetBucketReplicationState, KeySet::from_keys(&common_keys));
    map.insert(Action::PutObjectFanOut, KeySet::from_keys(&common_keys));

    ActionConditionKeyMap(map)
}
