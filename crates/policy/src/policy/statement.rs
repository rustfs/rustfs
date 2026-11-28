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

use super::{
    ActionSet, Args, BucketPolicyArgs, Effect, Error as IamError, Functions, ID, Principal, ResourceSet, Validator,
    action::{Action, KmsAction, S3Action, S3VectorsAction},
    function::key_name::{AwsKeyName, KeyName, S3KeyName, S3VectorsKeyName},
    resource::{Resource, s3vectors_request_resource},
    variables::{PolicyVariableResolver, VariableContext, VariableResolver},
};
use crate::error::{Error, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Serialize, Deserialize, Clone, Default, Debug)]
#[serde(deny_unknown_fields)]
pub struct Statement {
    #[serde(rename = "Sid", default, skip_serializing_if = "ID::is_empty")]
    pub sid: ID,
    #[serde(rename = "Effect")]
    pub effect: Effect,
    #[serde(rename = "Action", default, skip_serializing_if = "ActionSet::is_empty")]
    pub actions: ActionSet,
    #[serde(rename = "NotAction", default, skip_serializing_if = "ActionSet::is_empty")]
    pub not_actions: ActionSet,
    #[serde(rename = "Resource", default, skip_serializing_if = "ResourceSet::is_empty")]
    pub resources: ResourceSet,
    #[serde(rename = "NotResource", default, skip_serializing_if = "ResourceSet::is_empty")]
    pub not_resources: ResourceSet,
    #[serde(rename = "Condition", default, skip_serializing_if = "Functions::is_empty")]
    pub conditions: Functions,
}

/// Builds the same [`VariableResolver`] as [`Statement::is_allowed`].
pub(crate) fn variable_resolver_for_policy_args(args: &Args<'_>) -> VariableResolver {
    let mut context = VariableContext::new();
    context.claims = Some(args.claims.clone());
    context.conditions = args.conditions.clone();
    context.account_id = Some(args.account.to_string());

    let username = if let Some(parent) = args.claims.get("parent").and_then(|v| v.as_str()) {
        parent.to_string()
    } else {
        args.account.to_string()
    };

    context.username = Some(username);

    VariableResolver::new(context)
}

fn build_resource(action: &Action, bucket: &str, object: &str, bucket_resource_only: bool) -> String {
    let bucket_resource_only = matches!(
        action,
        Action::S3Action(
            S3Action::ListBucketAction | S3Action::ListBucketVersionsAction | S3Action::ListBucketMultipartUploadsAction
        )
    ) && bucket_resource_only;

    let mut resource = String::from(bucket);
    if bucket_resource_only || object.is_empty() {
        resource.push('/');
        return resource;
    }

    if !object.starts_with('/') {
        resource.push('/');
    }
    resource.push_str(object);
    resource
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ActionFamily {
    S3,
    S3Vectors,
    Admin,
    Sts,
    Kms,
    Mixed,
}

fn requested_vector_resource(
    action: S3VectorsAction,
    bucket: &str,
    index: &str,
    conditions: &HashMap<String, Vec<String>>,
) -> Option<String> {
    use S3VectorsAction::*;
    let target_matches = match action {
        AllActions => false,
        ListVectorBucketsAction => bucket.is_empty() && index.is_empty(),
        CreateVectorBucketAction
        | DeleteVectorBucketAction
        | DeleteVectorBucketPolicyAction
        | GetVectorBucketAction
        | GetVectorBucketPolicyAction
        | ListIndexesAction
        | PutVectorBucketDefaultIndexModeAction
        | PutVectorBucketPolicyAction => !bucket.is_empty() && index.is_empty(),
        TagResourceAction | UntagResourceAction | ListTagsForResourceAction => !bucket.is_empty(),
        CreateIndexAction
        | DeleteIndexAction
        | DeleteVectorsAction
        | GetIndexAction
        | GetVectorsAction
        | ListVectorsAction
        | PutVectorsAction
        | QueryVectorsAction
        | UpdateIndexModeAction => !bucket.is_empty() && !index.is_empty(),
    };
    if !target_matches {
        return None;
    }
    let [region] = conditions.get("RequestedRegion")?.as_slice() else {
        return None;
    };
    s3vectors_request_resource(region, bucket, index)
}

/// Vector callers supply public names and a server-owned deployment region.
/// This check also runs before owner and deny-only shortcuts.
pub(crate) fn s3vectors_request_is_valid(
    action: &Action,
    bucket: &str,
    index: &str,
    conditions: &HashMap<String, Vec<String>>,
) -> bool {
    match action {
        Action::S3VectorsAction(action) => requested_vector_resource(*action, bucket, index, conditions).is_some(),
        _ => true,
    }
}

fn vector_condition_is_supported(action: S3VectorsAction, key: &KeyName) -> bool {
    use S3VectorsAction::*;
    match key {
        KeyName::S3(_) => false,
        KeyName::Aws(AwsKeyName::AWSResourceTag) | KeyName::S3Vectors(S3VectorsKeyName::VectorBucketTag) => {
            action != ListVectorBucketsAction
        }
        KeyName::Aws(AwsKeyName::AWSRequestTag) => {
            matches!(action, AllActions | CreateIndexAction | CreateVectorBucketAction | TagResourceAction)
        }
        KeyName::Aws(AwsKeyName::AWSTagKeys) => matches!(
            action,
            AllActions | CreateIndexAction | CreateVectorBucketAction | TagResourceAction | UntagResourceAction
        ),
        KeyName::S3Vectors(S3VectorsKeyName::KmsKeyArn | S3VectorsKeyName::SseType) => {
            matches!(action, AllActions | CreateIndexAction | CreateVectorBucketAction)
        }
        _ => true,
    }
}

fn validate_vector_statement(
    actions: &ActionSet,
    resources: &ResourceSet,
    not_resources: &ResourceSet,
    conditions: &Functions,
) -> Result<()> {
    let has_vector_action = actions.iter().any(|action| matches!(action, Action::S3VectorsAction(_)));
    let has_vector_resource = resources
        .iter()
        .chain(not_resources.iter())
        .any(|resource| matches!(resource, Resource::S3Vectors(_)));
    if has_vector_action && actions.iter().any(|action| !matches!(action, Action::S3VectorsAction(_))) {
        return Err(IamError::MixedActionFamilies.into());
    }
    if (has_vector_action
        && resources
            .iter()
            .chain(not_resources.iter())
            .any(|resource| !matches!(resource, Resource::S3Vectors(_) | Resource::All)))
        || (has_vector_resource
            && actions
                .iter()
                .any(|action| !matches!(action, Action::S3VectorsAction(_) | Action::All)))
    {
        return Err(IamError::InvalidResource("s3vectors".into(), "resource and action families differ".into()).into());
    }
    if has_vector_action || has_vector_resource {
        for key in conditions.keys() {
            key.is_valid()?;
            let supported = if has_vector_action {
                actions.iter().any(|action| {
                    matches!(action, Action::S3VectorsAction(action) if vector_condition_is_supported(*action, &key.name))
                })
            } else {
                vector_condition_is_supported(S3VectorsAction::AllActions, &key.name)
            };
            if !supported {
                return Err(IamError::InvalidKey(String::from(key.clone())).into());
            }
        }
    }
    Ok(())
}

async fn vector_scope_matches(
    resources: &ResourceSet,
    not_resources: &ResourceSet,
    requested: &str,
    conditions: &HashMap<String, Vec<String>>,
    resolver: Option<&dyn PolicyVariableResolver>,
) -> bool {
    if resources.is_empty() && not_resources.is_empty() {
        return false;
    }
    if !resources.is_empty() {
        let mut matched = false;
        for resource in resources.iter() {
            if matches!(resource, Resource::S3Vectors(_) | Resource::All)
                && resource.is_match_with_resolver(requested, conditions, resolver).await
            {
                matched = true;
                break;
            }
        }
        if !matched {
            return false;
        }
    }
    for resource in not_resources.iter() {
        if matches!(resource, Resource::S3Vectors(_) | Resource::All)
            && resource.is_match_with_resolver(requested, conditions, resolver).await
        {
            return false;
        }
    }
    true
}

/// A requested KMS key identifier, reduced to what `key/<key_id>` patterns are
/// written against.
///
/// Policies name a key by its id, and [`Resource`] validation refuses a key
/// pattern containing a separator. An identifier reaching the matcher in any
/// other spelling of the same key was therefore compared as a different key: a
/// `Deny` on `key/<id>` did not cover a request naming that key by its AWS ARN.
/// Only the spellings the AWS KMS API itself accepts are recognized; anything
/// else is matched literally.
#[derive(Debug, PartialEq)]
pub enum RequestedKmsKey<'a> {
    /// A key id, taken from the identifier itself or from a key ARN
    /// (`arn:<partition>:kms:<region>:<account>:key/<key_id>`).
    Key(&'a str),
    /// An alias name or alias ARN: it names a key only the backend can resolve.
    Alias,
}

impl<'a> RequestedKmsKey<'a> {
    /// Whether two identifiers provably name the same key. An alias is never
    /// provably the same key as anything, itself included.
    pub fn same_key(left: &str, right: &str) -> bool {
        matches!(
            (RequestedKmsKey::parse(left), RequestedKmsKey::parse(right)),
            (RequestedKmsKey::Key(left), RequestedKmsKey::Key(right)) if left == right
        )
    }

    pub fn parse(identifier: &'a str) -> Self {
        let fields: Vec<&str> = identifier.splitn(6, ':').collect();
        let resource = match fields.as_slice() {
            ["arn", _partition, "kms", _region, _account, resource] => *resource,
            _ if identifier.starts_with(Resource::KMS_ALIAS_SEGMENT) => return Self::Alias,
            _ => return Self::Key(identifier),
        };
        if resource.starts_with(Resource::KMS_ALIAS_SEGMENT) {
            return Self::Alias;
        }
        match resource.strip_prefix(Resource::KMS_KEY_SEGMENT) {
            Some(key_id) if !key_id.is_empty() && !key_id.contains('/') => Self::Key(key_id),
            _ => Self::Key(identifier),
        }
    }
}

impl Statement {
    fn skips_resource_match_for_args(&self, args: &Args<'_>) -> bool {
        if self.is_sts() {
            return true;
        }

        if !self.is_admin() {
            return false;
        }

        !matches!(args.action, Action::AdminAction(action) if action.is_resource_scoped())
    }

    fn is_kms(&self) -> bool {
        for act in self.actions.iter() {
            if matches!(act, Action::KmsAction(_)) {
                return true;
            }
        }

        false
    }

    fn is_admin(&self) -> bool {
        for act in self.actions.iter() {
            if matches!(act, Action::AdminAction(_)) {
                return true;
            }
        }

        false
    }

    fn is_sts(&self) -> bool {
        for act in self.actions.iter() {
            if matches!(act, Action::StsAction(_)) {
                return true;
            }
        }

        false
    }

    fn action_family(&self) -> Option<ActionFamily> {
        if self.actions.is_empty() {
            return None;
        }

        let mut saw_s3 = false;
        let mut saw_vectors = false;
        let mut saw_admin = false;
        let mut saw_sts = false;
        let mut saw_kms = false;

        for action in self.actions.iter() {
            match action {
                Action::All | Action::S3Action(_) => saw_s3 = true,
                Action::S3VectorsAction(_) => saw_vectors = true,
                Action::AdminAction(_) => saw_admin = true,
                Action::StsAction(_) => saw_sts = true,
                Action::KmsAction(_) => saw_kms = true,
                Action::None => {}
            }
        }

        let family_count = u8::from(saw_s3) + u8::from(saw_vectors) + u8::from(saw_admin) + u8::from(saw_sts) + u8::from(saw_kms);

        if family_count != 1 {
            return Some(ActionFamily::Mixed);
        }

        if saw_s3 {
            return Some(ActionFamily::S3);
        }
        if saw_vectors {
            return Some(ActionFamily::S3Vectors);
        }
        if saw_admin {
            return Some(ActionFamily::Admin);
        }
        if saw_sts {
            return Some(ActionFamily::Sts);
        }
        if saw_kms {
            return Some(ActionFamily::Kms);
        }

        Some(ActionFamily::Mixed)
    }

    /// Resource scope check for KMS statements, which match `arn:aws:kms:::key/<key_id>`
    /// patterns instead of the bucket/object path grammar used by S3 statements.
    ///
    /// Call-site contract (wired up by the admin/SSE authorization paths): the requested
    /// key identifier travels in `args.object`, as the request spelled it, with `args.bucket`
    /// left empty; [`RequestedKmsKey`] reduces it to the key id the patterns are written
    /// against. An empty `args.object` means the caller did not scope the request to a
    /// key, which preserves the legacy match-every-key behaviour.
    async fn kms_key_scope_matches(&self, args: &Args<'_>, resolver: &VariableResolver) -> bool {
        if self
            .resources
            .iter()
            .chain(self.not_resources.iter())
            .any(|resource| matches!(resource, Resource::S3Vectors(_)))
        {
            return false;
        }
        if self.not_resources.iter().any(|resource| matches!(resource, Resource::All)) {
            return false;
        }

        if matches!(args.action, Action::KmsAction(KmsAction::BackupAction | KmsAction::RestoreAction))
            && ((!self.resources.is_empty() && !self.resources.iter().any(|resource| matches!(resource, Resource::All)))
                || !self.not_resources.is_empty())
        {
            // Global bundle operations require an unscoped Allow, while a Deny
            // covering any key must still block an operation covering every key.
            return matches!(self.effect, Effect::Deny);
        }

        let kms_resources: Vec<&Resource> = self
            .resources
            .iter()
            .filter(|resource| matches!(resource, Resource::All | Resource::Kms(_)))
            .collect();
        let kms_not_resources: Vec<&Resource> = self.not_resources.iter().filter(|resource| resource.is_kms()).collect();

        if kms_resources.len() != self.resources.len() || kms_not_resources.len() != self.not_resources.len() {
            // Statements combining KMS actions with S3 resources predate KMS resource
            // support and were always evaluated as if unscoped; keep that behaviour
            // but surface it, since the S3 patterns never constrain key access.
            tracing::warn!(
                sid = %self.sid.0,
                "KMS statement carries non-KMS resources; they are ignored and the statement matches every key"
            );
        }

        if kms_resources.is_empty() && kms_not_resources.is_empty() {
            // No KMS resources: the statement scopes by action only (legacy form).
            return true;
        }

        if args.object.is_empty() {
            // Call sites that do not pass a key resource keep the pre-resource-scoping
            // behaviour where any key matches.
            return true;
        }

        let key_id = match RequestedKmsKey::parse(args.object) {
            RequestedKmsKey::Key(key_id) => key_id,
            RequestedKmsKey::Alias => {
                // Which key an alias names is known only to the backend, so a
                // statement scoped to particular keys cannot be shown to cover
                // or to exclude it. Fail closed in both directions: every
                // key-scoped Deny applies, and only an Allow on all keys does.
                return match self.effect {
                    Effect::Deny => true,
                    Effect::Allow => {
                        kms_not_resources.is_empty()
                            && kms_resources.iter().any(|resource| match resource {
                                Resource::All => true,
                                Resource::Kms(pattern) => pattern == "*",
                                Resource::S3(_) | Resource::S3Vectors(_) => false,
                            })
                    }
                };
            }
        };
        let requested = format!("{}{}", Resource::KMS_KEY_SEGMENT, key_id);

        if !kms_resources.is_empty() {
            let mut matched = false;
            for resource in kms_resources {
                if resource
                    .is_match_with_unresolved(&requested, args.conditions, Some(resolver), matches!(self.effect, Effect::Deny))
                    .await
                {
                    matched = true;
                    break;
                }
            }
            if !matched {
                return false;
            }
        }

        for resource in kms_not_resources {
            if resource
                .is_match_with_unresolved(&requested, args.conditions, Some(resolver), matches!(self.effect, Effect::Allow))
                .await
            {
                return false;
            }
        }

        true
    }

    /// Returns true when this statement would reach `conditions.evaluate_with_resolver` in
    /// [`Statement::is_allowed`] (including the KMS resource path). Does not evaluate conditions.
    pub(crate) async fn request_reaches_condition_eval(&self, args: &Args<'_>, resolver: &VariableResolver) -> bool {
        let deny = matches!(self.effect, Effect::Deny);
        if !self.actions.statement_covers(&self.not_actions, &args.action, deny) {
            return false;
        }

        if let Action::S3VectorsAction(action) = args.action {
            let Some(resource) = requested_vector_resource(action, args.bucket, args.object, args.conditions) else {
                return false;
            };
            if validate_vector_statement(&self.actions, &self.resources, &self.not_resources, &self.conditions).is_err() {
                return false;
            }
            return vector_scope_matches(&self.resources, &self.not_resources, &resource, args.conditions, Some(resolver)).await;
        }

        if self.is_kms() {
            return self.kms_key_scope_matches(args, resolver).await;
        }

        let resource = build_resource(
            &args.action,
            args.bucket,
            args.object,
            self.conditions.references_key_name(&KeyName::S3(S3KeyName::S3Prefix)),
        );

        if self.resources.is_empty() && self.not_resources.is_empty() && !self.is_admin() && !self.is_sts() {
            return false;
        }

        if !self.resources.is_empty()
            && !self
                .resources
                .is_match_with_unresolved(&resource, args.conditions, Some(resolver), deny)
                .await
            && !self.skips_resource_match_for_args(args)
        {
            return false;
        }

        if !self.not_resources.is_empty()
            && self
                .not_resources
                .is_match_with_unresolved(&resource, args.conditions, Some(resolver), !deny)
                .await
            && !self.skips_resource_match_for_args(args)
        {
            return false;
        }

        true
    }

    pub async fn is_allowed(&self, args: &Args<'_>) -> bool {
        let resolver = variable_resolver_for_policy_args(args);

        let check = 'c: {
            if !self.request_reaches_condition_eval(args, &resolver).await {
                break 'c false;
            }

            self.conditions.evaluate_with_resolver(args.conditions, Some(&resolver)).await
        };

        self.effect.is_allowed(check)
    }
}

impl Validator for Statement {
    type Error = Error;
    fn is_valid(&self) -> Result<()> {
        self.effect.is_valid()?;
        // check sid
        self.sid.is_valid()?;

        if self.actions.is_empty() && self.not_actions.is_empty() {
            return Err(IamError::NonAction.into());
        }

        if !self.actions.is_empty() && !self.not_actions.is_empty() {
            return Err(IamError::BothActionAndNotAction.into());
        }

        let action_family = if self.not_actions.is_empty() {
            match self.action_family() {
                Some(ActionFamily::Mixed) => return Err(IamError::MixedActionFamilies.into()),
                family => family,
            }
        } else {
            None
        };

        // Policy must contain either Resource or NotResource (but not both), unless
        // the statement is Action-mode Admin/STS/KMS.
        if self.resources.is_empty() && self.not_resources.is_empty() {
            let allow_empty_resource = matches!(
                action_family,
                Some(ActionFamily::Admin) | Some(ActionFamily::Sts) | Some(ActionFamily::Kms)
            );
            if !allow_empty_resource {
                return Err(IamError::NonResource.into());
            }
        }

        if !self.resources.is_empty() && !self.not_resources.is_empty() {
            return Err(IamError::BothResourceAndNotResource.into());
        }

        // KMS resources only make sense on pure-KMS statements. The reverse
        // combination (KMS actions with S3 resources) predates KMS resources,
        // may already be stored, and stays loadable; evaluation treats it as
        // unscoped and warns.
        let has_kms_resource = self
            .resources
            .iter()
            .chain(self.not_resources.iter())
            .any(|resource| resource.is_kms());
        if has_kms_resource && !matches!(action_family, Some(ActionFamily::Kms)) {
            return Err(IamError::KmsResourceWithNonKmsAction.into());
        }

        self.actions.is_valid()?;
        self.not_actions.is_valid()?;
        self.resources.is_valid()?;
        self.not_resources.is_valid()?;
        validate_vector_statement(&self.actions, &self.resources, &self.not_resources, &self.conditions)?;

        Ok(())
    }
}

impl PartialEq for Statement {
    // This equality drives `Policy::drop_duplicate_statements`/`merge_policies`, so it
    // must compare every field that affects matching. Any new match-affecting field
    // added to `Statement` MUST be compared here too, otherwise semantically-distinct
    // statements can be silently dropped and shrink Deny coverage.
    fn eq(&self, other: &Self) -> bool {
        self.effect == other.effect
            && self.actions == other.actions
            && self.not_actions == other.not_actions
            && self.resources == other.resources
            && self.not_resources == other.not_resources
            && self.conditions == other.conditions
    }
}

/// Bucket Policy Statement with AWS S3-compatible JSON serialization.
/// Empty optional fields are omitted from output to match AWS format.
#[derive(Debug, Deserialize, Serialize, Default, Clone)]
#[serde(rename_all = "PascalCase", default, deny_unknown_fields)]
pub struct BPStatement {
    #[serde(rename = "Sid", default, skip_serializing_if = "ID::is_empty")]
    pub sid: ID,
    #[serde(rename = "Effect")]
    pub effect: Effect,
    #[serde(rename = "Principal")]
    pub principal: Principal,
    #[serde(rename = "Action", default, skip_serializing_if = "ActionSet::is_empty")]
    pub actions: ActionSet,
    #[serde(rename = "NotAction", default, skip_serializing_if = "ActionSet::is_empty")]
    pub not_actions: ActionSet,
    #[serde(rename = "Resource", default, skip_serializing_if = "ResourceSet::is_empty")]
    pub resources: ResourceSet,
    #[serde(rename = "NotResource", default, skip_serializing_if = "ResourceSet::is_empty")]
    pub not_resources: ResourceSet,
    #[serde(rename = "Condition", default, skip_serializing_if = "Functions::is_empty")]
    pub conditions: Functions,
}

impl BPStatement {
    /// Returns true when this statement would reach `conditions.evaluate` in [`BPStatement::is_allowed`].
    pub(crate) async fn request_reaches_condition_eval(&self, args: &BucketPolicyArgs<'_>) -> bool {
        if !self.actions.is_empty() && self.actions.iter().all(|action| matches!(action, Action::KmsAction(_))) {
            // Bucket policies cannot grant or deny KMS access; such statements are
            // rejected at validation but may exist in policies stored before that
            // check. Skip them so they never influence bucket traffic. Statements
            // mixing KMS with S3 actions keep evaluating their S3 actions as before.
            tracing::warn!(
                sid = %self.sid.0,
                "ignoring bucket policy statement with KMS actions during evaluation"
            );
            return false;
        }

        if !self.principal.is_match(args.account) {
            return false;
        }

        let deny = matches!(self.effect, Effect::Deny);
        if !self.actions.statement_covers(&self.not_actions, &args.action, deny) {
            return false;
        }

        if let Action::S3VectorsAction(action) = args.action {
            let Some(resource) = requested_vector_resource(action, args.bucket, args.object, args.conditions) else {
                return false;
            };
            if validate_vector_statement(&self.actions, &self.resources, &self.not_resources, &self.conditions).is_err() {
                return false;
            }
            return vector_scope_matches(&self.resources, &self.not_resources, &resource, args.conditions, None).await;
        }

        let resource = build_resource(
            &args.action,
            args.bucket,
            args.object,
            self.conditions.references_key_name(&KeyName::S3(S3KeyName::S3Prefix)),
        );

        // An unresolved resource must not broaden Allow or suppress Deny, even
        // when the match is inverted by NotResource.
        if !self.resources.is_empty()
            && !self
                .resources
                .is_match_with_unresolved(&resource, args.conditions, None, deny)
                .await
        {
            return false;
        }

        if !self.not_resources.is_empty()
            && self
                .not_resources
                .is_match_with_unresolved(&resource, args.conditions, None, !deny)
                .await
        {
            return false;
        }

        true
    }

    pub async fn is_allowed(&self, args: &BucketPolicyArgs<'_>) -> bool {
        let check = 'c: {
            if !self.request_reaches_condition_eval(args).await {
                break 'c false;
            }

            self.conditions.evaluate(args.conditions).await
        };

        self.effect.is_allowed(check)
    }
}

impl Validator for BPStatement {
    type Error = Error;
    fn is_valid(&self) -> Result<()> {
        self.effect.is_valid()?;
        // check sid
        self.sid.is_valid()?;

        self.principal.is_valid()?;

        if self.actions.is_empty() && self.not_actions.is_empty() {
            return Err(IamError::NonAction.into());
        }

        if !self.actions.is_empty() && !self.not_actions.is_empty() {
            return Err(IamError::BothActionAndNotAction.into());
        }

        // Bucket policies govern S3 access; KMS grants belong in identity policies.
        // Rejected here (PutBucketPolicy) only: deserialization stays permissive so
        // stored policies from before this check keep loading, and evaluation skips
        // pure-KMS statements with a warning.
        let has_kms_action = self
            .actions
            .iter()
            .chain(self.not_actions.iter())
            .any(|action| matches!(action, Action::KmsAction(_)));
        let has_kms_resource = self
            .resources
            .iter()
            .chain(self.not_resources.iter())
            .any(|resource| resource.is_kms());
        if has_kms_action || has_kms_resource {
            return Err(IamError::KmsUnsupportedInBucketPolicy.into());
        }

        if self.resources.is_empty() && self.not_resources.is_empty() {
            return Err(IamError::NonResource.into());
        }

        if !self.resources.is_empty() && !self.not_resources.is_empty() {
            return Err(IamError::BothResourceAndNotResource.into());
        }

        self.actions.is_valid()?;
        self.not_actions.is_valid()?;
        self.resources.is_valid()?;
        self.not_resources.is_valid()?;
        validate_vector_statement(&self.actions, &self.resources, &self.not_resources, &self.conditions)?;

        Ok(())
    }
}

#[cfg(test)]
mod s3vectors_tests {
    use crate::policy::action::Action;
    use crate::policy::resource::Resource;
    use crate::policy::{Args, BucketPolicy, BucketPolicyArgs, Policy, Validator};
    use serde::de::DeserializeOwned;
    use serde_json::{Value, json};
    use std::collections::HashMap;

    const OPERATIONS: [&str; 21] = [
        "CreateIndex",
        "CreateVectorBucket",
        "DeleteIndex",
        "DeleteVectorBucket",
        "DeleteVectorBucketPolicy",
        "DeleteVectors",
        "GetIndex",
        "GetVectorBucket",
        "GetVectorBucketPolicy",
        "GetVectors",
        "ListIndexes",
        "ListTagsForResource",
        "ListVectorBuckets",
        "ListVectors",
        "PutVectorBucketDefaultIndexMode",
        "PutVectorBucketPolicy",
        "PutVectors",
        "QueryVectors",
        "TagResource",
        "UntagResource",
        "UpdateIndexMode",
    ];
    const BUCKET: &str = "arn:aws:s3vectors:us-east-1::bucket/media";
    const INDEX: &str = "arn:aws:s3vectors:us-east-1::bucket/media/index/embeddings.v1";

    fn parse_json<T: DeserializeOwned>(value: Value) -> serde_json::Result<T> {
        let bytes = serde_json::to_vec(&value)?;
        serde_json::from_slice(&bytes)
    }

    fn policy(statements: Value) -> Policy {
        let policy: Policy = parse_json(json!({
            "Version": "2012-10-17",
            "Statement": statements,
        }))
        .expect("policy JSON must parse");
        policy.is_valid().expect("policy must validate");
        policy
    }

    fn conditions() -> HashMap<String, Vec<String>> {
        HashMap::from([("RequestedRegion".to_owned(), vec!["us-east-1".to_owned()])])
    }

    async fn allowed(
        policy: &Policy,
        operation: &str,
        bucket: &str,
        index: &str,
        conditions: &HashMap<String, Vec<String>>,
    ) -> bool {
        let action = Action::try_from(operation).expect("request action must parse");
        policy
            .is_allowed(&Args {
                account: "vector-user",
                groups: &None,
                action,
                bucket,
                conditions,
                is_owner: false,
                object: index,
                claims: &HashMap::new(),
                deny_only: false,
            })
            .await
    }

    #[test]
    fn s3vectors_actions_cover_fixed_model_and_round_trip() {
        for operation in OPERATIONS.into_iter().chain(["*"]) {
            let name = format!("s3vectors:{operation}");
            let action = Action::try_from(name.as_str()).expect("fixed S3 Vectors action must parse");
            assert_eq!(serde_json::to_value(action).expect("action must serialize"), name);
            assert_eq!(parse_json::<Action>(json!(name)).expect("action must deserialize"), action);
        }
    }

    #[test]
    fn s3vectors_actions_keep_service_wildcards_separate() {
        let vectors = Action::try_from("s3vectors:*").expect("Vector wildcard must parse");
        let request = Action::try_from("s3vectors:GetVectors").expect("Vector action must parse");
        assert!(vectors.is_match(&request));
        for pattern in ["s3:*", "kms:*", "sts:*", "admin:*", "*"] {
            let pattern = Action::try_from(pattern).expect("existing wildcard must parse");
            assert!(!pattern.is_match(&request));
        }
        let star = Action::try_from("*").expect("global wildcard must parse");
        assert!(star.is_match_for_effect(&request, true));
        for operation in ["s3:GetObject", "kms:Decrypt", "sts:AssumeRole", "admin:ServerInfo"] {
            let action = Action::try_from(operation).expect("existing action must parse");
            assert!(!vectors.is_match(&action));
            assert!(!vectors.is_match_for_effect(&action, true));
        }
        for action in [
            "s3vectors:GetVector",
            "s3vectors:getvectors",
            "s3vectors:Get*",
            "S3Vectors:GetVectors",
        ] {
            assert!(Action::try_from(action).is_err(), "unexpected action accepted: {action}");
        }
    }

    #[test]
    fn s3vectors_resource_round_trip_and_invalid_identity() {
        for arn in [BUCKET, INDEX, "arn:aws:s3vectors:*::bucket/media-*/index/*"] {
            let resource = Resource::try_from(arn).expect("Vector policy resource must parse");
            assert_eq!(serde_json::to_value(resource).expect("resource must serialize"), arn);
        }
        for arn in [
            "arn:aws:s3vectors:us-east-1:123456789012:bucket/media",
            "arn:aws-cn:s3vectors:us-east-1::bucket/media",
            "arn:aws:s3vectors:::bucket/media",
            "arn:aws:s3vectors:us-east-1::bucket/",
            "arn:aws:s3vectors:us-east-1::bucket/media/",
            "arn:aws:s3vectors:us-east-1::bucket/media/index/",
            "arn:aws:s3vectors:us-east-1::bucket/media/index/embeddings/extra",
            "arn:aws:s3vectors:us-east-1::bucket/media/index/../embeddings",
            "arn:aws:s3vectors:us-east-1::bucket/media%2fprivate",
            "arn:aws:s3vectors:us-east-1::bucket/MEDIA",
            "arn:aws:s3vectors:us-east-1:*:bucket/media",
        ] {
            assert!(Resource::try_from(arn).is_err(), "malformed Vector resource accepted: {arn}");
        }
    }

    #[tokio::test]
    async fn s3vectors_resource_matching_preserves_type_region_and_account() {
        let resource =
            Resource::try_from("arn:aws:s3vectors:us-east-1::bucket/media/index/*").expect("Vector Index wildcard must parse");
        assert!(resource.match_resource(INDEX).await);
        for request in [
            BUCKET,
            "arn:aws:s3vectors:us-west-2::bucket/media/index/embeddings.v1",
            "arn:aws:s3vectors:us-east-1:123456789012:bucket/media/index/embeddings.v1",
            "arn:aws:s3vectors:us-east-1::bucket/media-other/index/embeddings.v1",
            "arn:aws:s3vectors:us-east-1::bucket/media/index/embeddings.v1/extra",
            "arn:aws:s3vectors:us-east-1::bucket/media/index/embeddings.v1/../other",
            "arn:aws:s3vectors:us-east-1::bucket/media/index/*",
            "arn:aws:s3:::media/index/embeddings.v1",
            "media/index/embeddings.v1",
        ] {
            assert!(!resource.match_resource(request).await, "unexpected Vector resource match: {request}");
        }
        let bucket = Resource::try_from("arn:aws:s3vectors:us-east-1::bucket/*").expect("bucket wildcard must parse");
        assert!(bucket.match_resource(BUCKET).await);
        assert!(bucket.match_resource(INDEX).await);
        let exact_bucket = Resource::try_from(BUCKET).expect("bucket ARN must parse");
        assert!(!exact_bucket.match_resource(INDEX).await);
    }

    #[tokio::test]
    async fn s3vectors_policy_scopes_index_and_deployment_region() {
        let policy = policy(json!([{"Effect":"Allow","Action":"s3vectors:GetVectors","Resource":INDEX}]));
        let conditions = conditions();
        assert!(allowed(&policy, "s3vectors:GetVectors", "media", "embeddings.v1", &conditions).await);
        assert!(!allowed(&policy, "s3vectors:GetVectors", "other", "embeddings.v1", &conditions).await);
        assert!(!allowed(&policy, "s3vectors:GetVectors", "media", "other", &conditions).await);
        assert!(!allowed(&policy, "s3vectors:QueryVectors", "media", "embeddings.v1", &conditions).await);
        let foreign_region = HashMap::from([("RequestedRegion".to_owned(), vec!["us-west-2".to_owned()])]);
        assert!(!allowed(&policy, "s3vectors:GetVectors", "media", "embeddings.v1", &foreign_region).await);
        assert!(!allowed(&policy, "s3vectors:GetVectors", "media", "embeddings.v1", &HashMap::new()).await);
    }

    #[tokio::test]
    async fn s3vectors_invalid_context_is_denied_before_owner_and_deny_only() {
        let policy = Policy::default();
        let action = Action::try_from("s3vectors:GetVectors").expect("Vector action must parse");
        let claims = HashMap::new();
        let valid_conditions = conditions();
        let multiple_regions =
            HashMap::from([("RequestedRegion".to_owned(), vec!["us-east-1".to_owned(), "us-west-2".to_owned()])]);
        for (bucket, index, context) in [
            ("media", "embeddings.v1", HashMap::new()),
            ("media", "embeddings.v1", multiple_regions),
            ("media", "", valid_conditions.clone()),
            ("media", "../other", valid_conditions.clone()),
            (BUCKET, "embeddings.v1", valid_conditions.clone()),
        ] {
            for (is_owner, deny_only) in [(true, false), (false, true)] {
                let args = Args {
                    account: "vector-user",
                    groups: &None,
                    action,
                    bucket,
                    conditions: &context,
                    is_owner,
                    object: index,
                    claims: &claims,
                    deny_only,
                };
                assert!(!policy.is_allowed(&args).await);
                assert_eq!(crate::policy::probe::policy_probe_decision(&policy, &args).await, Some(false));
                assert!(!crate::policy::probe::policy_action_potential(&policy, &args).await);
                let bucket_args = BucketPolicyArgs {
                    account: args.account,
                    groups: args.groups,
                    action,
                    bucket,
                    conditions: &context,
                    is_owner,
                    object: index,
                };
                let bucket_policy = BucketPolicy::default();
                assert!(!bucket_policy.is_allowed(&bucket_args).await);
                assert_eq!(
                    crate::policy::probe::bucket_policy_probe_decision(&bucket_policy, &bucket_args).await,
                    Some(false)
                );
                assert!(!crate::policy::probe::bucket_policy_action_potential(&bucket_policy, &bucket_args).await);
            }
        }
    }

    #[tokio::test]
    async fn s3vectors_explicit_deny_precedes_allow() {
        for deny in ["s3vectors:GetVectors", "s3vectors:*", "*"] {
            let policy = policy(json!([
                {"Effect":"Allow","Action":"s3vectors:*","Resource":"*"},
                {"Effect":"Deny","Action":deny,"Resource":"*"}
            ]));
            assert!(!allowed(&policy, "s3vectors:GetVectors", "media", "embeddings.v1", &conditions()).await);
        }
    }

    #[tokio::test]
    async fn s3vectors_not_action_and_not_resource_keep_deny_scope() {
        let policy = policy(json!([
            {"Effect":"Allow","Action":"s3vectors:*","Resource":"*"},
            {"Effect":"Deny","NotAction":"s3vectors:GetVectors","Resource":"*"},
            {"Effect":"Deny","Action":"s3vectors:GetVectors","NotResource":INDEX}
        ]));
        let conditions = conditions();
        assert!(allowed(&policy, "s3vectors:GetVectors", "media", "embeddings.v1", &conditions).await);
        assert!(!allowed(&policy, "s3vectors:GetVectors", "media", "other", &conditions).await);
        assert!(!allowed(&policy, "s3vectors:QueryVectors", "media", "embeddings.v1", &conditions).await);
    }

    #[test]
    fn s3vectors_policy_rejects_resource_family_mismatches() {
        for (action, resource) in [
            ("s3vectors:GetVectors", "arn:aws:s3:::media/*"),
            ("s3vectors:GetVectors", "arn:aws:kms:::key/key-a"),
            ("s3:GetObject", INDEX),
            ("kms:Decrypt", INDEX),
        ] {
            let parsed: Policy = parse_json(json!({
                "Version":"2012-10-17",
                "Statement":[{"Effect":"Allow","Action":action,"Resource":resource}]
            }))
            .expect("all resource and action spellings must parse independently");
            assert!(parsed.is_valid().is_err(), "incompatible family validated: {action} {resource}");
        }
    }

    #[tokio::test]
    async fn s3vectors_unvalidated_policy_cannot_expand_s3_or_kms() {
        for action in ["s3:GetObject", "kms:Decrypt"] {
            let parsed: Policy = parse_json(json!({
                "Version":"2012-10-17",
                "Statement":[{"Effect":"Allow","Action":action,"Resource":INDEX}]
            }))
            .expect("policy must deserialize before semantic validation");
            let (bucket, object) = if action == "kms:Decrypt" {
                ("", "key-a")
            } else {
                ("media", "file.txt")
            };
            assert!(!allowed(&parsed, action, bucket, object, &conditions()).await);
        }
    }

    #[tokio::test]
    async fn s3vectors_bucket_and_account_list_targets_are_distinct() {
        let policy = policy(json!([
            {"Effect":"Allow","Action":"s3vectors:ListIndexes","Resource":BUCKET},
            {"Effect":"Allow","Action":"s3vectors:ListVectorBuckets","Resource":"*"}
        ]));
        let conditions = conditions();
        assert!(allowed(&policy, "s3vectors:ListIndexes", "media", "", &conditions).await);
        assert!(!allowed(&policy, "s3vectors:ListIndexes", "media", "embeddings.v1", &conditions).await);
        assert!(allowed(&policy, "s3vectors:ListVectorBuckets", "", "", &conditions).await);
        assert!(!allowed(&policy, "s3vectors:ListVectorBuckets", "media", "", &conditions).await);
    }

    #[tokio::test]
    async fn s3vectors_creation_conditions_use_verified_request_fields() {
        let policy = policy(json!([{
            "Effect":"Allow","Action":"s3vectors:CreateIndex","Resource":INDEX,
            "Condition":{
                "StringEquals":{
                    "aws:RequestedRegion":"us-east-1",
                    "aws:RequestTag/team":"search",
                    "s3vectors:VectorBucketTag/environment":"production",
                    "s3vectors:sseType":"aws:kms"
                },
                "ArnEquals":{"s3vectors:kmsKeyArn":"arn:aws:kms:::key/key-a"},
                "ForAllValues:StringEquals":{"aws:TagKeys":["team"]},
                "Null":{"aws:TagKeys":"false"}
            }
        }]));
        let mut context = conditions();
        for (name, value) in [
            ("RequestTag/team", "search"),
            ("VectorBucketTag/environment", "production"),
            ("sseType", "aws:kms"),
            ("kmsKeyArn", "arn:aws:kms:::key/key-a"),
            ("TagKeys", "team"),
        ] {
            context.insert(name.to_owned(), vec![value.to_owned()]);
        }
        assert!(allowed(&policy, "s3vectors:CreateIndex", "media", "embeddings.v1", &context).await);
        for key in [
            "RequestTag/team",
            "VectorBucketTag/environment",
            "sseType",
            "kmsKeyArn",
            "TagKeys",
        ] {
            let mut missing = context.clone();
            missing.remove(key);
            assert!(!allowed(&policy, "s3vectors:CreateIndex", "media", "embeddings.v1", &missing).await);
        }
        context.insert("TagKeys".to_owned(), vec!["team".to_owned(), "unapproved".to_owned()]);
        assert!(!allowed(&policy, "s3vectors:CreateIndex", "media", "embeddings.v1", &context).await);
    }

    #[tokio::test]
    async fn s3vectors_resource_tag_deny_uses_current_conditions() {
        let policy = policy(json!([
            {"Effect":"Allow","Action":"s3vectors:GetVectors","Resource":INDEX},
            {"Effect":"Deny","Action":"s3vectors:GetVectors","Resource":INDEX,
             "Condition":{"StringEquals":{"aws:ResourceTag/restricted":"true"}}}
        ]));
        let mut context = conditions();
        context.insert("ResourceTag/restricted".to_owned(), vec!["false".to_owned()]);
        assert!(allowed(&policy, "s3vectors:GetVectors", "media", "embeddings.v1", &context).await);
        context.insert("ResourceTag/restricted".to_owned(), vec!["true".to_owned()]);
        assert!(!allowed(&policy, "s3vectors:GetVectors", "media", "embeddings.v1", &context).await);
    }

    #[test]
    fn s3vectors_condition_key_shape_and_action_allowlist_are_validated() {
        for key in [
            "aws:RequestTag",
            "aws:RequestTag/",
            "aws:ResourceTag/",
            "s3vectors:VectorBucketTag/",
            "aws:RequestedRegion/extra",
            "aws:TagKeys/extra",
            "s3vectors:sseType/extra",
            "s3vectors:kmsKeyArn/extra",
            "s3vectors:Unknown",
        ] {
            let parsed = parse_json::<Policy>(json!({
                "Version":"2012-10-17",
                "Statement":[{"Effect":"Allow","Action":"s3vectors:CreateIndex","Resource":INDEX,
                              "Condition":{"StringEquals":{(key):"value"}}}]
            }));
            assert!(parsed.is_err() || parsed.expect("parsed policy must be available").is_valid().is_err());
        }
        for (action, key) in [
            ("s3vectors:GetVectors", "s3vectors:sseType"),
            ("s3vectors:GetVectors", "aws:RequestTag/team"),
            ("s3vectors:UntagResource", "aws:RequestTag/team"),
            ("s3vectors:ListVectorBuckets", "s3vectors:VectorBucketTag/team"),
            ("s3vectors:GetVectors", "s3:prefix"),
        ] {
            let parsed: Policy = parse_json(json!({
                "Version":"2012-10-17",
                "Statement":[{"Effect":"Allow","Action":action,"Resource":"*",
                              "Condition":{"StringEquals":{(key):"value"}}}]
            }))
            .expect("known condition key must deserialize");
            assert!(parsed.is_valid().is_err(), "unsupported action condition accepted: {action} {key}");
        }
    }

    #[test]
    fn s3vectors_condition_names_are_reserved_from_untrusted_input() {
        for key in [
            "RequestedRegion",
            "requestEDregion",
            "RequestTag/team",
            "ResourceTag/team",
            "TagKeys",
            "VectorBucketTag/team",
            "kmsKeyArn",
            "sseType",
        ] {
            assert!(
                crate::policy::is_server_derived_condition_key(key),
                "condition key must be reserved: {key}"
            );
        }
    }

    #[tokio::test]
    async fn s3vectors_bucket_policy_uses_same_resource_and_deny_rules() {
        let policy: BucketPolicy = parse_json(json!({
            "Version":"2012-10-17",
            "Statement":[
                {"Effect":"Allow","Principal":{"AWS":"vector-user"},"Action":"s3vectors:GetVectors","Resource":INDEX},
                {"Effect":"Deny","Principal":"*","Action":"s3vectors:GetVectors","Resource":INDEX,
                 "Condition":{"StringEquals":{"aws:ResourceTag/restricted":"true"}}}
            ]
        }))
        .expect("Vector bucket policy must parse");
        policy.is_valid().expect("Vector bucket policy must validate");
        let mut context = conditions();
        let action = Action::try_from("s3vectors:GetVectors").expect("Vector action must parse");
        for restricted in ["false", "true"] {
            context.insert("ResourceTag/restricted".to_owned(), vec![restricted.to_owned()]);
            let args = BucketPolicyArgs {
                account: "vector-user",
                groups: &None,
                action,
                bucket: "media",
                conditions: &context,
                is_owner: false,
                object: "embeddings.v1",
            };
            assert_eq!(policy.is_allowed(&args).await, restricted == "false");
        }
    }

    #[test]
    fn s3vectors_public_principal_classification_covers_every_action() {
        for operation in OPERATIONS {
            for principal in [json!("*"), json!({"AWS":"*"}), json!({"AWS":["vector-user", "*"]})] {
                let policy: BucketPolicy = parse_json(json!({
                    "Version":"2012-10-17",
                    "Statement":[{
                        "Effect":"Allow",
                        "Principal":principal,
                        "Action":format!("s3vectors:{operation}"),
                        "Resource":"*"
                    }]
                }))
                .expect("public principal policy must deserialize");
                policy
                    .is_valid()
                    .expect("public access restrictions are applied by the mutation entry point");
                assert!(policy.statements[0].principal.is_match("*"));
            }
        }
        let policy: BucketPolicy = parse_json(json!({
            "Version":"2012-10-17",
            "Statement":[{"Effect":"Allow","Principal":{"AWS":"vector-user"},
                          "Action":"s3vectors:GetVectors","Resource":INDEX}]
        }))
        .expect("named principal policy must deserialize");
        assert!(!policy.statements[0].principal.is_match("*"));
    }
}
