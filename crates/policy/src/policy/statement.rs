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
    action::{Action, S3Action},
    function::key_name::{KeyName, S3KeyName},
    variables::{VariableContext, VariableResolver},
};
use crate::error::{Error, Result};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Default, Debug)]
pub struct Statement {
    #[serde(rename = "Sid", default)]
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
    #[serde(rename = "Condition", default)]
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
    Admin,
    Sts,
    Kms,
    Mixed,
}

impl Statement {
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
        let mut saw_admin = false;
        let mut saw_sts = false;
        let mut saw_kms = false;

        for action in self.actions.iter() {
            match action {
                Action::S3Action(_) => saw_s3 = true,
                Action::AdminAction(_) => saw_admin = true,
                Action::StsAction(_) => saw_sts = true,
                Action::KmsAction(_) => saw_kms = true,
                Action::None => {}
            }
        }

        let family_count = saw_s3 as u8 + saw_admin as u8 + saw_sts as u8 + saw_kms as u8;

        if family_count != 1 {
            return Some(ActionFamily::Mixed);
        }

        if saw_s3 {
            return Some(ActionFamily::S3);
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

    /// Returns true when this statement would reach `conditions.evaluate_with_resolver` in
    /// [`Statement::is_allowed`] (including the KMS shortcut path). Does not evaluate conditions.
    pub(crate) async fn request_reaches_condition_eval(&self, args: &Args<'_>, resolver: &VariableResolver) -> bool {
        if (!self.actions.is_match(&args.action) && !self.actions.is_empty()) || self.not_actions.is_match(&args.action) {
            return false;
        }

        let resource = build_resource(
            &args.action,
            args.bucket,
            args.object,
            self.conditions.references_key_name(&KeyName::S3(S3KeyName::S3Prefix)),
        );

        if self.is_kms() && (resource == "/" || self.resources.is_empty()) {
            return true;
        }

        if self.resources.is_empty() && self.not_resources.is_empty() && !self.is_admin() && !self.is_sts() {
            return false;
        }

        if !self.resources.is_empty()
            && !self
                .resources
                .is_match_with_resolver(&resource, args.conditions, Some(resolver))
                .await
            && !self.is_admin()
            && !self.is_sts()
        {
            return false;
        }

        if !self.not_resources.is_empty()
            && self
                .not_resources
                .is_match_with_resolver(&resource, args.conditions, Some(resolver))
                .await
            && !self.is_admin()
            && !self.is_sts()
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

        self.actions.is_valid()?;
        self.not_actions.is_valid()?;
        self.resources.is_valid()?;
        self.not_resources.is_valid()?;

        Ok(())
    }
}

impl PartialEq for Statement {
    fn eq(&self, other: &Self) -> bool {
        self.effect == other.effect
            && self.actions == other.actions
            && self.not_actions == other.not_actions
            && self.resources == other.resources
            && self.conditions == other.conditions
    }
}

/// Bucket Policy Statement with AWS S3-compatible JSON serialization.
/// Empty optional fields are omitted from output to match AWS format.
#[derive(Debug, Deserialize, Serialize, Default, Clone)]
#[serde(rename_all = "PascalCase", default)]
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
        if !self.principal.is_match(args.account) {
            return false;
        }

        if (!self.actions.is_match(&args.action) && !self.actions.is_empty()) || self.not_actions.is_match(&args.action) {
            return false;
        }

        let resource = build_resource(
            &args.action,
            args.bucket,
            args.object,
            self.conditions.references_key_name(&KeyName::S3(S3KeyName::S3Prefix)),
        );

        if !self.resources.is_empty() && !self.resources.is_match(&resource, args.conditions).await {
            return false;
        }

        if !self.not_resources.is_empty() && self.not_resources.is_match(&resource, args.conditions).await {
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

        Ok(())
    }
}
