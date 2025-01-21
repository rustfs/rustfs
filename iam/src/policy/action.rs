use ecstore::error::{Error, Result};
use serde::{Deserialize, Serialize};
use std::{collections::HashSet, ops::Deref};
use strum::{EnumString, IntoStaticStr};

use crate::sys::Validator;

use super::{utils::wildcard, Error as IamError};

#[derive(Serialize, Deserialize, Clone, Default, Debug)]
pub struct ActionSet(pub HashSet<Action>);

impl ActionSet {
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

#[derive(Serialize, Deserialize, Hash, PartialEq, Eq, Clone, Debug)]
#[serde(try_from = "&str", untagged)]
pub enum Action {
    S3Action(S3Action),
    AdminAction(AdminAction),
    StsAction(StsAction),
    KmsAction(KmsAction),
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
    fn try_from(value: &str) -> Result<Self, Self::Error> {
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

#[derive(Serialize, Deserialize, Hash, PartialEq, Eq, Clone, EnumString, IntoStaticStr, Debug)]
#[cfg_attr(test, derive(Default))]
#[serde(try_from = "&str", into = "&str")]
pub enum S3Action {
    #[cfg_attr(test, default)]
    #[strum(serialize = "s3:*")]
    AllActions,
    #[strum(serialize = "s3:GetBucketLocation")]
    GetBucketLocationAction,
    #[strum(serialize = "s3:GetObject")]
    GetObjectAction,
    #[strum(serialize = "s3:PutObject")]
    PutObjectAction,
    #[strum(serialize = "s3:GetObjectVersion")]
    GetObjectVersionAction,
}

#[derive(Serialize, Deserialize, Hash, PartialEq, Eq, Clone, EnumString, IntoStaticStr, Debug)]
#[serde(try_from = "&str", into = "&str")]
pub enum AdminAction {
    #[strum(serialize = "admin:*")]
    AllActions,
    #[strum(serialize = "admin:Profiling")]
    ProfilingAdminAction,
    #[strum(serialize = "admin:ServerTrace")]
    TraceAdminAction,
    #[strum(serialize = "admin:ConsoleLog")]
    ConsoleLogAdminAction,
    #[strum(serialize = "admin:ServerInfo")]
    ServerInfoAdminAction,
    #[strum(serialize = "admin:OBDInfo")]
    HealthInfoAdminAction,
    #[strum(serialize = "admin:TopLocksInfo")]
    TopLocksAdminAction,
    #[strum(serialize = "admin:LicenseInfo")]
    LicenseInfoAdminAction,
    #[strum(serialize = "admin:BandwidthMonitor")]
    BandwidthMonitorAction,
    #[strum(serialize = "admin:InspectData")]
    InspectDataAction,
    #[strum(serialize = "admin:Prometheus")]
    PrometheusAdminAction,
    #[strum(serialize = "admin:ListServiceAccounts")]
    ListServiceAccountsAdminAction,
    #[strum(serialize = "admin:CreateServiceAccount")]
    CreateServiceAccountAdminAction,
}

#[derive(Serialize, Deserialize, Hash, PartialEq, Eq, Clone, EnumString, IntoStaticStr, Debug)]
#[serde(try_from = "&str", into = "&str")]
pub enum StsAction {}

#[derive(Serialize, Deserialize, Hash, PartialEq, Eq, Clone, EnumString, IntoStaticStr, Debug)]
#[serde(try_from = "&str", into = "&str")]
pub enum KmsAction {
    #[strum(serialize = "kms:*")]
    AllActions,
}
