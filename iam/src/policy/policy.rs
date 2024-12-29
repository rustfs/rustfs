use serde::{Deserialize, Serialize};

use super::{Args, Effect, Error, Statement, Validator, DEFAULT_VERSION, ID};

#[derive(Serialize, Deserialize, Clone, Default)]
pub struct Policy {
    pub id: ID,
    pub version: String,
    pub statements: Vec<Statement>,
}

impl Policy {
    pub fn is_allowed(&self, args: &Args) -> bool {
        for statement in self.statements.iter().filter(|s| matches!(s.effect, Effect::Deny)) {
            if !statement.is_allowed(args) {
                return false;
            }
        }

        if args.deny_only || args.is_owner {
            return true;
        }

        for statement in self.statements.iter().filter(|s| matches!(s.effect, Effect::Allow)) {
            if statement.is_allowed(args) {
                return true;
            }
        }

        false
    }
}

impl Validator for Policy {
    fn is_valid(&self) -> Result<(), Error> {
        if !self.id.is_empty() && !self.id.eq(DEFAULT_VERSION) {
            return Err(Error::InvalidVersion(self.id.0.clone()));
        }

        for statement in self.statements.iter() {
            statement.is_valid()?;
        }

        Ok(())
    }
}

pub mod default {
    use std::{collections::HashSet, sync::LazyLock};

    use crate::policy::{
        action::{Action, AdminAction, KmsAction, S3Action},
        resource::Resource,
        ActionSet, Effect, Functions, ResourceSet, Statement, DEFAULT_VERSION,
    };

    use super::Policy;

    pub const DEFAULT_POLICIES: LazyLock<[(&'static str, Policy); 6]> = LazyLock::new(|| {
        [
            (
                "readwrite",
                Policy {
                    id: "".into(),
                    version: DEFAULT_VERSION.into(),
                    statements: vec![Statement {
                        sid: "".into(),
                        effect: Effect::Allow,
                        actions: ActionSet({
                            let mut hash_set = HashSet::new();
                            hash_set.insert(Action::S3Action(S3Action::AllActions));
                            hash_set
                        }),
                        not_actions: ActionSet(Default::default()),
                        resources: ResourceSet({
                            let mut hash_set = HashSet::new();
                            hash_set.insert(Resource::S3("*".into()));
                            hash_set
                        }),
                        conditions: Functions::default(),
                    }],
                },
            ),
            (
                "readonly",
                Policy {
                    id: "".into(),
                    version: DEFAULT_VERSION.into(),
                    statements: vec![Statement {
                        sid: "".into(),
                        effect: Effect::Allow,
                        actions: ActionSet({
                            let mut hash_set = HashSet::new();
                            hash_set.insert(Action::S3Action(S3Action::GetBucketLocationAction));
                            hash_set.insert(Action::S3Action(S3Action::GetObjectAction));
                            hash_set
                        }),
                        not_actions: ActionSet(Default::default()),
                        resources: ResourceSet({
                            let mut hash_set = HashSet::new();
                            hash_set.insert(Resource::S3("*".into()));
                            hash_set
                        }),
                        conditions: Functions::default(),
                    }],
                },
            ),
            (
                "writeonly",
                Policy {
                    id: "".into(),
                    version: DEFAULT_VERSION.into(),
                    statements: vec![Statement {
                        sid: "".into(),
                        effect: Effect::Allow,
                        actions: ActionSet({
                            let mut hash_set = HashSet::new();
                            hash_set.insert(Action::S3Action(S3Action::PutObjectAction));
                            hash_set
                        }),
                        not_actions: ActionSet(Default::default()),
                        resources: ResourceSet({
                            let mut hash_set = HashSet::new();
                            hash_set.insert(Resource::S3("*".into()));
                            hash_set
                        }),
                        conditions: Functions::default(),
                    }],
                },
            ),
            (
                "writeonly",
                Policy {
                    id: "".into(),
                    version: DEFAULT_VERSION.into(),
                    statements: vec![Statement {
                        sid: "".into(),
                        effect: Effect::Allow,
                        actions: ActionSet({
                            let mut hash_set = HashSet::new();
                            hash_set.insert(Action::S3Action(S3Action::PutObjectAction));
                            hash_set
                        }),
                        not_actions: ActionSet(Default::default()),
                        resources: ResourceSet({
                            let mut hash_set = HashSet::new();
                            hash_set.insert(Resource::S3("*".into()));
                            hash_set
                        }),
                        conditions: Functions::default(),
                    }],
                },
            ),
            (
                "diagnostics",
                Policy {
                    id: "".into(),
                    version: DEFAULT_VERSION.into(),
                    statements: vec![Statement {
                        sid: "".into(),
                        effect: Effect::Allow,
                        actions: ActionSet({
                            let mut hash_set = HashSet::new();
                            hash_set.insert(Action::AdminAction(AdminAction::ProfilingAdminAction));
                            hash_set.insert(Action::AdminAction(AdminAction::TraceAdminAction));
                            hash_set.insert(Action::AdminAction(AdminAction::ConsoleLogAdminAction));
                            hash_set.insert(Action::AdminAction(AdminAction::ServerInfoAdminAction));
                            hash_set.insert(Action::AdminAction(AdminAction::TopLocksAdminAction));
                            hash_set.insert(Action::AdminAction(AdminAction::HealthInfoAdminAction));
                            hash_set.insert(Action::AdminAction(AdminAction::PrometheusAdminAction));
                            hash_set.insert(Action::AdminAction(AdminAction::BandwidthMonitorAction));
                            hash_set
                        }),
                        not_actions: ActionSet(Default::default()),
                        resources: ResourceSet({
                            let mut hash_set = HashSet::new();
                            hash_set.insert(Resource::S3("*".into()));
                            hash_set
                        }),
                        conditions: Functions::default(),
                    }],
                },
            ),
            (
                "consoleAdmin",
                Policy {
                    id: "".into(),
                    version: DEFAULT_VERSION.into(),
                    statements: vec![
                        Statement {
                            sid: "".into(),
                            effect: Effect::Allow,
                            actions: ActionSet({
                                let mut hash_set = HashSet::new();
                                hash_set.insert(Action::AdminAction(AdminAction::AllActions));
                                hash_set
                            }),
                            not_actions: ActionSet(Default::default()),
                            resources: ResourceSet(HashSet::new()),
                            conditions: Functions::default(),
                        },
                        Statement {
                            sid: "".into(),
                            effect: Effect::Allow,
                            actions: ActionSet({
                                let mut hash_set = HashSet::new();
                                hash_set.insert(Action::KmsAction(KmsAction::AllActions));
                                hash_set
                            }),
                            not_actions: ActionSet(Default::default()),
                            resources: ResourceSet(HashSet::new()),
                            conditions: Functions::default(),
                        },
                        Statement {
                            sid: "".into(),
                            effect: Effect::Allow,
                            actions: ActionSet({
                                let mut hash_set = HashSet::new();
                                hash_set.insert(Action::S3Action(S3Action::AllActions));
                                hash_set
                            }),
                            not_actions: ActionSet(Default::default()),
                            resources: ResourceSet({
                                let mut hash_set = HashSet::new();
                                hash_set.insert(Resource::S3("*".into()));
                                hash_set
                            }),
                            conditions: Functions::default(),
                        },
                    ],
                },
            ),
        ]
    });
}
