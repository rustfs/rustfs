use super::{Effect, Error as IamError, Statement, ID};
use crate::sys::{Args, Validator, DEFAULT_VERSION};
use ecstore::error::{Error, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

#[derive(Serialize, Deserialize, Clone, Default, Debug)]
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

    pub fn match_resource(&self, resource: &str) -> bool {
        for statement in self.statements.iter() {
            if statement.resources.match_resource(resource) {
                return true;
            }
        }
        false
    }

    fn drop_duplicate_statements(&mut self) {
        let mut dups = HashSet::new();
        for i in 0..self.statements.len() {
            if dups.contains(&i) {
                // i is already a duplicate of some statement, so we do not need to
                // compare with it.
                continue;
            }
            for j in (i + 1)..self.statements.len() {
                if !self.statements[i].eq(&self.statements[j]) {
                    continue;
                }

                // save duplicate statement index for removal.
                dups.insert(j);
            }
        }

        // remove duplicate items from the slice.
        let mut c = 0;
        for i in 0..self.statements.len() {
            if dups.contains(&i) {
                continue;
            }
            self.statements[c] = self.statements[i].clone();
            c += 1;
        }
        self.statements.truncate(c);
    }
    pub fn merge_policies(inputs: Vec<Policy>) -> Policy {
        let mut merged = Policy::default();

        for p in inputs {
            if merged.version.is_empty() {
                merged.version = p.version.clone();
            }
            for st in p.statements {
                merged.statements.push(st.clone());
            }
        }
        merged.drop_duplicate_statements();
        merged
    }

    pub fn is_empty(&self) -> bool {
        self.statements.is_empty()
    }

    pub fn validate(&self) -> Result<()> {
        self.is_valid()
    }

    pub fn parse_config(data: &[u8]) -> Result<Policy> {
        let policy: Policy = serde_json::from_slice(data)?;
        policy.validate()?;
        Ok(policy)
    }
}

impl Validator for Policy {
    type Error = Error;

    fn is_valid(&self) -> Result<()> {
        if !self.id.is_empty() && !self.id.eq(DEFAULT_VERSION) {
            return Err(IamError::InvalidVersion(self.id.0.clone()).into());
        }

        for statement in self.statements.iter() {
            statement.is_valid()?;
        }

        Ok(())
    }
}

pub mod default {
    use std::{collections::HashSet, sync::LazyLock};

    use crate::{
        policy::{
            action::{Action, AdminAction, KmsAction, S3Action},
            resource::Resource,
            ActionSet, Effect, Functions, ResourceSet, Statement,
        },
        sys::DEFAULT_VERSION,
    };

    use super::Policy;

    #[allow(clippy::incompatible_msrv)]
    pub static DEFAULT_POLICIES: LazyLock<[(&'static str, Policy); 6]> = LazyLock::new(|| {
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
