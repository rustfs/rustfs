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

//! Matching engine: a linear scan over all rules per event, with regexes
//! compiled once. The seed set is ~60 shallow rules — no indexing until a
//! profile says otherwise.

use super::model::{Matcher, Rule};
use super::rule_set::RuleSet;
use crate::model::{EventKind, LogEvent};
use regex::Regex;

enum CompiledMatcher {
    MessagePrefix(String),
    MessageContains(String),
    MessageRegex(Regex),
    FieldEquals { name: String, value: String },
    TargetPrefix(String),
    IsPanic,
    MinLevel(crate::model::LogLevel),
    All(Vec<CompiledMatcher>),
    Any(Vec<CompiledMatcher>),
}

fn compile(matcher: &Matcher) -> CompiledMatcher {
    match matcher {
        Matcher::MessagePrefix(p) => CompiledMatcher::MessagePrefix(p.clone()),
        Matcher::MessageContains(c) => CompiledMatcher::MessageContains(c.clone()),
        // RuleSet::new validated compilability; this cannot fail here.
        Matcher::MessageRegex(re) => CompiledMatcher::MessageRegex(Regex::new(re).expect("validated by RuleSet::new")),
        Matcher::FieldEquals { name, value } => CompiledMatcher::FieldEquals {
            name: name.clone(),
            value: value.clone(),
        },
        Matcher::TargetPrefix(p) => CompiledMatcher::TargetPrefix(p.clone()),
        Matcher::IsPanic => CompiledMatcher::IsPanic,
        Matcher::MinLevel(level) => CompiledMatcher::MinLevel(*level),
        Matcher::All(inner) => CompiledMatcher::All(inner.iter().map(compile).collect()),
        Matcher::Any(inner) => CompiledMatcher::Any(inner.iter().map(compile).collect()),
    }
}

fn eval(matcher: &CompiledMatcher, event: &LogEvent) -> bool {
    match matcher {
        CompiledMatcher::MessagePrefix(p) => event.message.starts_with(p.as_str()),
        CompiledMatcher::MessageContains(c) => event.message.contains(c.as_str()),
        CompiledMatcher::MessageRegex(re) => re.is_match(&event.message),
        CompiledMatcher::FieldEquals { name, value } => event.field_display(name).as_deref() == Some(value.as_str()),
        CompiledMatcher::TargetPrefix(p) => event.target.as_deref().is_some_and(|t| t.starts_with(p.as_str())),
        CompiledMatcher::IsPanic => event.kind == EventKind::Panic,
        CompiledMatcher::MinLevel(level) => event.level.is_some_and(|l| l >= *level),
        CompiledMatcher::All(inner) => inner.iter().all(|m| eval(m, event)),
        CompiledMatcher::Any(inner) => inner.iter().any(|m| eval(m, event)),
    }
}

pub struct RuleEngine {
    set: RuleSet,
    compiled: Vec<CompiledMatcher>,
}

impl RuleEngine {
    pub fn new(set: RuleSet) -> Self {
        let compiled = set.rules().iter().map(|r| compile(&r.matcher)).collect();
        Self { set, compiled }
    }

    /// Indexes (aligned with [`Self::rules`]) of every rule the event hits.
    /// An event may hit several rules; arbitration happens at report time.
    pub fn matches(&self, event: &LogEvent) -> Vec<usize> {
        self.compiled
            .iter()
            .enumerate()
            .filter(|(_, m)| eval(m, event))
            .map(|(idx, _)| idx)
            .collect()
    }

    pub fn rules(&self) -> &[Rule] {
        self.set.rules()
    }
}
