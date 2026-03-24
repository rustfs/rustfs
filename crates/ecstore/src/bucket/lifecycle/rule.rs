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
use crate::bucket::tagging::decode_tags_to_map;
use s3s::dto::{LifecycleRuleAndOperator, LifecycleRuleFilter, Tag, Transition};

const _ERR_TRANSITION_INVALID_DAYS: &str = "Days must be 0 or greater when used with Transition";
const _ERR_TRANSITION_INVALID_DATE: &str = "Date must be provided in ISO 8601 format";
const ERR_TRANSITION_INVALID: &str =
    "Exactly one of Days (0 or greater) or Date (positive ISO 8601 format) should be present in Transition.";
const _ERR_TRANSITION_DATE_NOT_MIDNIGHT: &str = "'Date' must be at midnight GMT";

pub trait Filter {
    fn test_tags(&self, user_tags: &str) -> bool;
    fn by_size(&self, sz: i64) -> bool;
}

impl Filter for LifecycleRuleFilter {
    fn test_tags(&self, user_tags: &str) -> bool {
        if !requires_tag_matching(self) {
            return true;
        }

        let user_tags = decode_tags_to_map(user_tags);

        self.tag.as_ref().is_none_or(|tag| tag_matches(tag, &user_tags))
            && self.and.as_ref().is_none_or(|and| and_tags_match(and, &user_tags))
    }

    fn by_size(&self, sz: i64) -> bool {
        let sz = sz.max(0);

        self.object_size_greater_than.is_none_or(|min| sz > min)
            && self.object_size_less_than.is_none_or(|max| sz < max)
            && self.and.as_ref().is_none_or(|and| and_size_matches(and, sz))
    }
}

fn requires_tag_matching(filter: &LifecycleRuleFilter) -> bool {
    filter.tag.is_some()
        || filter
            .and
            .as_ref()
            .and_then(|and| and.tags.as_ref())
            .is_some_and(|tags| !tags.is_empty())
}

fn tag_matches(tag: &Tag, user_tags: &std::collections::HashMap<String, String>) -> bool {
    let Some(key) = tag.key.as_deref() else {
        return false;
    };
    let Some(value) = tag.value.as_deref() else {
        return false;
    };

    user_tags.get(key).is_some_and(|actual| actual == value)
}

fn and_tags_match(and: &LifecycleRuleAndOperator, user_tags: &std::collections::HashMap<String, String>) -> bool {
    and.tags
        .as_ref()
        .is_none_or(|tags| tags.iter().all(|tag| tag_matches(tag, user_tags)))
}

fn and_size_matches(and: &LifecycleRuleAndOperator, sz: i64) -> bool {
    and.object_size_greater_than.is_none_or(|min| sz > min) && and.object_size_less_than.is_none_or(|max| sz < max)
}

pub trait TransitionOps {
    fn validate(&self) -> Result<(), std::io::Error>;
}

impl TransitionOps for Transition {
    fn validate(&self) -> Result<(), std::io::Error> {
        if self.date.is_some() && self.days.is_some_and(|d| d > 0) {
            return Err(std::io::Error::other(ERR_TRANSITION_INVALID));
        }

        if self.storage_class.is_none() {
            return Err(std::io::Error::other("ERR_XML_NOT_WELL_FORMED"));
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn lifecycle_rule_filter_matches_single_tag() {
        let filter = LifecycleRuleFilter {
            tag: Some(Tag {
                key: Some("env".to_string()),
                value: Some("prod".to_string()),
            }),
            ..Default::default()
        };

        assert!(<LifecycleRuleFilter as Filter>::test_tags(&filter, "env=prod&team=storage"));
        assert!(!<LifecycleRuleFilter as Filter>::test_tags(&filter, "env=dev&team=storage"));
        assert!(!<LifecycleRuleFilter as Filter>::test_tags(&filter, "team=storage"));
    }

    #[test]
    fn lifecycle_rule_filter_matches_all_and_tags() {
        let filter = LifecycleRuleFilter {
            and: Some(LifecycleRuleAndOperator {
                tags: Some(vec![
                    Tag {
                        key: Some("env".to_string()),
                        value: Some("prod".to_string()),
                    },
                    Tag {
                        key: Some("team".to_string()),
                        value: Some("storage".to_string()),
                    },
                ]),
                ..Default::default()
            }),
            ..Default::default()
        };

        assert!(<LifecycleRuleFilter as Filter>::test_tags(&filter, "env=prod&team=storage"));
        assert!(!<LifecycleRuleFilter as Filter>::test_tags(&filter, "env=prod&team=platform"));
    }

    #[test]
    fn lifecycle_rule_filter_respects_size_bounds() {
        let filter = LifecycleRuleFilter {
            object_size_greater_than: Some(5),
            object_size_less_than: Some(10),
            ..Default::default()
        };

        assert!(!filter.by_size(5));
        assert!(filter.by_size(6));
        assert!(!filter.by_size(10));
    }

    #[test]
    fn lifecycle_rule_filter_without_tag_constraints_accepts_any_tags() {
        let filter = LifecycleRuleFilter {
            object_size_greater_than: Some(5),
            ..Default::default()
        };

        assert!(<LifecycleRuleFilter as Filter>::test_tags(&filter, "env=prod&team=storage"));
        assert!(<LifecycleRuleFilter as Filter>::test_tags(&filter, ""));
    }
}
