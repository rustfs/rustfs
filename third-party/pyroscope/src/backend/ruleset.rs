use super::{StackTrace, Tag};
use crate::error::Result;
use std::collections::HashSet;
use std::sync::{Arc, Mutex};

#[derive(Debug, Eq, PartialEq, Hash, Clone)]
pub struct ThreadTag {
    tid: crate::utils::ThreadId,
    tag: Tag,
}

impl ThreadTag {
    pub fn new(tid: crate::ThreadId, tag: Tag) -> Self {
        Self { tid, tag }
    }
}

#[derive(Debug, Default, Clone)]
pub struct ThreadTagsSet {
    pub rules: Arc<Mutex<HashSet<ThreadTag>>>,
}

impl ThreadTagsSet {
    pub fn new() -> Self {
        Self {
            rules: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    pub fn add(&self, rule: ThreadTag) -> Result<bool> {
        let rules = self.rules.clone();

        let insert = rules.lock()?.insert(rule);

        Ok(insert)
    }

    pub fn remove(&self, rule: ThreadTag) -> Result<bool> {
        let rules = self.rules.clone();

        let remove = rules.lock()?.remove(&rule);

        Ok(remove)
    }

    #[cfg(test)]
    pub fn thread_tags(&self, tid: crate::ThreadId) -> Vec<Tag> {
        let s = StackTrace {
            pid: None,
            thread_id: Some(tid.clone()),
            thread_name: None,
            frames: vec![],
            metadata: Default::default(),
        };
        let tags: Vec<Tag> = s.add_tag_rules(self).metadata.tags.into_iter().collect();
        tags
    }
}

impl StackTrace {
    pub fn add_tag_rules(self, other: &ThreadTagsSet) -> Self {
        let mut metadata = self.metadata;

        if let Ok(rules) = other.rules.lock() {
            rules.iter().for_each(|rule| {
                if let Some(stack_thread_id) = &self.thread_id {
                    if rule.tid == *stack_thread_id {
                        metadata.add_tag(rule.tag.clone());
                    }
                }
            })
        }

        Self {
            pid: self.pid,
            thread_id: self.thread_id,
            thread_name: self.thread_name,
            frames: self.frames,
            metadata,
        }
    }
}
