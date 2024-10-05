use super::and::And;
use super::tag::Tag;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Deserialize, Serialize, Default,Clone)]
pub struct Filter {
    prefix: String,
    and: And,
    tag: Tag,
    cached_tags: HashMap<String, String>,
}
