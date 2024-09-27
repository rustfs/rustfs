use super::and::And;
use super::tag::Tag;
use std::collections::HashMap;

#[derive(Debug)]
pub struct Filter {
    prefix: String,
    and: And,
    tag: Tag,
    cached_tags: HashMap<String, String>,
}
