use std::collections::HashSet;

#[derive(Debug, Clone, Default)]
pub struct Principal {
    aws: HashSet<String>,
}
