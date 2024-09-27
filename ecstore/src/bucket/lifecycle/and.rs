use super::{prefix::Prefix, tag::Tag};

pub struct And {
    pub object_size_greater_than: i64,
    pub object_size_less_than: i64,
    pub prefix: Prefix,
    pub tags: Vec<Tag>,
}
