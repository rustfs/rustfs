use super::tag::Tag;

// 定义And结构体
#[derive(Debug)]
pub struct And {
    prefix: Option<String>,
    tags: Option<Vec<Tag>>,
}
