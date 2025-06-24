#![allow(clippy::all)]

pub struct TargetID {
    id: String,
    name: String,
}

impl TargetID {
    fn to_string(&self) -> String {
        format!("{}:{}", self.id, self.name)
    }
}
