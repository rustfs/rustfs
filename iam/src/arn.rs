use std::str::FromStr;

#[derive(PartialEq, Eq, Hash)]
pub struct ARN {
    partition: String,
    service: String,
    region: String,
    resource_type: String,
    resource_id: String,
}

impl FromStr for ARN {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        todo!()
    }
}
