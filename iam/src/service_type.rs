use crate::Error;

#[derive(PartialEq, Eq, Debug)]
pub enum ServiceType {
    S3,
    STS,
}

impl TryFrom<&str> for ServiceType {
    type Error = Error;
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let service_type = match value {
            "s3" => Self::S3,
            "sts" => Self::STS,
            _ => return Err(Error::InvalidServiceType(value.to_owned())),
        };

        Ok(service_type)
    }
}
