use super::key_name::KeyName;
use crate::policy::{Error, Validator};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(test, derive(PartialEq, Eq))]
#[serde(into = "String")]
#[serde(try_from = "&str")]
pub struct Key {
    pub name: KeyName,
    pub variable: Option<String>,
}

impl Validator for Key {}

impl Key {
    pub fn is(&self, other: &KeyName) -> bool {
        self.name.eq(other)
    }

    pub fn var_name(&self) -> String {
        self.name.var_name()
    }

    pub fn name(&self) -> String {
        if let Some(ref x) = self.variable {
            format!("{}/{}", self.name.name(), x)
        } else {
            self.name.name().to_owned()
        }
    }
}

impl From<Key> for String {
    fn from(value: Key) -> Self {
        let mut data = String::from(Into::<&str>::into(&value.name));
        if let Some(x) = value.variable.as_ref() {
            data.push('/');
            data.push_str(&x);
        }
        data
    }
}

impl TryFrom<&str> for Key {
    type Error = Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let mut iter = value.splitn(2, '/');
        let name = iter.next().ok_or_else(|| Error::InvalidKey(value.to_string()))?;
        let variable = iter.next().map(Into::into);

        Ok(Self {
            name: KeyName::try_from(name)?,
            variable,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::Key;
    use test_case::test_case;

    fn new_key(name: &str, value: Option<&str>) -> Key {
        Key {
            name: name.try_into().unwrap(),
            variable: value.map(ToString::to_string),
        }
    }

    #[test_case(new_key("s3:x-amz-copy-source", Some("aaa")), r#""s3:x-amz-copy-source/aaa""#)]
    #[test_case(new_key("s3:x-amz-copy-source", None), r#""s3:x-amz-copy-source""#)]
    #[test_case(new_key("aws:Referer", Some("bbb")), r#""aws:Referer/bbb""#)]
    #[test_case(new_key("aws:Referer", None), r#""aws:Referer""#)]
    #[test_case(new_key("jwt:website", None), r#""jwt:website""#)]
    #[test_case(new_key("jwt:website", Some("aaa")), r#""jwt:website/aaa""#)]
    #[test_case(new_key("svc:DurationSeconds", None), r#""svc:DurationSeconds""#)]
    #[test_case(new_key("svc:DurationSeconds", Some("aaa")), r#""svc:DurationSeconds/aaa""#)]
    fn test_serialize_successful(key: Key, except: &str) -> Result<(), serde_json::Error> {
        let val = serde_json::to_string(&key)?;
        assert_eq!(val.as_str(), except);
        Ok(())
    }

    #[test_case("s3:x-amz-copy-source1/aaa")]
    #[test_case("s33:x-amz-copy-source")]
    #[test_case("aw2s:Referer/bbb")]
    #[test_case("aws:Referera")]
    #[test_case("jwdt:website")]
    #[test_case("jwt:dwebsite/aaa")]
    #[test_case("sfvc:DuratdionSeconds")]
    #[test_case("svc:DursationSeconds/aaa")]
    fn test_deserialize_falied(key: &str) {
        let val = serde_json::from_str::<Key>(key);
        assert!(val.is_err());
    }

    #[test_case(new_key("s3:x-amz-copy-source", Some("aaa")), r#""s3:x-amz-copy-source/aaa""#)]
    #[test_case(new_key("s3:x-amz-copy-source", None), r#""s3:x-amz-copy-source""#)]
    #[test_case(new_key("aws:Referer", Some("bbb")), r#""aws:Referer/bbb""#)]
    #[test_case(new_key("aws:Referer", None), r#""aws:Referer""#)]
    #[test_case(new_key("jwt:website", None), r#""jwt:website""#)]
    #[test_case(new_key("jwt:website", Some("aaa")), r#""jwt:website/aaa""#)]
    #[test_case(new_key("svc:DurationSeconds", None), r#""svc:DurationSeconds""#)]
    #[test_case(new_key("svc:DurationSeconds", Some("aaa")), r#""svc:DurationSeconds/aaa""#)]
    fn test_deserialize(except: Key, input: &str) -> Result<(), serde_json::Error> {
        let v = serde_json::from_str::<Key>(input)?;
        assert_eq!(v.name, except.name);
        assert_eq!(v.variable, except.variable);

        Ok(())
    }
}
