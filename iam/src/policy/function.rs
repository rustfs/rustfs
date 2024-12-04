use std::{collections::HashMap, ops::Deref};

use func::Func;
use serde::{de, Deserialize, Serialize};

pub mod addr;
pub mod binary;
pub mod bool_null;
pub mod condition;
pub mod date;
pub mod func;
pub mod key;
pub mod key_name;
pub mod number;
pub mod string;

#[derive(Clone, Default, Serialize)]
pub struct Functions(pub Vec<Func>);

impl Functions {
    pub fn evaluate(&self, values: &HashMap<String, Vec<String>>) -> bool {
        self.0.iter().all(|x| x.evaluate(values))
    }
}

impl<'de> Deserialize<'de> for Functions {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct FuncVisitor;
        use serde::de::Visitor;

        impl<'de> Visitor<'de> for FuncVisitor {
            type Value = Functions;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("Functions")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: de::MapAccess<'de>,
            {
                use serde::de::Error;

                let inner_data = Vec::with_capacity(map.size_hint().unwrap_or(0));
                while let Some(key) = map.next_key::<&str>()? {
                    let mut tokens = key.split(":");
                    let name = tokens.next();
                    let qualifier = tokens.next();

                    // 多个:
                    if tokens.next().is_some() {
                        return Err(A::Error::custom("invalid codition"));
                    }

                    let Some(_name) = name else { return Err(A::Error::custom("invalid codition")) };

                    let f = match qualifier {
                        Some("ForAnyValues") => Func::ForAnyValues,
                        Some("ForAllValues") => Func::ForAllValues,
                        Some(q) => return Err(A::Error::custom(format!("invalid qualifier `{q}`"))),
                        None => Func::ForNormal,
                    };

                    // inner_data.push(f(name.try_into()?))
                }

                Ok(Functions(inner_data))
            }
        }

        deserializer.deserialize_map(FuncVisitor)
    }
}

impl Deref for Functions {
    type Target = Vec<Func>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Value;

#[cfg(test)]
mod tests {

    #[test_case::test_case(
        r#"{
            "Null": {
                "s3:x-amz-server-side-encryption-customer-algorithm": true
            },
            "Null": {
                "s3:x-amz-server-side-encryption-customer-algorithm": "true"
            }
        }"# => true; "1")]
    #[test_case::test_case(r#"{}"# => true; "2")]
    #[test_case::test_case(
        r#"{
            "StringLike": {
                "s3:x-amz-metadata-directive": "REPL*"
            },
            "StringEquals": {
                "s3:x-amz-copy-source": "mybucket/myobject"
            },
            "StringNotEquals": {
                "s3:x-amz-server-side-encryption": "AES256"
            },
            "NotIpAddress": {
                "aws:SourceIp": [
                    "10.1.10.0/24",
                    "10.10.1.0/24"
                ]
            },
            "StringNotLike": {
                "s3:x-amz-storage-class": "STANDARD"
            },
            "Null": {
                "s3:x-amz-server-side-encryption-customer-algorithm": true
            },
            "IpAddress": {
                "aws:SourceIp": [
                    "192.168.1.0/24",
                    "192.168.2.0/24"
                ]
            }
        }"# => true; "3"
    )]
    #[test_case::test_case(
        r#"{
            "StringLike": {
                "s3:x-amz-metadata-directive": "REPL*"
            },
            "StringEquals": {
                "s3:x-amz-copy-source": "mybucket/myobject",
                "s3:prefix": [
                   "",
                   "home/"
                ],
                "s3:delimiter": [
                   "/"
                ]
            },
            "StringNotEquals": {
                "s3:x-amz-server-side-encryption": "AES256"
            },
            "NotIpAddress": {
                "aws:SourceIp": [
                    "10.1.10.0/24",
                    "10.10.1.0/24"
                ]
            },
            "StringNotLike": {
                "s3:x-amz-storage-class": "STANDARD"
            },
            "Null": {
                "s3:x-amz-server-side-encryption-customer-algorithm": true
            },
            "IpAddress": {
                "aws:SourceIp": [
                    "192.168.1.0/24",
                    "192.168.2.0/24"
                ]
            }
        }"# => true; "4"
    )]
    fn test_serde(input: &str) -> bool {
        true
    }
}
