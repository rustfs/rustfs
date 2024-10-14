use std::{collections::HashMap, marker::PhantomData};

use serde::{
    de::{self, Visitor},
    Deserialize, Deserializer, Serialize,
};

use super::{condition::Condition, key::Key};

#[derive(Clone, Serialize, Deserialize)]
pub enum Func {
    ForAnyValues(Vec<Condition>),
    ForAllValues(Vec<Condition>),
    ForNormal(Vec<Condition>),
}

impl Func {
    pub fn evaluate(&self, values: &HashMap<String, Vec<String>>) -> bool {
        match self {
            Self::ForAnyValues(conditions) => conditions.iter().all(|x| x.evaluate(true, values)),
            Self::ForAllValues(conditions) => conditions.iter().all(|x| x.evaluate(false, values)),
            Self::ForNormal(conditions) => conditions.iter().all(|x| x.evaluate(false, values)),
        }
    }
}

#[cfg_attr(test, derive(PartialEq, Eq, Debug))]
pub struct InnerFunc<T> {
    pub key: Key,
    pub values: T,
}

impl<T: Clone> Clone for InnerFunc<T> {
    fn clone(&self) -> Self {
        Self {
            key: self.key.clone(),
            values: self.values.clone(),
        }
    }
}

impl<T: Serialize> Serialize for InnerFunc<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeMap;

        let mut map = serializer.serialize_map(Some(1))?;
        map.serialize_key(&self.key)?;
        map.serialize_value(&self.values)?;
        map.end()
    }
}

impl<'de, T> Deserialize<'de> for InnerFunc<T>
where
    T: Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct FuncVisitor<T>(PhantomData<T>);
        impl<'v, T> Visitor<'v> for FuncVisitor<T>
        where
            T: Deserialize<'v>,
        {
            type Value = InnerFunc<T>;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("struct StringFunc")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: de::MapAccess<'v>,
            {
                use serde::de::Error;

                let Some((key, values)) = map.next_entry::<Key, T>()? else {
                    return Err(A::Error::custom("no k-v pair"));
                };

                Ok(InnerFunc { key, values })
            }
        }

        deserializer.deserialize_map(FuncVisitor::<T>(PhantomData))
    }
}
