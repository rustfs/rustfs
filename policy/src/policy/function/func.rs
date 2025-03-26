use std::marker::PhantomData;

use serde::{
    de::{self, Visitor},
    Deserialize, Deserializer, Serialize,
};

use super::key::Key;

#[derive(PartialEq, Eq, Debug)]
pub struct InnerFunc<T>(pub(crate) Vec<FuncKeyValue<T>>);

#[derive(PartialEq, Eq, Debug)]
pub struct FuncKeyValue<T> {
    pub key: Key,
    pub values: T,
}

impl<T: Clone> Clone for FuncKeyValue<T> {
    fn clone(&self) -> Self {
        Self {
            key: self.key.clone(),
            values: self.values.clone(),
        }
    }
}

impl<T: Clone> Clone for InnerFunc<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T: Serialize> Serialize for InnerFunc<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeMap;

        let mut map = serializer.serialize_map(Some(self.0.len()))?;

        for kv in self.0.iter() {
            map.serialize_key(&kv.key)?;
            map.serialize_value(&kv.values)?;
        }

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

                let mut inner = Vec::with_capacity(map.size_hint().unwrap_or(0));
                while let Some((key, values)) = map.next_entry::<Key, T>()? {
                    inner.push(FuncKeyValue { key, values });
                }

                if inner.is_empty() {
                    return Err(Error::custom("has no condition key"));
                }

                Ok(InnerFunc(inner))
            }
        }

        deserializer.deserialize_map(FuncVisitor::<T>(PhantomData))
    }
}
