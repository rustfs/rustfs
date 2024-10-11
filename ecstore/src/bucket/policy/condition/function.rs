use super::{
    key::{Key, KeySet},
    keyname::KeyName,
    name::Name,
};
use serde::{
    de::{MapAccess, Visitor},
    ser::SerializeMap,
    Deserialize, Serialize,
};
use std::{
    collections::{HashMap, HashSet},
    fmt::{self, Debug, Display},
    marker::PhantomData,
};

// 定义ValueSet类型
pub type ValueSet = HashSet<String>;

// 定义Function trait
pub trait FunctionApi: 'static + Send + Sync {
    // evaluate方法
    fn evaluate(&self, values: &HashMap<String, Vec<String>>) -> bool;

    // key方法
    fn key(&self) -> Key;

    // name方法
    fn name(&self) -> Name;

    // String方法
    fn to_string(&self) -> String;

    // to_map方法
    fn to_map(&self) -> HashMap<Key, ValueSet>;

    fn clone_box(&self) -> Box<dyn FunctionApi>;
}

// #[derive(Debug, Deserialize, Serialize, Clone)]
// pub enum Function {
//     Test(TestFunction),
// }

// impl FunctionApi for Function {
//     // evaluate方法
//     fn evaluate(&self, values: &HashMap<String, Vec<String>>) -> bool {
//         match self {
//             Function::Test(f) => f.evaluate(values),
//         }
//     }

//     // key方法
//     fn key(&self) -> Key {
//         match self {
//             Function::Test(f) => f.key(),
//         }
//     }

//     // name方法
//     fn name(&self) -> Name {
//         match self {
//             Function::Test(f) => f.name(),
//         }
//     }

//     // String方法
//     fn to_string(&self) -> String {
//         match self {
//             Function::Test(f) => f.to_string(),
//         }
//     }

//     // to_map方法
//     fn to_map(&self) -> HashMap<Key, ValueSet> {
//         match self {
//             Function::Test(f) => f.to_map(),
//         }
//     }

//     fn clone_box(&self) -> Box<dyn FunctionApi> {
//         match self {
//             Function::Test(f) => f.clone_box(),
//         }
//     }
// }

// 定义Functions类型
#[derive(Default)]
pub struct Functions(Vec<Box<dyn FunctionApi>>);

impl Functions {
    pub fn evaluate(&self, values: &HashMap<String, Vec<String>>) -> bool {
        for f in self.0.iter() {
            if f.evaluate(values) {
                return true;
            }
        }

        false
    }
    pub fn keys(&self) -> KeySet {
        let mut set = KeySet::new();
        for f in self.0.iter() {
            set.add(f.key())
        }
        set
    }
}

impl Debug for Functions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let funs: Vec<String> = self.0.iter().map(|v| v.to_string()).collect();
        f.debug_list().entries(funs.iter()).finish()
    }
}

impl Display for Functions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let funs: Vec<String> = self.0.iter().map(|v| v.to_string()).collect();
        write!(f, "{:?}", funs)
    }
}

impl Clone for Functions {
    fn clone(&self) -> Self {
        let mut list = Vec::new();
        for v in self.0.iter() {
            list.push(v.clone_box())
        }

        Functions(list)
    }
}

impl PartialEq for Functions {
    fn eq(&self, other: &Self) -> bool {
        if self.0.len() != other.0.len() {
            return false;
        }

        for v in self.0.iter() {
            let s = v.to_string();
            let mut found = false;
            for o in other.0.iter() {
                if s == o.to_string() {
                    found = true;
                    break;
                }
            }

            if !found {
                return false;
            }
        }

        true
    }
}

impl Eq for Functions {}

type FunctionsMap = HashMap<String, HashMap<String, ValueSet>>;

impl Serialize for Functions {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut nm: FunctionsMap = HashMap::new();
        for f in self.0.iter() {
            let fname = f.name().to_string();

            if !nm.contains_key(&fname) {
                nm.insert(fname.clone(), HashMap::new());
            }

            for (k, v) in f.to_map() {
                if let Some(hm) = nm.get_mut(&fname) {
                    hm.insert(k.to_string(), v);
                }
            }
        }

        let mut map = serializer.serialize_map(Some(nm.len()))?;
        for (k, v) in nm.iter() {
            map.serialize_entry(k, v)?;
        }

        map.end()
    }
}

struct MyMapVisitor {
    marker: PhantomData<fn() -> FunctionsMap>,
}

impl MyMapVisitor {
    fn new() -> Self {
        MyMapVisitor { marker: PhantomData }
    }
}

// This is the trait that Deserializers are going to be driving. There
// is one method for each type of data that our type knows how to
// deserialize from. There are many other methods that are not
// implemented here, for example deserializing from integers or strings.
// By default those methods will return an error, which makes sense
// because we cannot deserialize a MyMap from an integer or string.
impl<'de> Visitor<'de> for MyMapVisitor {
    // The type that our Visitor is going to produce.
    type Value = FunctionsMap;

    // Format a message stating what data this Visitor expects to receive.
    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a very special map")
    }

    // Deserialize MyMap from an abstract "map" provided by the
    // Deserializer. The MapAccess input is a callback provided by
    // the Deserializer to let us see each entry in the map.
    fn visit_map<M>(self, mut access: M) -> Result<Self::Value, M::Error>
    where
        M: MapAccess<'de>,
    {
        let mut map = FunctionsMap::with_capacity(access.size_hint().unwrap_or(0));

        // While there are entries remaining in the input, add them
        // into our map.
        while let Some((key, value)) = access.next_entry()? {
            map.insert(key, value);
        }

        Ok(map)
    }
}

// This is the trait that informs Serde how to deserialize MyMap.
impl<'de> Deserialize<'de> for Functions {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        // Instantiate our Visitor and ask the Deserializer to drive
        // it over the input data, resulting in an instance of MyMap.
        let _map = deserializer.deserialize_map(MyMapVisitor::new())?;

        // TODO: FIXME: create functions from name

        Ok(Functions(Vec::new()))
    }
}

// impl<'de> Deserialize<'de> for Functions {
//     fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
//     where
//         D: serde::Deserializer<'de>,
//     {
//         todo!()
//     }
// }

#[derive(Debug, Deserialize, Serialize, Default, Clone)]
pub struct TestFunction {}

impl FunctionApi for TestFunction {
    // evaluate方法
    fn evaluate(&self, _values: &HashMap<String, Vec<String>>) -> bool {
        true
    }

    // key方法
    fn key(&self) -> Key {
        Key {
            name: KeyName::JWTPrefUsername,
            variable: "".to_string(),
        }
    }

    // name方法
    fn name(&self) -> Name {
        Name::StringEquals
    }

    // String方法
    fn to_string(&self) -> String {
        Name::StringEquals.to_string()
    }

    // to_map方法
    fn to_map(&self) -> HashMap<Key, ValueSet> {
        HashMap::new()
    }

    fn clone_box(&self) -> Box<dyn FunctionApi> {
        Box::new(self.clone())
    }
}
