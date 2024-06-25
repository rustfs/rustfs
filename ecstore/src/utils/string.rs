use std::collections::HashMap;
use std::fmt;

#[derive(Debug, Clone)]
pub struct StringSet(HashMap<String, ()>);

impl StringSet {
    // ToSlice - returns StringSet as a vector of strings.
    pub fn to_slice(&self) -> Vec<String> {
        let mut keys = self.0.keys().cloned().collect::<Vec<String>>();
        keys.sort();
        keys
    }

    // IsEmpty - returns whether the set is empty or not.
    pub fn is_empty(&self) -> bool {
        self.0.len() == 0
    }

    // Add - adds a string to the set.
    pub fn add(&mut self, s: String) {
        self.0.insert(s, ());
    }

    // Remove - removes a string from the set. It does nothing if the string does not exist in the set.
    pub fn remove(&mut self, s: &str) {
        self.0.remove(s);
    }

    // Contains - checks if a string is in the set.
    pub fn contains(&self, s: &str) -> bool {
        self.0.contains_key(s)
    }

    // FuncMatch - returns a new set containing each value that passes the match function.
    pub fn func_match<F>(&self, match_fn: F, match_string: &str) -> StringSet
    where
        F: Fn(&str, &str) -> bool,
    {
        StringSet(
            self.0
                .iter()
                .filter(|(k, _)| match_fn(k, match_string))
                .map(|(k, _)| (k.clone(), ()))
                .collect::<HashMap<String, ()>>(),
        )
    }

    // ApplyFunc - returns a new set containing each value processed by 'apply_fn'.
    pub fn apply_func<F>(&self, apply_fn: F) -> StringSet
    where
        F: Fn(&str) -> String,
    {
        StringSet(
            self.0
                .iter()
                .map(|(k, _)| (apply_fn(k), ()))
                .collect::<HashMap<String, ()>>(),
        )
    }

    // Equals - checks whether the given set is equal to the current set or not.
    pub fn equals(&self, other: &StringSet) -> bool {
        if self.0.len() != other.0.len() {
            return false;
        }
        self.0.iter().all(|(k, _)| other.0.contains_key(k))
    }

    // Intersection - returns the intersection with the given set as a new set.
    pub fn intersection(&self, other: &StringSet) -> StringSet {
        StringSet(
            self.0
                .iter()
                .filter(|(k, _)| other.0.contains_key::<String>(k))
                .map(|(k, _)| (k.clone(), ()))
                .collect::<HashMap<String, ()>>(),
        )
    }

    // Difference - returns the difference with the given set as a new set.
    pub fn difference(&self, other: &StringSet) -> StringSet {
        StringSet(
            self.0
                .iter()
                .filter(|(k, _)| !other.0.contains_key::<String>(k))
                .map(|(k, _)| (k.clone(), ()))
                .collect::<HashMap<String, ()>>(),
        )
    }

    // Union - returns the union with the given set as a new set.
    pub fn union(&self, other: &StringSet) -> StringSet {
        let mut new_set = self.clone();
        for (k, _) in other.0.iter() {
            new_set.0.insert(k.clone(), ());
        }
        new_set
    }
}

// Implementing JSON serialization and deserialization would require the serde crate.
// You would also need to implement Display and PartialEq traits for more idiomatic Rust.

// Implementing Display trait to provide a string representation of the set.
impl fmt::Display for StringSet {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.to_slice().join(", "))
    }
}

// Implementing PartialEq and Eq traits to allow comparison of StringSet instances.
impl PartialEq for StringSet {
    fn eq(&self, other: &StringSet) -> bool {
        self.equals(other)
    }
}

impl Eq for StringSet {}

// NewStringSet - creates a new string set.
pub fn new_string_set() -> StringSet {
    StringSet(HashMap::new())
}

// CreateStringSet - creates a new string set with given string values.
pub fn create_string_set(sl: Vec<String>) -> StringSet {
    let mut set = new_string_set();
    for k in sl {
        set.add(k);
    }
    set
}
