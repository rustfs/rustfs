use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Event(String);

impl From<String> for Event {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl AsRef<str> for Event {
    fn as_ref(&self) -> &str {
        self.0.as_ref()
    }
}

impl From<Event> for String {
    fn from(value: Event) -> Self {
        value.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_string() {
        let event = Event::from("s3:ObjectCreated:Put".to_owned());
        assert_eq!(event.as_ref(), "s3:ObjectCreated:Put");
    }

    #[test]
    fn into_string() {
        let event = Event::from("s3:ObjectRemoved:Delete".to_owned());
        let s: String = event.into();
        assert_eq!(s, "s3:ObjectRemoved:Delete");
    }

    #[test]
    fn as_ref_str() {
        let event = Event::from(String::new());
        let s: &str = event.as_ref();
        assert_eq!(s, "");
    }

    #[test]
    fn debug_impl() {
        let event = Event::from("test".to_owned());
        let debug = format!("{event:?}");
        assert!(debug.contains("test"));
    }

    #[test]
    fn clone_and_eq() {
        let event = Event::from("ev".to_owned());
        let cloned = event.clone();
        assert_eq!(event, cloned);
    }

    #[test]
    fn serde_roundtrip() {
        let event = Event::from("s3:ObjectCreated:*".to_owned());
        let json = serde_json::to_string(&event).unwrap();
        let deserialized: Event = serde_json::from_str(&json).unwrap();
        assert_eq!(event, deserialized);
    }
}
