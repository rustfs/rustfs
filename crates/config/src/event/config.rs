/// Event configuration module
pub struct EventConfig {
    pub event_type: String,
    pub event_source: String,
    pub event_destination: String,
}

impl EventConfig {
    /// Creates a new instance of `EventConfig` with default values.
    pub fn new() -> Self {
        Self {
            event_type: "default".to_string(),
            event_source: "default".to_string(),
            event_destination: "default".to_string(),
        }
    }
}

impl Default for EventConfig {
    fn default() -> Self {
        Self::new()
    }
}
