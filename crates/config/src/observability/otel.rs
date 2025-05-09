use serde::Deserialize;

/// OpenTelemetry configuration
#[derive(Debug, Deserialize, Clone)]
pub struct OtelConfig {
    pub endpoint: String,
    pub service_name: String,
    pub service_version: String,
    pub resource_attributes: Vec<String>,
}

impl OtelConfig {
    pub fn new() -> Self {
        Self {
            endpoint: "http://localhost:4317".to_string(),
            service_name: "rustfs".to_string(),
            service_version: "0.1.0".to_string(),
            resource_attributes: vec![],
        }
    }
}

impl Default for OtelConfig {
    fn default() -> Self {
        Self::new()
    }
}
