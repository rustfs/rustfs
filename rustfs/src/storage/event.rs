use rustfs_event::{Event, Metadata};

/// Create a new metadata object
#[allow(dead_code)]
pub(crate) fn create_metadata() -> Metadata {
    // Create a new metadata object
    let mut metadata = Metadata::new();
    metadata.set_configuration_id("test-config".to_string());
    // Return the created metadata object
    metadata
}

/// Create a new event object
#[allow(dead_code)]
pub(crate) async fn send_event(event: Event) -> Result<(), Box<dyn std::error::Error>> {
    rustfs_event::send_event(event).await.map_err(|e| e.into())
}
