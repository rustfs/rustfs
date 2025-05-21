use crate::Error;
use crate::Event;
use crate::MqttConfig;
use crate::{ChannelAdapter, ChannelAdapterType};
use async_trait::async_trait;
use rumqttc::{AsyncClient, MqttOptions, QoS};
use std::time::Duration;
use tokio::time::sleep;

/// MQTT adapter for sending events to an MQTT broker.
pub struct MqttAdapter {
    client: AsyncClient,
    topic: String,
    max_retries: u32,
}

impl MqttAdapter {
    /// Creates a new MQTT adapter.
    pub fn new(config: &MqttConfig) -> (Self, rumqttc::EventLoop) {
        let mqtt_options = MqttOptions::new(&config.client_id, &config.broker, config.port);
        let (client, event_loop) = rumqttc::AsyncClient::new(mqtt_options, 10);
        (
            Self {
                client,
                topic: config.topic.clone(),
                max_retries: config.max_retries,
            },
            event_loop,
        )
    }
}

#[async_trait]
impl ChannelAdapter for MqttAdapter {
    fn name(&self) -> String {
        ChannelAdapterType::Mqtt.to_string()
    }

    async fn send(&self, event: &Event) -> Result<(), Error> {
        let payload = serde_json::to_string(event).map_err(Error::Serde)?;
        let mut attempt = 0;
        loop {
            match self
                .client
                .publish(&self.topic, QoS::AtLeastOnce, false, payload.clone())
                .await
            {
                Ok(()) => return Ok(()),
                Err(e) if attempt < self.max_retries => {
                    attempt += 1;
                    tracing::warn!("MQTT attempt {} failed: {}. Retrying...", attempt, e);
                    sleep(Duration::from_secs(2u64.pow(attempt))).await;
                }
                Err(e) => return Err(Error::Mqtt(e)),
            }
        }
    }
}
