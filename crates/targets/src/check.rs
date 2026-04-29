// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

/// Check if MQTT Broker is available
///
/// # Arguments
/// * `broker_url` - URL of MQTT Broker, for example `mqtt://localhost:1883`
/// * `topic` - Topic for testing connections
/// * `username` - Optional username for authentication
/// * `password` - Optional password for authentication
/// # Returns
/// * `Ok(())` - If the connection is successful
/// * `Err(TargetError)` - If the check fails.
///   `TargetError::Configuration` indicates a bad configuration (invalid URL, TLS settings, etc.).
///   Other variants indicate a connectivity or runtime failure.
///
/// # Example
/// ```rust,no_run
///  #[tokio::main]
///  async fn main() {
///     let result = rustfs_targets::check_mqtt_broker_available(
///         "mqtt://localhost:1883",
///         "test/topic",
///         Some("myuser"),
///         Some("mypass"),
///     ).await;
///     if result.is_ok() {
///         println!("MQTT Broker is available");
///     } else {
///         println!("MQTT Broker is not available: {}", result.err().unwrap());
///     }
///  }
/// ```
///
pub async fn check_mqtt_broker_available(
    broker_url: &str,
    topic: &str,
    username: Option<&str>,
    password: Option<&str>,
) -> Result<(), crate::TargetError> {
    use crate::target::mqtt::MQTTTlsConfig;

    check_mqtt_broker_available_with_tls(broker_url, topic, username, password, &MQTTTlsConfig::default()).await
}

pub async fn check_mqtt_broker_available_with_tls(
    broker_url: &str,
    topic: &str,
    username: Option<&str>,
    password: Option<&str>,
    tls: &crate::target::mqtt::MQTTTlsConfig,
) -> Result<(), crate::TargetError> {
    use crate::target::mqtt::build_mqtt_options;
    use rumqttc::{AsyncClient, QoS};

    let url = rustfs_utils::parse_url(broker_url)
        .map_err(|e| crate::TargetError::Configuration(format!("Broker URL parsing failed: {e}")))?;
    let url = url.url();

    // build_mqtt_options returns TargetError directly; Configuration variants propagate as-is.
    let mqtt_options = build_mqtt_options(
        "rustfs_check".to_string(),
        url,
        username,
        password,
        tls,
        std::time::Duration::from_secs(5),
        None,
    )?;
    let (client, mut eventloop) = AsyncClient::new(mqtt_options, 1);

    // Try to connect and subscribe
    client
        .subscribe(topic, QoS::AtLeastOnce)
        .await
        .map_err(|e| crate::TargetError::Network(format!("MQTT subscription failed: {e}")))?;
    // Wait for eventloop to receive at least one event
    match tokio::time::timeout(std::time::Duration::from_secs(3), eventloop.poll()).await {
        Ok(Ok(_)) => Ok(()),
        Ok(Err(e)) => Err(crate::TargetError::Network(format!("MQTT connection failed: {e}"))),
        Err(_) => Err(crate::TargetError::Timeout("MQTT connection timed out".to_string())),
    }
}

pub async fn check_nats_server_available(args: &crate::target::nats::NATSArgs) -> Result<(), crate::TargetError> {
    match tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let client = crate::target::nats::connect_nats(args).await?;
        client
            .flush()
            .await
            .map_err(|e| crate::TargetError::Network(format!("NATS connection check failed: {e}")))?;
        client
            .drain()
            .await
            .map_err(|e| crate::TargetError::Network(format!("Failed to close NATS check connection: {e}")))?;
        Ok(())
    })
    .await
    {
        Ok(result) => result,
        Err(_) => Err(crate::TargetError::Timeout("NATS connection timed out".to_string())),
    }
}

pub async fn check_pulsar_broker_available(args: &crate::target::pulsar::PulsarArgs) -> Result<(), crate::TargetError> {
    match tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let client = crate::target::pulsar::connect_pulsar(args).await?;
        client
            .lookup_partitioned_topic(args.topic.clone())
            .await
            .map_err(|e| crate::TargetError::Network(format!("Pulsar topic lookup failed: {e}")))?;
        Ok(())
    })
    .await
    {
        Ok(result) => result,
        Err(_) => Err(crate::TargetError::Timeout("Pulsar connection timed out".to_string())),
    }
}

pub async fn check_kafka_broker_available(args: &crate::target::kafka::KafkaArgs) -> Result<(), crate::TargetError> {
    use rustfs_kafka_async::error::{ConnectionError, Error as KafkaError};
    use rustfs_kafka_async::{AsyncProducer, AsyncProducerConfig, RequiredAcks, SecurityConfig};
    use std::time::Duration;

    let map_kafka_error = |err: KafkaError, context: &str| match err {
        KafkaError::Connection(ConnectionError::NoHostReachable) => crate::TargetError::NotConnected,
        KafkaError::Connection(ConnectionError::Timeout(_)) => crate::TargetError::Timeout(format!("{context}: {err}")),
        KafkaError::Connection(_) => crate::TargetError::Network(format!("{context}: {err}")),
        KafkaError::Config(_) => crate::TargetError::Configuration(format!("{context}: {err}")),
        _ => crate::TargetError::Request(format!("{context}: {err}")),
    };

    let acks = match args.acks {
        0 => RequiredAcks::None,
        1 => RequiredAcks::One,
        _ => RequiredAcks::All,
    };

    let mut config = AsyncProducerConfig::new()
        .with_ack_timeout(Duration::from_secs(5))
        .with_required_acks(acks);

    if args.tls_enable {
        let mut security = SecurityConfig::new();
        if !args.tls_ca.is_empty() {
            security = security.with_ca_cert(args.tls_ca.clone());
        }
        if !args.tls_client_cert.is_empty() && !args.tls_client_key.is_empty() {
            security = security.with_client_cert(args.tls_client_cert.clone(), args.tls_client_key.clone());
        }
        config = config.with_security(security);
    }

    match tokio::time::timeout(Duration::from_secs(5), async {
        let _ = AsyncProducer::from_hosts_with_config(args.brokers.clone(), config)
            .await
            .map_err(|err| map_kafka_error(err, "Kafka broker check failed to create producer"))?;
        Ok(())
    })
    .await
    {
        Ok(result) => result,
        Err(_) => Err(crate::TargetError::Timeout("Kafka connection timed out".to_string())),
    }
}

pub async fn check_redis_server_available(args: &crate::target::redis::RedisArgs) -> Result<(), crate::TargetError> {
    match tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let client = crate::target::redis::build_redis_client(args)?;
        crate::target::redis::ping_redis_server(&client, args).await
    })
    .await
    {
        Ok(result) => result,
        Err(_) => Err(crate::TargetError::Timeout("Redis connection timed out".to_string())),
    }
}
