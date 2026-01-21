---
name: domain-iot
description: "Use when building IoT apps. Keywords: IoT, Internet of Things, sensor, MQTT, device, edge computing, telemetry, actuator, smart home, gateway, protocol, 物联网, 传感器, 边缘计算, 智能家居"
---

# IoT Domain

> **Layer 3: Domain Constraints**

## Domain Constraints → Design Implications

| Domain Rule | Design Constraint | Rust Implication |
|-------------|-------------------|------------------|
| Unreliable network | Offline-first | Local buffering |
| Power constraints | Efficient code | Sleep modes, minimal alloc |
| Resource limits | Small footprint | no_std where needed |
| Security | Encrypted comms | TLS, signed firmware |
| Reliability | Self-recovery | Watchdog, error handling |
| OTA updates | Safe upgrades | Rollback capability |

---

## Critical Constraints

### Network Unreliability

```
RULE: Network can fail at any time
WHY: Wireless, remote locations
RUST: Local queue, retry with backoff
```

### Power Management

```
RULE: Minimize power consumption
WHY: Battery life, energy costs
RUST: Sleep modes, efficient algorithms
```

### Device Security

```
RULE: All communication encrypted
WHY: Physical access possible
RUST: TLS, signed messages
```

---

## Trace Down ↓

From constraints to design (Layer 2):

```
"Need offline-first design"
    ↓ m12-lifecycle: Local buffer with persistence
    ↓ m13-domain-error: Retry with backoff

"Need power efficiency"
    ↓ domain-embedded: no_std patterns
    ↓ m10-performance: Minimal allocations

"Need reliable messaging"
    ↓ m07-concurrency: Async with timeout
    ↓ MQTT: QoS levels
```

---

## Environment Comparison

| Environment | Stack | Crates |
|-------------|-------|--------|
| Linux gateway | tokio + std | rumqttc, reqwest |
| MCU device | embassy + no_std | embedded-hal |
| Hybrid | Split workloads | Both |

## Key Crates

| Purpose | Crate |
|---------|-------|
| MQTT (std) | rumqttc, paho-mqtt |
| Embedded | embedded-hal, embassy |
| Async (std) | tokio |
| Async (no_std) | embassy |
| Logging (no_std) | defmt |
| Logging (std) | tracing |

## Design Patterns

| Pattern | Purpose | Implementation |
|---------|---------|----------------|
| Pub/Sub | Device comms | MQTT topics |
| Edge compute | Local processing | Filter before upload |
| OTA updates | Firmware upgrade | Signed + rollback |
| Power mgmt | Battery life | Sleep + wake events |
| Store & forward | Network reliability | Local queue |

## Code Pattern: MQTT Client

```rust
use rumqttc::{AsyncClient, MqttOptions, QoS};

async fn run_mqtt() -> anyhow::Result<()> {
    let mut options = MqttOptions::new("device-1", "broker.example.com", 1883);
    options.set_keep_alive(Duration::from_secs(30));

    let (client, mut eventloop) = AsyncClient::new(options, 10);

    // Subscribe to commands
    client.subscribe("devices/device-1/commands", QoS::AtLeastOnce).await?;

    // Publish telemetry
    tokio::spawn(async move {
        loop {
            let data = read_sensor().await;
            client.publish("devices/device-1/telemetry", QoS::AtLeastOnce, false, data).await.ok();
            tokio::time::sleep(Duration::from_secs(60)).await;
        }
    });

    // Process events
    loop {
        match eventloop.poll().await {
            Ok(event) => handle_event(event).await,
            Err(e) => {
                tracing::error!("MQTT error: {}", e);
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        }
    }
}
```

---

## Common Mistakes

| Mistake | Domain Violation | Fix |
|---------|-----------------|-----|
| No retry logic | Lost data | Exponential backoff |
| Always-on radio | Battery drain | Sleep between sends |
| Unencrypted MQTT | Security risk | TLS |
| No local buffer | Network outage = data loss | Persist locally |

---

## Trace to Layer 1

| Constraint | Layer 2 Pattern | Layer 1 Implementation |
|------------|-----------------|------------------------|
| Offline-first | Store & forward | Local queue + flush |
| Power efficiency | Sleep patterns | Timer-based wake |
| Network reliability | Retry | tokio-retry, backoff |
| Security | TLS | rustls, native-tls |

---

## Related Skills

| When | See |
|------|-----|
| Embedded patterns | domain-embedded |
| Async patterns | m07-concurrency |
| Error recovery | m13-domain-error |
| Performance | m10-performance |
