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

use std::time::Duration;

use serde::{Deserialize, Serialize};

use super::DiagnosticReceipt;
use crate::connect::config::HeartbeatConfig;
use crate::connect::heartbeat::HeartbeatError;
use crate::connect::telemetry::{TelemetryDelivery, TelemetryTransport};

const PROTOCOL_VERSION: &str = "v1";

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct DiagnosticReceiptRequest<'a> {
    protocol_version: &'static str,
    request_id: &'a str,
    #[serde(flatten)]
    receipt: &'a DiagnosticReceipt,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct DiagnosticReceiptResponse {
    accepted_version: String,
}

pub(crate) enum DiagnosticReceiptDelivery {
    Accepted,
    Retry { retry_after: Option<Duration> },
    AuthenticationStopped,
    Rejected,
}

pub(crate) struct DiagnosticReceiptSender {
    transport: TelemetryTransport,
}

impl DiagnosticReceiptSender {
    pub(crate) fn new(config: HeartbeatConfig) -> Result<Self, HeartbeatError> {
        Ok(Self {
            transport: TelemetryTransport::new(config)?,
        })
    }

    pub(crate) async fn send(&self, receipt: &DiagnosticReceipt) -> Result<DiagnosticReceiptDelivery, HeartbeatError> {
        let request = DiagnosticReceiptRequest {
            protocol_version: PROTOCOL_VERSION,
            request_id: &receipt.receipt_id,
            receipt,
        };
        Ok(match self.transport.post("diagnosticExecutionReceipts", &request).await? {
            TelemetryDelivery::Accepted { body, .. } => {
                let response: DiagnosticReceiptResponse = serde_json::from_slice(&body).map_err(|_| HeartbeatError::Response)?;
                if response.accepted_version != PROTOCOL_VERSION {
                    return Err(HeartbeatError::Response);
                }
                DiagnosticReceiptDelivery::Accepted
            }
            TelemetryDelivery::Retry { retry_after } => DiagnosticReceiptDelivery::Retry { retry_after },
            TelemetryDelivery::AuthenticationStopped { .. } => DiagnosticReceiptDelivery::AuthenticationStopped,
            TelemetryDelivery::Rejected { .. } => DiagnosticReceiptDelivery::Rejected,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connect::diagnostics::ReceiptOutcome;

    #[test]
    fn request_is_the_versioned_frozen_receipt_with_receipt_id_as_idempotency_key() {
        let receipt = DiagnosticReceipt {
            receipt_id: "123e4567-e89b-42d3-a456-426614174001".to_owned(),
            policy_revision: 8,
            tool_id: "inventory.environment".to_owned(),
            interval_started_at: "2030-01-01T00:00:00Z".to_owned(),
            completed_at: "2030-01-01T00:00:07Z".to_owned(),
            outcome: ReceiptOutcome::Failed,
            attempt_count: 3,
            reason: Some("connect_diagnostic_inventory_unavailable".to_owned()),
            result_sha256: None,
            result_bytes: None,
        };
        let value = serde_json::to_value(DiagnosticReceiptRequest {
            protocol_version: PROTOCOL_VERSION,
            request_id: &receipt.receipt_id,
            receipt: &receipt,
        })
        .expect("receipt request");

        assert_eq!(value["protocolVersion"], "v1");
        assert_eq!(value["requestId"], value["receiptId"]);
        assert_eq!(value["attemptCount"], 3);
        assert_eq!(value["outcome"], "FAILED");
        assert!(value.get("command").is_none());
    }
}
