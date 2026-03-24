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

use super::*;
use rustfs_notify::notification_system;

impl NodeService {
    pub(super) async fn handle_get_live_events(
        &self,
        request: Request<GetLiveEventsRequest>,
    ) -> Result<Response<GetLiveEventsResponse>, Status> {
        let request = request.into_inner();
        let limit = usize::try_from(request.limit).unwrap_or(0).clamp(1, 256);

        let Some(system) = notification_system() else {
            return Ok(Response::new(GetLiveEventsResponse {
                success: true,
                events: Bytes::new(),
                next_sequence: request.after_sequence,
                truncated: false,
                error_info: None,
            }));
        };

        let batch = system.recent_live_events_since(request.after_sequence, limit).await;
        let events = batch.events.into_iter().map(|event| (*event).clone()).collect::<Vec<_>>();

        let payload = match serde_json::to_vec(&events) {
            Ok(payload) => payload,
            Err(err) => {
                return Ok(Response::new(GetLiveEventsResponse {
                    success: false,
                    events: Bytes::new(),
                    next_sequence: request.after_sequence,
                    truncated: false,
                    error_info: Some(format!("failed to serialize live events: {err}")),
                }));
            }
        };

        Ok(Response::new(GetLiveEventsResponse {
            success: true,
            events: payload.into(),
            next_sequence: batch.next_sequence,
            truncated: batch.truncated,
            error_info: None,
        }))
    }
}
