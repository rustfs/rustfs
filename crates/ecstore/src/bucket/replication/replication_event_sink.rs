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

pub(crate) use crate::services::event_notification::EventArgs;

use super::runtime_boundary;

pub(crate) fn send_event(args: EventArgs) {
    crate::services::event_notification::send_event(args);
}

pub(crate) fn send_local_event(mut args: EventArgs) {
    args.host = runtime_boundary::default_local_node_name();
    send_event(args);
}
