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

mod audit;
mod cert;
mod compress;
mod event;
mod http;
mod hybrid;
mod layer;
mod prefix;
mod readiness;
mod runtime;
mod service_state;

pub(crate) use audit::{start_audit_system, stop_audit_system};
pub(crate) use cert::init_cert;
pub(crate) use event::{init_event_notifier, shutdown_event_notifier};
pub(crate) use http::start_http_server;
pub(crate) use prefix::*;
pub(crate) use readiness::ReadinessGateLayer;
pub(crate) use runtime::get_tokio_runtime_builder;
pub(crate) use service_state::SHUTDOWN_TIMEOUT;
pub(crate) use service_state::ServiceState;
pub(crate) use service_state::ServiceStateManager;
pub(crate) use service_state::ShutdownSignal;
pub(crate) use service_state::wait_for_shutdown;

#[derive(Clone, Copy, Debug)]
pub struct RemoteAddr(pub std::net::SocketAddr);
