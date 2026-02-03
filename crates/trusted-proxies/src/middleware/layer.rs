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

//! Tower layer implementation for the trusted proxy middleware.

use std::sync::Arc;
use tower::Layer;

use crate::ProxyMetrics;
use crate::ProxyValidator;
use crate::TrustedProxyConfig;
use crate::TrustedProxyMiddleware;

/// Tower Layer for the trusted proxy middleware.
#[derive(Clone, Debug)]
pub struct TrustedProxyLayer {
    /// The validator used to verify proxy chains.
    pub(crate) validator: Arc<ProxyValidator>,
    /// Whether the middleware is enabled.
    pub(crate) enabled: bool,
}

impl TrustedProxyLayer {
    /// Creates a new `TrustedProxyLayer`.
    pub fn new(config: TrustedProxyConfig, metrics: Option<ProxyMetrics>, enabled: bool) -> Self {
        let validator = ProxyValidator::new(config, metrics);

        Self {
            validator: Arc::new(validator),
            enabled,
        }
    }

    /// Creates a new `TrustedProxyLayer` that is enabled by default.
    pub fn enabled(config: TrustedProxyConfig, metrics: Option<ProxyMetrics>) -> Self {
        Self::new(config, metrics, true)
    }

    /// Creates a new `TrustedProxyLayer` that is disabled.
    pub fn disabled() -> Self {
        Self::new(
            TrustedProxyConfig::new(Vec::new(), crate::config::ValidationMode::Lenient, true, 10, true, Vec::new()),
            None,
            false,
        )
    }

    /// Returns true if the middleware is enabled.
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }
}

impl<S> Layer<S> for TrustedProxyLayer {
    type Service = TrustedProxyMiddleware<S>;

    fn layer(&self, inner: S) -> Self::Service {
        TrustedProxyMiddleware {
            inner,
            validator: self.validator.clone(),
            enabled: self.enabled,
        }
    }
}
