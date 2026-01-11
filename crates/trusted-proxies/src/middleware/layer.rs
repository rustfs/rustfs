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

//! Tower layer implementation for trusted proxy middleware

use std::sync::Arc;
use tower::Layer;

use crate::config::TrustedProxyConfig;
use crate::middleware::TrustedProxyMiddleware;
use crate::proxy::ProxyMetrics;
use crate::proxy::ProxyValidator;

/// 可信代理中间件层
#[derive(Clone)]
pub struct TrustedProxyLayer {
    /// 代理验证器
    pub(crate) validator: Arc<ProxyValidator>,
    /// 是否启用中间件
    pub(crate) enabled: bool,
}

impl TrustedProxyLayer {
    /// 创建新的中间件层
    pub fn new(config: TrustedProxyConfig, metrics: Option<ProxyMetrics>, enabled: bool) -> Self {
        let validator = ProxyValidator::new(config, metrics);

        Self {
            validator: Arc::new(validator),
            enabled,
        }
    }

    /// 创建启用的中间件层
    pub fn enabled(config: TrustedProxyConfig, metrics: Option<ProxyMetrics>) -> Self {
        Self::new(config, metrics, true)
    }

    /// 创建禁用的中间件层
    pub fn disabled() -> Self {
        Self::new(
            TrustedProxyConfig::new(Vec::new(), crate::config::ValidationMode::Lenient, true, 10, true, Vec::new()),
            None,
            false,
        )
    }

    /// 检查中间件是否启用
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
