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

// #730: diagnostics constants are staged for request-path telemetry migration.

pub(crate) mod admin_server_info;
#[cfg(feature = "diagnostic-local-batch")]
pub(crate) mod batch_read_diagnostics;
pub(crate) mod get;
#[cfg(feature = "diagnostic-get-pipeline")]
pub(crate) mod get_pipeline;
pub(crate) mod object_lock;

// The disabled expansion contains neither diagnostic arguments nor calls.
macro_rules! get_pipeline_sync {
    ($stage:ident, $bytes:expr, result $operation:expr) => {{
        #[cfg(feature = "diagnostic-get-pipeline")]
        {
            $crate::diagnostics::get_pipeline::measure_sync(
                $crate::diagnostics::get_pipeline::Stage::$stage,
                $bytes,
                $operation,
                |result| result.is_ok(),
            )
        }
        #[cfg(not(feature = "diagnostic-get-pipeline"))]
        {
            ($operation)()
        }
    }};
    ($stage:ident, $bytes:expr, $operation:expr) => {{
        #[cfg(feature = "diagnostic-get-pipeline")]
        {
            $crate::diagnostics::get_pipeline::measure_sync(
                $crate::diagnostics::get_pipeline::Stage::$stage,
                $bytes,
                $operation,
                |_| true,
            )
        }
        #[cfg(not(feature = "diagnostic-get-pipeline"))]
        {
            ($operation)()
        }
    }};
}
pub(crate) use get_pipeline_sync;

#[cfg(all(test, not(feature = "diagnostic-get-pipeline")))]
mod pipeline_disabled_tests {
    #[test]
    fn disabled_macro_preserves_operations_without_evaluating_diagnostic_arguments() {
        let result = super::get_pipeline_sync!(Copy, panic!("disabled diagnostics evaluated bytes"), || 17);
        assert_eq!(result, 17);
        let result =
            super::get_pipeline_sync!(Verify, panic!("disabled diagnostics evaluated bytes"), result || Err::<(), _>("original"));
        assert_eq!(result, Err("original"));
    }
}
