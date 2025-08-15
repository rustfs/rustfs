//  Copyright 2024 RustFS Team
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use crate::admin::router::Operation;
use http::header::CONTENT_TYPE;
use http::{HeaderMap, StatusCode};
use matchit::Params;
use s3s::{Body, S3Request, S3Response, S3Result, s3_error};
use tracing::info;

#[cfg(not(target_env = "msvc"))]
async fn dump_memory_profile() -> Result<String, String> {
    // Get the profiling controller of jemalloc
    let prof_ctl = jemalloc_pprof::PROF_CTL
        .as_ref()
        .ok_or_else(|| "Profiling controller not available".to_string())?;

    let mut prof_ctl = prof_ctl.lock().await;

    // Check if profiling is activated
    if !prof_ctl.activated() {
        return Err("Jemalloc profiling is not activated".to_string());
    }

    // Call dump_pprof() method to generate pprof data
    let pprof_data = prof_ctl.dump_pprof().map_err(|e| format!("Failed to dump pprof: {}", e))?;

    // Generate unique file names using timestamps
    let timestamp = chrono::Utc::now().format("%Y%m%d_%H%M%S");
    let filename = format!("memory_profile_{}.pb", timestamp);

    // Write pprof data to local file
    std::fs::write(&filename, pprof_data).map_err(|e| format!("Failed to write profile file: {}", e))?;

    info!("Memory profile dumped to: {}", filename);
    Ok(filename)
}

#[cfg(not(target_env = "msvc"))]
async fn dump_cpu_profile() -> Result<String, String> {
    use pprof::ProfilerGuard;
    use pprof::protos::Message;

    info!("Starting CPU profiling for 60 seconds...");

    // Create a CPU profiler and set the sampling frequency to 100 Hz
    let guard = ProfilerGuard::new(100).map_err(|e| format!("Failed to create profiler: {}", e))?;

    // Continuous sampling for 60 seconds
    tokio::time::sleep(std::time::Duration::from_secs(60)).await;

    // Generate a report
    let report = guard.report().build().map_err(|e| format!("Failed to build report: {}", e))?;

    // Generate file names using timestamps
    let timestamp = chrono::Utc::now().format("%Y%m%d_%H%M%S");
    let filename = format!("cpu_profile_{}.pb", timestamp);

    // Create a file and write pprof data
    let mut file = std::fs::File::create(&filename).map_err(|e| format!("Failed to create file: {}", e))?;

    report
        .pprof()
        .map_err(|e| format!("Failed to convert to pprof: {}", e))?
        .write_to_writer(&mut file)
        .map_err(|e| format!("Failed to write profile: {}", e))?;

    info!("CPU profile dumped to: {}", filename);
    Ok(filename)
}

pub fn start_profilers() {
    // Memory profiler
    tokio::spawn(async {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(300));
        loop {
            interval.tick().await;
            #[cfg(not(target_env = "msvc"))]
            {
                info!("Starting memory profiler...");
                match dump_memory_profile().await {
                    Ok(profile_path) => info!("Memory profile dumped successfully: {}", profile_path),
                    Err(e) => info!("Failed to dump memory profile: {}", e),
                }
            }
        }
    });

    // CPU profiler
    tokio::spawn(async {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(300));
        loop {
            interval.tick().await;
            #[cfg(not(target_env = "msvc"))]
            {
                info!("Starting cpu profiler...");
                match dump_cpu_profile().await {
                    Ok(profile_path) => info!("Cpu profile dumped successfully: {}", profile_path),
                    Err(e) => info!("Failed to dump cpu profile: {}", e),
                }
            }
        }
    });
}

pub struct TriggerProfileCPU {}

#[async_trait::async_trait]
impl Operation for TriggerProfileCPU {
    async fn call(&self, _req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        info!("Triggering CPU profile dump via S3 request...");

        #[cfg(not(target_env = "msvc"))]
        {
            match dump_cpu_profile().await {
                Ok(profile_path) => {
                    let mut header = HeaderMap::new();
                    header.insert(CONTENT_TYPE, "text/html".parse().unwrap());
                    Ok(S3Response::with_headers((StatusCode::OK, Body::from(profile_path)), header))
                }
                Err(e) => {
                    info!("Failed to dump CPU profile: {}", e);
                    // Return an error response with the error message
                    Err(s3_error!(InternalError, "{}", format!("Failed to dump CPU profile: {e}")))
                }
            }
        }

        #[cfg(target_env = "msvc")]
        {
            Err(s3_error!(InternalError, "CPU profiling is not supported on this platform"))
        }
    }
}
pub struct TriggerProfileMemory {}

#[async_trait::async_trait]
impl Operation for TriggerProfileMemory {
    async fn call(&self, _req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        info!("Triggering Memory profile dump via S3 request...");

        #[cfg(not(target_env = "msvc"))]
        {
            match dump_memory_profile().await {
                Ok(profile_path) => {
                    let mut header = HeaderMap::new();
                    header.insert(CONTENT_TYPE, "text/html".parse().unwrap());
                    Ok(S3Response::with_headers((StatusCode::OK, Body::from(profile_path)), header))
                }
                Err(e) => {
                    info!("Failed to dump Memory profile: {}", e);
                    // Return an error response with the error message
                    Err(s3_error!(InternalError, "{}", format!("Failed to dump Memory profile: {e}")))
                }
            }
        }
        #[cfg(target_env = "msvc")]
        {
            Err(s3_error!(InternalError, "Memory profiling is not supported on this platform"))
        }
    }
}
