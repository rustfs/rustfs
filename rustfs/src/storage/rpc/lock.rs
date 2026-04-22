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

fn lock_result_from_response(response: rustfs_lock::LockResponse) -> GenerallyLockResult {
    GenerallyLockResult {
        success: response.success,
        error_info: response.error,
        lock_info: response.lock_info.and_then(|info| serde_json::to_string(&info).ok()),
    }
}

fn lock_result_from_error(error: impl Into<String>) -> GenerallyLockResult {
    GenerallyLockResult {
        success: false,
        error_info: Some(error.into()),
        lock_info: None,
    }
}

fn lock_result_from_release(lock_id: &rustfs_lock::LockId, success: bool) -> GenerallyLockResult {
    if success {
        GenerallyLockResult {
            success: true,
            error_info: None,
            lock_info: None,
        }
    } else {
        lock_result_from_error(format!("lock not found for release: {lock_id}"))
    }
}

impl NodeService {
    pub(super) async fn handle_refresh(
        &self,
        request: Request<GenerallyLockRequest>,
    ) -> Result<Response<GenerallyLockResponse>, Status> {
        let request = request.into_inner();
        let _args: LockRequest = match serde_json::from_str(&request.args) {
            Ok(args) => args,
            Err(err) => {
                return Ok(Response::new(GenerallyLockResponse {
                    success: false,
                    error_info: Some(format!("can not decode args, err: {err}")),
                    lock_info: None,
                }));
            }
        };

        Ok(Response::new(GenerallyLockResponse {
            success: true,
            error_info: None,
            lock_info: None,
        }))
    }

    pub(super) async fn handle_force_un_lock(
        &self,
        request: Request<GenerallyLockRequest>,
    ) -> Result<Response<GenerallyLockResponse>, Status> {
        let request = request.into_inner();
        let args: LockRequest = match serde_json::from_str(&request.args) {
            Ok(args) => args,
            Err(err) => {
                return Ok(Response::new(GenerallyLockResponse {
                    success: false,
                    error_info: Some(format!("can not decode args, err: {err}")),
                    lock_info: None,
                }));
            }
        };

        let lock_client = self.get_lock_client()?;
        match lock_client.force_release(&args.lock_id).await {
            Ok(success) => {
                let result = lock_result_from_release(&args.lock_id, success);
                Ok(Response::new(GenerallyLockResponse {
                    success: result.success,
                    error_info: result.error_info,
                    lock_info: None,
                }))
            }
            Err(err) => Ok(Response::new(GenerallyLockResponse {
                success: false,
                error_info: Some(format!(
                    "can not force_unlock, resource: {0}, owner: {1}, err: {2}",
                    args.resource, args.owner, err
                )),
                lock_info: None,
            })),
        }
    }

    pub(super) async fn handle_un_lock(
        &self,
        request: Request<GenerallyLockRequest>,
    ) -> Result<Response<GenerallyLockResponse>, Status> {
        let request = request.into_inner();
        let args: LockRequest = match serde_json::from_str(&request.args) {
            Ok(args) => args,
            Err(err) => {
                return Ok(Response::new(GenerallyLockResponse {
                    success: false,
                    error_info: Some(format!("can not decode args, err: {err}")),
                    lock_info: None,
                }));
            }
        };

        let lock_client = self.get_lock_client()?;
        match lock_client.release(&args.lock_id).await {
            Ok(success) => {
                let result = lock_result_from_release(&args.lock_id, success);
                Ok(Response::new(GenerallyLockResponse {
                    success: result.success,
                    error_info: result.error_info,
                    lock_info: None,
                }))
            }
            Err(err) => Ok(Response::new(GenerallyLockResponse {
                success: false,
                error_info: Some(format!(
                    "can not unlock, resource: {0}, owner: {1}, err: {2}",
                    args.resource, args.owner, err
                )),
                lock_info: None,
            })),
        }
    }

    pub(super) async fn handle_lock(
        &self,
        request: Request<GenerallyLockRequest>,
    ) -> Result<Response<GenerallyLockResponse>, Status> {
        let request = request.into_inner();
        // Parse the request to extract resource and owner
        let args: LockRequest = match serde_json::from_str(&request.args) {
            Ok(args) => args,
            Err(err) => {
                return Ok(Response::new(GenerallyLockResponse {
                    success: false,
                    error_info: Some(format!("can not decode args, err: {err}")),
                    lock_info: None,
                }));
            }
        };

        let lock_client = self.get_lock_client()?;
        match lock_client.acquire_lock(&args).await {
            Ok(result) => {
                // Serialize lock_info if available
                let lock_info_json = result.lock_info.as_ref().and_then(|info| serde_json::to_string(info).ok());
                Ok(Response::new(GenerallyLockResponse {
                    success: result.success,
                    error_info: result.error,
                    lock_info: lock_info_json,
                }))
            }
            Err(err) => Ok(Response::new(GenerallyLockResponse {
                success: false,
                error_info: Some(format!(
                    "can not lock, resource: {0}, owner: {1}, err: {2}",
                    args.resource, args.owner, err
                )),
                lock_info: None,
            })),
        }
    }

    pub(super) async fn handle_lock_batch(
        &self,
        request: Request<BatchGenerallyLockRequest>,
    ) -> Result<Response<BatchGenerallyLockResponse>, Status> {
        let request = request.into_inner();
        let mut results = vec![lock_result_from_error("request was not processed"); request.args.len()];
        let mut valid_requests = Vec::with_capacity(request.args.len());
        let mut valid_indices = Vec::with_capacity(request.args.len());

        for (idx, arg) in request.args.iter().enumerate() {
            match serde_json::from_str::<LockRequest>(arg) {
                Ok(args) => {
                    valid_requests.push(args);
                    valid_indices.push(idx);
                }
                Err(err) => {
                    results[idx] = lock_result_from_error(format!("can not decode args, err: {err}"));
                }
            }
        }

        if !valid_requests.is_empty() {
            let lock_client = self.get_lock_client()?;
            match lock_client.acquire_locks_batch(&valid_requests).await {
                Ok(batch_results) => {
                    for (result_idx, response) in batch_results.into_iter().enumerate() {
                        if let Some(request_idx) = valid_indices.get(result_idx) {
                            results[*request_idx] = lock_result_from_response(response);
                        }
                    }
                }
                Err(err) => {
                    for request_idx in valid_indices {
                        results[request_idx] = lock_result_from_error(format!("can not batch lock, err: {err}"));
                    }
                }
            }
        }

        Ok(Response::new(BatchGenerallyLockResponse { results }))
    }

    pub(super) async fn handle_un_lock_batch(
        &self,
        request: Request<BatchGenerallyLockRequest>,
    ) -> Result<Response<BatchGenerallyLockResponse>, Status> {
        let request = request.into_inner();
        let mut results = vec![lock_result_from_error("request was not processed"); request.args.len()];
        let mut lock_ids = Vec::with_capacity(request.args.len());
        let mut valid_indices = Vec::with_capacity(request.args.len());

        for (idx, arg) in request.args.iter().enumerate() {
            match serde_json::from_str::<LockRequest>(arg) {
                Ok(args) => {
                    lock_ids.push(args.lock_id);
                    valid_indices.push(idx);
                }
                Err(err) => {
                    results[idx] = lock_result_from_error(format!("can not decode args, err: {err}"));
                }
            }
        }

        if !lock_ids.is_empty() {
            let lock_client = self.get_lock_client()?;
            match lock_client.release_locks_batch(&lock_ids).await {
                Ok(batch_results) => {
                    for (result_idx, success) in batch_results.into_iter().enumerate() {
                        if let Some(request_idx) = valid_indices.get(result_idx) {
                            results[*request_idx] = match lock_ids.get(result_idx) {
                                Some(lock_id) => lock_result_from_release(lock_id, success),
                                None => lock_result_from_error(format!("unlock response index out of range: {result_idx}")),
                            };
                        }
                    }
                }
                Err(err) => {
                    for request_idx in valid_indices {
                        results[request_idx] = lock_result_from_error(format!("can not batch unlock, err: {err}"));
                    }
                }
            }
        }

        Ok(Response::new(BatchGenerallyLockResponse { results }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_lock_id() -> rustfs_lock::LockId {
        rustfs_lock::LockRequest::new(rustfs_lock::ObjectKey::new("bucket", "object"), rustfs_lock::LockType::Exclusive, "owner")
            .lock_id
    }

    #[test]
    fn lock_result_from_release_reports_missing_lock() {
        let lock_id = test_lock_id();
        let result = lock_result_from_release(&lock_id, false);

        assert!(!result.success);
        assert!(result.lock_info.is_none());
        assert!(
            result
                .error_info
                .expect("missing release should include error")
                .contains("lock not found for release")
        );
    }

    #[test]
    fn lock_result_from_response_preserves_lock_failure_error() {
        let response = rustfs_lock::LockResponse::failure("lock conflict", std::time::Duration::ZERO);
        let result = lock_result_from_response(response);

        assert!(!result.success);
        assert_eq!(result.error_info.as_deref(), Some("lock conflict"));
        assert!(result.lock_info.is_none());
    }
}
