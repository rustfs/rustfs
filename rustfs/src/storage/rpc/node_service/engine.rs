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

//! Bucket engine calls from peers, answered by this node's engine registry.

use super::NodeService;
use crate::bucket_engine::EngineRegistry;
use crate::bucket_engine::peer::{PeerCall, response, serve};
use rustfs_protos::proto_gen::node_service::*;
use tonic::{Request, Response};

impl NodeService {
    pub(super) async fn handle_engine_call(&self, request: Request<EngineCallRequest>) -> Response<EngineCallResponse> {
        let result = match PeerCall::from_request(request.into_inner()) {
            Ok(call) => serve(EngineRegistry::global(), call).await,
            Err(err) => Err(err),
        };
        Response::new(response(result))
    }
}
