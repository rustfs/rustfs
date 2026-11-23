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

use super::{NixlChunkReader, ReadStreamRequest, tests::VecPool};
use crate::cluster::rpc::{internode_data_transport::InternodeDataTransport, remote_disk::RemoteDisk};
use crate::disk::{Disk, DiskOption, DiskStore, endpoint::Endpoint};
use rustfs_rio::ChunkReaderBox;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use url::Url;

/// Serve a single response over HTTP/1.1 and return the request URI for assertions.
pub(crate) async fn read_peer(response: Vec<u8>) -> (Endpoint, JoinHandle<Url>) {
    crate::runtime::sources::ensure_test_rpc_secret();
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind loopback peer");
    let address = listener.local_addr().expect("peer address");
    let url = Url::parse(&format!("http://{address}/disk")).expect("disk URL");
    let endpoint = Endpoint {
        url: url.clone(),
        is_local: false,
        pool_idx: 0,
        set_idx: 0,
        disk_idx: 0,
    };
    let peer = tokio::spawn(async move {
        let (mut socket, _) = tokio::time::timeout(std::time::Duration::from_secs(10), listener.accept())
            .await
            .expect("peer received a request")
            .expect("accept read");
        let mut lines = BufReader::new(&mut socket).lines();
        let request = lines.next_line().await.expect("request line").expect("request");
        let target = request.split_whitespace().nth(1).expect("request target");
        while !lines
            .next_line()
            .await
            .expect("read headers")
            .expect("header terminator")
            .is_empty()
        {}
        socket.write_all(&response).await.expect("send response");
        url.join(target).expect("request URI")
    });
    (endpoint, peer)
}

pub(crate) async fn remote_disk(endpoint: &Endpoint, transport: Arc<dyn InternodeDataTransport>) -> DiskStore {
    let disk = RemoteDisk::new(
        endpoint,
        &DiskOption {
            cleanup: false,
            health_check: false,
        },
        transport,
    )
    .await
    .expect("create remote disk");
    Arc::new(Disk::Remote(Box::new(disk)))
}

pub(crate) fn nixl_reader(endpoint: &Endpoint, length: usize) -> ChunkReaderBox {
    let request = ReadStreamRequest {
        endpoint: endpoint.grid_host(),
        disk: "disk".to_owned(),
        volume: "bucket".to_owned(),
        path: "part.1".to_owned(),
        offset: 0,
        length,
        stall_timeout: None,
    };
    Box::new(NixlChunkReader::new(Arc::new(VecPool), request, 1))
}
