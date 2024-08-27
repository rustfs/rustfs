use ecstore::{disk::DiskStore, peer::LocalPeerS3Client};
use tonic::{Request, Response, Status};
use tracing::{debug, error, info};

use protos::{
    models::{PingBody, PingBodyBuilder},
    proto_gen::node_service::{
        node_service_server::{NodeService as Node, NodeServiceServer as NodeServer},
        MakeBucketRequest, MakeBucketResponse, PingRequest, PingResponse,
    },
};

#[derive(Debug)]
struct NodeService {
    pub local_peer: LocalPeerS3Client,
}

pub fn make_server(local_disks: Vec<DiskStore>) -> NodeServer<impl Node> {
    let local_peer = LocalPeerS3Client::new(local_disks, None, None);
    NodeServer::new(NodeService { local_peer })
}

#[tonic::async_trait]
impl Node for NodeService {
    async fn ping(&self, request: Request<PingRequest>) -> Result<Response<PingResponse>, Status> {
        debug!("PING");

        let ping_req = request.into_inner();
        let ping_body = flatbuffers::root::<PingBody>(&ping_req.body);
        if let Err(e) = ping_body {
            error!("{}", e);
        } else {
            info!("ping_req:body(flatbuffer): {:?}", ping_body);
        }

        let mut fbb = flatbuffers::FlatBufferBuilder::new();
        let payload = fbb.create_vector(b"hello, caller");

        let mut builder = PingBodyBuilder::new(&mut fbb);
        builder.add_payload(payload);
        let root = builder.finish();
        fbb.finish(root, None);

        let finished_data = fbb.finished_data();

        Ok(tonic::Response::new(PingResponse {
            version: 1,
            body: finished_data.to_vec(),
        }))
    }

    async fn make_bucket(&self, request: Request<MakeBucketRequest>) -> Result<Response<MakeBucketResponse>, Status> {
        debug!("make bucket");

        let _req = request.into_inner();

        // match self.local_peer.make_bucket(&name, &MakeBucketOptions::default()).await {}
        unimplemented!()
    }
}
