#![cfg(test)]

use protos::{
    models::{PingBody, PingBodyBuilder},
    proto_gen::node_service::{node_service_client::NodeServiceClient, PingRequest, PingResponse},
};
use std::error::Error;
use tonic::Request;

#[tokio::test]
async fn main() -> Result<(), Box<dyn Error>> {
    let mut fbb = flatbuffers::FlatBufferBuilder::new();
    let payload = fbb.create_vector(b"hello world");

    let mut builder = PingBodyBuilder::new(&mut fbb);
    builder.add_payload(payload);
    let root = builder.finish();
    fbb.finish(root, None);

    let finished_data = fbb.finished_data();

    let decoded_payload = flatbuffers::root::<PingBody>(finished_data);
    assert!(decoded_payload.is_ok());

    // 创建客户端
    let mut client = NodeServiceClient::connect("http://localhost:9000").await?;

    // 构造 PingRequest
    let request = Request::new(PingRequest {
        version: 1,
        body: finished_data.to_vec(),
    });

    // 发送请求并获取响应
    let response: PingResponse = client.ping(request).await?.into_inner();

    // 打印响应
    let ping_response_body = flatbuffers::root::<PingBody>(&response.body);
    if let Err(e) = ping_response_body {
        eprintln!("{}", e);
    } else {
        println!("ping_resp:body(flatbuffer): {:?}", ping_response_body);
    }

    Ok(())
}
