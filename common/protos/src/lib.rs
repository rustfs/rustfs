mod generated;
use std::time::Duration;

pub use generated::*;
use proto_gen::node_service::node_service_client::NodeServiceClient;
use tonic::{codec::CompressionEncoding, transport::Channel};
use tower::timeout::Timeout;

// Default 100 MB
pub const DEFAULT_GRPC_SERVER_MESSAGE_LEN: usize = 100 * 1024 * 1024;

pub fn node_service_time_out_client(
    channel: Channel,
    time_out: Duration,
    max_message_size: usize,
    grpc_enable_gzip: bool,
) -> NodeServiceClient<Timeout<Channel>> {
    let timeout_channel = Timeout::new(channel, time_out);
    let client = NodeServiceClient::<Timeout<Channel>>::new(timeout_channel);
    let client = NodeServiceClient::max_decoding_message_size(client, max_message_size);
    if grpc_enable_gzip {
        NodeServiceClient::max_encoding_message_size(client, max_message_size)
            .accept_compressed(CompressionEncoding::Gzip)
            .send_compressed(CompressionEncoding::Gzip)
    } else {
        NodeServiceClient::max_encoding_message_size(client, max_message_size)
    }
}
