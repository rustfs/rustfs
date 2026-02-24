use super::*;

impl NodeService {
    pub(super) async fn handle_get_metrics(
        &self,
        request: Request<GetMetricsRequest>,
    ) -> Result<Response<GetMetricsResponse>, Status> {
        let request = request.into_inner();

        // Deserialize metric_type with error handling
        let mut buf_t = Deserializer::new(Cursor::new(request.metric_type));
        let t: MetricType = match Deserialize::deserialize(&mut buf_t) {
            Ok(t) => t,
            Err(err) => {
                error!("Failed to deserialize metric_type: {}", err);
                return Ok(Response::new(GetMetricsResponse {
                    success: false,
                    realtime_metrics: Bytes::new(),
                    error_info: Some(format!("Invalid metric_type: {err}")),
                }));
            }
        };

        // Deserialize opts with error handling
        let mut buf_o = Deserializer::new(Cursor::new(request.opts));
        let opts: CollectMetricsOpts = match Deserialize::deserialize(&mut buf_o) {
            Ok(opts) => opts,
            Err(err) => {
                error!("Failed to deserialize opts: {}", err);
                return Ok(Response::new(GetMetricsResponse {
                    success: false,
                    realtime_metrics: Bytes::new(),
                    error_info: Some(format!("Invalid opts: {err}")),
                }));
            }
        };

        let info = collect_local_metrics(t, &opts).await;

        let mut buf = Vec::new();
        if let Err(err) = info.serialize(&mut Serializer::new(&mut buf)) {
            return Ok(Response::new(GetMetricsResponse {
                success: false,
                realtime_metrics: Bytes::new(),
                error_info: Some(err.to_string()),
            }));
        }
        Ok(Response::new(GetMetricsResponse {
            success: true,
            realtime_metrics: buf.into(),
            error_info: None,
        }))
    }
}
