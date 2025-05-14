/// Format the path to the metric name format
/// Replace '/' and '-' with '_'
pub fn format_path_to_metric_name(path: &str) -> String {
    path.trim_start_matches('/').replace('/', "_").replace('-', "_")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_path_to_metric_name() {
        assert_eq!(format_path_to_metric_name("/api/requests"), "api_requests");
        assert_eq!(format_path_to_metric_name("/system/network/internode"), "system_network_internode");
        assert_eq!(format_path_to_metric_name("/bucket-api"), "bucket_api");
        assert_eq!(format_path_to_metric_name("cluster/health"), "cluster_health");
    }
}
