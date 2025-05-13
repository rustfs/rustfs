/// The metric name is the individual name of the metric
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetricName {
    // 通用指标名称
    AuthTotal,
    CanceledTotal,
    ErrorsTotal,
    HeaderTotal,
    HealTotal,
    HitsTotal,
    InflightTotal,
    InvalidTotal,
    LimitTotal,
    MissedTotal,
    WaitingTotal,
    IncomingTotal,
    ObjectTotal,
    VersionTotal,
    DeleteMarkerTotal,
    OfflineTotal,
    OnlineTotal,
    OpenTotal,
    ReadTotal,
    TimestampTotal,
    WriteTotal,
    Total,
    FreeInodes,

    // 失败统计指标
    LastMinFailedCount,
    LastMinFailedBytes,
    LastHourFailedCount,
    LastHourFailedBytes,
    TotalFailedCount,
    TotalFailedBytes,

    // 工作线程指标
    CurrActiveWorkers,
    AvgActiveWorkers,
    MaxActiveWorkers,
    RecentBacklogCount,
    CurrInQueueCount,
    CurrInQueueBytes,
    ReceivedCount,
    SentCount,
    CurrTransferRate,
    AvgTransferRate,
    MaxTransferRate,
    CredentialErrors,

    // 链接延迟指标
    CurrLinkLatency,
    AvgLinkLatency,
    MaxLinkLatency,

    // 链接状态指标
    LinkOnline,
    LinkOfflineDuration,
    LinkDowntimeTotalDuration,

    // 队列指标
    AvgInQueueCount,
    AvgInQueueBytes,
    MaxInQueueCount,
    MaxInQueueBytes,

    // 代理请求指标
    ProxiedGetRequestsTotal,
    ProxiedHeadRequestsTotal,
    ProxiedPutTaggingRequestsTotal,
    ProxiedGetTaggingRequestsTotal,
    ProxiedDeleteTaggingRequestsTotal,
    ProxiedGetRequestsFailures,
    ProxiedHeadRequestsFailures,
    ProxiedPutTaggingRequestFailures,
    ProxiedGetTaggingRequestFailures,
    ProxiedDeleteTaggingRequestFailures,

    // 字节相关指标
    FreeBytes,
    ReadBytes,
    RcharBytes,
    ReceivedBytes,
    LatencyMilliSec,
    SentBytes,
    TotalBytes,
    UsedBytes,
    WriteBytes,
    WcharBytes,

    // 延迟指标
    LatencyMicroSec,
    LatencyNanoSec,

    // 信息指标
    CommitInfo,
    UsageInfo,
    VersionInfo,

    // 分布指标
    SizeDistribution,
    VersionDistribution,
    TtfbDistribution,
    TtlbDistribution,

    // 时间指标
    LastActivityTime,
    StartTime,
    UpTime,
    Memory,
    Vmemory,
    Cpu,

    // 过期和转换指标
    ExpiryMissedTasks,
    ExpiryMissedFreeVersions,
    ExpiryMissedTierJournalTasks,
    ExpiryNumWorkers,
    TransitionMissedTasks,
    TransitionedBytes,
    TransitionedObjects,
    TransitionedVersions,

    // Tier 请求指标
    TierRequestsSuccess,
    TierRequestsFailure,

    // KMS 指标
    KmsOnline,
    KmsRequestsSuccess,
    KmsRequestsError,
    KmsRequestsFail,
    KmsUptime,

    // Webhook 指标
    WebhookOnline,

    // API 拒绝指标
    ApiRejectedAuthTotal,
    ApiRejectedHeaderTotal,
    ApiRejectedTimestampTotal,
    ApiRejectedInvalidTotal,

    // API 请求指标
    ApiRequestsWaitingTotal,
    ApiRequestsIncomingTotal,
    ApiRequestsInFlightTotal,
    ApiRequestsTotal,
    ApiRequestsErrorsTotal,
    ApiRequests5xxErrorsTotal,
    ApiRequests4xxErrorsTotal,
    ApiRequestsCanceledTotal,

    // API 分布指标
    ApiRequestsTTFBSecondsDistribution,

    // API 流量指标
    ApiTrafficSentBytes,
    ApiTrafficRecvBytes,

    // 自定义指标
    Custom(String),
}

impl MetricName {
    pub fn as_str(&self) -> String {
        match self {
            Self::AuthTotal => "auth_total".to_string(),
            Self::CanceledTotal => "canceled_total".to_string(),
            Self::ErrorsTotal => "errors_total".to_string(),
            Self::HeaderTotal => "header_total".to_string(),
            Self::HealTotal => "heal_total".to_string(),
            Self::HitsTotal => "hits_total".to_string(),
            Self::InflightTotal => "inflight_total".to_string(),
            Self::InvalidTotal => "invalid_total".to_string(),
            Self::LimitTotal => "limit_total".to_string(),
            Self::MissedTotal => "missed_total".to_string(),
            Self::WaitingTotal => "waiting_total".to_string(),
            Self::IncomingTotal => "incoming_total".to_string(),
            Self::ObjectTotal => "object_total".to_string(),
            Self::VersionTotal => "version_total".to_string(),
            Self::DeleteMarkerTotal => "deletemarker_total".to_string(),
            Self::OfflineTotal => "offline_total".to_string(),
            Self::OnlineTotal => "online_total".to_string(),
            Self::OpenTotal => "open_total".to_string(),
            Self::ReadTotal => "read_total".to_string(),
            Self::TimestampTotal => "timestamp_total".to_string(),
            Self::WriteTotal => "write_total".to_string(),
            Self::Total => "total".to_string(),
            Self::FreeInodes => "free_inodes".to_string(),

            Self::LastMinFailedCount => "last_minute_failed_count".to_string(),
            Self::LastMinFailedBytes => "last_minute_failed_bytes".to_string(),
            Self::LastHourFailedCount => "last_hour_failed_count".to_string(),
            Self::LastHourFailedBytes => "last_hour_failed_bytes".to_string(),
            Self::TotalFailedCount => "total_failed_count".to_string(),
            Self::TotalFailedBytes => "total_failed_bytes".to_string(),

            Self::CurrActiveWorkers => "current_active_workers".to_string(),
            Self::AvgActiveWorkers => "average_active_workers".to_string(),
            Self::MaxActiveWorkers => "max_active_workers".to_string(),
            Self::RecentBacklogCount => "recent_backlog_count".to_string(),
            Self::CurrInQueueCount => "last_minute_queued_count".to_string(),
            Self::CurrInQueueBytes => "last_minute_queued_bytes".to_string(),
            Self::ReceivedCount => "received_count".to_string(),
            Self::SentCount => "sent_count".to_string(),
            Self::CurrTransferRate => "current_transfer_rate".to_string(),
            Self::AvgTransferRate => "average_transfer_rate".to_string(),
            Self::MaxTransferRate => "max_transfer_rate".to_string(),
            Self::CredentialErrors => "credential_errors".to_string(),

            Self::CurrLinkLatency => "current_link_latency_ms".to_string(),
            Self::AvgLinkLatency => "average_link_latency_ms".to_string(),
            Self::MaxLinkLatency => "max_link_latency_ms".to_string(),

            Self::LinkOnline => "link_online".to_string(),
            Self::LinkOfflineDuration => "link_offline_duration_seconds".to_string(),
            Self::LinkDowntimeTotalDuration => "link_downtime_duration_seconds".to_string(),

            Self::AvgInQueueCount => "average_queued_count".to_string(),
            Self::AvgInQueueBytes => "average_queued_bytes".to_string(),
            Self::MaxInQueueCount => "max_queued_count".to_string(),
            Self::MaxInQueueBytes => "max_queued_bytes".to_string(),

            Self::ProxiedGetRequestsTotal => "proxied_get_requests_total".to_string(),
            Self::ProxiedHeadRequestsTotal => "proxied_head_requests_total".to_string(),
            Self::ProxiedPutTaggingRequestsTotal => "proxied_put_tagging_requests_total".to_string(),
            Self::ProxiedGetTaggingRequestsTotal => "proxied_get_tagging_requests_total".to_string(),
            Self::ProxiedDeleteTaggingRequestsTotal => "proxied_delete_tagging_requests_total".to_string(),
            Self::ProxiedGetRequestsFailures => "proxied_get_requests_failures".to_string(),
            Self::ProxiedHeadRequestsFailures => "proxied_head_requests_failures".to_string(),
            Self::ProxiedPutTaggingRequestFailures => "proxied_put_tagging_requests_failures".to_string(),
            Self::ProxiedGetTaggingRequestFailures => "proxied_get_tagging_requests_failures".to_string(),
            Self::ProxiedDeleteTaggingRequestFailures => "proxied_delete_tagging_requests_failures".to_string(),

            Self::FreeBytes => "free_bytes".to_string(),
            Self::ReadBytes => "read_bytes".to_string(),
            Self::RcharBytes => "rchar_bytes".to_string(),
            Self::ReceivedBytes => "received_bytes".to_string(),
            Self::LatencyMilliSec => "latency_ms".to_string(),
            Self::SentBytes => "sent_bytes".to_string(),
            Self::TotalBytes => "total_bytes".to_string(),
            Self::UsedBytes => "used_bytes".to_string(),
            Self::WriteBytes => "write_bytes".to_string(),
            Self::WcharBytes => "wchar_bytes".to_string(),

            Self::LatencyMicroSec => "latency_us".to_string(),
            Self::LatencyNanoSec => "latency_ns".to_string(),

            Self::CommitInfo => "commit_info".to_string(),
            Self::UsageInfo => "usage_info".to_string(),
            Self::VersionInfo => "version_info".to_string(),

            Self::SizeDistribution => "size_distribution".to_string(),
            Self::VersionDistribution => "version_distribution".to_string(),
            Self::TtfbDistribution => "seconds_distribution".to_string(),
            Self::TtlbDistribution => "ttlb_seconds_distribution".to_string(),

            Self::LastActivityTime => "last_activity_nano_seconds".to_string(),
            Self::StartTime => "starttime_seconds".to_string(),
            Self::UpTime => "uptime_seconds".to_string(),
            Self::Memory => "resident_memory_bytes".to_string(),
            Self::Vmemory => "virtual_memory_bytes".to_string(),
            Self::Cpu => "cpu_total_seconds".to_string(),

            Self::ExpiryMissedTasks => "expiry_missed_tasks".to_string(),
            Self::ExpiryMissedFreeVersions => "expiry_missed_freeversions".to_string(),
            Self::ExpiryMissedTierJournalTasks => "expiry_missed_tierjournal_tasks".to_string(),
            Self::ExpiryNumWorkers => "expiry_num_workers".to_string(),
            Self::TransitionMissedTasks => "transition_missed_immediate_tasks".to_string(),

            Self::TransitionedBytes => "transitioned_bytes".to_string(),
            Self::TransitionedObjects => "transitioned_objects".to_string(),
            Self::TransitionedVersions => "transitioned_versions".to_string(),

            Self::TierRequestsSuccess => "requests_success".to_string(),
            Self::TierRequestsFailure => "requests_failure".to_string(),

            Self::KmsOnline => "online".to_string(),
            Self::KmsRequestsSuccess => "request_success".to_string(),
            Self::KmsRequestsError => "request_error".to_string(),
            Self::KmsRequestsFail => "request_failure".to_string(),
            Self::KmsUptime => "uptime".to_string(),

            Self::WebhookOnline => "online".to_string(),

            Self::ApiRejectedAuthTotal => "rejected_auth_total".to_string(),
            Self::ApiRejectedHeaderTotal => "rejected_header_total".to_string(),
            Self::ApiRejectedTimestampTotal => "rejected_timestamp_total".to_string(),
            Self::ApiRejectedInvalidTotal => "rejected_invalid_total".to_string(),

            Self::ApiRequestsWaitingTotal => "waiting_total".to_string(),
            Self::ApiRequestsIncomingTotal => "incoming_total".to_string(),
            Self::ApiRequestsInFlightTotal => "inflight_total".to_string(),
            Self::ApiRequestsTotal => "total".to_string(),
            Self::ApiRequestsErrorsTotal => "errors_total".to_string(),
            Self::ApiRequests5xxErrorsTotal => "5xx_errors_total".to_string(),
            Self::ApiRequests4xxErrorsTotal => "4xx_errors_total".to_string(),
            Self::ApiRequestsCanceledTotal => "canceled_total".to_string(),

            Self::ApiRequestsTTFBSecondsDistribution => "ttfb_seconds_distribution".to_string(),

            Self::ApiTrafficSentBytes => "traffic_sent_bytes".to_string(),
            Self::ApiTrafficRecvBytes => "traffic_received_bytes".to_string(),

            Self::Custom(name) => name.clone(),
        }
    }
}

impl From<String> for MetricName {
    fn from(s: String) -> Self {
        Self::Custom(s)
    }
}

impl From<&str> for MetricName {
    fn from(s: &str) -> Self {
        Self::Custom(s.to_string())
    }
}
