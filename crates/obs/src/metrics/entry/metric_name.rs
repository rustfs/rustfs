/// The metric name is the individual name of the metric
#[allow(dead_code)]
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

    // 审计指标
    AuditFailedMessages,
    AuditTargetQueueLength,
    AuditTotalMessages,

    // 集群配置相关指标
    ConfigRRSParity,
    ConfigStandardParity,

    // 纠删码集合相关指标
    ErasureSetOverallWriteQuorum,
    ErasureSetOverallHealth,
    ErasureSetReadQuorum,
    ErasureSetWriteQuorum,
    ErasureSetOnlineDrivesCount,
    ErasureSetHealingDrivesCount,
    ErasureSetHealth,
    ErasureSetReadTolerance,
    ErasureSetWriteTolerance,
    ErasureSetReadHealth,
    ErasureSetWriteHealth,

    // 集群健康相关指标
    HealthDrivesOfflineCount,
    HealthDrivesOnlineCount,
    HealthDrivesCount,

    // IAM 相关指标
    LastSyncDurationMillis,
    PluginAuthnServiceFailedRequestsMinute,
    PluginAuthnServiceLastFailSeconds,
    PluginAuthnServiceLastSuccSeconds,
    PluginAuthnServiceSuccAvgRttMsMinute,
    PluginAuthnServiceSuccMaxRttMsMinute,
    PluginAuthnServiceTotalRequestsMinute,
    SinceLastSyncMillis,
    SyncFailures,
    SyncSuccesses,

    // 通知相关指标
    NotificationCurrentSendInProgress,
    NotificationEventsErrorsTotal,
    NotificationEventsSentTotal,
    NotificationEventsSkippedTotal,

    // 集群对象使用情况相关指标
    UsageSinceLastUpdateSeconds,
    UsageTotalBytes,
    UsageObjectsCount,
    UsageVersionsCount,
    UsageDeleteMarkersCount,
    UsageBucketsCount,
    UsageSizeDistribution,
    UsageVersionCountDistribution,

    // 桶使用情况相关指标
    UsageBucketQuotaTotalBytes,
    UsageBucketTotalBytes,
    UsageBucketObjectsCount,
    UsageBucketVersionsCount,
    UsageBucketDeleteMarkersCount,
    UsageBucketObjectSizeDistribution,
    UsageBucketObjectVersionCountDistribution,

    // ILM 相关指标
    IlmExpiryPendingTasks,
    IlmTransitionActiveTasks,
    IlmTransitionPendingTasks,
    IlmTransitionMissedImmediateTasks,
    IlmVersionsScanned,

    // Webhook 日志相关指标
    WebhookQueueLength,
    WebhookTotalMessages,
    WebhookFailedMessages,

    // 复制相关指标
    ReplicationAverageActiveWorkers,
    ReplicationAverageQueuedBytes,
    ReplicationAverageQueuedCount,
    ReplicationAverageDataTransferRate,
    ReplicationCurrentActiveWorkers,
    ReplicationCurrentDataTransferRate,
    ReplicationLastMinuteQueuedBytes,
    ReplicationLastMinuteQueuedCount,
    ReplicationMaxActiveWorkers,
    ReplicationMaxQueuedBytes,
    ReplicationMaxQueuedCount,
    ReplicationMaxDataTransferRate,
    ReplicationRecentBacklogCount,

    // 扫描器相关指标
    ScannerBucketScansFinished,
    ScannerBucketScansStarted,
    ScannerDirectoriesScanned,
    ScannerObjectsScanned,
    ScannerVersionsScanned,
    ScannerLastActivitySeconds,

    // CPU 系统相关指标
    SysCPUAvgIdle,
    SysCPUAvgIOWait,
    SysCPULoad,
    SysCPULoadPerc,
    SysCPUNice,
    SysCPUSteal,
    SysCPUSystem,
    SysCPUUser,

    // 驱动器相关指标
    DriveUsedBytes,
    DriveFreeBytes,
    DriveTotalBytes,
    DriveUsedInodes,
    DriveFreeInodes,
    DriveTotalInodes,
    DriveTimeoutErrorsTotal,
    DriveIOErrorsTotal,
    DriveAvailabilityErrorsTotal,
    DriveWaitingIO,
    DriveAPILatencyMicros,
    DriveHealth,

    DriveOfflineCount,
    DriveOnlineCount,
    DriveCount,

    // iostat 相关指标
    DriveReadsPerSec,
    DriveReadsKBPerSec,
    DriveReadsAwait,
    DriveWritesPerSec,
    DriveWritesKBPerSec,
    DriveWritesAwait,
    DrivePercUtil,

    // 内存相关指标
    MemTotal,
    MemUsed,
    MemUsedPerc,
    MemFree,
    MemBuffers,
    MemCache,
    MemShared,
    MemAvailable,

    // 网络相关指标
    InternodeErrorsTotal,
    InternodeDialErrorsTotal,
    InternodeDialAvgTimeNanos,
    InternodeSentBytesTotal,
    InternodeRecvBytesTotal,

    // 进程相关指标
    ProcessLocksReadTotal,
    ProcessLocksWriteTotal,
    ProcessCPUTotalSeconds,
    ProcessGoRoutineTotal,
    ProcessIORCharBytes,
    ProcessIOReadBytes,
    ProcessIOWCharBytes,
    ProcessIOWriteBytes,
    ProcessStartTimeSeconds,
    ProcessUptimeSeconds,
    ProcessFileDescriptorLimitTotal,
    ProcessFileDescriptorOpenTotal,
    ProcessSyscallReadTotal,
    ProcessSyscallWriteTotal,
    ProcessResidentMemoryBytes,
    ProcessVirtualMemoryBytes,
    ProcessVirtualMemoryMaxBytes,

    // 自定义指标
    Custom(String),
}

impl MetricName {
    #[allow(dead_code)]
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

            Self::AuditFailedMessages => "failed_messages".to_string(),
            Self::AuditTargetQueueLength => "target_queue_length".to_string(),
            Self::AuditTotalMessages => "total_messages".to_string(),

            // metrics related to cluster configurations
            Self::ConfigRRSParity => "rrs_parity".to_string(),
            Self::ConfigStandardParity => "standard_parity".to_string(),

            // 纠删码集合相关指标
            Self::ErasureSetOverallWriteQuorum => "overall_write_quorum".to_string(),
            Self::ErasureSetOverallHealth => "overall_health".to_string(),
            Self::ErasureSetReadQuorum => "read_quorum".to_string(),
            Self::ErasureSetWriteQuorum => "write_quorum".to_string(),
            Self::ErasureSetOnlineDrivesCount => "online_drives_count".to_string(),
            Self::ErasureSetHealingDrivesCount => "healing_drives_count".to_string(),
            Self::ErasureSetHealth => "health".to_string(),
            Self::ErasureSetReadTolerance => "read_tolerance".to_string(),
            Self::ErasureSetWriteTolerance => "write_tolerance".to_string(),
            Self::ErasureSetReadHealth => "read_health".to_string(),
            Self::ErasureSetWriteHealth => "write_health".to_string(),

            // 集群健康相关指标
            Self::HealthDrivesOfflineCount => "drives_offline_count".to_string(),
            Self::HealthDrivesOnlineCount => "drives_online_count".to_string(),
            Self::HealthDrivesCount => "drives_count".to_string(),

            // IAM 相关指标
            Self::LastSyncDurationMillis => "last_sync_duration_millis".to_string(),
            Self::PluginAuthnServiceFailedRequestsMinute => "plugin_authn_service_failed_requests_minute".to_string(),
            Self::PluginAuthnServiceLastFailSeconds => "plugin_authn_service_last_fail_seconds".to_string(),
            Self::PluginAuthnServiceLastSuccSeconds => "plugin_authn_service_last_succ_seconds".to_string(),
            Self::PluginAuthnServiceSuccAvgRttMsMinute => "plugin_authn_service_succ_avg_rtt_ms_minute".to_string(),
            Self::PluginAuthnServiceSuccMaxRttMsMinute => "plugin_authn_service_succ_max_rtt_ms_minute".to_string(),
            Self::PluginAuthnServiceTotalRequestsMinute => "plugin_authn_service_total_requests_minute".to_string(),
            Self::SinceLastSyncMillis => "since_last_sync_millis".to_string(),
            Self::SyncFailures => "sync_failures".to_string(),
            Self::SyncSuccesses => "sync_successes".to_string(),

            // 通知相关指标
            Self::NotificationCurrentSendInProgress => "current_send_in_progress".to_string(),
            Self::NotificationEventsErrorsTotal => "events_errors_total".to_string(),
            Self::NotificationEventsSentTotal => "events_sent_total".to_string(),
            Self::NotificationEventsSkippedTotal => "events_skipped_total".to_string(),

            // 集群对象使用情况相关指标
            Self::UsageSinceLastUpdateSeconds => "since_last_update_seconds".to_string(),
            Self::UsageTotalBytes => "total_bytes".to_string(),
            Self::UsageObjectsCount => "count".to_string(),
            Self::UsageVersionsCount => "versions_count".to_string(),
            Self::UsageDeleteMarkersCount => "delete_markers_count".to_string(),
            Self::UsageBucketsCount => "buckets_count".to_string(),
            Self::UsageSizeDistribution => "size_distribution".to_string(),
            Self::UsageVersionCountDistribution => "version_count_distribution".to_string(),

            // 桶使用情况相关指标
            Self::UsageBucketQuotaTotalBytes => "quota_total_bytes".to_string(),
            Self::UsageBucketTotalBytes => "total_bytes".to_string(),
            Self::UsageBucketObjectsCount => "objects_count".to_string(),
            Self::UsageBucketVersionsCount => "versions_count".to_string(),
            Self::UsageBucketDeleteMarkersCount => "delete_markers_count".to_string(),
            Self::UsageBucketObjectSizeDistribution => "object_size_distribution".to_string(),
            Self::UsageBucketObjectVersionCountDistribution => "object_version_count_distribution".to_string(),

            // ILM 相关指标
            Self::IlmExpiryPendingTasks => "expiry_pending_tasks".to_string(),
            Self::IlmTransitionActiveTasks => "transition_active_tasks".to_string(),
            Self::IlmTransitionPendingTasks => "transition_pending_tasks".to_string(),
            Self::IlmTransitionMissedImmediateTasks => "transition_missed_immediate_tasks".to_string(),
            Self::IlmVersionsScanned => "versions_scanned".to_string(),

            // Webhook 日志相关指标
            Self::WebhookQueueLength => "queue_length".to_string(),
            Self::WebhookTotalMessages => "total_messages".to_string(),
            Self::WebhookFailedMessages => "failed_messages".to_string(),

            // 复制相关指标
            Self::ReplicationAverageActiveWorkers => "average_active_workers".to_string(),
            Self::ReplicationAverageQueuedBytes => "average_queued_bytes".to_string(),
            Self::ReplicationAverageQueuedCount => "average_queued_count".to_string(),
            Self::ReplicationAverageDataTransferRate => "average_data_transfer_rate".to_string(),
            Self::ReplicationCurrentActiveWorkers => "current_active_workers".to_string(),
            Self::ReplicationCurrentDataTransferRate => "current_data_transfer_rate".to_string(),
            Self::ReplicationLastMinuteQueuedBytes => "last_minute_queued_bytes".to_string(),
            Self::ReplicationLastMinuteQueuedCount => "last_minute_queued_count".to_string(),
            Self::ReplicationMaxActiveWorkers => "max_active_workers".to_string(),
            Self::ReplicationMaxQueuedBytes => "max_queued_bytes".to_string(),
            Self::ReplicationMaxQueuedCount => "max_queued_count".to_string(),
            Self::ReplicationMaxDataTransferRate => "max_data_transfer_rate".to_string(),
            Self::ReplicationRecentBacklogCount => "recent_backlog_count".to_string(),

            // 扫描器相关指标
            Self::ScannerBucketScansFinished => "bucket_scans_finished".to_string(),
            Self::ScannerBucketScansStarted => "bucket_scans_started".to_string(),
            Self::ScannerDirectoriesScanned => "directories_scanned".to_string(),
            Self::ScannerObjectsScanned => "objects_scanned".to_string(),
            Self::ScannerVersionsScanned => "versions_scanned".to_string(),
            Self::ScannerLastActivitySeconds => "last_activity_seconds".to_string(),

            // CPU 系统相关指标
            Self::SysCPUAvgIdle => "avg_idle".to_string(),
            Self::SysCPUAvgIOWait => "avg_iowait".to_string(),
            Self::SysCPULoad => "load".to_string(),
            Self::SysCPULoadPerc => "load_perc".to_string(),
            Self::SysCPUNice => "nice".to_string(),
            Self::SysCPUSteal => "steal".to_string(),
            Self::SysCPUSystem => "system".to_string(),
            Self::SysCPUUser => "user".to_string(),

            // 驱动器相关指标
            Self::DriveUsedBytes => "used_bytes".to_string(),
            Self::DriveFreeBytes => "free_bytes".to_string(),
            Self::DriveTotalBytes => "total_bytes".to_string(),
            Self::DriveUsedInodes => "used_inodes".to_string(),
            Self::DriveFreeInodes => "free_inodes".to_string(),
            Self::DriveTotalInodes => "total_inodes".to_string(),
            Self::DriveTimeoutErrorsTotal => "timeout_errors_total".to_string(),
            Self::DriveIOErrorsTotal => "io_errors_total".to_string(),
            Self::DriveAvailabilityErrorsTotal => "availability_errors_total".to_string(),
            Self::DriveWaitingIO => "waiting_io".to_string(),
            Self::DriveAPILatencyMicros => "api_latency_micros".to_string(),
            Self::DriveHealth => "health".to_string(),

            Self::DriveOfflineCount => "offline_count".to_string(),
            Self::DriveOnlineCount => "online_count".to_string(),
            Self::DriveCount => "count".to_string(),

            // iostat 相关指标
            Self::DriveReadsPerSec => "reads_per_sec".to_string(),
            Self::DriveReadsKBPerSec => "reads_kb_per_sec".to_string(),
            Self::DriveReadsAwait => "reads_await".to_string(),
            Self::DriveWritesPerSec => "writes_per_sec".to_string(),
            Self::DriveWritesKBPerSec => "writes_kb_per_sec".to_string(),
            Self::DriveWritesAwait => "writes_await".to_string(),
            Self::DrivePercUtil => "perc_util".to_string(),

            // 内存相关指标
            Self::MemTotal => "total".to_string(),
            Self::MemUsed => "used".to_string(),
            Self::MemUsedPerc => "used_perc".to_string(),
            Self::MemFree => "free".to_string(),
            Self::MemBuffers => "buffers".to_string(),
            Self::MemCache => "cache".to_string(),
            Self::MemShared => "shared".to_string(),
            Self::MemAvailable => "available".to_string(),

            // 网络相关指标
            Self::InternodeErrorsTotal => "errors_total".to_string(),
            Self::InternodeDialErrorsTotal => "dial_errors_total".to_string(),
            Self::InternodeDialAvgTimeNanos => "dial_avg_time_nanos".to_string(),
            Self::InternodeSentBytesTotal => "sent_bytes_total".to_string(),
            Self::InternodeRecvBytesTotal => "recv_bytes_total".to_string(),

            // 进程相关指标
            Self::ProcessLocksReadTotal => "locks_read_total".to_string(),
            Self::ProcessLocksWriteTotal => "locks_write_total".to_string(),
            Self::ProcessCPUTotalSeconds => "cpu_total_seconds".to_string(),
            Self::ProcessGoRoutineTotal => "go_routine_total".to_string(),
            Self::ProcessIORCharBytes => "io_rchar_bytes".to_string(),
            Self::ProcessIOReadBytes => "io_read_bytes".to_string(),
            Self::ProcessIOWCharBytes => "io_wchar_bytes".to_string(),
            Self::ProcessIOWriteBytes => "io_write_bytes".to_string(),
            Self::ProcessStartTimeSeconds => "start_time_seconds".to_string(),
            Self::ProcessUptimeSeconds => "uptime_seconds".to_string(),
            Self::ProcessFileDescriptorLimitTotal => "file_descriptor_limit_total".to_string(),
            Self::ProcessFileDescriptorOpenTotal => "file_descriptor_open_total".to_string(),
            Self::ProcessSyscallReadTotal => "syscall_read_total".to_string(),
            Self::ProcessSyscallWriteTotal => "syscall_write_total".to_string(),
            Self::ProcessResidentMemoryBytes => "resident_memory_bytes".to_string(),
            Self::ProcessVirtualMemoryBytes => "virtual_memory_bytes".to_string(),
            Self::ProcessVirtualMemoryMaxBytes => "virtual_memory_max_bytes".to_string(),

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
