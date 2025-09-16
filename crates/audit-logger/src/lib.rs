mod entry;
mod logger;

pub use entry::args::Args;
pub use entry::audit::{ApiDetails, AuditLogEntry};
pub use entry::base::BaseLogEntry;
pub use entry::unified::{ConsoleLogEntry, ServerLogEntry, UnifiedLogEntry};
pub use entry::{LogKind, LogRecord, ObjectVersion, SerializableLevel};

// Export new audit system components
pub use logger::{
    AuditConfig, AuditError, AuditLogger, AuditManager, AuditResult, AuditSystem, AuditTarget, AuditTargetConfig,
    AuditTargetFactory, DefaultAuditTargetFactory, TargetRegistry, TargetResult, TargetStatus, audit_logger,
    disable_audit_logging, enable_audit_logging, initialize, is_audit_logging_enabled, log_audit, log_audit_entry,
    shutdown_audit_logger,
};

// Re-export the old Target trait for backward compatibility
pub use logger::Target;
