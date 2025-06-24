use crate::client::admin_handler_utils::AdminError;
use http::status::StatusCode;

pub const ERR_TIER_ALREADY_EXISTS: AdminError = AdminError {
    code: "XRustFSAdminTierAlreadyExists",
    message: "Specified remote tier already exists",
    status_code: StatusCode::CONFLICT,
};

pub const ERR_TIER_NOT_FOUND: AdminError = AdminError {
    code: "XRustFSAdminTierNotFound",
    message: "Specified remote tier was not found",
    status_code: StatusCode::NOT_FOUND,
};

pub const ERR_TIER_NAME_NOT_UPPERCASE: AdminError = AdminError {
    code: "XRustFSAdminTierNameNotUpperCase",
    message: "Tier name must be in uppercase",
    status_code: StatusCode::BAD_REQUEST,
};

pub const ERR_TIER_BUCKET_NOT_FOUND: AdminError = AdminError {
    code: "XRustFSAdminTierBucketNotFound",
    message: "Remote tier bucket not found",
    status_code: StatusCode::BAD_REQUEST,
};

pub const ERR_TIER_INVALID_CREDENTIALS: AdminError = AdminError {
    code: "XRustFSAdminTierInvalidCredentials",
    message: "Invalid remote tier credentials",
    status_code: StatusCode::BAD_REQUEST,
};

pub const ERR_TIER_RESERVED_NAME: AdminError = AdminError {
    code: "XRustFSAdminTierReserved",
    message: "Cannot use reserved tier name",
    status_code: StatusCode::BAD_REQUEST,
};

pub const ERR_TIER_PERM_ERR: AdminError = AdminError {
    code: "TierPermErr",
    message: "Tier Perm Err",
    status_code: StatusCode::OK,
};

pub const ERR_TIER_CONNECT_ERR: AdminError = AdminError {
    code: "TierConnectErr",
    message: "Tier Connect Err",
    status_code: StatusCode::OK,
};
