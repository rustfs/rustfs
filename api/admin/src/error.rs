use axum::{
    body::Body,
    http::{header::CONTENT_TYPE, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
};
use mime::APPLICATION_JSON;
use serde::Serialize;

#[derive(Serialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct ErrorResponse {
    pub code: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bucket_name: Option<String>,
    pub resource: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub region: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    pub host_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub actual_object_size: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub range_requested: Option<String>,
}

impl IntoResponse for APIError {
    fn into_response(self) -> Response {
        let code = self.http_status_code;
        let err_response = ErrorResponse::from(self);
        let json_res = match serde_json::to_vec(&err_response) {
            Ok(r) => r,
            Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, format!("{e}")).into_response(),
        };

        Response::builder()
            .status(code)
            .header(CONTENT_TYPE, HeaderValue::from_static(APPLICATION_JSON.as_ref()))
            .body(Body::from(json_res))
            .unwrap_or_else(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e}")).into_response())
    }
}

#[derive(Default)]
pub struct APIError {
    code: String,
    description: String,
    http_status_code: StatusCode,
    object_size: Option<String>,
    range_requested: Option<String>,
}

pub enum ErrorCode {
    ErrNotImplemented,
    ErrServerNotInitialized,
}

impl IntoResponse for ErrorCode {
    fn into_response(self) -> Response {
        APIError::from(self).into_response()
    }
}

impl From<ErrorCode> for APIError {
    fn from(value: ErrorCode) -> Self {
        use ErrorCode::*;

        match value {
            ErrNotImplemented => APIError {
                code: "NotImplemented".into(),
                description: "A header you provided implies functionality that is not implemented.".into(),
                http_status_code: StatusCode::NOT_IMPLEMENTED,
                ..Default::default()
            },
            ErrServerNotInitialized => APIError {
                code: "ServerNotInitialized".into(),
                description: "Server not initialized yet, please try again.".into(),
                http_status_code: StatusCode::SERVICE_UNAVAILABLE,
                ..Default::default()
            },
        }
    }
}

impl From<APIError> for ErrorResponse {
    fn from(value: APIError) -> Self {
        Self {
            code: value.code,
            message: value.description,
            actual_object_size: value.object_size,
            range_requested: value.range_requested,
            ..Default::default()
        }
    }
}
