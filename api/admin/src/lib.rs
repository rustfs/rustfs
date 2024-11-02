pub mod error;
pub mod handlers;

use axum::{extract::Request, response::Response, routing::get, BoxError, Router};
use error::ErrorCode;
use handlers::list_pools;
use tower::Service;

pub type Result<T> = std::result::Result<T, ErrorCode>;

const API_VERSION: &str = "/v3";

pub fn register_admin_router() -> impl Service<Request, Response = Response, Error: Into<BoxError>, Future: Send> + Clone {
    Router::new()
        .nest(
            "/rustfs/admin",
            Router::new().nest(API_VERSION, Router::new().route("/pools/list", get(list_pools::handler))),
        )
        .into_service()
}
