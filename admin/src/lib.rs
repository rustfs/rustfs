pub mod error;
pub mod handlers;
pub mod object_api;

use axum::{extract::Request, response::Response, routing::get, BoxError, Router};
use ecstore::store::ECStore;
use error::ErrorCode;
use handlers::list_pools;
use object_api::ObjectApi;
use tower::Service;

pub type Result<T> = std::result::Result<T, ErrorCode>;

pub fn register_admin_router(
    ec_store: Option<ECStore>,
) -> impl Service<Request, Response = Response, Error: Into<BoxError>, Future: Send> + Clone {
    Router::new()
        .nest("/admin/v3", Router::new().route("/pools/list", get(list_pools::handler)))
        .with_state::<()>(ObjectApi::new(ec_store))
        .into_service()
}
