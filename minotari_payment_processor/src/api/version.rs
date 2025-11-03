use axum::Json;
use serde::Serialize;
use utoipa::ToSchema;

#[derive(Debug, Clone, ToSchema, Serialize)]
pub struct ServiceVersion {
    pub version: String,
}

impl ServiceVersion {
    pub fn new() -> Self {
        Self {
            version: env!("CARGO_PKG_VERSION").to_string(),
        }
    }
}

#[utoipa::path(
    get,
    path = "/health/version",
    responses(
        (status = 200, description = "Service version", body = ServiceVersion),
    )
)]
pub async fn api_get_version() -> Json<ServiceVersion> {
    Json(ServiceVersion::new())
}
