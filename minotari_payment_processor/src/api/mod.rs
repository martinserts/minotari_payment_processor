use axum::{
    Router,
    extract::FromRef,
    routing::{get, post},
};
use sqlx::SqlitePool;
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

mod error;
mod payments;
mod version;

#[derive(Clone)]
pub struct AppState {
    pub db_pool: SqlitePool,
}

impl FromRef<AppState> for SqlitePool {
    fn from_ref(state: &AppState) -> Self {
        state.db_pool.clone()
    }
}

#[derive(OpenApi)]
#[openapi(
    paths(
        version::api_get_version,
        payments::api_create_payment,
        payments::api_get_payment,
    ),
    components(
        schemas(
            version::ServiceVersion,
            payments::PaymentRequest,
            payments::PaymentResponse,
        )
    ),
    tags(
        (name = "minotari-payment-processor", description = "Minotari Payment Processor API"),
    )
)]
pub struct ApiDoc;

pub fn create_router(db_pool: SqlitePool) -> Router {
    let app_state = AppState { db_pool };

    Router::new()
        .merge(SwaggerUi::new("/swagger-ui").url("/openapi.json", ApiDoc::openapi()))
        .route("/health/version", get(version::api_get_version))
        .route("/v1/payments", post(payments::api_create_payment))
        .route("/v1/payments/{payment_id}", get(payments::api_get_payment))
        .with_state(app_state)
}
