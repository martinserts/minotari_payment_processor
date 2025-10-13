use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::SqlitePool;
use utoipa::ToSchema;

use crate::{
    api::error::ApiError,
    db::{
        payment::{Payment, PaymentStatus},
        payment_batch::PaymentBatch,
    },
};
#[derive(Debug, Clone, Deserialize, ToSchema)]
pub struct PaymentRequest {
    pub client_id: String,
    pub account_name: String,
    pub recipient_address: String,
    pub amount: i64,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct PaymentResponse {
    pub payment_id: String,
    pub status: PaymentStatus,
    pub client_id: String,
    pub account_name: String,
    pub recipient_address: String,
    pub amount: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failure_reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mined_height: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mined_header_hash: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mined_timestamp: Option<i64>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl PaymentResponse {
    pub fn from_payment_and_batch(payment: Payment, payment_batch: Option<PaymentBatch>) -> Self {
        let (mined_height, mined_header_hash, mined_timestamp) = if let Some(batch) = payment_batch {
            (batch.mined_height, batch.mined_header_hash, batch.mined_timestamp)
        } else {
            (None, None, None)
        };

        PaymentResponse {
            payment_id: payment.id,
            status: payment.status,
            client_id: payment.client_id,
            account_name: payment.account_name,
            recipient_address: payment.recipient_address,
            amount: payment.amount,
            failure_reason: payment.failure_reason,
            mined_height,
            mined_header_hash,
            mined_timestamp,
            created_at: payment.created_at,
            updated_at: payment.updated_at,
        }
    }
}

impl From<Payment> for PaymentResponse {
    fn from(payment: Payment) -> Self {
        PaymentResponse::from_payment_and_batch(payment, None)
    }
}

#[utoipa::path(
    post,
    path = "/v1/payments",
    request_body = PaymentRequest,
    responses(
        (status = 202, description = "Payment request accepted for processing", body = PaymentResponse),
        (status = 200, description = "Payment request already exists (idempotent)", body = PaymentResponse),
        (status = 400, description = "Bad request", body = ApiError),
        (status = 500, description = "Internal server error", body = ApiError)
    )
)]
pub async fn api_create_payment(
    State(db_pool): State<SqlitePool>,
    Json(request): Json<PaymentRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let mut transaction = db_pool.begin().await?;

    // Idempotency check
    if let Some(existing_payment) =
        Payment::get_by_client_id(&mut transaction, &request.client_id, &request.account_name).await?
    {
        transaction.commit().await?;
        return Ok((StatusCode::OK, Json(PaymentResponse::from(existing_payment))));
    }

    // Validate amount
    if request.amount <= 0 {
        return Err(ApiError::BadRequest("Amount must be positive".to_string()));
    }

    let new_payment = Payment::create(
        &mut transaction,
        &request.client_id,
        &request.account_name,
        &request.recipient_address,
        request.amount,
        None, // payment_id is generated internally
    )
    .await?;

    transaction.commit().await?;

    Ok((StatusCode::ACCEPTED, Json(PaymentResponse::from(new_payment))))
}

#[utoipa::path(
    get,
    path = "/v1/payments/{payment_id}",
    responses(
        (status = 200, description = "Payment status retrieved successfully", body = PaymentResponse),
        (status = 404, description = "Payment not found", body = ApiError),
        (status = 500, description = "Internal server error", body = ApiError)
    )
)]
pub async fn api_get_payment(
    State(db_pool): State<SqlitePool>,
    Path(payment_id): Path<String>,
) -> Result<Json<PaymentResponse>, ApiError> {
    let mut conn = db_pool.acquire().await?;

    let (payment, payment_batch) = Payment::get_by_id_with_batch_info(&mut conn, &payment_id)
        .await?
        .ok_or_else(|| ApiError::NotFound("Payment not found".to_string()))?;

    Ok(Json(PaymentResponse::from_payment_and_batch(payment, payment_batch)))
}
