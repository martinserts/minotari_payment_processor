use minotari_client::apis::{Error as ApiError, accounts_api, configuration::Configuration};
use minotari_client::models::{CreateTransactionRequest, RecipientRequest};
use sqlx::SqlitePool;
use std::sync::Arc;
use tokio::time::{self, Duration};

use crate::db::payment_batch::PaymentBatchStatus;
use crate::db::{payment::Payment, payment_batch::PaymentBatch};

const DEFAULT_SLEEP_SECS: u64 = 15;

pub async fn run(db_pool: SqlitePool, client_config: Arc<Configuration>, sleep_secs: Option<u64>) {
    let sleep_secs = sleep_secs.unwrap_or(DEFAULT_SLEEP_SECS);
    let mut interval = time::interval(Duration::from_secs(sleep_secs));

    loop {
        interval.tick().await;
        if let Err(e) = process_unsigned_transactions(&db_pool, &client_config).await {
            eprintln!("Unsigned Transaction Creator worker error: {:?}", e);
        }
    }
}

async fn process_unsigned_transactions(
    db_pool: &SqlitePool,
    client_config: &Configuration,
) -> Result<(), anyhow::Error> {
    let mut conn = db_pool.acquire().await?;
    let batches = PaymentBatch::find_by_status(&mut conn, PaymentBatchStatus::PendingBatching).await?;

    for batch in batches {
        let associated_payments = Payment::find_by_batch_id(&mut conn, &batch.id).await?;
        let recipients: Vec<RecipientRequest> = associated_payments
            .into_iter()
            .map(|p| RecipientRequest {
                address: p.recipient_address,
                amount: p.amount,
                payment_id: p.payment_id.map(Some),
            })
            .collect();

        let request_body = CreateTransactionRequest {
            idempotency_key: Some(Some(batch.pr_idempotency_key)),
            recipients,
            seconds_to_lock_utxos: None, // Use default lock timeout
        };

        match accounts_api::api_create_unsigned_transaction(client_config, &batch.account_name, request_body).await {
            Ok(response) => {
                let response_text = serde_json::to_string(&response)?;
                PaymentBatch::update_to_awaiting_signature(&mut conn, &batch.id, &response_text).await?;
            },
            Err(ApiError::ResponseError(response_content)) => {
                let status = response_content.status;
                let response_text = response_content.content;

                let error_message = format!(
                    "PR API returned unexpected status for batch {}: {} - {}",
                    batch.id, status, response_text
                );
                eprintln!("{}", error_message);
                PaymentBatch::increment_retry_count(&mut conn, &batch.id, &error_message).await?;
            },
            Err(e) => {
                let error_message = format!("Network error calling PR API for batch {}: {:?}", batch.id, e);
                eprintln!("{}", error_message);
                PaymentBatch::increment_retry_count(&mut conn, &batch.id, &error_message).await?;
            },
        }
    }

    Ok(())
}
