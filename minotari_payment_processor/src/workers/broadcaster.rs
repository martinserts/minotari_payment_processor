use minotari_node_wallet_client::{BaseNodeWalletClient, http::Client};
use sqlx::SqlitePool;
use tari_transaction_components::offline_signing::models::{SignedOneSidedTransactionResult, TransactionResult};
use tokio::time::{self, Duration};

use crate::db::payment_batch::{PaymentBatch, PaymentBatchStatus};

const DEFAULT_SLEEP_SECS: u64 = 15;

pub async fn run(db_pool: SqlitePool, base_node_client: Client, sleep_secs: Option<u64>) {
    let sleep_secs = sleep_secs.unwrap_or(DEFAULT_SLEEP_SECS);
    let mut interval = time::interval(Duration::from_secs(sleep_secs));
    loop {
        interval.tick().await;
        if let Err(e) = process_transactions_to_broadcast(&db_pool, &base_node_client).await {
            eprintln!("Transaction Broadcaster worker error: {:?}", e);
        }
    }
}

async fn process_transactions_to_broadcast(
    db_pool: &SqlitePool,
    base_node_client: &Client,
) -> Result<(), anyhow::Error> {
    let mut conn = db_pool.acquire().await?;
    let batches = PaymentBatch::find_by_status(&mut conn, PaymentBatchStatus::AwaitingBroadcast).await?;

    for batch in batches {
        // Update its status to `BROADCASTING`.
        PaymentBatch::update_to_broadcasting(&mut conn, &batch.id).await?;

        let batch_id = batch.id.clone();
        let signed_tx_json = batch
            .signed_tx_json
            .clone()
            .ok_or_else(|| anyhow::anyhow!("Batch {} has no signed_tx_json", batch_id))?;
        let signed_tx = SignedOneSidedTransactionResult::from_json(&signed_tx_json)?;

        let response = base_node_client
            .submit_transaction(signed_tx.signed_transaction.transaction)
            .await?;

        if response.accepted {
            PaymentBatch::update_to_awaiting_confirmation(&mut conn, &batch_id).await?;
        } else {
            let error_message = format!(
                "Tari base node rejected transaction for batch {}: {}",
                batch_id, response.rejection_reason
            );
            eprintln!("{}", error_message);
            PaymentBatch::increment_retry_count(&mut conn, &batch_id, &error_message).await?;
        }
    }

    Ok(())
}
