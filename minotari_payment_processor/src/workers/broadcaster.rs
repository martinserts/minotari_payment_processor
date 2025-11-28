use anyhow::{Context, anyhow};
use minotari_node_wallet_client::{BaseNodeWalletClient, http::Client};
use sqlx::{SqliteConnection, SqlitePool};
use tari_transaction_components::offline_signing::models::SignedOneSidedTransactionResult;
use tari_utilities::message_format::MessageFormat;
use tokio::time::{self, Duration};

use crate::db::payment_batch::{PaymentBatch, PaymentBatchStatus};

const DEFAULT_SLEEP_SECS: u64 = 15;

pub async fn run(db_pool: SqlitePool, base_node_client: Client, sleep_secs: Option<u64>) {
    let sleep_secs = sleep_secs.unwrap_or(DEFAULT_SLEEP_SECS);
    println!(
        "Transaction Broadcaster worker started. Polling every {} seconds.",
        sleep_secs
    );

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

    if !batches.is_empty() {
        println!("INFO: Found {} batches awaiting broadcast.", batches.len());
    }

    for batch in batches {
        if let Err(e) = process_single_batch(&mut conn, base_node_client, &batch).await {
            let error_message = e.to_string();
            eprintln!(
                "Error broadcasting batch {}: {}. Incrementing retry count.",
                batch.id, error_message
            );

            if let Err(db_err) = PaymentBatch::increment_retry_count(&mut conn, &batch.id, &error_message).await {
                eprintln!(
                    "CRITICAL: Failed to update retry count for batch {}: {:?}",
                    batch.id, db_err
                );
            }
        }
    }

    Ok(())
}

async fn process_single_batch(
    conn: &mut SqliteConnection,
    base_node_client: &Client,
    batch: &PaymentBatch,
) -> Result<(), anyhow::Error> {
    let batch_id = &batch.id;
    println!("INFO: Starting broadcast sequence for Batch ID: {}", batch_id);

    let signed_tx_json = batch
        .signed_tx_json
        .clone()
        .ok_or_else(|| anyhow!("Batch {} has no signed_tx_json", batch_id))?;

    let signed_tx = SignedOneSidedTransactionResult::from_json(&signed_tx_json)
        .map_err(|e| anyhow!("Failed to deserialize signed tx: {}", e))?;

    PaymentBatch::update_to_broadcasting(conn, batch_id)
        .await
        .context("Failed to set status to broadcasting")?;

    println!(
        "INFO: Batch {}: Status updated to 'Broadcasting'. Submitting to Base Node...",
        batch_id
    );

    let response = base_node_client
        .submit_transaction(signed_tx.signed_transaction.transaction)
        .await
        .context("Network error submitting transaction to Base Node")?;

    if response.accepted {
        println!("INFO: Batch {}: Transaction ACCEPTED by Base Node.", batch_id);

        PaymentBatch::update_to_awaiting_confirmation(conn, batch_id)
            .await
            .context("Failed to update status to AwaitingConfirmation")?;

        println!("INFO: Batch {}: Status updated to 'AwaitingConfirmation'.", batch_id);
    } else {
        println!(
            "WARN: Batch {}: Transaction REJECTED by Base Node. Reason: {}",
            batch_id, response.rejection_reason
        );
        return Err(anyhow!(
            "Tari base node rejected transaction: {}",
            response.rejection_reason
        ));
    }

    Ok(())
}
