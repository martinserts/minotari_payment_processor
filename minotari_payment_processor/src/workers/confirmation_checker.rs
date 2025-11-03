use anyhow::anyhow;
use minotari_node_wallet_client::{BaseNodeWalletClient, http::Client};
use sqlx::{Acquire, SqlitePool};
use tari_transaction_components::offline_signing::models::{SignedOneSidedTransactionResult, TransactionResult};
use tari_transaction_components::rpc::models::TxLocation;
use tari_utilities::byte_array::ByteArray;
use tokio::time::{self, Duration};

use crate::db::payment_batch::PaymentBatchStatus;
use crate::db::{payment::Payment, payment_batch::PaymentBatch};

const DEFAULT_SLEEP_SECS: u64 = 60;
const REQUIRED_CONFIRMATIONS: u64 = 10;

pub async fn run(db_pool: SqlitePool, base_node_client: Client, sleep_secs: Option<u64>) {
    let sleep_secs = sleep_secs.unwrap_or(DEFAULT_SLEEP_SECS);
    let mut interval = time::interval(Duration::from_secs(sleep_secs));
    loop {
        interval.tick().await;
        if let Err(e) = check_transaction_confirmations(&db_pool, &base_node_client).await {
            eprintln!("Confirmation Checker worker error: {:?}", e);
        }
    }
}

async fn check_transaction_confirmations(db_pool: &SqlitePool, base_node_client: &Client) -> Result<(), anyhow::Error> {
    let mut conn = db_pool.acquire().await?;
    let batches = PaymentBatch::find_by_status(&mut conn, PaymentBatchStatus::AwaitingConfirmation).await?;

    for batch in batches {
        let batch_id = batch.id.clone();

        let signed_tx_json = batch
            .signed_tx_json
            .clone()
            .ok_or_else(|| anyhow::anyhow!("Batch {} has no signed_tx_json", batch_id))?;
        let signed_tx = SignedOneSidedTransactionResult::from_json(&signed_tx_json)?;
        let sig = &signed_tx.signed_transaction.transaction.body.kernels()[0].excess_sig;
        let excess_sig_nonce = sig.get_compressed_public_nonce().to_vec();
        let excess_sig_sig = sig.get_signature().to_vec();
        let tx_query_response = base_node_client
            .transaction_query(excess_sig_nonce, excess_sig_sig)
            .await?;

        match tx_query_response.location {
            TxLocation::Mined => {
                let mined_height = tx_query_response
                    .mined_height
                    .ok_or_else(|| anyhow!("Mined transaction has no mined_height"))?;

                let tip_info = base_node_client.get_tip_info().await?;
                let best_block_height = tip_info
                    .metadata
                    .ok_or_else(|| anyhow!("Tip info has no metadata"))?
                    .best_block_height();

                let confirmations = best_block_height.saturating_sub(mined_height) + 1;

                if confirmations >= REQUIRED_CONFIRMATIONS {
                    let mined_header_hash = tx_query_response
                        .mined_header_hash
                        .ok_or_else(|| anyhow!("Mined transaction has no mined_header_hash"))?;
                    let mined_timestamp = tx_query_response
                        .mined_timestamp
                        .ok_or_else(|| anyhow!("Mined transaction has no mined_timestamp"))?;

                    let mut tx = conn.begin().await?;
                    PaymentBatch::update_to_confirmed(
                        &mut tx,
                        &batch_id,
                        mined_height,
                        mined_header_hash,
                        mined_timestamp,
                    )
                    .await?;
                    let associated_payments = Payment::find_by_batch_id(&mut tx, &batch_id).await?;
                    let payment_ids: Vec<String> = associated_payments.iter().map(|p| p.id.clone()).collect();
                    Payment::update_payments_to_confirmed(&mut tx, &payment_ids).await?;
                    tx.commit().await?;
                    println!("Batch {} confirmed successfully.", batch_id);
                } else {
                    println!(
                        "Batch {} awaiting more confirmations ({} received).",
                        batch_id, confirmations
                    );
                }
            },
            TxLocation::InMempool => {
                println!("Batch {} is in mempool, awaiting mining.", batch_id);
            },
            TxLocation::None | TxLocation::NotStored => {
                eprintln!("Batch {} transaction not found on base node or mempool.", batch_id);
            },
        }
    }

    Ok(())
}
