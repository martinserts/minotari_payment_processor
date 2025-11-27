use anyhow::{Context, anyhow};
use minotari_client::apis::{Error as ApiError, accounts_api, configuration::Configuration};
use minotari_client::models::LockFundsRequest;
use sqlx::{SqliteConnection, SqlitePool};
use std::collections::HashMap;
use std::sync::Arc;
use tari_common::configuration::Network;
use tari_common_types::tari_address::TariAddress;
use tari_common_types::transaction::TxId;
use tari_transaction_components::consensus::ConsensusConstantsBuilder;
use tari_transaction_components::key_manager::{
    KeyManager,
    wallet_types::{ViewWallet, WalletType},
};
use tari_transaction_components::offline_signing::{PaymentRecipient, prepare_one_sided_transaction_for_signing};
use tari_transaction_components::{
    TransactionBuilder,
    // offline_signing::offline_signer::OfflineSigner,
    tari_amount::MicroMinotari,
    transaction_components::{MemoField, OutputFeatures, WalletOutput, memo_field::TxType},
};
use tokio::time::{self, Duration};

use crate::config::PaymentReceiverAccount;
use crate::db::payment::Payment;
use crate::db::payment_batch::{PaymentBatch, PaymentBatchStatus};

const DEFAULT_SLEEP_SECS: u64 = 15;

pub async fn run(
    db_pool: SqlitePool,
    client_config: Arc<Configuration>,
    network: Network,
    accounts: HashMap<String, PaymentReceiverAccount>,
    sleep_secs: Option<u64>,
) {
    let sleep_secs = sleep_secs.unwrap_or(DEFAULT_SLEEP_SECS);
    println!(
        "Unsigned Transaction Creator worker started. Polling every {} seconds.",
        sleep_secs
    );

    let mut interval = time::interval(Duration::from_secs(sleep_secs));

    loop {
        interval.tick().await;
        if let Err(e) = process_unsigned_transactions(&db_pool, &client_config, network, &accounts).await {
            eprintln!("Unsigned Transaction Creator worker error: {:?}", e);
        }
    }
}

async fn process_unsigned_transactions(
    db_pool: &SqlitePool,
    client_config: &Configuration,
    network: Network,
    accounts: &HashMap<String, PaymentReceiverAccount>,
) -> Result<(), anyhow::Error> {
    let mut conn = db_pool.acquire().await?;

    let batches = PaymentBatch::find_by_status(&mut conn, PaymentBatchStatus::PendingBatching).await?;

    if !batches.is_empty() {
        println!(
            "INFO: Found {} batches pending unsigned transaction creation.",
            batches.len()
        );
    }

    for batch in batches {
        if let Err(e) = process_single_batch(&mut conn, client_config, network, accounts, &batch).await {
            let error_message = e.to_string();
            eprintln!(
                "Error processing batch {}: {}. Incrementing retry count.",
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
    client_config: &Configuration,
    network: Network,
    accounts: &HashMap<String, PaymentReceiverAccount>,
    batch: &PaymentBatch,
) -> Result<(), anyhow::Error> {
    let batch_id = &batch.id;
    println!("INFO: Starting processing for Batch ID: {}", batch_id);

    let associated_payments = Payment::find_by_batch_id(conn, batch_id)
        .await
        .context("Failed to fetch payments for batch")?;

    if associated_payments.is_empty() {
        return Err(anyhow!("Batch {} has no associated payments", batch_id));
    }

    let total_amount: i64 = associated_payments.iter().map(|p| p.amount).sum();
    let account_name = &batch.account_name;

    println!(
        "INFO: Batch {}: Locking funds via API. Account: '{}', Total Amount: {}",
        batch_id, account_name, total_amount
    );

    let lock_funds_request = LockFundsRequest {
        amount: total_amount,
        idempotency_key: Some(Some(batch.pr_idempotency_key.clone())),
        ..Default::default()
    };

    let locked_funds = match accounts_api::api_lock_funds(client_config, account_name, lock_funds_request).await {
        Ok(response) => {
            println!(
                "INFO: Batch {}: Funds locked successfully. Received {} inputs (UTXOs).",
                batch_id,
                response.utxos.len()
            );
            response
        },
        Err(ApiError::ResponseError(response_content)) => {
            return Err(anyhow!(
                "PR API returned unexpected status: {} - {}",
                response_content.status,
                response_content.content
            ));
        },
        Err(e) => return Err(anyhow!("Network error calling PR API: {:?}", e)),
    };

    println!("INFO: Batch {}: Preparing local offline signer...", batch_id);

    let first_recipient = &associated_payments[0];
    let account = accounts
        .get(&first_recipient.account_name.to_lowercase())
        .ok_or_else(|| {
            anyhow!(
                "Account '{}' not found in local configuration",
                first_recipient.account_name
            )
        })?;

    let view_wallet = ViewWallet::new(account.public_spend_key.clone(), account.view_key.clone(), None);
    let key_manager = KeyManager::new(WalletType::ViewWallet(view_wallet)).context("Failed to create KeyManager")?;

    let consensus_constants = ConsensusConstantsBuilder::new(network).build();
    let mut tx_builder = TransactionBuilder::new(consensus_constants, key_manager.clone(), network)
        .context("Failed to create TransactionBuilder")?;

    tx_builder.with_fee_per_gram(MicroMinotari(5));

    for utxo_value in &locked_funds.utxos {
        let utxo: WalletOutput =
            serde_json::from_value(utxo_value.clone()).map_err(|e| anyhow!("Failed to deserialize utxo: {}", e))?;
        tx_builder
            .with_input(utxo)
            .context("Failed to add input to transaction")?;
    }

    let output_features = OutputFeatures::default();
    let tx_id = TxId::new_random();
    let recipients: Vec<PaymentRecipient> = associated_payments
        .iter()
        .map(|p| -> Result<PaymentRecipient, anyhow::Error> {
            let payment_id = match &p.payment_id {
                Some(s) => MemoField::new_open_from_string(s, TxType::PaymentToOther)
                    .map_err(|e| anyhow!(e))
                    .context("Failed to create payment ID memo")?,
                None => MemoField::new_empty(),
            };

            let recipient_address = TariAddress::from_base58(&p.recipient_address)
                .map_err(|e| anyhow!(e.to_string()))
                .context("Invalid recipient address")?;

            Ok(PaymentRecipient {
                amount: MicroMinotari(p.amount as u64),
                output_features: output_features.clone(),
                address: recipient_address,
                payment_id,
            })
        })
        .collect::<Result<Vec<PaymentRecipient>, anyhow::Error>>()?;

    println!("INFO: Batch {}: Building transaction outputs...", batch_id);

    let payment_id = MemoField::new_empty();
    let result =
        prepare_one_sided_transaction_for_signing(tx_id, tx_builder, &recipients, payment_id, account.address.clone())
            .context("Failed to prepare one-sided transaction")?;

    let response_text = serde_json::to_string(&result).context("Failed to serialize transaction result")?;

    PaymentBatch::update_to_awaiting_signature(conn, batch_id, &response_text)
        .await
        .context("Failed to update batch status to AwaitingSignature")?;

    println!(
        "INFO: Batch {}: Unsigned transaction created. Status updated to 'AwaitingSignature'.",
        batch_id
    );

    Ok(())
}
