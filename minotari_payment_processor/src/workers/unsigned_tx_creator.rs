use anyhow::anyhow;
use minotari_client::apis::{Error as ApiError, accounts_api, configuration::Configuration};
use minotari_client::models::LockFundsRequest;
use sqlx::SqlitePool;
use std::collections::HashMap;
use std::sync::Arc;
use tari_common::configuration::Network;
use tari_common_types::tari_address::TariAddress;
use tari_common_types::transaction::TxId;
use tari_transaction_components::TransactionBuilder;
use tari_transaction_components::consensus::ConsensusConstantsBuilder;
use tari_transaction_components::key_manager::KeyManager;
use tari_transaction_components::key_manager::wallet_types::{ViewWallet, WalletType};
use tari_transaction_components::{
    offline_signing::offline_signer::OfflineSigner,
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

    for batch in batches {
        let associated_payments = Payment::find_by_batch_id(&mut conn, &batch.id).await?;
        if associated_payments.is_empty() {
            continue;
        }
        if associated_payments.len() > 1 {
            return Err(anyhow!("Only one recipient is supported for now"));
        }
        let total_amount = associated_payments.iter().map(|p| p.amount).sum();

        let lock_funds_request = LockFundsRequest {
            amount: total_amount,
            idempotency_key: Some(Some(batch.pr_idempotency_key.clone())),
            ..Default::default()
        };

        let locked_funds =
            match accounts_api::api_lock_funds(client_config, &batch.account_name, lock_funds_request).await {
                Ok(response) => response,
                Err(ApiError::ResponseError(response_content)) => {
                    let status = response_content.status;
                    let response_text = response_content.content;

                    let error_message = format!(
                        "PR API returned unexpected status for batch {}: {} - {}",
                        batch.id, status, response_text
                    );
                    eprintln!("{}", error_message);
                    PaymentBatch::increment_retry_count(&mut conn, &batch.id, &error_message).await?;
                    continue;
                },
                Err(e) => {
                    let error_message = format!("Network error calling PR API for batch {}: {:?}", batch.id, e);
                    eprintln!("{}", error_message);
                    PaymentBatch::increment_retry_count(&mut conn, &batch.id, &error_message).await?;
                    continue;
                },
            };

        let recipient = &associated_payments[0];
        let recipient_address =
            TariAddress::from_hex(&recipient.recipient_address).map_err(|e| anyhow!(e.to_string()))?;

        let account = accounts
            .get(&recipient.account_name.to_lowercase())
            .ok_or_else(|| anyhow!("Account '{}' not found in local configuration", recipient.account_name))?;

        let view_wallet = ViewWallet::new(account.public_spend_key.clone(), account.view_key.clone(), None);
        let wallet_type = WalletType::ViewWallet(view_wallet);
        let key_manager = KeyManager::new(wallet_type)?;
        let consensus_constants = ConsensusConstantsBuilder::new(network).build();
        let mut tx_builder = TransactionBuilder::new(consensus_constants, key_manager.clone(), network)?;
        tx_builder.with_fee_per_gram(MicroMinotari(5));
        for utxo_value in &locked_funds.utxos {
            let utxo: WalletOutput =
                serde_json::from_value(utxo_value.clone()).map_err(|e| anyhow!("Failed to deserialize utxo: {}", e))?;
            tx_builder.with_input(utxo)?;
        }

        let mut offline_signing = OfflineSigner::new(key_manager);
        let tx_id = TxId::new_random();
        let payment_id = match &recipient.payment_id {
            Some(s) => MemoField::new_open_from_string(s, TxType::PaymentToOther).map_err(|e| anyhow!(e))?,
            None => MemoField::new_empty(),
        };
        let output_features = OutputFeatures::default();

        let result = offline_signing.prepare_one_sided_transaction_for_signing(
            tx_id,
            tx_builder,
            recipient_address,
            MicroMinotari(recipient.amount as u64),
            output_features,
            payment_id,
            account.address.clone(),
        )?;

        let response_text = serde_json::to_string(&result)?;
        PaymentBatch::update_to_awaiting_signature(&mut conn, &batch.id, &response_text).await?;
    }

    Ok(())
}
