use anyhow::Context;
use log::{error, info};
use sqlx::SqlitePool;
use std::collections::HashMap;
use tokio::time::{self, Duration};
use uuid::Uuid;

use crate::MAX_BATCH_SIZE;
use crate::db::{payment::Payment, payment_batch::PaymentBatch};

const DEFAULT_SLEEP_SECS: u64 = 10 * 60; // 10 minutes

pub async fn run(db_pool: SqlitePool, sleep_secs: Option<u64>) {
    let sleep_duration = Duration::from_secs(sleep_secs.unwrap_or(DEFAULT_SLEEP_SECS));

    info!("Batch Creator worker started. Cycle interval: {:?}.", sleep_duration);

    loop {
        match process_payment_cycle(&db_pool).await {
            Ok(more_batches_expected) => {
                if !more_batches_expected {
                    time::sleep(sleep_duration).await;
                } else {
                    info!("Max batch size reached. Continuing to next cycle immediately.");
                }
            },
            Err(e) => {
                error!("Batch Creator worker critical error: {:?}. Sleeping...", e);
                time::sleep(sleep_duration).await;
            },
        }
    }
}

async fn process_payment_cycle(db_pool: &SqlitePool) -> Result<bool, anyhow::Error> {
    let mut conn = db_pool.acquire().await.context("Failed to acquire DB connection")?;

    let limit = MAX_BATCH_SIZE as i64;
    let payments = Payment::find_receivable_payments(&mut conn, limit)
        .await
        .context("Failed to find receivable payments")?;

    let payments_count = payments.len();

    if payments.is_empty() {
        return Ok(false);
    }

    info!("Found {} receivable payments to process.", payments_count);

    let mut payments_by_account: HashMap<String, Vec<Payment>> = HashMap::new();
    for payment in payments {
        payments_by_account
            .entry(payment.account_name.clone())
            .or_default()
            .push(payment);
    }

    for (account_name, account_payments) in payments_by_account {
        info!(
            "Processing group for account '{}' with {} payments.",
            account_name,
            account_payments.len()
        );

        if let Err(e) = process_account_batch(db_pool, &account_name, &account_payments).await {
            error!("Failed to create batch for account '{}': {:?}", account_name, e);
        }
    }

    Ok(payments_count == MAX_BATCH_SIZE)
}

async fn process_account_batch(
    db_pool: &SqlitePool,
    account_name: &str,
    payments: &[Payment],
) -> Result<(), anyhow::Error> {
    if payments.is_empty() {
        return Ok(());
    }

    let payment_ids: Vec<String> = payments.iter().map(|p| p.id.clone()).collect();
    let pr_idempotency_key = Uuid::new_v4().to_string();

    info!(
        "Creating batch for Account: '{}'. Idempotency Key: {}. Payment Count: {}",
        account_name,
        pr_idempotency_key,
        payments.len()
    );

    let mut tx = db_pool.begin().await.context("Failed to start transaction")?;

    let batch = PaymentBatch::create_with_payments(&mut tx, account_name, &pr_idempotency_key, &payment_ids)
        .await
        .with_context(|| format!("Failed to create batch entry for account {}", account_name))?;

    tx.commit().await.context("Failed to commit batch transaction")?;

    info!(
        target: "audit",
        "Batch Created Successfully. BatchID: {}, Account: {}, Count: {}",
        batch.id,
        account_name,
        payments.len()
    );

    Ok(())
}
