use sqlx::SqlitePool;
use std::collections::HashMap;
use tokio::time::{self, Duration};
use uuid::Uuid;

use crate::db::{payment::Payment, payment_batch::PaymentBatch};

const DEFAULT_SLEEP_SECS: u64 = 10 * 60; // 10 minutes
const MAX_BATCH_SIZE: i64 = 100;

pub async fn run(db_pool: SqlitePool, sleep_secs: Option<u64>) {
    let sleep_secs = sleep_secs.unwrap_or(DEFAULT_SLEEP_SECS);
    loop {
        let mut should_sleep = true;
        match process_batches(&db_pool).await {
            Ok(more_batches_expected) => {
                if more_batches_expected {
                    should_sleep = false;
                }
            },
            Err(e) => {
                eprintln!("Batch Creator worker error: {:?}", e);
            },
        }

        if should_sleep {
            time::sleep(Duration::from_secs(sleep_secs)).await;
        }
    }
}

async fn process_batches(db_pool: &SqlitePool) -> Result<bool, anyhow::Error> {
    let mut conn = db_pool.acquire().await?;
    let payments = Payment::find_receivable_payments(&mut conn, MAX_BATCH_SIZE).await?;
    let payments_count = payments.len();

    if payments.is_empty() {
        return Ok(false);
    }

    let mut payments_by_account: HashMap<String, Vec<Payment>> = HashMap::new();
    for payment in payments {
        payments_by_account
            .entry(payment.account_name.clone())
            .or_default()
            .push(payment);
    }

    for (account_name, account_payments) in payments_by_account {
        let payment_ids: Vec<String> = account_payments.iter().map(|p| p.id.clone()).collect();
        let pr_idempotency_key = Uuid::new_v4().to_string();
        if let Err(e) =
            PaymentBatch::create_with_payments(&mut conn, &account_name, &pr_idempotency_key, &payment_ids).await
        {
            eprintln!("Failed to create batch for account {}: {:?}", account_name, e);
        }
    }

    Ok(payments_count == MAX_BATCH_SIZE as usize)
}
