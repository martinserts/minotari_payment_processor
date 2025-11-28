use anyhow::{Context, anyhow};
use sqlx::{SqliteConnection, SqlitePool};
use std::io::Write;
use tari_common::configuration::Network;
use tempfile::NamedTempFile;
use tokio::fs;
use tokio::process::Command;
use tokio::time::{self, Duration};

use crate::db::payment_batch::{PaymentBatch, PaymentBatchStatus};

const DEFAULT_SLEEP_SECS: u64 = 10;

pub async fn run(
    db_pool: SqlitePool,
    network: Network,
    console_wallet_path: String,
    console_wallet_base_path: String,
    console_wallet_password: String,
    sleep_secs: Option<u64>,
) {
    let sleep_secs = sleep_secs.unwrap_or(DEFAULT_SLEEP_SECS);
    println!(
        "Transaction Signer worker started. Polling every {} seconds.",
        sleep_secs
    );

    let mut interval = time::interval(Duration::from_secs(sleep_secs));

    loop {
        interval.tick().await;
        if let Err(e) = process_transactions_to_sign(
            &db_pool,
            network,
            &console_wallet_path,
            &console_wallet_base_path,
            &console_wallet_password,
        )
        .await
        {
            eprintln!("Transaction Signer worker error: {:?}", e);
        }
    }
}

async fn process_transactions_to_sign(
    db_pool: &SqlitePool,
    network: Network,
    console_wallet_path: &str,
    console_wallet_base_path: &str,
    console_wallet_password: &str,
) -> Result<(), anyhow::Error> {
    let mut conn = db_pool.acquire().await?;

    let batches = PaymentBatch::find_by_status(&mut conn, PaymentBatchStatus::AwaitingSignature).await?;

    if !batches.is_empty() {
        println!("INFO: Found {} batches awaiting signature.", batches.len());
    }

    for batch in batches {
        if let Err(e) = process_single_batch(
            &mut conn,
            network,
            console_wallet_path,
            console_wallet_base_path,
            console_wallet_password,
            &batch,
        )
        .await
        {
            let error_message = format!("{:#}", e);
            eprintln!(
                "Error signing batch {}: {}. Attempting to revert status...",
                batch.id, error_message
            );

            let revert_result = if let Some(json) = &batch.unsigned_tx_json {
                PaymentBatch::update_to_awaiting_signature(&mut conn, &batch.id, json).await
            } else {
                Err(anyhow::anyhow!("Cannot revert: Batch missing unsigned_tx_json"))?
            };

            match revert_result {
                Ok(_) => println!("INFO: Batch {} reverted to 'AwaitingSignature'.", batch.id),
                Err(revert_e) => eprintln!("CRITICAL: Failed to revert batch {} status: {:?}", batch.id, revert_e),
            }

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
    network: Network,
    console_wallet_path: &str,
    console_wallet_base_path: &str,
    console_wallet_password: &str,
    batch: &PaymentBatch,
) -> Result<(), anyhow::Error> {
    let batch_id = &batch.id;
    println!("INFO: Starting processing for Batch ID: {}", batch_id);

    PaymentBatch::update_to_signing_in_progress(conn, batch_id)
        .await
        .context("Failed to update status to SigningInProgress")?;

    println!("INFO: Batch {}: Status updated to 'SigningInProgress'.", batch_id);

    let unsigned_tx_json = batch
        .unsigned_tx_json
        .clone()
        .ok_or_else(|| anyhow!("Batch {} has no unsigned_tx_json", batch_id))?;

    let mut input_file = NamedTempFile::with_prefix("unsigned-tx-").context("Failed to create temp input file")?;
    let input_path = input_file.path().to_path_buf();

    input_file
        .write_all(unsigned_tx_json.as_bytes())
        .context("Failed to write unsigned tx to temp file")?;
    input_file.flush().context("Failed to flush input file")?;

    let output_file = NamedTempFile::with_prefix("signed-tx-").context("Failed to create temp output file")?;
    let output_path = output_file.path().to_path_buf();

    println!(
        "INFO: Batch {}: Temp files prepared. Input: {:?}, Output: {:?}",
        batch_id, input_path, output_path
    );

    println!("INFO: Batch {}: Invoking external signer CLI...", batch_id);

    sign_with_cli(
        network,
        console_wallet_path,
        console_wallet_password,
        console_wallet_base_path,
        &input_path,
        &output_path,
    )
    .await
    .context("External signing process failed")?;

    println!("INFO: Batch {}: External signer CLI finished successfully.", batch_id);

    let signed_tx_json = fs::read_to_string(&output_path)
        .await
        .context("Failed to read signed transaction from output file")?;

    PaymentBatch::update_to_awaiting_broadcast(conn, batch_id, &signed_tx_json)
        .await
        .context("Failed to update status to AwaitingBroadcast")?;

    println!(
        "INFO: Batch {}: Status updated to 'AwaitingBroadcast'. Processing complete.",
        batch_id
    );

    Ok(())
}

/// Executes the Minotari Console Wallet.
async fn sign_with_cli(
    network: Network,
    executable_path: &str,
    password: &str,
    base_path: &str,
    input_path: &std::path::Path,
    output_path: &std::path::Path,
) -> Result<(), anyhow::Error> {
    let mut cmd = Command::new(executable_path);
    cmd.current_dir(base_path)
        .env("MINOTARI_WALLET_PASSWORD", password)
        .arg("--command-mode-auto-exit")
        .arg("--base-path")
        .arg(base_path)
        .arg("--network")
        .arg(network.to_string())
        .arg("--skip-recovery")
        .arg("sign-one-sided-transaction")
        .arg("--input-file")
        .arg(input_path)
        .arg("--output-file")
        .arg(output_path);

    let command_string = format!(
        "MINOTARI_WALLET_PASSWORD=*** {} {}",
        cmd.as_std().get_program().to_string_lossy(),
        cmd.as_std()
            .get_args()
            .map(|arg| arg.to_string_lossy())
            .collect::<Vec<_>>()
            .join(" ")
    );

    println!("DEBUG: Executing Command: {}", command_string);

    let cmd_output = cmd.output().await.context("Failed to execute console wallet command")?;

    if !cmd_output.status.success() {
        let stderr = String::from_utf8_lossy(&cmd_output.stderr);
        let stdout = String::from_utf8_lossy(&cmd_output.stdout);
        return Err(anyhow!(
            "CLI exited with error code: {}.\nStderr: {}\nStdout: {}",
            cmd_output.status,
            stderr,
            stdout
        ));
    } else {
        let stdout = String::from_utf8_lossy(&cmd_output.stdout);
        if !stdout.trim().is_empty() {
            println!("DEBUG: CLI Stdout: {}", stdout);
        }
    }

    Ok(())
}
