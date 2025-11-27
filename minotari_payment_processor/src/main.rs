use dotenv::dotenv;
use minotari_client::apis::configuration::Configuration as MinotariConfiguration;
use minotari_node_wallet_client::http::Client as BaseNodeClient;
use minotari_payment_processor::{api, config::PaymentProcessorEnv, db, workers};
use std::sync::Arc;
use tokio::{net::TcpListener, signal};
use url::Url;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv().ok();
    let env = PaymentProcessorEnv::load()?;
    let app_env = env.clone();

    println!("Starting Minotari Payment Processor...");

    let db_pool = db::init_db(&env.database_url).await?;
    println!("Database initialized.");

    let client_config = Arc::new(MinotariConfiguration {
        base_path: env.payment_receiver,
        ..MinotariConfiguration::default()
    });

    let base_node_url = Url::parse(&env.base_node)?;
    let base_node_client = BaseNodeClient::new(base_node_url.clone(), base_node_url.clone());

    // Spawn workers
    tokio::spawn(workers::batch_creator::run(
        db_pool.clone(),
        env.batch_creator_sleep_secs,
    ));
    tokio::spawn(workers::unsigned_tx_creator::run(
        db_pool.clone(),
        client_config.clone(),
        env.tari_network,
        env.accounts.clone(),
        env.unsigned_tx_creator_sleep_secs,
    ));
    tokio::spawn(workers::transaction_signer::run(
        db_pool.clone(),
        env.tari_network,
        env.console_wallet_path.clone(),
        env.console_wallet_base_path.clone(),
        env.console_wallet_password.clone(),
        env.transaction_signer_sleep_secs,
    ));
    tokio::spawn(workers::broadcaster::run(
        db_pool.clone(),
        base_node_client.clone(),
        env.broadcaster_sleep_secs,
    ));
    tokio::spawn(workers::confirmation_checker::run(
        db_pool.clone(),
        base_node_client.clone(),
        env.confirmation_checker_sleep_secs,
        env.confirmation_checker_required_confirmations.unwrap_or(10),
    ));
    println!("Minotari Payment Processor started. Press Ctrl+C to shut down.");

    // Create Axum API router
    let app = api::create_router(db_pool.clone(), app_env);
    let addr = format!("{}:{}", env.listen_ip, env.listen_port);
    let listener = TcpListener::bind(&addr).await?;
    println!("Axum API server listening on {}", addr);
    tokio::spawn(async move {
        axum::serve(listener, app.into_make_service()).await.unwrap();
    });

    signal::ctrl_c().await?;
    println!("Ctrl+C received, shutting down.");

    Ok(())
}
