use anyhow::Context;
use serde::{Deserialize, Serialize};
use tari_transaction_components::transaction_components::WalletOutput;

#[derive(Debug, Serialize, Deserialize)]
pub struct IntermediateContext {
    pub utxos: Vec<WalletOutput>,
}

impl IntermediateContext {
    pub fn from_json(json: &str) -> anyhow::Result<Self> {
        serde_json::from_str(json).context("Failed to deserialize intermediate context")
    }

    pub fn to_json(&self) -> anyhow::Result<String> {
        serde_json::to_string(self).context("Failed to serialize intermediate context")
    }
}
