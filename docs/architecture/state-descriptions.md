#### State Descriptions

| Status | Worker Responsible | Description |
| :--- | :--- | :--- |
| **PENDING_BATCHING** | `Batch Creator` / `Broadcaster` | A new batch created, OR a batch that has completed a consolidation cycle (Split) and is waiting for the final transaction to be built using the new UTXOs. |
| **AWAITING_SIGNATURE** | `Unsigned TX Creator` | The transaction structure (unsigned) has been retrieved. It may be a CoinJoin (Split) or a Final Payment. |
| **SIGNING_IN_PROGRESS** | `Transaction Signer` | A worker has picked up the batch and is calculating signatures. |
| **AWAITING_BROADCAST** | `Transaction Signer` | The transaction is fully signed and stored in the DB. |
| **BROADCASTING** | `Broadcaster` | Submitting transactions. If `is_consolidation=true`, it verifies mempool presence and loops status back to `PENDING_BATCHING`. If `false`, moves to `AWAITING_CONFIRMATION`. |
| **AWAITING_CONFIRMATION** | `Broadcaster` | The final transaction was accepted. System polls for block depth. |
| **CONFIRMED** | `Confirmation Checker` | The transaction has reached the required block depth. |
| **FAILED** | All | Terminal error state. |