#### State Descriptions

| Status | Worker Responsible | Description |
| :--- | :--- | :--- |
| **PENDING_BATCHING** | `Batch Creator` | A new batch has been created from individual payments. It waits for the transaction structure to be built. |
| **AWAITING_SIGNATURE** | `Unsigned TX Creator` | The transaction structure (unsigned) has been retrieved from the Wallet API. It is ready to be signed. |
| **SIGNING_IN_PROGRESS** | `Transaction Signer` | A worker has picked up the batch and is currently calculating the signature (CPU intensive). |
| **AWAITING_BROADCAST** | `Transaction Signer` | The transaction is fully signed and stored in the DB, ready to be sent to the network. |
| **BROADCASTING** | `Broadcaster` | A worker is currently attempting to submit the transaction to the Base Node. |
| **AWAITING_CONFIRMATION** | `Broadcaster` | The transaction was accepted by the mempool. The system is now polling for block depth. |
| **CONFIRMED** | `Confirmation Checker` | The transaction has reached the required block depth (e.g., 10 blocks). |
| **FAILED** | All | A terminal state indicating a non-recoverable error (or max retries reached). |
