CREATE TABLE _sqlx_migrations (
    version BIGINT PRIMARY KEY,
    description TEXT NOT NULL,
    installed_on TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    success BOOLEAN NOT NULL,
    checksum BLOB NOT NULL,
    execution_time BIGINT NOT NULL
);
CREATE TABLE payments (
    -- The unique ID for this payment, returned to the client.
    id TEXT PRIMARY KEY NOT NULL,

    -- The idempotency key provided by the client (e.g., their order ID).
    client_id TEXT NOT NULL,

    -- The name of the PR account to use for this payment.
    -- Used for batching payments together.
    account_name TEXT NOT NULL,

    -- The current state of this payment in its lifecycle.
    -- States: RECEIVED, BATCHED, CONFIRMED, FAILED
    status TEXT NOT NULL,

    -- Foreign key to the batch this payment belongs to. NULL until batched.
    payment_batch_id TEXT,

    -- Payment details
    recipient_address TEXT NOT NULL,
    amount BIGINT NOT NULL,
    payment_id TEXT,

    -- If the payment fails, this will contain the reason.
    failure_reason TEXT,

    -- Timestamps for tracking
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (payment_batch_id) REFERENCES payment_batches(id),
    -- Ensures a client can't accidentally submit the same payment twice.
    UNIQUE (account_name, client_id)
);
CREATE TABLE payment_batches (
    -- The unique ID for this internal batch.
    id TEXT PRIMARY KEY NOT NULL,

    -- The PR account name for all payments in this batch.
    account_name TEXT NOT NULL,

    -- The current state of the batch in the processing pipeline.
    -- States:
    -- PENDING_BATCHING: New batch, ready for PR.
    -- AWAITING_SIGNATURE: Unsigned TX received from PR.
    -- SIGNING_IN_PROGRESS: Handed off to the signing worker.
    -- AWAITING_BROADCAST: TX has been signed successfully.
    -- BROADCASTING: Handed off to the broadcast worker.
    -- AWAITING_CONFIRMATION: TX is on-chain, waiting for enough confirmations.
    -- CONFIRMED: The final success state.
    -- FAILED: A non-recoverable error occurred.
    status TEXT NOT NULL,

    -- The idempotency key sent to PR for creating the unsigned transaction.
    -- This is crucial for retrying the PR call safely.
    pr_idempotency_key TEXT NOT NULL,

    -- Stores the JSON response from PR containing the unsigned transaction.
    unsigned_tx_json TEXT,

    -- Stores the file path or content of the signed transaction from the CLI.
    signed_tx_json TEXT,

    -- Stores any error message related to the batch's failure.
    error_message TEXT,

    -- Retry tracking for recoverable failures.
    retry_count INTEGER NOT NULL DEFAULT 0,

    -- Transaction mining details
    mined_height BIGINT,
    mined_header_hash TEXT,
    mined_timestamp BIGINT,

    -- Timestamps
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX idx_payments_status ON payments(status);
CREATE INDEX idx_payment_batches_status ON payment_batches(status);
