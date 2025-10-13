.PHONY: regenerate-client db-dump fmt-check lint-check

regenerate-client:
	@scripts/regenerate_client.sh

db-dump:
	./scripts/dump_db.sh

fmt-check:
	cargo fmt --all -- --check

lint-check:
	cargo clippy --all-targets --all-features -- -D warnings
