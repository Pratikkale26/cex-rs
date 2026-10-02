.PHONY: dev run build test clippy clean

# Run both engine and gateway concurrently with a single command
dev:
	@./dev.sh

run: dev

# Build all workspace crates
build:
	cargo build --workspace

# Run all unit tests
test:
	cargo test --workspace

# Run clippy linter across workspace
clippy:
	cargo clippy --workspace --all-targets

# Clean build artifacts
clean:
	cargo clean
