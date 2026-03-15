.PHONY: quick-check test build build-wasm fmt clippy

CARGO := cargo

quick-check: fmt clippy test

fmt:
	$(CARGO) fmt --all

clippy:
	$(CARGO) clippy --all-targets --all-features -- -D warnings

test:
	$(CARGO) test --all-features

build:
	$(CARGO) build --release

build-wasm:
	$(CARGO) build --target wasm32-unknown-unknown --no-default-features --features workers,cachekitio,encryption

