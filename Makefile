.PHONY: quick-check test build build-wasm fmt clippy security deny audit

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

# Supply-chain gate — the same commands CI runs in
# .github/workflows/security.yml, so a local pass means a CI pass. (CI runs the
# audit step even when deny fails; make stops at the first failure.)
# Kept out of `quick-check`: both tools fetch the RustSec advisory database over
# the network, which does not belong in a per-commit loop.
# Why both tools, and why --all-features: see the table in README.md.
security: deny audit

deny:
	$(CARGO) deny --locked --all-features check

audit:
	$(CARGO) audit
