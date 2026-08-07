.PHONY: quick-check test test-wasm build build-wasm fmt clippy security deny audit

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

# wasm32 runtime tests (LAB-1079) — same invocation as the CI `wasm` job.
# Needs a wasm-bindgen-test-runner binary on PATH whose version matches the
# wasm-bindgen pin in Cargo.lock, plus Node.
test-wasm:
	CARGO_TARGET_WASM32_UNKNOWN_UNKNOWN_RUNNER=wasm-bindgen-test-runner \
	$(CARGO) test -p cachekit-rs --target wasm32-unknown-unknown --no-default-features --features workers,cachekitio,encryption,macros --test wasm_session_tests

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
