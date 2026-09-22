.PHONY: quick-check test test-wasm build build-wasm fmt clippy security deny audit

CARGO := cargo

# Native feature set for clippy/test — must equal the `test` job's string in
# .github/workflows/ci.yml. Not --all-features: `workers` is mutually exclusive
# with redis/l1/reliability/memcached/file (compile_error! guards in
# crates/cachekit/src/lib.rs), so --all-features can never compile. `deny`
# keeps --all-features on purpose: cargo-deny resolves the graph without
# compiling (see README).
NATIVE_FEATURES := cachekitio,redis,encryption,l1,macros,memcached,file

quick-check: fmt clippy test

fmt:
	$(CARGO) fmt --all

clippy:
	$(CARGO) clippy --all-targets --features $(NATIVE_FEATURES) -- -D warnings

test:
	$(CARGO) test --features $(NATIVE_FEATURES)

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

# Supply-chain gate — deny is identical to CI's; audit runs the strict form
# (`--deny yanked`, which CI applies only on the weekly schedule run), so a
# local pass covers every CI event in .github/workflows/security.yml. (CI runs
# the audit step even when deny fails; make stops at the first failure.)
# Kept out of `quick-check`: both tools fetch the RustSec advisory database over
# the network, which does not belong in a per-commit loop.
# Why both tools, and why --all-features: see the table in README.md.
security: deny audit

deny:
	$(CARGO) deny --locked --all-features check

audit:
	$(CARGO) audit --deny yanked
