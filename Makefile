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

# Supply-chain gate. Byte-for-byte the invocations CI runs in
# .github/workflows/security.yml — if it passes here it passes there.
# Kept out of `quick-check` on purpose: both tools fetch the RustSec advisory
# database over the network, which does not belong in a per-commit loop.
security: deny audit

# --all-features is load-bearing. The default feature set excludes the memcached,
# redis, file and macros backends, so a banned crate reintroduced behind an
# optional feature — precisely the LAB-429 openssl-sys/native-tls regression —
# is invisible to a bare `cargo deny check`.
deny:
	$(CARGO) deny --all-features check

# Complements `deny` rather than duplicating it: cargo-audit reads Cargo.lock
# verbatim, so it also sees advisories against crates no feature activates and
# against transitive deps that deny's default "workspace" scope skips.
audit:
	$(CARGO) audit
