.PHONY: quick-check test test-wasm build build-wasm fmt clippy security deny audit bench bench-instr

CARGO := cargo

# Native feature set for clippy/test — must equal the `test` job's string in
# .github/workflows/ci.yml. Not --all-features: `workers` is mutually exclusive
# with redis/l1/reliability/memcached/file (compile_error! guards in
# crates/cachekit/src/lib.rs), so --all-features can never compile. `deny`
# keeps --all-features on purpose: cargo-deny resolves the graph without
# compiling (see README).
NATIVE_FEATURES := cachekitio,redis,encryption,l1,macros,memcached,file,tracing

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

# wasm32 runtime tests — the CI `wasm` job runs this target.
# Needs a wasm-bindgen-test-runner binary on PATH whose version matches the
# wasm-bindgen pin in Cargo.lock, plus Node.
# The runner's exit code is not enough: when a test's future never settles,
# Node exits mid-suite with no summary and the run exits 0 ("no tests to run!"
# exits 0 too). So pass only when the summary reports every test the runner
# found ("running N tests") as passed: "test result: ok. N passed; 0 failed".
test-wasm: SHELL := /bin/bash
test-wasm: .SHELLFLAGS := -eu -o pipefail -c
test-wasm:
	log=$$(mktemp); trap 'rm -f "$$log"' EXIT; \
	CARGO_TARGET_WASM32_UNKNOWN_UNKNOWN_RUNNER=wasm-bindgen-test-runner \
	$(CARGO) test -p cachekit-rs --target wasm32-unknown-unknown --no-default-features --features workers,cachekitio,encryption,macros --test wasm_session_tests 2>&1 | tee "$$log"; \
	n=$$(sed -n 's/^running \([0-9][0-9]*\) tests\{0,1\}$$/\1/p' "$$log"); \
	if ! [[ "$$n" =~ ^[1-9][0-9]*$$ ]] || ! grep -q "^test result: ok\. $$n passed; 0 failed" "$$log"; then \
		echo "test-wasm: expected 'test result: ok. $${n:-N} passed; 0 failed' (tests found: $${n:-none}); a test failed or never completed" >&2; \
		exit 1; \
	fi

# Hot-path CPU cost (examples/bench_hot_path.rs; README "Measuring performance").
# `bench` prints wall time per op, which is indicative only. `bench-instr`
# counts instructions per op under valgrind's callgrind: the number a change is
# judged on. BASE=<a copy of the example built at the base commit> compares two
# builds and exits 1 when a case regresses past its noise floor (2 when it
# measured nothing: bad flag, empty filter, valgrind failure); FILTER=<text>
# narrows the cases (e.g. FILTER=l2_hit).
bench:
	$(CARGO) run --release --features macros --example bench_hot_path -- wall $(if $(FILTER),'$(FILTER)')

bench-instr:
	$(CARGO) build --release --features macros --example bench_hot_path
	target/release/examples/bench_hot_path instr $(if $(BASE),--base '$(BASE)') $(if $(FILTER),'$(FILTER)')

# Supply-chain gate — deny is identical to CI's; audit runs the strict form
# (`--deny yanked`, which CI applies only on the weekly schedule run), so a
# local pass covers every CI event in .github/workflows/security.yml. (CI runs
# the audit step even when deny fails, plus a PR-only gate tamper check with no
# local equivalent; make stops at the first failure.)
# Kept out of `quick-check`: both tools fetch the RustSec advisory database over
# the network, which does not belong in a per-commit loop.
# Why both tools, and why --all-features: see the table in README.md.
security: deny audit

deny:
	$(CARGO) deny --locked --all-features check

audit:
	$(CARGO) audit --deny yanked
