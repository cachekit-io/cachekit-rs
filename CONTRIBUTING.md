# Contributing to cachekit-rs

How to build, test and benchmark cachekit-rs, and how its CI gates and release pins work. What the SDK does, and how to use it, is in the [README](README.md) and on [docs.cachekit.io](https://docs.cachekit.io/rust/quickstart/).

## Pre-commit hooks

`prek install` (or `pre-commit install`) sets up hooks that reject internal references in README
files, `docs/` and commit messages, plus a pre-push hook that runs `make test` when a push
touches `.rs` files.

## Development

```bash
make quick-check   # fmt + clippy + test (run before every commit)
make security      # cargo deny + cargo audit (the CI supply-chain gate)
make test          # cargo test --features $(NATIVE_FEATURES) (CI's list; see Makefile)
make build         # cargo build --release
make build-wasm    # wasm32-unknown-unknown (workers feature)
make test-wasm     # wasm32 runtime tests under Node: session clock, Workers fetch, encryption (see Makefile)
```

`make security` runs the same two enforcement commands as the `supply-chain`
job in `.github/workflows/security.yml`, with `cargo audit` in its strictest CI
form (`--deny yanked`) — so a local pass means a pass on every CI event, with
two asymmetries: the weekly run additionally proves the yank check actually
executed (see the guard in `security.yml`), so with crates.io unreachable a
local run warns and passes where the weekly run goes red; and the job's final
step, the gate tamper check below, is PR-context-only and has no local
equivalent. It needs
`cargo-deny` and `cargo-audit` installed, and it reaches the network to refresh
the RustSec advisory database — which is why it is not folded into
`quick-check`.

Both tools are required, because they answer different questions. "Fails" below
means it turns the check red — anything else is reported but not enforced:

| | `cargo deny --locked --all-features check` | `cargo audit` |
| :--- | :--- | :--- |
| Reads | feature-resolved dependency graph | `Cargo.lock` verbatim |
| Licence allowlist, banned crates, registry/source policy | **fails** | not checked |
| Vulnerabilities in crates no enabled feature activates | not seen (pruned) | **fails** |
| Yanked crates in `Cargo.lock` | warns (feature-resolved graph only, so lockfile-only crates are missed) | warns on PR and push runs; **fails** only the weekly scheduled run (`--deny yanked`) |
| Unsound / unmaintained advisories on *transitive* deps | not seen — `deny.toml` narrows `unmaintained` to `workspace`; `unsound` already defaults to that scope | reports only, does **not** fail — deliberate (see `deny.toml`) |

`--all-features` is load-bearing: the default feature set excludes the
`memcached`, `redis`, `file` and `macros` backends, so a banned crate
reintroduced behind an optional feature passes a bare `cargo deny check`.

`deny.toml` is the policy — notably a hard ban on `openssl-sys`, `native-tls`
and `toxiproxy_rust`, because this SDK is rustls-only. Run `make deny` before
adding or bumping a dependency.

### Performance practices

These practices come from measured changes in this crate and its sibling SDKs.
Each says what to do, why, and the PR that measured it. Follow them in any
change to a hot path: an L1 hit, an L2 hit, a miss, a `set`, their encrypted
variants, and a short-lived process's first read. A perf PR shows its counts;
[Measuring performance](#measuring-performance) says how to take them.

1. **Do per-call work once.** Anything that depends only on configuration, the
   tenant, key material or the cached function is computed when the client or
   keyring is built. [#185](https://github.com/cachekit-io/cachekit-rs/pull/185)
   binds the keyring to its tenant once, so reads run no HKDF: the encrypted
   64 B L1 hit went from 34,178 to 5,124 instructions (−85%). Eager work has a
   price: with three previous keys, a one-op process whose read the current key
   serves costs +24.0%, so measure the `oneshot` case and state the trade. A
   memo on an encrypting client never keeps plaintext alive; every memo is
   bounded by entries and bytes.
2. **Allocate once and don't copy.** Size buffers exactly, write in place,
   borrow instead of owning. [#178](https://github.com/cachekit-io/cachekit-rs/pull/178)
   borrows the key and checks it 16 bytes at a time: a 64 B L1 hit went from
   2,927 to 2,564 instructions (−12.4%). The keys it accepts and rejects, and
   its error text, did not change: only a rejection scans byte by byte, to name
   the offending byte. [cachekit-core#127](https://github.com/cachekit-io/cachekit-core/pull/127)
   seals in one buffer: encrypt at 64 KiB went from 33.80 µs to 10.93 µs.
3. **Don't let allocator luck hide a copy.** When a change slows a path it did
   not touch, find the work the old timing hid. In
   [#182](https://github.com/cachekit-io/cachekit-rs/pull/182), moka's late
   drop of a replaced L1 value had let glibc grow the encrypt buffer in place;
   quick_cache frees it at once, and `_int_realloc` rose from about 14.8 k to
   66.5 k instructions per op. The fix went to the root cause, cachekit-core#127,
   not to recreating moka's drop timing. Removing an allocation can cost too:
   glibc's adaptive mmap threshold made borrowing a 1 MB payload about 7×
   slower on retrieve, so [cachekit-core#132](https://github.com/cachekit-io/cachekit-core/pull/132)
   borrows only below 64 KiB. Count page faults at 64 KiB and up: instruction
   counts miss kernel work, and cachekit-core#127's encrypt read only −3.2% at
   1 MiB.
4. **Before removing a check, find what else it guarded.**
   [#202](https://github.com/cachekit-io/cachekit-rs/pull/202) dropped
   memcached's per-operation `version` ping: `get` p50 at 0.5 ms RTT went from
   1.493 ms to 0.782 ms, and 32-way throughput nearly doubled. The ping was the
   only desync guard (rust-memcache's `has_broken` always returns `false`), and
   setting only `test_on_check_out(false)` returned another key's value in the
   desync tests. Now a connection goes back to the idle list only after a fully
   parsed `Ok`; any `Err` drops it.
5. **No per-operation cost that grows with the number of entries.** #182 moved
   L1 off moka, which recorded every read for its eviction policy, onto
   quick_cache: at 64 B, the L1 hit −33.4% and `set` −55.1%.
   [cachekit-py#658](https://github.com/cachekit-io/cachekit-py/pull/658)
   replaced an expiry scan with a deadline heap, −97.2% instructions per put at
   10,000 entries; a min-expiry watermark still scanned on 18–51% of puts in
   simulation, so fix the structure, not the heuristic. A new engine changes behaviour, so
   measure and state it: #182 took a 1,100-key loop at capacity 1,000 from a
   90.9% to an 82.3% hit ratio, and its first expiry guard, creation instant
   plus buffer pointer, could drop a fresh empty value because two writes can
   share both.
6. **Count round trips per operation.** Pipeline dependent commands, skip
   checks an uncontended path cannot need, and keep follow-up work off the
   caller's path. [cachekit-ts#209](https://github.com/cachekit-io/cachekit-ts/pull/209)
   skipped the uncontended double-check and released the fill lock off the
   reply path, so a cold miss went from 5 requests to 4.
7. **Small values get their own path.** Fixed per-call costs dominate the small
   values most caches hold, so judge a change at 40 B and 64 B too.
   [cachekit-core#128](https://github.com/cachekit-io/cachekit-core/pull/128)
   gave LZ4 a literals-only block below 64 B: −80.8% on a 63 B store.
8. **No shared mutable state on the operation path.** Use per-thread cells,
   atomics and lock-free reads: per-thread metric cells gave +52.3% calls/s at
   16 threads on free-threaded Python
   ([cachekit-py#661](https://github.com/cachekit-io/cachekit-py/pull/661)).
9. **Measure it, or it didn't happen.** Judge CPU work on `make bench-instr`
   against a build of the base on the same machine, and run an A/A: code layout
   alone moved unchanged code by −4% to +5% across rebuilds
   ([cachekit-core#144](https://github.com/cachekit-io/cachekit-core/pull/144)),
   and `delete` drifted 4.38% in an A/A
   ([#193](https://github.com/cachekit-io/cachekit-rs/pull/193)), so judge it
   against its own. Bench realistic inputs: #178's 21 B bench key, against an
   interop key of about 120 B, understated its win. Size the bench L1 at twice
   its keys, or sharded eviction during setup leaks into the timed loop (#182).
   Valgrind runs `sha2`'s portable code, about 29,000 instructions per HKDF,
   which overstates the native share (#185). Pair and interleave wall-time
   runs. Use `black_box`: [cachekit-core#117](https://github.com/cachekit-io/cachekit-core/pull/117)
   found a benchmark reporting 230–290 GB/s, faster than memory. Publish no
   figure you did not measure.

**Sibling paths.** A fix applied to one path tends to leave its siblings slow.
When a perf PR changes a hot path, its description lists the sibling paths and
says which it changed: plaintext and encrypted (current and previous key), L1
hit and L2 hit, the client API and `#[cachekit]`, with and without the
reliability stack, native and wasm32, each backend, and the same path in the
other SDKs.

### Measuring performance

Two examples measure the client. Neither runs in the test suite, and neither
adds a dependency.

**CPU cost of the hot path** — `examples/bench_hot_path.rs` drives the public
`CacheKit` API over an in-memory backend, so it measures the client's own work
(key handling, MessagePack, L1, the reliability stack, AES-256-GCM) and no
network. Cases are `l1_hit`, `l2_hit`, `set`, `delete` and `exists` (a warm
key), each `plain`, with reliability (`rel`) or with encryption (`enc`), at
40 B, 64 B, 1 KiB and 64 KiB of `String` value (40 B, stored as 42 B of
MessagePack, is the smallest case, where per-call fixed costs dominate). Two more price key rotation and short-lived processes:
`prev` reads, L1 and L2 hits, on a client holding one previous key, of entries
written under that key; `oneshot` builds an encrypting client (`enc` or `prev`)
and reads one L2 hit, the cost a one-read process pays beyond its start-up.
`macro_hit` reads an L1 hit through a `#[cachekit]` function (`plain`, `rel`,
or `enc` through a `secure` function) whose interop key is 120 B, so it prices
the macro's key derivation; it needs the `macros` feature, which the `make`
targets turn on.
`l1_hit_row` reads an L1 hit whose value is a 20-field struct, stored as a
MessagePack map of 20 entries, so it prices decoding a many-field value: a
bounds walk, where one runs, covers 41 headers where a `String` value has one.
It runs `plain` and `enc`, where the value decoded is the decrypted
plaintext.
`ns` and `nsenc` run `l1_hit`, `l2_hit` and `set` as `plain` and `enc` on a
client with a `.namespace()`, which builds `{namespace}:{key}` on every op.
`delete/parked256` and `delete/parked1024` run the plain 64 B `delete` while
256 or 1,024 other deletes wait on the backend, each holding its key's
mutation lock as a delete on a slow backend does: the cost of many keys in
flight at once.

```bash
make bench                                  # wall ns/op: indicative only
make bench-instr                            # instructions/op + run-to-run spread (needs valgrind)
make bench-instr BASE=../bench-base FILTER=l2_hit   # A/B against another build
```

`bench_hot_path readme` prints the table in [Performance](README.md#performance).

Wall time on a shared machine moves by tens of percent between runs, so
`bench-instr` is the number a change is judged on. It counts only the timed
loop (`--toggle-collect`), on a single-threaded tokio runtime, five runs per
case, and reports the median and the spread. `JOBS=<n>` measures n cases at a
time; counts do not depend on it. For an A/B, check out the base commit, copy
your working tree's `crates/cachekit/examples/bench_hot_path.rs` into it so
both builds run one workload, build the example there with `--features macros`, copy its
`target/release/examples/bench_hot_path` outside `target/`, and pass it as
`BASE`. The example copies both builds to paths of equal length
before it measures them, because the length of a binary's path moves its
counts (by 6 instructions per `exists` op). The runs interleave ABBA, and a
delta counts only when it beats the case's floor, which is fixed per op and
never taken from a run's spread, so a change that makes a case noisy cannot
widen its own floor. Every op's floor is 0.5%, new ops included, and its
cases also fail when the head's runs spread by more than 0.5%, because new
nondeterminism on a hot path is itself a finding. Two ops vary between runs
of one build, from the single-flight and per-key mutation lock maps, hash
tables seeded afresh in each process: `l2_hit` by up to about 2% in CI, and
`delete` by up to about 10%. `l2_hit` gets a fixed 2% floor and no spread
check (in CI, an unchanged build moved its median by at most 0.52%). `delete`
is reported but never fails the comparison: a floor that wide would hide any
regression worth catching. The example exits 1 when a gated case got slower
or noisier, and 2 when nothing was measured or the run stopped part way (an
unknown flag, a filter that matches no case, a valgrind failure); `make`
reports either one as exit 2.

CI runs this comparison on every pull request (the **Instruction counts vs
base** check): it builds the pull request and the commit it merges into, each
in its own target directory and with `--features macros` (the `#[cachekit]`
cases need it), measures both on the same runner, and posts the verdict as
one comment it edits in place (a pull request from a fork gets no comment; its
verdict is in the run summary). A case it did not measure reads
INCOMPLETE, never PASS. A release pull request is measured against the last
release tag instead. The check is not required: a deliberate cost increase is
accepted at review.

**Client wall time** — `examples/wall_time_probe.rs` times `CacheKit` calls
against the CacheKit Cloud dev endpoint and writes one JSON object per request. It runs
two arms in one process, interleaved in ABBA blocks, with a new client
(warmed by one GET miss) for every block: `sdk` is the real
`CachekitIO` backend, and `transport` is a copy of it over the same `reqwest`
configuration that also reads `cf-ray`, status and time to first byte. Run
`sdk,sdk` first: the difference between two identical arms is the noise floor
any later comparison has to clear. Then run `sdk,transport`, which must agree
within that floor before the transport arm's extra fields are trusted. Both
arms speak HTTP/1.1 (`reqwest` is built without HTTP/2).

It writes and deletes keys, so it runs only against a non-production
endpoint (an allowlist in the example), appends every key
to `--ledger` before the PUT that writes it, caps every TTL at 900 s, and never
retries. A 429, a 503, any other 4xx but 404, or a transport error stops the run;
other 5xx responses are recorded and the run goes on, up to five, so an
endpoint's sporadic errors become a counted rate rather than ending the run.

```bash
cargo build --release --example wall_time_probe --features macros
CACHEKIT_API_KEY=… CACHEKIT_API_URL=<allowlisted endpoint> \
  target/release/examples/wall_time_probe --run r1 --phase warm \
  --out rows.jsonl --ledger keys.txt --arms sdk,sdk --samples 40 --block 10
```

`--fresh-conn` builds a new client per sample, `--gap-ms` idles between
samples, `--concurrency N` sends bursts (after one failed request, the burst
sends nothing more), and `--macro-cold-miss` times a
`#[cachekit]` cold miss (GET, lock, origin, PUT, unlock; each request it sends
is checked, and a call sending more than five stops the run; the unlock, sent
after the call returns, is waited for off the clock and checked with its call). `--hold-lock`
tests those rules against a live refusal: a second client holds the first cold
miss's fill lock, so the run must stop (exit 1 if it does not). Exit 3 is any
stop: only a `STOPPED` line naming `LOCK in macro-cold-miss` and `lock not
granted` is the held lock. `--help` lists every flag. Delete the ledger's keys when the run ends; the TTL
is the backstop.

### Gate tamper-evidence

The `supply-chain` check reads both its policy (`deny.toml`) and its own
definition (`security.yml`) from the PR head, so a PR could weaken the gate it
is being graded by — delete a `[bans]` entry, or drop `--all-features` while
keeping the job name green. Two properties make that visible:

- **Deletion fails closed.** `supply-chain` is a required status check: if
  nothing reports it, the PR cannot merge. The check is pinned to the GitHub
  Actions app, so a status posted from outside Actions cannot satisfy it.
- **Modification trips a wire.** The job's final step fails the required check
  when a PR changes `deny.toml` or `security.yml` relative to its base, or
  changes any other workflow file that mentions `supply-chain`, unless the PR
  body contains the exact, case-sensitive string `[gate-change-approved]` (add
  it after human sign-off, *then* push a commit — the marker is read from the
  push-time event, so a body edit alone does not re-trigger).

### Why cachekit-macros is pinned exactly

`cachekit-rs` requires `cachekit-macros` with `=`, never a caret, so each
runtime release that carries the pin only ever meets the macros released with
it; releases published before the pin require a caret. `#[cachekit]`
expands to calls into `#[doc(hidden)]` items of `cachekit-rs`, and a patch
release of the macros can call one that an older runtime lacks. Under a caret,
a project that holds `cachekit-rs` at an exact older patch would still resolve
the newest macros in that minor line, and stop compiling at every
`#[cachekit]` site.

The requirement lives in the root `Cargo.toml` under `[workspace.dependencies]`,
and `crates/cachekit/Cargo.toml` inherits it. release-please rewrites a
dependency version in a member manifest to the bare new version, dropping the
`=`, so the root line carries a release-please version marker instead, and a
generic `extra-files` entry in `release-please-config.json` bumps only the
number. `crates/cachekit/tests/macros_pin_tests.rs` fails if the requirement
is not `=` this crate's version.
