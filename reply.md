Solved — [cachekit-rs#35](https://github.com/cachekit-io/cachekit-rs/pull/35) is mergeable and fully green again.

Two things were outstanding:

1. **Merge conflict with main** — #33 (interop mode) merged and hit the exact `lib.rs` conflict predicted in the PR description. Resolved in merge commit `896b257` per the documented resolution: kept `pub mod interop;` from #33 and the private `mod key;` from this branch. The `compile_fail` guard confirmed `cachekit::key` stayed private through the merge; full suite green on the merged tree (146 tests, now including #33's interop vectors), clippy `-D warnings` and fmt clean.
2. **Red CodeRabbit check** — stale rate-limit failure, not a real review finding (its content reviews produced zero actionable comments). Cleared to SUCCESS on the new push.

Current state: all checks green (stable / beta / MSRV 1.85 / wasm32 / conventional-title / CodeRabbit), `MERGEABLE`, blocked only on branch-protection review. The critical-stakes panel already returned unanimous SHIP earlier on this thread's PR, so it's ready for human signoff + merge — merging will auto-move this issue to Done.
