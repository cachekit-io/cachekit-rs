Closes LAB-1151.

`[gate-change-approved]` — this PR modifies `security.yml` itself, so its own tamper check fires; this marker is the sanctioned path, and its presence here is the live positive test of the warning path. Sign-off: ray owns the merge decision on this PR, which is the sign-off.

## What

The required `supply-chain` check read both its policy (`deny.toml`) and its own definition (`security.yml`) from the PR head, so one commit could violate a ban and delete the ban — green rollup, merges. Deletion already fails closed (required context, `bypass_actors: []` on ruleset 17788230, proven by the frozen PRs after [#54](https://github.com/cachekit-io/cachekit-rs/pull/54)); this PR closes the modification half:

- **Gate tamper check** — final step of the `supply-chain` job. Diffs `deny.toml` + `security.yml` against `HEAD^1` (the base tip the merge commit was computed against — exact PR effect, no network fetch, no base-drift false positives; checkout gains `fetch-depth: 2`). Any change fails the required check unless the PR body contains the exact case-sensitive string documented in README §Gate tamper-evidence.
- **Shadow detection** — a PR could also ship a replacement check under the same `supply-chain` name in a different workflow file. Any *other* changed workflow file mentioning `supply-chain` trips the wire too. (The API-commit-status variant of the same shadow is closed in the ruleset: the `supply-chain` context is now pinned to `integration_id` 15368, the GitHub Actions app.)
- **Injection-safe marker matching** — the PR body enters the step via `env:` only and is matched by `grep -qF` as data; `set -o pipefail` makes a failing `git diff` fail the step rather than silently disarm it.

## What this deliberately does not defend against

Documented in README §Gate tamper-evidence: a PR that edits the tamper step itself out in the same commit, a self-served marker, and a marker hidden in an HTML comment. All deliberate evasion, not the lazy path this defends against. Mechanical closure of the first needs an org-ruleset `workflows` rule (verified available on this plan) — escalated on LAB-1151 for ray's decision, deliberately not self-served.

## Verification

- Step logic verified locally against synthetic merge commits (plumbing-built, same shape as `refs/pull/N/merge`): innocent PR passes, gate-file edit trips, shadow workflow trips, unrelated workflow deletion does not trip; marker matching is exact, case-sensitive, empty-body-safe.
- Expert panel (bug-hunter, security, craftsman, catchphrase) reviewed at high stakes; all surviving findings applied (pipefail fail-open, base-drift false positive, name-shadowing CRIT, marker-in-error-text copy-paste self-approval, case-insensitive `contains()`). Full adjudication on LAB-1151.
- Negative proofs #1 (policy edit) and #2 (job neutering) run as throwaway PRs against this branch's tripwire — run URLs on LAB-1151; closed unmerged, branches deleted.

## Docs

README §Gate tamper-evidence (new), `make security` wording, Makefile comment — all state what the gate does and does not defend against.
