#!/usr/bin/env bash
# Local check for the gate tamper step logic (LAB-1151). Builds real merge
# commits with plumbing — the same shape as refs/pull/N/merge — and runs the
# exact step logic against them. Deleted before push; asserts fail loudly.
set -euo pipefail

run_step() { # $1 = merge commit; prints "changed=[...] shadow=[...]"
  local M=$1 changed shadow="" f
  changed=$(git diff --name-only "$M^1" "$M" -- deny.toml .github/workflows/security.yml | tr '\n' ' ')
  while IFS= read -r f; do
    [ -n "$f" ] || continue
    if git grep -qe supply-chain "$M" -- "$f"; then shadow="$shadow$f "; fi
  done < <(git diff --name-only "$M^1" "$M" -- '.github/workflows/' ':!.github/workflows/security.yml')
  printf 'changed=[%s] shadow=[%s]\n' "$changed" "$shadow"
}

# Test B: this PR's own merge commit
tree=$(git merge-tree --write-tree origin/main HEAD)
M=$(git commit-tree "$tree" -p origin/main -p HEAD -m "test merge")
out=$(run_step "$M")
echo "B: $out"
[ "$out" = "changed=[.github/workflows/security.yml ] shadow=[]" ] || { echo "FAIL B"; exit 1; }

# Test A: innocent PR touching only LICENSE
echo "x" > /tmp/dummy; blob=$(git hash-object -w /tmp/dummy)
tree2=$(git ls-tree origin/main | sed "s|^100644 blob [0-9a-f]*\tLICENSE$|100644 blob $blob\tLICENSE|" | git mktree)
C2=$(git commit-tree "$tree2" -p origin/main -m "innocent")
M2=$(git commit-tree "$tree2" -p origin/main -p "$C2" -m "merge2")
out=$(run_step "$M2")
echo "A: $out"
[ "$out" = "changed=[] shadow=[]" ] || { echo "FAIL A"; exit 1; }

# Test C: shadow workflow adding a job named supply-chain in a new file
printf 'name: Fake\njobs:\n  supply-chain:\n    runs-on: ubuntu-latest\n' > /tmp/fake.yml
blob3=$(git hash-object -w /tmp/fake.yml)
wftree=$(git ls-tree origin/main:.github/workflows | { cat; printf '100644 blob %s\tfake.yml\n' "$blob3"; } | git mktree)
ghtree=$(git ls-tree origin/main:.github | sed "s|^040000 tree [0-9a-f]*\tworkflows$|040000 tree $wftree\tworkflows|" | git mktree)
tree3=$(git ls-tree origin/main | sed "s|^040000 tree [0-9a-f]*\t\.github$|040000 tree $ghtree\t.github|" | git mktree)
C3=$(git commit-tree "$tree3" -p origin/main -m "add fake wf")
M3=$(git commit-tree "$tree3" -p origin/main -p "$C3" -m "merge3")
out=$(run_step "$M3")
echo "C: $out"
[ "$out" = "changed=[] shadow=[.github/workflows/fake.yml ]" ] || { echo "FAIL C"; exit 1; }

# Test D: PR deleting an unrelated workflow (no supply-chain content) — must NOT trip
wftree4=$(git ls-tree origin/main:.github/workflows | grep -v $'\tci.yml$' | git mktree)
ghtree4=$(git ls-tree origin/main:.github | sed "s|^040000 tree [0-9a-f]*\tworkflows$|040000 tree $wftree4\tworkflows|" | git mktree)
tree4=$(git ls-tree origin/main | sed "s|^040000 tree [0-9a-f]*\t\.github$|040000 tree $ghtree4\t.github|" | git mktree)
C4=$(git commit-tree "$tree4" -p origin/main -m "del ci.yml")
M4=$(git commit-tree "$tree4" -p origin/main -p "$C4" -m "merge4")
out=$(run_step "$M4")
echo "D: $out"
[ "$out" = "changed=[] shadow=[]" ] || { echo "FAIL D"; exit 1; }

# Marker matching (exact, case-sensitive, empty-safe)
grep -qF -- '[gate-change-approved]' <<<"approved: [gate-change-approved] here" || { echo "FAIL marker-present"; exit 1; }
grep -qF -- '[gate-change-approved]' <<<"GATE-CHANGE-APPROVED" && { echo "FAIL case"; exit 1; }
grep -qF -- '[gate-change-approved]' <<<"" && { echo "FAIL empty"; exit 1; }
echo "ALL PASS"
