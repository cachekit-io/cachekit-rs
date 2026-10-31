//! The `cachekit-macros` requirement: exactly this crate's version, with `=`.
//!
//! `#[cachekit]` expands to calls into this crate's doc-hidden items, such as
//! `__count`. Under a caret requirement, a user who holds this crate at
//! `=0.14.0` still resolves the newest 0.14.x macros crate, and if that
//! release calls an item 0.14.0 lacks, every `#[cachekit]` site stops
//! compiling.
//!
//! release-please rewrites a dependency version in a member manifest to the
//! bare new version, which drops the `=`. So this crate's manifest inherits
//! the requirement from the root `[workspace.dependencies]`, where a generic
//! `extra-files` entry in `release-please-config.json` bumps only the number.
//! This file fails a PR or release PR that moves the requirement back into
//! this crate's manifest, drops the `=`, or drops the marker. A number that
//! release-please stopped bumping fails earlier, in cargo's own resolution,
//! because the path crate's version no longer matches it.

const ROOT: &str = include_str!("../../../Cargo.toml");
const MEMBER: &str = include_str!("../Cargo.toml");

/// The `cachekit-macros = ...` line in `[section]` of `manifest`.
fn macros_line<'a>(manifest: &'a str, section: &str) -> &'a str {
    let header = format!("[{section}]");
    manifest
        .lines()
        .map(str::trim)
        .skip_while(|line| *line != header)
        .skip(1)
        .take_while(|line| !line.starts_with('['))
        .find(|line| line.starts_with("cachekit-macros "))
        .unwrap_or_else(|| {
            panic!(
                "no `cachekit-macros = ...` line in [{section}]; did the manifest format change?"
            )
        })
}

fn check(root: &str, member: &str, version: &str) {
    let line = macros_line(member, "dependencies");
    // The TOML before any trailing comment, so a comment cannot satisfy the check.
    let toml = line.split('#').next().unwrap_or_default();
    assert!(
        toml.contains("workspace = true") && !toml.contains("version"),
        "crates/cachekit/Cargo.toml: `{line}` must inherit the root requirement (`workspace = true`, no `version`); release-please drops the `=` from a requirement declared here"
    );
    let line = macros_line(root, "workspace.dependencies");
    assert!(
        line.contains("x-release-please-version"),
        "Cargo.toml: `{line}` lost its x-release-please-version marker, so no release will bump it"
    );
    let requirement = line
        .split("version = \"")
        .nth(1)
        .and_then(|rest| rest.split('"').next())
        .unwrap_or_else(|| panic!("Cargo.toml: `{line}` has no inline `version = \"...\"`"));
    assert_eq!(
        requirement,
        format!("={version}"),
        "Cargo.toml: cachekit-macros must be required as exactly this crate's version, with `=`"
    );
}

#[test]
fn macros_requirement_is_exactly_this_version() {
    check(ROOT, MEMBER, env!("CARGO_PKG_VERSION"));
}

const INHERITING_MEMBER: &str =
    "[dependencies]\ncachekit-macros = { workspace = true, optional = true }\n";

fn root(requirement: &str) -> String {
    format!(
        "[workspace.dependencies]\ncachekit-macros = {{ version = \"{requirement}\", path = \"crates/cachekit-macros\" }} # x-release-please-version\n"
    )
}

#[test]
fn exact_requirement_passes() {
    check(&root("=0.14.0"), INHERITING_MEMBER, "0.14.0");
}

#[test]
#[should_panic(expected = "exactly this crate's version")]
fn caret_requirement_fails() {
    check(&root("0.14.0"), INHERITING_MEMBER, "0.14.0");
}

#[test]
#[should_panic(expected = "exactly this crate's version")]
fn stale_exact_requirement_fails() {
    check(&root("=0.13.1"), INHERITING_MEMBER, "0.14.0");
}

#[test]
#[should_panic(expected = "must inherit the root requirement")]
fn member_declaring_its_own_requirement_fails() {
    let member = "[dependencies]\ncachekit-macros = { version = \"=0.14.0\", path = \"../cachekit-macros\", optional = true }\n";
    check(&root("=0.14.0"), member, "0.14.0");
}

#[test]
#[should_panic(expected = "must inherit the root requirement")]
fn comment_cannot_satisfy_the_member_check() {
    let member = "[dependencies]\ncachekit-macros = { path = \"../cachekit-macros\", optional = true } # workspace = true\n";
    check(&root("=0.14.0"), member, "0.14.0");
}
