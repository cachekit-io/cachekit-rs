//! README dependency pins: every line in the two published READMEs (this
//! crate's, and cachekit-macros') that names `cachekit-rs` with a version must
//! sit inside an `x-release-please-start-version` block, and each line in such
//! a block must carry exactly one version, this crate's.
//!
//! crates.io shows these READMEs on each release's page, and a reader copies
//! the pin along with the examples, so a stale pin installs an older release
//! than the one the README documents. The pins drifted unnoticed for two
//! releases because nothing bumped them. Now release-please rewrites the
//! marked blocks on each release (`extra-files` in `release-please-config.json`).
//! This file fails the PR that adds a pin outside a marked block, and the
//! release PR if release-please stopped rewriting them.
//!
//! It fails closed without knowing any TOML spelling. In a `toml` or shell
//! fence (judged by the first word of the info string, so ` ```toml title=... `
//! counts), where install snippets live, every line that names `cachekit-rs`
//! must carry its version on the same line, which forces the inline form (`cachekit-rs = ...`, a renamed
//! `{ package = "cachekit-rs", version = ... }`, `cargo add cachekit-rs@...`)
//! and rejects a table whose version sits on another line. In a block,
//! release-please rewrites only the first `X.Y.Z` on each line, so a second
//! version on a line fails, and so does any version that is not this crate's.

const READMES: [(&str, &str); 2] = [
    ("README.md", include_str!("../../../README.md")),
    (
        "crates/cachekit-macros/README.md",
        include_str!("../../cachekit-macros/README.md"),
    ),
];

/// Each dotted number on `line`, such as `0.10.0` in `version = "0.10.0"` or
/// `0.8` in `cachekit-rs@0.8`, with the semver suffix release-please writes
/// after `-` or `+`, so `0.11.0-rc.1` reads whole.
fn versions(line: &str) -> Vec<&str> {
    line.split(|c: char| !(c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '+')))
        .filter_map(|token| {
            // A prefix such as `v` in `v0.10.0` is not part of the version.
            let token = token.trim_start_matches(|c: char| !c.is_ascii_digit());
            let digits = token
                .find(|c: char| !(c.is_ascii_digit() || c == '.'))
                .unwrap_or(token.len());
            let core = token[..digits].trim_end_matches('.');
            // `1.x` is no version, and `.html` in `1.85.0.html` is no suffix.
            let version = if token[core.len()..].starts_with(['-', '+']) {
                token.trim_end_matches(['.', '-', '+'])
            } else {
                core
            };
            core.contains('.').then_some(version)
        })
        .collect()
}

fn check(path: &str, text: &str, version: &str) {
    let mut in_block = false;
    // First word of the open code fence's info string, such as `toml` or `rust`.
    let mut fence: Option<&str> = None;
    let mut pins = 0;
    for (n, line) in text.lines().enumerate() {
        let at = format!("{path}:{}", n + 1);
        if line.contains("x-release-please-start-version") {
            in_block = true;
            continue;
        }
        if line.contains("x-release-please-end") {
            in_block = false;
            continue;
        }
        if let Some(info) = line.trim_start().strip_prefix("```") {
            fence = match fence {
                Some(_) => None,
                None => info
                    .trim_start()
                    .split(|c: char| c.is_whitespace() || matches!(c, ',' | '{'))
                    .next(),
            };
            continue;
        }
        let install_fence = matches!(fence, Some("toml" | "sh" | "bash" | "shell" | "console"));
        let pin = line.contains("cachekit-rs");
        let found = versions(line);
        assert!(
            !(install_fence && pin && found.is_empty()),
            "{at}: names cachekit-rs with no version on the line; write the pin inline (`cachekit-rs = {{ version = ... }}`) so this test can check it"
        );
        if found.is_empty() {
            continue;
        }
        if in_block {
            assert_eq!(
                found,
                [version],
                "{at}: a block line must carry exactly one version, this crate's; release-please rewrites only the first X.Y.Z on each line"
            );
        } else {
            assert!(
                !pin,
                "{at}: cachekit-rs pin {found:?} is outside an x-release-please-start-version block, so no release will bump it"
            );
        }
        if pin {
            pins += 1;
        }
    }
    assert!(
        !in_block,
        "{path}: unclosed x-release-please-start-version block; release-please would rewrite versions to the end of the file"
    );
    assert!(
        pins > 0,
        "{path}: no cachekit-rs pin found; did the snippet format change?"
    );
}

#[test]
fn readme_pins_name_this_version_inside_release_please_blocks() {
    for (path, text) in READMES {
        check(path, text, env!("CARGO_PKG_VERSION"));
    }
}

/// A renamed dependency table split across lines, then the fence's close.
const SPLIT_TABLE: &str =
    "[dependencies.cache]\npackage = \"cachekit-rs\"\nversion = \"0.8\"\n```\n";

#[test]
#[should_panic(expected = "names cachekit-rs with no version on the line")]
fn split_table_in_titled_toml_fence_fails() {
    check(
        "t",
        &format!("```toml title=\"Cargo.toml\"\n{SPLIT_TABLE}"),
        "0.10.0",
    );
}

#[test]
#[should_panic(expected = "names cachekit-rs with no version on the line")]
fn split_table_in_toml_fence_with_attributes_fails() {
    check("t", &format!("```toml,ignore\n{SPLIT_TABLE}"), "0.10.0");
}

const PRERELEASE_BLOCK: &str = "<!-- x-release-please-start-version -->\n```toml\ncachekit-rs = \"0.11.0-rc.1\"\n```\n<!-- x-release-please-end -->\n";

#[test]
fn prerelease_pin_equal_to_the_crate_version_passes() {
    check("t", PRERELEASE_BLOCK, "0.11.0-rc.1");
}

#[test]
#[should_panic(expected = "a block line must carry exactly one version")]
fn stale_prerelease_pin_fails() {
    check("t", PRERELEASE_BLOCK, "0.11.0-rc.2");
}

#[test]
fn versions_keep_semver_suffixes_and_skip_non_versions() {
    let none: [&str; 0] = [];
    assert_eq!(
        versions("cachekit-rs = \"0.11.0-rc.1+build.5\""),
        ["0.11.0-rc.1+build.5"]
    );
    assert_eq!(versions("cargo add cachekit-rs@0.8"), ["0.8"]);
    assert_eq!(
        versions("tag v0.10.0, cachekit-rs-v0.9.0."),
        ["0.10.0", "0.9.0"]
    );
    assert_eq!(versions("redis://127.0.0.1:6379"), ["127.0.0.1"]);
    assert_eq!(versions("redis://localhost:6379"), none);
    assert_eq!(versions("rustc-1.85.0.html"), ["1.85.0"]);
    assert_eq!(versions("tokio 1.x, AES-256-GCM, sha256.hex"), none);
}
