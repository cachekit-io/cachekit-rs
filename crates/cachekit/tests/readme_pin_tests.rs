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
//! fence, where install snippets live, every line that names `cachekit-rs`
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

/// Each run of digits and dots on `line` that holds a dotted number, such as
/// `0.10.0` in `version = "0.10.0"` or `0.8` in `cachekit-rs@0.8`.
fn versions(line: &str) -> Vec<&str> {
    line.split(|c: char| !(c.is_ascii_digit() || c == '.'))
        .map(|token| token.trim_matches('.'))
        .filter(|token| token.contains('.'))
        .collect()
}

#[test]
fn readme_pins_name_this_version_inside_release_please_blocks() {
    let version = env!("CARGO_PKG_VERSION");
    for (path, text) in READMES {
        let mut in_block = false;
        // Info string of the open code fence, such as `toml` or `rust`.
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
                    None => Some(info.trim()),
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
}
