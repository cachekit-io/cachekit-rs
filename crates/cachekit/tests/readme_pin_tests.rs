//! README dependency pins: every line in the two published READMEs (this
//! crate's, and cachekit-macros') that names `cachekit-rs` with a version must
//! sit inside an `x-release-please-start-version` block, and every version in
//! such a block must be exactly this crate's version.
//!
//! crates.io shows these READMEs on each release's page, and a reader copies
//! the pin along with the examples, so a stale pin installs an older release
//! than the one the README documents. The pins drifted unnoticed for two
//! releases because nothing bumped them. Now release-please rewrites the
//! marked blocks on each release (`extra-files` in `release-please-config.json`).
//! This file fails the PR that adds a pin outside a marked block, and the
//! release PR if release-please stopped rewriting them.
//!
//! It fails closed: any spelling counts as a pin (`cachekit-rs = ...`, a
//! renamed `package = "cachekit-rs"` dependency, a `[dependencies.cachekit-rs]`
//! table, `cargo add cachekit-rs@...`), and so does any other version inside a
//! block, because release-please rewrites every `X.Y.Z` there.

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

/// A TOML table for the crate, such as `[dependencies.cachekit-rs]` or
/// `[dependencies."cachekit-rs"]`. A markdown link line ends in `)`, not `]`.
fn is_table_header(trimmed: &str) -> bool {
    trimmed.starts_with('[')
        && trimmed.ends_with(']')
        && trimmed
            .trim_end_matches([']', '"'])
            .ends_with("cachekit-rs")
}

#[test]
fn readme_pins_name_this_version_inside_release_please_blocks() {
    let version = env!("CARGO_PKG_VERSION");
    for (path, text) in READMES {
        let mut in_block = false;
        // The header line of an open `[...cachekit-rs]` table that has not yet
        // shown its version line.
        let mut table: Option<usize> = None;
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
            let trimmed = line.trim_start();
            if trimmed.starts_with('[') || trimmed.starts_with("```") {
                if let Some(header) = table {
                    panic!(
                        "{path}:{}: cachekit-rs table has no version line",
                        header + 1
                    );
                }
                if is_table_header(trimmed) {
                    table = Some(n);
                    continue;
                }
            }
            let found = versions(line);
            if found.is_empty() {
                continue;
            }
            let pin = line.contains("cachekit-rs") || table.take().is_some();
            if in_block {
                for v in &found {
                    assert_eq!(
                        *v, version,
                        "{at}: {v:?} is not this crate's version {version}; release-please rewrites every version in the block"
                    );
                }
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
            "{path}: unclosed x-release-please-start-version block; release-please would rewrite every version to the end of the file"
        );
        if let Some(header) = table {
            panic!(
                "{path}:{}: cachekit-rs table has no version line",
                header + 1
            );
        }
        assert!(
            pins > 0,
            "{path}: no cachekit-rs pin found; did the snippet format change?"
        );
    }
}
