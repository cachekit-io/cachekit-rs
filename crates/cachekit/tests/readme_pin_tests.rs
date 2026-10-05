//! README dependency pins: every `cachekit-rs = ...` line in the two published
//! READMEs (this crate's, and cachekit-macros') must name exactly this crate's
//! version and sit inside an `x-release-please-start-version` block.
//!
//! crates.io shows these READMEs on each release's page, and a reader copies
//! the pin along with the examples, so a stale pin installs an older release
//! than the one the README documents. The pins drifted unnoticed for two
//! releases because nothing bumped them. Now release-please rewrites the
//! marked blocks on each release (`extra-files` in `release-please-config.json`).
//! This file fails the PR that adds a pin outside a marked block, and the
//! release PR if release-please stopped rewriting them.
//!
//! Excluded from the published package (`exclude` in Cargo.toml): it reads
//! files outside the crate directory.

const READMES: [(&str, &str); 2] = [
    ("README.md", include_str!("../../../README.md")),
    (
        "crates/cachekit-macros/README.md",
        include_str!("../../cachekit-macros/README.md"),
    ),
];

#[test]
fn readme_pins_name_this_version_inside_release_please_blocks() {
    let version = env!("CARGO_PKG_VERSION");
    for (path, text) in READMES {
        let mut in_block = false;
        let mut pins = 0;
        for (n, line) in text.lines().enumerate() {
            if line.contains("x-release-please-start-version") {
                in_block = true;
                continue;
            }
            if line.contains("x-release-please-end") {
                in_block = false;
                continue;
            }
            let Some(spec) = line
                .split_once("cachekit-rs")
                .and_then(|(_, rest)| rest.trim_start().strip_prefix('='))
            else {
                continue;
            };
            // `"0.10.0"` or `{ version = "0.10.0", ... }`: the first quoted string.
            let pin = spec.split('"').nth(1).unwrap_or_default();
            let at = format!("{path}:{}", n + 1);
            assert!(
                in_block,
                "{at}: pin is outside an x-release-please-start-version block, so no release will bump it"
            );
            assert_eq!(
                pin, version,
                "{at}: pins {pin:?}, but this crate is {version}"
            );
            pins += 1;
        }
        assert!(
            pins > 0,
            "{path}: no cachekit-rs pin found; did the snippet format change?"
        );
    }
}
