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
//! fence (backtick or tilde, judged by the first word of the info string, so
//! ` ```toml title=... ` counts), where install snippets live, every line that names `cachekit-rs`
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
    let mut found = Vec::new();
    let mut rest = line;
    // Each run of digits and dots; `v` in `v0.10.0` and `h2-` in `h2-0.4.0` fall between runs.
    while let Some(start) = rest.find(|c: char| c.is_ascii_digit()) {
        rest = &rest[start..];
        let mut end = rest
            .find(|c: char| !(c.is_ascii_digit() || c == '.'))
            .unwrap_or(rest.len());
        let core = rest[..end].trim_end_matches('.');
        // `1.x` is no version, and `.html` in `1.85.0.html` is no suffix.
        if core.contains('.') {
            if rest[core.len()..].starts_with(['-', '+']) {
                end = rest
                    .find(|c: char| !(c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '+')))
                    .unwrap_or(rest.len());
                found.push(rest[..end].trim_end_matches(['.', '-', '+']));
            } else {
                found.push(core);
            }
        }
        rest = &rest[end..];
    }
    found
}

/// The marker (` ``` `, `~~~~`, ...) and the rest of a code fence line.
fn fence_line(line: &str) -> Option<(&str, &str)> {
    let line = line.trim_start();
    let mark = line.chars().next().filter(|c| matches!(c, '`' | '~'))?;
    let rest = line.trim_start_matches(mark);
    let marker = &line[..line.len() - rest.len()];
    (marker.len() >= 3).then_some((marker, rest))
}

fn check(path: &str, text: &str, version: &str) {
    let mut in_block = false;
    // The open code fence's marker, and the first word of its info string,
    // such as `toml` or `rust`.
    let mut fence: Option<(&str, &str)> = None;
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
        if let Some((marker, info)) = fence_line(line) {
            match fence {
                None => {
                    let word = info
                        .trim_start()
                        .split(|c: char| c.is_whitespace() || matches!(c, ',' | '{'))
                        .next();
                    fence = Some((marker, word.unwrap_or_default()));
                    continue;
                }
                // Only the opener's character, at least as long, with nothing after it, closes.
                Some((open, _)) if marker.starts_with(open) && info.trim().is_empty() => {
                    fence = None;
                    continue;
                }
                // Anything else, such as ` ```text ` in a ` ```` ` fence, is content.
                Some(_) => {}
            }
        }
        let install_fence = matches!(
            fence,
            Some((_, "toml" | "sh" | "bash" | "shell" | "console"))
        );
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

/// A renamed dependency table split across lines.
const SPLIT_TABLE: &str = "[dependencies.cache]\npackage = \"cachekit-rs\"\nversion = \"0.8\"\n";

#[test]
fn split_table_fails_in_every_toml_fence_spelling() {
    for (open, close) in [
        ("```toml title=\"Cargo.toml\"", "```"),
        ("```toml,ignore", "```"),
        ("~~~toml", "~~~"),
        ("````toml", "````"),
        // A marker with text after it is content, so it does not close the fence.
        ("```toml\n```text", "```"),
    ] {
        let text = format!("{open}\n{SPLIT_TABLE}{close}\n");
        let panic = std::panic::catch_unwind(|| check("t", &text, "0.10.0")).expect_err(&text);
        let message = panic.downcast_ref::<String>().cloned().unwrap_or_default();
        assert!(
            message.contains("names cachekit-rs with no version on the line"),
            "{text}: {message}"
        );
    }
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
    assert_eq!(versions("h2-0.4.0, python3-3.12.1"), ["0.4.0", "3.12.1"]);
    assert_eq!(versions("tokio 1.x, AES-256-GCM, sha256.hex"), none);
}
