# Changelog

## [0.11.0](https://github.com/cachekit-io/cachekit-rs/compare/cachekit-macros-v0.10.0...cachekit-macros-v0.11.0) (2026-10-09)


### Bug Fixes

* **macros:** accept any E: From&lt;CachekitError&gt; in #[cachekit] ([#155](https://github.com/cachekit-io/cachekit-rs/issues/155)) ([13c3798](https://github.com/cachekit-io/cachekit-rs/commit/13c3798de4d040129b53897ef5e8c6f2e8d2b425))
* **readme:** pin the current release and bump the pins on every release (LAB-8219) ([#141](https://github.com/cachekit-io/cachekit-rs/issues/141)) ([68409f5](https://github.com/cachekit-io/cachekit-rs/commit/68409f5f9a9da2eb3dd431583faaa3bb9cda7cfe))
* **swr:** stand a refresh down on a held lock instead of polling it (LAB-8236) ([#147](https://github.com/cachekit-io/cachekit-rs/issues/147)) ([dcec81e](https://github.com/cachekit-io/cachekit-rs/commit/dcec81e944e25121dfb7c39b0520fd9997a1f41d))


### Performance Improvements

* **flight:** detach a stored fill's unlock and bound the contested poll by a deadline (LAB-7122) ([#149](https://github.com/cachekit-io/cachekit-rs/issues/149)) ([f5b13b9](https://github.com/cachekit-io/cachekit-rs/commit/f5b13b946d013245d7f1883244cead5960c4f2fd))

## [0.10.0](https://github.com/cachekit-io/cachekit-rs/compare/cachekit-macros-v0.9.0...cachekit-macros-v0.10.0) (2026-10-05)


### ⚠ BREAKING CHANGES

* **encryption:** a client with encryption configured (`.encryption()`, `.encryption_from_bytes()`, `.encryption_from_bytes_with_previous()`, the `secure` / `secure_from_env` presets, or `CacheKit::from_env()` with `CACHEKIT_MASTER_KEY` set) now encrypts every value read and write, not only those made through `secure_cache()`: plain `get`, `set`, `set_with_ttl`, `interop_get`, `interop_get_swr` and non-`secure` `#[cachekit]` functions store and read AES-256-GCM ciphertext, and L1 holds ciphertext. Entries those calls wrote in plaintext before this release are not migrated: reading one now returns `CachekitError::Encryption`, never a silent miss, until it is overwritten, deleted or expires, so delete or let expire what they wrote before upgrading. Builds without the `encryption` feature now get `CachekitError::Config` from all three builder encryption methods, and from `CacheKit::from_env()` when `CACHEKIT_MASTER_KEY` is set, instead of a plaintext client. On every build, a `CACHEKIT_MASTER_KEY` that is set but not valid UTF-8 is now a `Config` error from `CachekitConfig::from_env()` and `CacheKit::from_env()`, where it was read as unset and gave a plaintext client.

### Bug Fixes

* **encryption:** encrypt every value read and write once a key is configured (LAB-4676) ([#133](https://github.com/cachekit-io/cachekit-rs/issues/133)) ([5a963d9](https://github.com/cachekit-io/cachekit-rs/commit/5a963d9dcbc3b7de103b02610b8203e2bf2c56ee))

## [0.9.0](https://github.com/cachekit-io/cachekit-rs/compare/cachekit-macros-v0.8.0...cachekit-macros-v0.9.0) (2026-10-02)


### ⚠ BREAKING CHANGES

* **interop:** an interop namespace or operation containing `..` now fails: `interop_key` returns `InvalidKey`, and `#[cachekit]` does not compile. Rename the segment; its keys become a full cache miss.

### Bug Fixes

* **interop:** reject double-dot interop segments (LAB-5906) ([#98](https://github.com/cachekit-io/cachekit-rs/issues/98)) ([8480457](https://github.com/cachekit-io/cachekit-rs/commit/84804578390cefb2dd5ecf5cbcdef234b9b3b414))

## [0.8.0](https://github.com/cachekit-io/cachekit-rs/compare/cachekit-macros-v0.7.0...cachekit-macros-v0.8.0) (2026-09-29)


### ⚠ BREAKING CHANGES

* **interop:** reject reserved namespaces ns and nsapi (LAB-5876) ([#89](https://github.com/cachekit-io/cachekit-rs/issues/89))
* **intents:** `CacheKit::encrypted(url, key)` is now `CacheKit::secure(url, key)`, and the `SecureCache` accessor `cache.secure()` is now `cache.secure_cache()`. Migration: rename both call sites; behaviour, defaults and feature gates are unchanged.

### Bug Fixes

* **interop:** reject reserved namespaces ns and nsapi (LAB-5876) ([#89](https://github.com/cachekit-io/cachekit-rs/issues/89)) ([5e3aa2f](https://github.com/cachekit-io/cachekit-rs/commit/5e3aa2f3d9900d546d4e89d102f3c018b6e38215))
* **secure:** evict L1 copy when decryption fails (LAB-5570) ([#87](https://github.com/cachekit-io/cachekit-rs/issues/87)) ([fe5170d](https://github.com/cachekit-io/cachekit-rs/commit/fe5170d8da12f201ea8cb431aa70b0dad66275ba))


### Code Refactoring

* **intents:** rename CacheKit::encrypted to CacheKit::secure (LAB-4651) ([#82](https://github.com/cachekit-io/cachekit-rs/issues/82)) ([11802d9](https://github.com/cachekit-io/cachekit-rs/commit/11802d9c3a002f4782269222ff610d9f18331ca7))

## [0.7.0](https://github.com/cachekit-io/cachekit-rs/compare/cachekit-macros-v0.6.1...cachekit-macros-v0.7.0) (2026-08-07)


### ⚠ BREAKING CHANGES

* the public module cachekit::key (and cachekit::key::generate_cache_key) is removed. It was never protocol-conformant and had no supported use. For cross-SDK, spec-conformant keys use the interop/v1 keygen (interop_key(), arriving with cachekit-rs#33 / LAB-246). The #[cachekit] macro's derived keys are unchanged.

### Features

* #[cachekit] mints interop/v1 keys — retire legacy non-conformant keygen (LAB-424) ([#35](https://github.com/cachekit-io/cachekit-rs/issues/35)) ([ff1d490](https://github.com/cachekit-io/cachekit-rs/commit/ff1d4902da40c9a99dae8e8e8179a6b83f4771c3))
* implement #[cachekit] proc-macro and Workers backend ([7ae2f05](https://github.com/cachekit-io/cachekit-rs/commit/7ae2f05b20582b72008ba900853edd173573d72a))
* **l1:** LAB-728 stale-while-revalidate — serve stale + single-flight background refresh ([#47](https://github.com/cachekit-io/cachekit-rs/issues/47)) ([068b84a](https://github.com/cachekit-io/cachekit-rs/commit/068b84ac407cefa20c13a706798faf5354ade5d8))
* **reliability:** retry, circuit breaker, graceful degradation, single-flight (LAB-518) ([#43](https://github.com/cachekit-io/cachekit-rs/issues/43)) ([e9b9a1e](https://github.com/cachekit-io/cachekit-rs/commit/e9b9a1e7ddf42225a81bc5247ad90e011a690937))


### Bug Fixes

* **l1:** guard LAB-728 SWR refresh commits ([#48](https://github.com/cachekit-io/cachekit-rs/issues/48)) ([e31109b](https://github.com/cachekit-io/cachekit-rs/commit/e31109bfb09c31970d940819a088c1a975ea4f45))
* resolve critical issues from expert panel review ([41d2189](https://github.com/cachekit-io/cachekit-rs/commit/41d218964468b5833f273e8f84a9e9d479672584))

## [0.6.1](https://github.com/cachekit-io/cachekit-rs/compare/cachekit-macros-v0.6.0...cachekit-macros-v0.6.1) (2026-08-05)


### Miscellaneous

* **cachekit-macros:** Synchronize cachekit-rs versions

## [0.6.0](https://github.com/cachekit-io/cachekit-rs/compare/cachekit-macros-v0.5.0...cachekit-macros-v0.6.0) (2026-08-03)


### Features

* **l1:** LAB-728 stale-while-revalidate — serve stale + single-flight background refresh ([#47](https://github.com/cachekit-io/cachekit-rs/issues/47)) ([068b84a](https://github.com/cachekit-io/cachekit-rs/commit/068b84ac407cefa20c13a706798faf5354ade5d8))
* **reliability:** retry, circuit breaker, graceful degradation, single-flight (LAB-518) ([#43](https://github.com/cachekit-io/cachekit-rs/issues/43)) ([e9b9a1e](https://github.com/cachekit-io/cachekit-rs/commit/e9b9a1e7ddf42225a81bc5247ad90e011a690937))


### Bug Fixes

* **l1:** guard LAB-728 SWR refresh commits ([#48](https://github.com/cachekit-io/cachekit-rs/issues/48)) ([e31109b](https://github.com/cachekit-io/cachekit-rs/commit/e31109bfb09c31970d940819a088c1a975ea4f45))

## [0.5.0](https://github.com/cachekit-io/cachekit-rs/compare/cachekit-macros-v0.4.0...cachekit-macros-v0.5.0) (2026-07-24)


### Miscellaneous

* **cachekit-macros:** Synchronize cachekit-rs versions

## [0.4.0](https://github.com/cachekit-io/cachekit-rs/compare/cachekit-macros-v0.3.0...cachekit-macros-v0.4.0) (2026-07-23)


### ⚠ BREAKING CHANGES

* the public module cachekit::key (and cachekit::key::generate_cache_key) is removed. It was never protocol-conformant and had no supported use. For cross-SDK, spec-conformant keys use the interop/v1 keygen (interop_key(), arriving with cachekit-rs#33 / LAB-246). The #[cachekit] macro's derived keys are unchanged.

### Features

* #[cachekit] mints interop/v1 keys — retire legacy non-conformant keygen (LAB-424) ([#35](https://github.com/cachekit-io/cachekit-rs/issues/35)) ([ff1d490](https://github.com/cachekit-io/cachekit-rs/commit/ff1d4902da40c9a99dae8e8e8179a6b83f4771c3))

## [0.3.0](https://github.com/cachekit-io/cachekit-rs/compare/cachekit-macros-v0.2.0...cachekit-macros-v0.3.0) (2026-04-26)


### Miscellaneous

* **cachekit-macros:** Synchronize cachekit-rs versions

## [0.2.0](https://github.com/cachekit-io/cachekit-rs/compare/cachekit-macros-v0.1.0...cachekit-macros-v0.2.0) (2026-04-26)


### Features

* implement #[cachekit] proc-macro and Workers backend ([7ae2f05](https://github.com/cachekit-io/cachekit-rs/commit/7ae2f05b20582b72008ba900853edd173573d72a))


### Bug Fixes

* resolve critical issues from expert panel review ([41d2189](https://github.com/cachekit-io/cachekit-rs/commit/41d218964468b5833f273e8f84a9e9d479672584))
