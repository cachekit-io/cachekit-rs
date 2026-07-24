# Changelog

## [0.5.0](https://github.com/cachekit-io/cachekit-rs/compare/cachekit-rs-v0.4.0...cachekit-rs-v0.5.0) (2026-07-24)


### Features

* **backend:** add Memcached and File backends (LAB-429) ([#44](https://github.com/cachekit-io/cachekit-rs/issues/44)) ([3afe8e7](https://github.com/cachekit-io/cachekit-rs/commit/3afe8e7c0138f770eb66f2a9975d70aa5b953f01))
* **backend:** Redis lock + Workers lock/TTL capability parity (LAB-426) ([#37](https://github.com/cachekit-io/cachekit-rs/issues/37)) ([f6cf7b7](https://github.com/cachekit-io/cachekit-rs/commit/f6cf7b7f6c00d24afc9e4d5978639595f08c426b))


### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * cachekit-macros bumped from 0.4.0 to 0.5.0

## [0.4.0](https://github.com/cachekit-io/cachekit-rs/compare/cachekit-rs-v0.3.0...cachekit-rs-v0.4.0) (2026-07-23)


### ⚠ BREAKING CHANGES

* the public module cachekit::key (and cachekit::key::generate_cache_key) is removed. It was never protocol-conformant and had no supported use. For cross-SDK, spec-conformant keys use the interop/v1 keygen (interop_key(), arriving with cachekit-rs#33 / LAB-246). The #[cachekit] macro's derived keys are unchanged.

### Features

* #[cachekit] mints interop/v1 keys — retire legacy non-conformant keygen (LAB-424) ([#35](https://github.com/cachekit-io/cachekit-rs/issues/35)) ([ff1d490](https://github.com/cachekit-io/cachekit-rs/commit/ff1d4902da40c9a99dae8e8e8179a6b83f4771c3))
* intent-based cache API ([#19](https://github.com/cachekit-io/cachekit-rs/issues/19)) ([e86172b](https://github.com/cachekit-io/cachekit-rs/commit/e86172b9440cb11105856bf13563a5d4d1425a47))
* interop mode (interop/v1) — first in-SDK keygen + Rust vector verification [LAB-246] ([#33](https://github.com/cachekit-io/cachekit-rs/issues/33)) ([188c170](https://github.com/cachekit-io/cachekit-rs/commit/188c1709e3bf0e7e741ffa9a6ee357f6ee7d1487))


### Security

* send lock_id via X-CacheKit-Lock-Id header, not query string ([#24](https://github.com/cachekit-io/cachekit-rs/issues/24)) ([#29](https://github.com/cachekit-io/cachekit-rs/issues/29)) ([f381e41](https://github.com/cachekit-io/cachekit-rs/commit/f381e41662c2f9d131fd2cb40c9a1790132c51be))


### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * cachekit-macros bumped from 0.3.0 to 0.4.0

## [0.3.0](https://github.com/cachekit-io/cachekit-rs/compare/cachekit-rs-v0.2.0...cachekit-rs-v0.3.0) (2026-04-26)


### Features

* unsync feature flag for ?Send contexts ([#16](https://github.com/cachekit-io/cachekit-rs/issues/16)) ([c52c3f2](https://github.com/cachekit-io/cachekit-rs/commit/c52c3f22317b589d9ee41d955868977ed3d0a27a))


### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * cachekit-macros bumped from 0.2.0 to 0.3.0

## [0.2.0](https://github.com/cachekit-io/cachekit-rs/compare/cachekit-rs-v0.0.1-alpha.1...cachekit-rs-v0.2.0) (2026-04-26)


### Features

* CachekitIO backend full parity — session, metrics, SSRF, errors, locking, TTL ([88f1344](https://github.com/cachekit-io/cachekit-rs/commit/88f1344f119f5e344f39c4ebdb30c7e21b17b427))
* CachekitIO backend full parity (session, metrics, SSRF, locking, TTL) ([b8bc4bb](https://github.com/cachekit-io/cachekit-rs/commit/b8bc4bb4e76c5d49aa77fc34fb2723aee4eb2354))
* implement #[cachekit] proc-macro and Workers backend ([7ae2f05](https://github.com/cachekit-io/cachekit-rs/commit/7ae2f05b20582b72008ba900853edd173573d72a))
* implement Backend and TtlInspectable traits with wasm32 support ([fa8e612](https://github.com/cachekit-io/cachekit-rs/commit/fa8e612b10bfda1119515a295d2c1fb309a80584))
* implement Blake2b-256 cache key generation ([0bb1df0](https://github.com/cachekit-io/cachekit-rs/commit/0bb1df0f96013b6deeb91646c060cd060e255d3b))
* implement CacheKit client with L1 cache and builder pattern ([5fb63e7](https://github.com/cachekit-io/cachekit-rs/commit/5fb63e7fb9fd71cb359e1a70f944b6f520b23a95))
* implement CachekitConfig with builder and env parsing ([8caad1d](https://github.com/cachekit-io/cachekit-rs/commit/8caad1d70be4067f47d1e2a939cdf897799ce2ab))
* implement CachekitIO HTTP backend for native targets ([556d935](https://github.com/cachekit-io/cachekit-rs/commit/556d93543e4764bd10d37ce09dc1bacb3f55067e))
* implement error types with HTTP status mapping ([219e737](https://github.com/cachekit-io/cachekit-rs/commit/219e7379af932bbc629ed3d14141c2f09cffcef7))
* implement L1 in-memory cache with per-entry TTL via moka Expiry ([c458f6c](https://github.com/cachekit-io/cachekit-rs/commit/c458f6c21a56379fd22b6191319c5ef777ee2641))
* implement MessagePack serializer ([e97cd82](https://github.com/cachekit-io/cachekit-rs/commit/e97cd82f68d4072efbe3054804599d0bb5b69106))
* implement Redis backend with TtlInspectable support ([4777e3a](https://github.com/cachekit-io/cachekit-rs/commit/4777e3a68f74ae94c2a932991327657d8b654dae))
* implement zero-knowledge encryption layer with AAD v0x03 ([3ced335](https://github.com/cachekit-io/cachekit-rs/commit/3ced335e7bdd60866d97e42acb736afda67ae1bc))


### Bug Fixes

* resolve critical issues from expert panel review ([41d2189](https://github.com/cachekit-io/cachekit-rs/commit/41d218964468b5833f273e8f84a9e9d479672584))
* serialize config env var tests to prevent race condition ([79a1359](https://github.com/cachekit-io/cachekit-rs/commit/79a135978e717b181d37c464a2c0445f0d0b447e))


### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * cachekit-macros bumped from 0.1 to 0.2.0
