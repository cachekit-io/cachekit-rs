# Changelog

## [0.8.0](https://github.com/cachekit-io/cachekit-rs/compare/cachekit-macros-v0.7.0...cachekit-macros-v0.8.0) (2026-08-08)


### Miscellaneous

* **cachekit-macros:** Synchronize cachekit-rs versions

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
