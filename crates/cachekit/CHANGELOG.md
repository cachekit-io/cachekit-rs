# Changelog

## [0.9.0](https://github.com/cachekit-io/cachekit-rs/compare/cachekit-rs-v0.8.0...cachekit-rs-v0.9.0) (2026-10-02)


### ⚠ BREAKING CHANGES

* **encryption:** the raw-bytes master-key APIs (`EncryptionLayer::new`, `EncryptionLayer::with_previous_keys`, `CacheKitBuilder::encryption_from_bytes` and `CacheKitBuilder::encryption_from_bytes_with_previous`) now require keys of exactly 32 bytes, current and previous, and return `CachekitError::Config` for any other length. Callers that pass longer keys to a raw-bytes API, including hex keys decoded manually, now get an error at construction. Use exactly 32-byte keys, the only length every SDK accepts (`openssl rand -hex 32`), or pass the hex string to `.encryption()` or use `CacheKit::from_env()`.
* **interop:** an interop namespace or operation containing `..` now fails: `interop_key` returns `InvalidKey`, and `#[cachekit]` does not compile. Rename the segment; its keys become a full cache miss.

### Bug Fixes

* **cachekitio:** reject the empty cache key; test path encoding from the protocol fixture (LAB-6551) ([#100](https://github.com/cachekit-io/cachekit-rs/issues/100)) ([4d11fc1](https://github.com/cachekit-io/cachekit-rs/commit/4d11fc1e856dadddc53bc1fdcfc4ba5d8ff50668))
* **encryption:** raw-bytes master keys must be exactly 32 bytes (LAB-4663) ([#103](https://github.com/cachekit-io/cachekit-rs/issues/103)) ([f85d121](https://github.com/cachekit-io/cachekit-rs/commit/f85d1210df3cf73af2656b57939f088cef9f3c48))
* **file:** return a miss for entries with nonzero flags or reserved bytes (LAB-7153) ([#107](https://github.com/cachekit-io/cachekit-rs/issues/107)) ([6d48ece](https://github.com/cachekit-io/cachekit-rs/commit/6d48ecece00532d42755cd593da255a90ada7750))
* **intents:** secure_from_env reads CACHEKIT_PREVIOUS_MASTER_KEYS (LAB-6591) ([#102](https://github.com/cachekit-io/cachekit-rs/issues/102)) ([dda7f42](https://github.com/cachekit-io/cachekit-rs/commit/dda7f42900bc9eabed8e932df783d279c55fa674))
* **interop:** reject double-dot interop segments (LAB-5906) ([#98](https://github.com/cachekit-io/cachekit-rs/issues/98)) ([8480457](https://github.com/cachekit-io/cachekit-rs/commit/84804578390cefb2dd5ecf5cbcdef234b9b3b414))
* **l1:** honour X-CacheKit-Freshness and Fresh-For before backfilling L1 (LAB-7155) ([#109](https://github.com/cachekit-io/cachekit-rs/issues/109)) ([658b198](https://github.com/cachekit-io/cachekit-rs/commit/658b198808092bea55d2ff1a5888065a52fc1b25))
* **redis:** set a 5 s command timeout on the fred client (LAB-7154) ([#108](https://github.com/cachekit-io/cachekit-rs/issues/108)) ([72db7c7](https://github.com/cachekit-io/cachekit-rs/commit/72db7c74ef09442b98e0ce28b79ba068fd603486))


### Performance Improvements

* **bench:** instruction-gated hot-path bench and a client wall-time probe (LAB-7057) ([#110](https://github.com/cachekit-io/cachekit-rs/issues/110)) ([d561d70](https://github.com/cachekit-io/cachekit-rs/commit/d561d70e38b27382d62ebb4749e90590b93c0f80))
* **file:** stripe the in-process lock per key so reads skip unrelated fsyncs (LAB-7086) ([#106](https://github.com/cachekit-io/cachekit-rs/issues/106)) ([e9980df](https://github.com/cachekit-io/cachekit-rs/commit/e9980dfb519e124c568ce47a45e7d996291f36b5))
* **file:** sweep orphaned temp files on first set, not on build (LAB-7381) ([#113](https://github.com/cachekit-io/cachekit-rs/issues/113)) ([222a916](https://github.com/cachekit-io/cachekit-rs/commit/222a916cb16c7435d97bcd6c17415aa81455ebce))


### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * cachekit-macros bumped from 0.8.0 to 0.9.0

## [0.8.0](https://github.com/cachekit-io/cachekit-rs/compare/cachekit-rs-v0.7.0...cachekit-rs-v0.8.0) (2026-09-29)


### ⚠ BREAKING CHANGES

* **intents:** `CacheKit::secure` takes the master key as a hex string (`master_key_hex: &str`) instead of raw bytes (`master_key: &[u8]`). The key must decode to at least 32 bytes and is validated before any Redis connection is attempted. There is no longer a raw-bytes preset; decoded bytes go through `CacheKitBuilder::encryption_from_bytes`. The new `CacheKit::secure_from_env` reads the key from `CACHEKIT_MASTER_KEY`.
* **interop:** reject reserved namespaces ns and nsapi (LAB-5876) ([#89](https://github.com/cachekit-io/cachekit-rs/issues/89))
* **intents:** `CacheKit::minimal` now enables an in-process L1 cache (1000 entries, no stale-while-revalidate, no cross-process invalidation). A `minimal` read may serve an L1 entry for up to its 300 s TTL after another writer changed the value in Redis. Chain `.no_l1()` on the returned builder to restore read-through-every-time behaviour.
* **intents:** `CacheKit::encrypted(url, key)` is now `CacheKit::secure(url, key)`, and the `SecureCache` accessor `cache.secure()` is now `cache.secure_cache()`. Migration: rename both call sites; behaviour, defaults and feature gates are unchanged.

### Features

* **encryption:** keyring rotation — previous_master_keys + sequential decrypt (LAB-686) ([#63](https://github.com/cachekit-io/cachekit-rs/issues/63)) ([8b9e7ac](https://github.com/cachekit-io/cachekit-rs/commit/8b9e7ac03f803b7e9f2979537f525136fdc59c40))
* **encryption:** surface hardware-acceleration detection (LAB-523) ([fd03a12](https://github.com/cachekit-io/cachekit-rs/commit/fd03a12a1ddcd08f72c41522d26b4a0c989a3df9))
* **intents:** CacheKit::io falls back to CACHEKIT_API_KEY via io_from_env (LAB-4647) ([#86](https://github.com/cachekit-io/cachekit-rs/issues/86)) ([464b9d2](https://github.com/cachekit-io/cachekit-rs/commit/464b9d29ad551be9e3d169f73a07a3748e8739b6))
* **intents:** CacheKit::secure takes a hex master key, add secure_from_env (LAB-4645) ([#88](https://github.com/cachekit-io/cachekit-rs/issues/88)) ([50211d3](https://github.com/cachekit-io/cachekit-rs/commit/50211d32f4f7744e6e94fbc566ba4ef7d1e1257c))
* **observability:** live read counters, auto-wired SaaS telemetry, tracing events (LAB-521) ([#81](https://github.com/cachekit-io/cachekit-rs/issues/81)) ([afe77ad](https://github.com/cachekit-io/cachekit-rs/commit/afe77add961601eb05772599ff9ef7d6d1947308))
* **rs:** surface rotation drain signal via decrypt_indexed (LAB-1678) ([#74](https://github.com/cachekit-io/cachekit-rs/issues/74)) ([15b94cf](https://github.com/cachekit-io/cachekit-rs/commit/15b94cfb0ebab032af67b39363a3d5ef89a6c82c))


### Bug Fixes

* **cachekitio:** reject reserved cache-key segments in request path (LAB-2878) ([#76](https://github.com/cachekit-io/cachekit-rs/issues/76)) ([0ed7e1d](https://github.com/cachekit-io/cachekit-rs/commit/0ed7e1d0e75a21b8deffb59031897fe664b3dc6e))
* **intents:** CacheKit::minimal enables L1 with SWR off per intent-preset contract (LAB-4644) ([#85](https://github.com/cachekit-io/cachekit-rs/issues/85)) ([e0d4388](https://github.com/cachekit-io/cachekit-rs/commit/e0d4388acb7536d3710845254de131d343fc2bf1))
* **interop:** reject reserved namespaces ns and nsapi (LAB-5876) ([#89](https://github.com/cachekit-io/cachekit-rs/issues/89)) ([5e3aa2f](https://github.com/cachekit-io/cachekit-rs/commit/5e3aa2f3d9900d546d4e89d102f3c018b6e38215))
* **secure:** evict L1 copy when decryption fails (LAB-5570) ([#87](https://github.com/cachekit-io/cachekit-rs/issues/87)) ([fe5170d](https://github.com/cachekit-io/cachekit-rs/commit/fe5170d8da12f201ea8cb431aa70b0dad66275ba))
* **serializer:** enforce decode depth in the structural guard and vendor decode-bounds 1.1.0 (LAB-3481) ([b45be1a](https://github.com/cachekit-io/cachekit-rs/commit/b45be1a9423bb57e75b808b4f8bddf53c0858695))
* **serializer:** own the msgpack decode depth bound and add a structural walk (LAB-2503) ([#73](https://github.com/cachekit-io/cachekit-rs/issues/73)) ([306c1b1](https://github.com/cachekit-io/cachekit-rs/commit/306c1b1529ddfa025d809bc465a1dedaa98e2139))


### Code Refactoring

* **intents:** rename CacheKit::encrypted to CacheKit::secure (LAB-4651) ([#82](https://github.com/cachekit-io/cachekit-rs/issues/82)) ([11802d9](https://github.com/cachekit-io/cachekit-rs/commit/11802d9c3a002f4782269222ff610d9f18331ca7))


### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * cachekit-macros bumped from 0.7.0 to 0.8.0

## [0.7.0](https://github.com/cachekit-io/cachekit-rs/compare/cachekit-rs-v0.6.1...cachekit-rs-v0.7.0) (2026-08-07)


### ⚠ BREAKING CHANGES

* the public module cachekit::key (and cachekit::key::generate_cache_key) is removed. It was never protocol-conformant and had no supported use. For cross-SDK, spec-conformant keys use the interop/v1 keygen (interop_key(), arriving with cachekit-rs#33 / LAB-246). The #[cachekit] macro's derived keys are unchanged.

### Features

* #[cachekit] mints interop/v1 keys — retire legacy non-conformant keygen (LAB-424) ([#35](https://github.com/cachekit-io/cachekit-rs/issues/35)) ([ff1d490](https://github.com/cachekit-io/cachekit-rs/commit/ff1d4902da40c9a99dae8e8e8179a6b83f4771c3))
* **backend:** add Memcached and File backends (LAB-429) ([#44](https://github.com/cachekit-io/cachekit-rs/issues/44)) ([3afe8e7](https://github.com/cachekit-io/cachekit-rs/commit/3afe8e7c0138f770eb66f2a9975d70aa5b953f01))
* **backend:** Redis lock + Workers lock/TTL capability parity (LAB-426) ([#37](https://github.com/cachekit-io/cachekit-rs/issues/37)) ([f6cf7b7](https://github.com/cachekit-io/cachekit-rs/commit/f6cf7b7f6c00d24afc9e4d5978639595f08c426b))
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
* intent-based cache API ([#19](https://github.com/cachekit-io/cachekit-rs/issues/19)) ([e86172b](https://github.com/cachekit-io/cachekit-rs/commit/e86172b9440cb11105856bf13563a5d4d1425a47))
* interop mode (interop/v1) — first in-SDK keygen + Rust vector verification [LAB-246] ([#33](https://github.com/cachekit-io/cachekit-rs/issues/33)) ([188c170](https://github.com/cachekit-io/cachekit-rs/commit/188c1709e3bf0e7e741ffa9a6ee357f6ee7d1487))
* **l1:** LAB-728 stale-while-revalidate — serve stale + single-flight background refresh ([#47](https://github.com/cachekit-io/cachekit-rs/issues/47)) ([068b84a](https://github.com/cachekit-io/cachekit-rs/commit/068b84ac407cefa20c13a706798faf5354ade5d8))
* **reliability:** retry, circuit breaker, graceful degradation, single-flight (LAB-518) ([#43](https://github.com/cachekit-io/cachekit-rs/issues/43)) ([e9b9a1e](https://github.com/cachekit-io/cachekit-rs/commit/e9b9a1e7ddf42225a81bc5247ad90e011a690937))
* unsync feature flag for ?Send contexts ([#16](https://github.com/cachekit-io/cachekit-rs/issues/16)) ([c52c3f2](https://github.com/cachekit-io/cachekit-rs/commit/c52c3f22317b589d9ee41d955868977ed3d0a27a))


### Bug Fixes

* **l1:** guard LAB-728 SWR refresh commits ([#48](https://github.com/cachekit-io/cachekit-rs/issues/48)) ([e31109b](https://github.com/cachekit-io/cachekit-rs/commit/e31109bfb09c31970d940819a088c1a975ea4f45))
* **reliability:** clamp max_concurrent to Semaphore::MAX_PERMITS; add ReliabilityConfig::disabled() (LAB-729) ([#50](https://github.com/cachekit-io/cachekit-rs/issues/50)) ([d851797](https://github.com/cachekit-io/cachekit-rs/commit/d85179744dc284dcf227ea2e256ef6ed9c15ee73))
* resolve critical issues from expert panel review ([41d2189](https://github.com/cachekit-io/cachekit-rs/commit/41d218964468b5833f273e8f84a9e9d479672584))
* serialize config env var tests to prevent race condition ([79a1359](https://github.com/cachekit-io/cachekit-rs/commit/79a135978e717b181d37c464a2c0445f0d0b447e))
* use snake_case wire names for lock acquire request/response ([#34](https://github.com/cachekit-io/cachekit-rs/issues/34)) ([40b7277](https://github.com/cachekit-io/cachekit-rs/commit/40b7277e75b5019ad27206323f97135db14dc8b9))
* wasm32-safe session clock + wasm32 runtime CI tests (LAB-1079) ([#60](https://github.com/cachekit-io/cachekit-rs/issues/60)) ([1394a37](https://github.com/cachekit-io/cachekit-rs/commit/1394a37e44a9df3c6d22ea7348458232e491dbe7))


### Security

* send lock_id via X-CacheKit-Lock-Id header, not query string ([#24](https://github.com/cachekit-io/cachekit-rs/issues/24)) ([#29](https://github.com/cachekit-io/cachekit-rs/issues/29)) ([f381e41](https://github.com/cachekit-io/cachekit-rs/commit/f381e41662c2f9d131fd2cb40c9a1790132c51be))


### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * cachekit-macros bumped from 0.6.1 to 0.7.0

## [0.6.1](https://github.com/cachekit-io/cachekit-rs/compare/cachekit-rs-v0.6.0...cachekit-rs-v0.6.1) (2026-08-05)


### Bug Fixes

* use snake_case wire names for lock acquire request/response ([#34](https://github.com/cachekit-io/cachekit-rs/issues/34)) ([40b7277](https://github.com/cachekit-io/cachekit-rs/commit/40b7277e75b5019ad27206323f97135db14dc8b9))


### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * cachekit-macros bumped from 0.6.0 to 0.6.1

## [0.6.0](https://github.com/cachekit-io/cachekit-rs/compare/cachekit-rs-v0.5.0...cachekit-rs-v0.6.0) (2026-08-03)


### Features

* **l1:** LAB-728 stale-while-revalidate — serve stale + single-flight background refresh ([#47](https://github.com/cachekit-io/cachekit-rs/issues/47)) ([068b84a](https://github.com/cachekit-io/cachekit-rs/commit/068b84ac407cefa20c13a706798faf5354ade5d8))
* **reliability:** retry, circuit breaker, graceful degradation, single-flight (LAB-518) ([#43](https://github.com/cachekit-io/cachekit-rs/issues/43)) ([e9b9a1e](https://github.com/cachekit-io/cachekit-rs/commit/e9b9a1e7ddf42225a81bc5247ad90e011a690937))


### Bug Fixes

* **l1:** guard LAB-728 SWR refresh commits ([#48](https://github.com/cachekit-io/cachekit-rs/issues/48)) ([e31109b](https://github.com/cachekit-io/cachekit-rs/commit/e31109bfb09c31970d940819a088c1a975ea4f45))
* **reliability:** clamp max_concurrent to Semaphore::MAX_PERMITS; add ReliabilityConfig::disabled() (LAB-729) ([#50](https://github.com/cachekit-io/cachekit-rs/issues/50)) ([d851797](https://github.com/cachekit-io/cachekit-rs/commit/d85179744dc284dcf227ea2e256ef6ed9c15ee73))


### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * cachekit-macros bumped from 0.5.0 to 0.6.0

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
