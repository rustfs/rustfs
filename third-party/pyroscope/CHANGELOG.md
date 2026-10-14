# Changelog

## [2.1.1](https://github.com/grafana/pyroscope-rs/compare/lib-2.1.0...lib-2.1.1) (2026-07-21)


### Bug Fixes

* avoid slice construction in address validation ([#569](https://github.com/grafana/pyroscope-rs/issues/569)) ([d6f45fe](https://github.com/grafana/pyroscope-rs/commit/d6f45fef1443d26c64a40721013ecfa7b1d3eccd))


### Miscellaneous Chores

* **deps:** lock file maintenance ([#570](https://github.com/grafana/pyroscope-rs/issues/570)) ([c37f52e](https://github.com/grafana/pyroscope-rs/commit/c37f52e71719da9ddb4a32ac8386a4c94534c805))

## [2.1.0](https://github.com/grafana/pyroscope-rs/compare/lib-2.0.6...lib-2.1.0) (2026-07-07)


### Features

* add a method to set the timer's ticker in PyroscopeAgent ([#557](https://github.com/grafana/pyroscope-rs/issues/557)) ([cc13d38](https://github.com/grafana/pyroscope-rs/commit/cc13d38cb5e84017a4f5e229648457b790bd6a1a))
* add otel.scope.name, otel.scope.version, process.runtime.name, process.runtime.version labels ([#562](https://github.com/grafana/pyroscope-rs/issues/562)) ([32f076e](https://github.com/grafana/pyroscope-rs/commit/32f076efd45aa2f3f203eb0bf8b03f222cb40648))


### Bug Fixes

* **security/unknown/:** update security memmap2 to v0.9.11 [security] ([#558](https://github.com/grafana/pyroscope-rs/issues/558)) ([d4d83f8](https://github.com/grafana/pyroscope-rs/commit/d4d83f8d350d927739c79a53bcfd7cd0115c77a4))


### Miscellaneous Chores

* bump nix ([#566](https://github.com/grafana/pyroscope-rs/issues/566)) ([1c02f30](https://github.com/grafana/pyroscope-rs/commit/1c02f3058135d1db06262ca57ead33fdc1593e6f))
* **deps:** lock file maintenance ([#555](https://github.com/grafana/pyroscope-rs/issues/555)) ([3917b99](https://github.com/grafana/pyroscope-rs/commit/3917b99d6de4f4d6cec078aba2ec7b6f4e7044c7))

## [2.0.6](https://github.com/grafana/pyroscope-rs/compare/lib-2.0.5...lib-2.0.6) (2026-05-28)


### Miscellaneous Chores

* **deps:** vendor pprofrs ([#552](https://github.com/grafana/pyroscope-rs/issues/552)) ([6684026](https://github.com/grafana/pyroscope-rs/commit/6684026ba0ea4fc997e4869703d671f23ee377a7))


### Continuous Integration

* remove pr-title-check workflow ([#549](https://github.com/grafana/pyroscope-rs/issues/549)) ([e4739e4](https://github.com/grafana/pyroscope-rs/commit/e4739e4bfa280e771fbd932f39d53faa599718ba))

## [2.0.5](https://github.com/grafana/pyroscope-rs/compare/lib-2.0.4...lib-2.0.5) (2026-05-27)


### Miscellaneous Chores

* **deps:** Remove unnecessary framehop dep ([#550](https://github.com/grafana/pyroscope-rs/issues/550)) ([d9c0e02](https://github.com/grafana/pyroscope-rs/commit/d9c0e02c7386b3a55e0270083c1135cac0e3ce0e))

## [2.0.4](https://github.com/grafana/pyroscope-rs/compare/lib-2.0.3...lib-2.0.4) (2026-05-20)


### Bug Fixes

* do not recreate profiling guard, just reset the data ([#518](https://github.com/grafana/pyroscope-rs/issues/518)) ([b1fe114](https://github.com/grafana/pyroscope-rs/commit/b1fe1141ee0d86caa59db383298c818763f3fa95))
* remove kindasafe CI leftovers ([#521](https://github.com/grafana/pyroscope-rs/issues/521)) ([f8bc0f3](https://github.com/grafana/pyroscope-rs/commit/f8bc0f31c3b775b9d0e97fb412af76059f0819ec))


### Miscellaneous Chores

* **deps:** bump openssl from 0.10.79 to 0.10.80 ([#548](https://github.com/grafana/pyroscope-rs/issues/548)) ([f3ed911](https://github.com/grafana/pyroscope-rs/commit/f3ed911e8b179d6fd63eacc2a793e884f126dfa0))
* **deps:** lock file maintenance ([#535](https://github.com/grafana/pyroscope-rs/issues/535)) ([9554564](https://github.com/grafana/pyroscope-rs/commit/955456438ea768adfe0a89cd1ef7da22e7974bcd))
* **deps:** lock file maintenance ([#541](https://github.com/grafana/pyroscope-rs/issues/541)) ([3abc7b6](https://github.com/grafana/pyroscope-rs/commit/3abc7b6d0c01f420a16cd2d08f44f10424c3ef3c))
* **deps:** lock file maintenance ([#547](https://github.com/grafana/pyroscope-rs/issues/547)) ([cd30ebf](https://github.com/grafana/pyroscope-rs/commit/cd30ebfb07bfbf243125df53f051a2f2d1dd5d63))
* **deps:** pin rust docker tag to a9cfb75 ([#544](https://github.com/grafana/pyroscope-rs/issues/544)) ([ea14091](https://github.com/grafana/pyroscope-rs/commit/ea140911e4b8bbabc8c42fbb2c7d43e0384ec4fe))
* **deps:** remove names crate dependency ([#527](https://github.com/grafana/pyroscope-rs/issues/527)) ([5893aac](https://github.com/grafana/pyroscope-rs/commit/5893aacb5bc3660dcd269b2fee33847f1fe1ab82))
* **deps:** update rust crate libc to v0.2.186 ([#533](https://github.com/grafana/pyroscope-rs/issues/533)) ([a7468c1](https://github.com/grafana/pyroscope-rs/commit/a7468c1db5d7955dfa0eb702a8b0371775ff2350))
* **deps:** update rust crate reqwest to v0.13.3 ([#539](https://github.com/grafana/pyroscope-rs/issues/539)) ([b8c6fbb](https://github.com/grafana/pyroscope-rs/commit/b8c6fbb97cc5f3b22185be35c9e19e72237b912e))
* **deps:** update rust:trixie docker digest to a9cfb75 ([#538](https://github.com/grafana/pyroscope-rs/issues/538)) ([bff1d5d](https://github.com/grafana/pyroscope-rs/commit/bff1d5d98f2a81d7a291c257bfb8291b41ffc8e5))
* **renovate:** vulnerabilities-only with profiling deps allowlist ([#540](https://github.com/grafana/pyroscope-rs/issues/540)) ([4aa8e56](https://github.com/grafana/pyroscope-rs/commit/4aa8e5656472a08ddea21cb2128b85203920243b))


### Continuous Integration

* add ARM cross-compilation test job ([#536](https://github.com/grafana/pyroscope-rs/issues/536)) ([54bb03f](https://github.com/grafana/pyroscope-rs/commit/54bb03f6996a74ad98cc75bdd08d790bce61efa3))
* auto-publish to crates.io after release-please PR merge ([#529](https://github.com/grafana/pyroscope-rs/issues/529)) ([a4f76b6](https://github.com/grafana/pyroscope-rs/commit/a4f76b6f9de6b6c35e4d947aba4dbfe49d8c019a))
* Potential fix for code scanning alert no. 14: Workflow does not contain permissions ([#542](https://github.com/grafana/pyroscope-rs/issues/542)) ([51ac4b7](https://github.com/grafana/pyroscope-rs/commit/51ac4b78e4a8cc0c8130984b52181fce12e3589f))

## [2.0.3](https://github.com/grafana/pyroscope-rs/compare/lib-2.0.2...lib-2.0.3) (2026-04-26)


### Bug Fixes

* **epoll:** use libc::c_long for itimerspec fields ([#532](https://github.com/grafana/pyroscope-rs/issues/532)) ([093406e](https://github.com/grafana/pyroscope-rs/commit/093406e667175e5bc28ca7a1c5efe62be6b9b4a0))


### Miscellaneous Chores

* **deps:** update googleapis/release-please-action action to v5 ([#530](https://github.com/grafana/pyroscope-rs/issues/530)) ([6c97b9d](https://github.com/grafana/pyroscope-rs/commit/6c97b9d414c7a1e26b07334a1cf0922e1c2cd2c9))

## [2.0.2](https://github.com/grafana/pyroscope-rs/compare/lib-2.0.1...lib-2.0.2) (2026-04-24)


### Miscellaneous Chores

* **deps:** bump rustls-webpki from 0.103.12 to 0.103.13 ([#524](https://github.com/grafana/pyroscope-rs/issues/524)) ([935384d](https://github.com/grafana/pyroscope-rs/commit/935384dc4d3479a1a12df2502898a8813b775884))
* **deps:** lock file maintenance ([#522](https://github.com/grafana/pyroscope-rs/issues/522)) ([4bd95bb](https://github.com/grafana/pyroscope-rs/commit/4bd95bb47f84f3aa6db6aa22173a7d74f774ab45))
* **deps:** update dependency ruby to v4.0.3 ([#523](https://github.com/grafana/pyroscope-rs/issues/523)) ([e126aa2](https://github.com/grafana/pyroscope-rs/commit/e126aa24517ba6efea62302e465ea736e4cef54c))
* **deps:** update rust:trixie docker digest to e4f09e8 ([#515](https://github.com/grafana/pyroscope-rs/issues/515)) ([f450327](https://github.com/grafana/pyroscope-rs/commit/f450327b1901f43d97d96eee5e0d73ccb64aa768))

## [2.0.1](https://github.com/grafana/pyroscope-rs/compare/lib-2.0.0...lib-2.0.1) (2026-04-19)


### Bug Fixes

* revert 0976d999e4a50c14459b8a0b39e72434be6d4bfc     ([#483](https://github.com/grafana/pyroscope-rs/issues/483)) ([ad3d032](https://github.com/grafana/pyroscope-rs/commit/ad3d03217a14a18868b8ba21f3dbf4ed844d759a))


### Miscellaneous Chores

* **deps:** lock file maintenance ([#467](https://github.com/grafana/pyroscope-rs/issues/467)) ([09411da](https://github.com/grafana/pyroscope-rs/commit/09411daca91120c4863983273863c55b02b4dd89))
* **deps:** lock file maintenance ([#478](https://github.com/grafana/pyroscope-rs/issues/478)) ([447d8ee](https://github.com/grafana/pyroscope-rs/commit/447d8eef3621be5b9b09b0b9eca3968bca081c2a))
* **deps:** lock file maintenance ([#498](https://github.com/grafana/pyroscope-rs/issues/498)) ([4072a04](https://github.com/grafana/pyroscope-rs/commit/4072a04626abd98aa18bc6cda37fb0ccc0803f59))
* **deps:** lock file maintenance ([#502](https://github.com/grafana/pyroscope-rs/issues/502)) ([bef3bc3](https://github.com/grafana/pyroscope-rs/commit/bef3bc3ec08b57b74d5d177204118492cdaa50b2))
* **deps:** pin dependencies ([#427](https://github.com/grafana/pyroscope-rs/issues/427)) ([a46830a](https://github.com/grafana/pyroscope-rs/commit/a46830ac9a75a7634d9cc1ecc514a21b5c0f0603))
* **deps:** pin rust docker tag to e8e2bb5 ([#501](https://github.com/grafana/pyroscope-rs/issues/501)) ([c177d5b](https://github.com/grafana/pyroscope-rs/commit/c177d5b8f28041d3956851d93e61be3aebc6d975))
* **deps:** update dtolnay/rust-toolchain digest to 3c5f7ea ([#480](https://github.com/grafana/pyroscope-rs/issues/480)) ([c4f2e7d](https://github.com/grafana/pyroscope-rs/commit/c4f2e7dd1b1234f8870ce0f858f92542aef37c2e))
* **deps:** update dtolnay/rust-toolchain digest to 3c5f7ea ([#500](https://github.com/grafana/pyroscope-rs/issues/500)) ([fc21bda](https://github.com/grafana/pyroscope-rs/commit/fc21bda959bdf5d5d5a44f559a2b9de8072d2fba))
* **deps:** update googleapis/release-please-action digest to 5c625bf ([#509](https://github.com/grafana/pyroscope-rs/issues/509)) ([c347e17](https://github.com/grafana/pyroscope-rs/commit/c347e1775487456b996b87ce4672a20d9b9f4541))
* **deps:** update rust crate env_logger to v0.11.10 ([#490](https://github.com/grafana/pyroscope-rs/issues/490)) ([24fb12d](https://github.com/grafana/pyroscope-rs/commit/24fb12d4c26c9cc167585f3fd93172015864a0e4))
* **deps:** update rust crate libc to v0.2.183 ([#489](https://github.com/grafana/pyroscope-rs/issues/489)) ([b44bac8](https://github.com/grafana/pyroscope-rs/commit/b44bac8f4274882d793e9e9f92ab0ad9fc4692f0))
* **deps:** update rust crate libc to v0.2.185 ([#514](https://github.com/grafana/pyroscope-rs/issues/514)) ([e72f5c6](https://github.com/grafana/pyroscope-rs/commit/e72f5c6e4944e7b2ac949d6dbd3201dc426704d5))
* **deps:** update rust crate libflate to v2.3.0 ([#516](https://github.com/grafana/pyroscope-rs/issues/516)) ([da2835f](https://github.com/grafana/pyroscope-rs/commit/da2835f536771e089635e9fd8b97a860b09b9479))
* **deps:** update rust crate rbspy to 0.43 ([#471](https://github.com/grafana/pyroscope-rs/issues/471)) ([1e524d3](https://github.com/grafana/pyroscope-rs/commit/1e524d38d84dfedc799515cb70ee434ef91a28e6))
* **deps:** update rust crate rbspy to 0.44 ([#477](https://github.com/grafana/pyroscope-rs/issues/477)) ([c7cfe39](https://github.com/grafana/pyroscope-rs/commit/c7cfe39fecd8a0624df5c5d22b709e73747ddb4d))
* **deps:** update rust crate uuid to v1.23.0 ([#481](https://github.com/grafana/pyroscope-rs/issues/481)) ([9c4b53a](https://github.com/grafana/pyroscope-rs/commit/9c4b53aef55b13dc6da99396119e33ec9dc02aa0))
* **deps:** update rust crate uuid to v1.23.1 ([#517](https://github.com/grafana/pyroscope-rs/issues/517)) ([6a89238](https://github.com/grafana/pyroscope-rs/commit/6a89238e7127cb5f3fa0f480ba7447369a21fc12))
* **deps:** update rust-lang/crates-io-auth-action action to v1.0.4 ([#479](https://github.com/grafana/pyroscope-rs/issues/479)) ([f4e08f3](https://github.com/grafana/pyroscope-rs/commit/f4e08f34aa2b14a1e417c005d37f75981940d675))
* **deps:** update rust:trixie docker digest to 652612f ([#510](https://github.com/grafana/pyroscope-rs/issues/510)) ([7f76541](https://github.com/grafana/pyroscope-rs/commit/7f76541afc2a03cd783b0a0627b7a876f18c4918))
* **deps:** update softprops/action-gh-release action to v2.5.2 ([#473](https://github.com/grafana/pyroscope-rs/issues/473)) ([1700be3](https://github.com/grafana/pyroscope-rs/commit/1700be3e34e7351a2f5f030187a2fac1ce1082e9))
* **deps:** update softprops/action-gh-release action to v2.5.3 ([#474](https://github.com/grafana/pyroscope-rs/issues/474)) ([12e34c1](https://github.com/grafana/pyroscope-rs/commit/12e34c109bd0e408dc0401430f4da4b5245a09da))
* **deps:** update softprops/action-gh-release action to v2.6.1 ([#475](https://github.com/grafana/pyroscope-rs/issues/475)) ([209c4c0](https://github.com/grafana/pyroscope-rs/commit/209c4c03ce08fe411046316dc333654fb64ab75b))
* remove FFI code and kindasafe (moved to separate repos) ([#488](https://github.com/grafana/pyroscope-rs/issues/488)) ([35a4c6a](https://github.com/grafana/pyroscope-rs/commit/35a4c6aa0808f0392e39e752d95a91536068ab74))


### Documentation

* fix broken Rust documentation and add doc-test CI ([#484](https://github.com/grafana/pyroscope-rs/issues/484)) ([98ae8a5](https://github.com/grafana/pyroscope-rs/commit/98ae8a595f18946f2edf27544ea6e2bb29724cbf))


### Continuous Integration

* add musl/Alpine to Rust test matrix ([#495](https://github.com/grafana/pyroscope-rs/issues/495)) ([a4b7ad4](https://github.com/grafana/pyroscope-rs/commit/a4b7ad4c9e530576adbbf278b104d65c970d43b0))
* add release-please configuration ([#491](https://github.com/grafana/pyroscope-rs/issues/491)) ([fbb2d8e](https://github.com/grafana/pyroscope-rs/commit/fbb2d8edc7cf400343650e77580464d32369d93d))

## v2.0.0

## Breaking Changes
- `Backend::report()` now returns `ReportBatch` instead of `Vec<Report>` ([#437](https://github.com/grafana/pyroscope-rs/pull/437), [#447](https://github.com/grafana/pyroscope-rs/pull/447))
- New `ReportData` enum replaces the plain `Vec<Report>` in `ReportBatch`, supporting both structured reports (`ReportData::Reports`) and pre-encoded pprof bytes (`ReportData::RawPprof`) ([#447](https://github.com/grafana/pyroscope-rs/pull/447))
- Minimum supported Rust version bumped from 1.64 to 1.66

## New Features
- **Jemalloc memory profiling backend** — new `backend-jemalloc` feature flag enables heap profiling via `jemalloc_pprof`. Use `pyroscope::backend::jemalloc::jemalloc_backend()` to get started ([#378](https://github.com/grafana/pyroscope-rs/pull/378))
- **`ReportBatch` type** — backends now return a `ReportBatch` with a `profile_type` field (e.g. `"process_cpu"`, `"memory"`), enabling multi-profile support ([#437](https://github.com/grafana/pyroscope-rs/pull/437))

## Dependencies
- Updated `pprof` (pprof-pyroscope-fork) to v0.1500.3 ([#407](https://github.com/grafana/pyroscope-rs/pull/407))
- Updated `object` crate to 0.38 ([#430](https://github.com/grafana/pyroscope-rs/pull/430))
- Disabled py-spy default features to exclude CLI dependencies ([#418](https://github.com/grafana/pyroscope-rs/pull/418))
- Added `jemalloc_pprof` 0.8 and `tokio` 1 as workspace dependencies

## v1.0.0
## Breaking Changes
- Removed `auth_token` from Python and Ruby FFI bindings and related code
- Removed `detect_subprocesses` from Python and Ruby configs
- Config constructor now requires app/spy identity and sample rate inputs
- Removed support for collapsed format
- Removed global tags from ruleset

## New Features
- Integrated pprof-rs backend into main crate behind optional `backend-pprof-rs` feature
- Switched to push API (from `/ingest` to `/push`)
- Generated push API protos
- Added `ThreadId` type
- Added `rustls-no-provider` TLS feature

## Bug Fixes / Improvements
- Unified signal logic
- Report cleanup functions
- Optimized ruleset
- Removed obscure thread id hash check
- Ruby: inline thread_id crate; remove detect_subprocess
- Dependency updates (reqwest 0.13, prost 0.14, thiserror 2.0, serde_json 1.0.115, uuid 1.20, libflate 2.1)

## v0.5.4
## New Features
- Add report transformation function which allows changing the report before
  sending it to the Pyroscope Server.
- Add Gzip support.

## Bug Fixes
- Use URL Builder. ([786c89b](https://github.com/pyroscope-io/pyroscope-rs/commit/786c89bb99839c45778410012a6da60267d395df))

## v0.5.3
## New Features
- Add BackendConfig to make reporting of pid, thread_id and thread_name
  optional.
- Backends can add a suffix to the "application_name"

## Bug Fixes
- **main**: fixed an obsecure bug when counting stacktraces ([Abid Omar](https://github.com/pyroscope-io/pyroscope-rs/commit/bdecaa13aeae3ce7d4c3d97f88bdd104ec35e7c5))

## v0.5.2
## New features
- Authentication Token support

## API Changes
- use rust-tls instead of openssl

## v0.5.1
## API Changes
- Backend shutdown bug
- Docs update

## v0.5.0
## API Changes
- New API for building, starting and stopping the profiling agent.
- Backend supports reporting multiple threads.
- Tagging within local thread-scope

## v0.4.0
## API Changes
- Backend now support passing a configuration struct.
- TimerSignal enum
- pprof-rs backend is split into a different package. It has to be imported manually.

## What's Changed
* fix: avoid binding two unrelated vars to the same type by @drahnr in https://github.com/pyroscope-io/pyroscope-rs/pull/18
* avoid almost all unwraps by @drahnr in https://github.com/pyroscope-io/pyroscope-rs/pull/14
* use more features of `thiserror` by @drahnr in https://github.com/pyroscope-io/pyroscope-rs/pull/11
* introduce LOG_TAGs, avoid repetitive prefixes by @drahnr in https://github.com/pyroscope-io/pyroscope-rs/pull/10
* allow configurable accumulation_cycle by @drahnr in https://github.com/pyroscope-io/pyroscope-rs/pull/21
* Add CI Targets by @omarabid in https://github.com/pyroscope-io/pyroscope-rs/pull/22
* 0.4.0 release by @omarabid in https://github.com/pyroscope-io/pyroscope-rs/pull/23

## New Contributors
* @drahnr made their first contribution in https://github.com/pyroscope-io/pyroscope-rs/pull/18

**Full Changelog**: https://github.com/pyroscope-io/pyroscope-rs/compare/0.3.1...lib-0.4.0

## v0.3.1
Minor release with bug fixes.

## Bug Fixes
- **session**: avoid breaking SessionManager if request fails ([Abid Omar](https://github.com/pyroscope-io/pyroscope-rs/commit/1254bcc9a3b0d76b7adbeb82ba21f33b875c0b39))
- **typo**: variable name typo ([Abid Omar](https://github.com/pyroscope-io/pyroscope-rs/commit/0d8caffbe7855bec8158333dba2923cd07286a5f))
- **pprof**: fix #12 ([Abid Omar](https://github.com/pyroscope-io/pyroscope-rs/commit/936d3d99a2cc82812bc8251fd2fbf8152a87d4cb))
- **copyright**: fix #13 ([Abid Omar](https://github.com/pyroscope-io/pyroscope-rs/commit/b8eaf13137810df932e1b70e33b3ad3e25b65ece))

## Code Refactoring
- **option**: replace unwrap for various Options ([Abid Omar](https://github.com/pyroscope-io/pyroscope-rs/commit/3fd4e794d74523855c66f65c0b7fc8ff9dfe4604))
- **time**: add get_time_range ([Abid Omar](https://github.com/pyroscope-io/pyroscope-rs/commit/a6d4dbcef494b2bfe8016a817201499937cf3528))

## v0.3.0
First stable release

## v0.0.2-alpha
Second beta release

## v0.0.1-alpha
Initial beta release
