# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## 0.7.0 (UNRELEASED)
### Changed
- Edition changed to 2024 and MSRV bumped to 1.85 ([#228])
- Relax MSRV policy and allow MSRV bumps in patch releases
- Update to `universal-hash` v0.6 ([#213])

[#213]: https://github.com/RustCrypto/universal-hashes/pull/213
[#228]: https://github.com/RustCrypto/universal-hashes/pull/228

## 0.6.2 (2022-03-03)
### Added
- add `new_with_init_block` ([#195])

### Changed 
- implement Karatsuba multiplication for arm64 ([#181])

[#195]: https://github.com/RustCrypto/universal-hashes/pull/195
[#181]: https://github.com/RustCrypto/universal-hashes/pull/181

## 0.6.1 (2023-06-16)
### Added
- Support for `polyval_armv8` on Rust 1.61+ ([#179])

[#179]: https://github.com/RustCrypto/universal-hashes/pull/179

## 0.6.0 (2022-07-31)
### Added
- Impl `Reset` ([#157])

### Changed
- Remove `sse4.1` from CPU feature requirements for PCLMUL backend ([#143])
- Relax `zeroize` constraints ([#147])
- Upgrade to Rust 2021 edition ([#147])
- Use stable `aarch64_target_feature` ([#154])
- Replace `armv8`/`force-soft` features with `cfg` attributes ([#159])
- Bump `universal-hash` to v0.5 ([#155], [#162])

[#143]: https://github.com/RustCrypto/universal-hashes/pull/143
[#147]: https://github.com/RustCrypto/universal-hashes/pull/147
[#154]: https://github.com/RustCrypto/universal-hashes/pull/154
[#155]: https://github.com/RustCrypto/universal-hashes/pull/155
[#157]: https://github.com/RustCrypto/universal-hashes/pull/157
[#159]: https://github.com/RustCrypto/universal-hashes/pull/159
[#162]: https://github.com/RustCrypto/universal-hashes/pull/162

## 0.5.3 (2021-08-27)
### Changed
- Bump `cpufeatures` dependency to v0.2 ([#136], [#138])
- Remove use of ARMv8 `crypto` feature ([#137])

[#136]: https://github.com/RustCrypto/universal-hashes/pull/136
[#137]: https://github.com/RustCrypto/universal-hashes/pull/137
[#138]: https://github.com/RustCrypto/universal-hashes/pull/138

## 0.5.2 (2021-07-20)
### Changed
- Pin `zeroize` dependency to v1.3 ([#134])

[#134]: https://github.com/RustCrypto/universal-hashes/pull/134

## 0.5.1 (2021-05-31)
### Added
- Nightly-only ARMv8 intrinsics support gated under the `armv8` feature ([#126])

[#126]: https://github.com/RustCrypto/universal-hashes/pull/126

## 0.5.0 (2021-04-29)
### Changed
- Use `ManuallyDrop` unions; MSRV 1.49+ ([#113], [#114])
- Replace `cpuid-bool` with `cpufeatures` ([#116])

### Removed
- `mulx` feature: now always built-in ([#118])

[#113]: https://github.com/RustCrypto/universal-hashes/pull/113
[#114]: https://github.com/RustCrypto/universal-hashes/pull/114
[#116]: https://github.com/RustCrypto/universal-hashes/pull/116
[#118]: https://github.com/RustCrypto/universal-hashes/pull/118

## 0.4.5 (2020-12-26)
### Changed
- Use `u128` to impl `mulx` ([#111])

[#111]: https://github.com/RustCrypto/universal-hashes/pull/111

## 0.4.4 (2020-12-26)
### Added
- `Debug` impl using `opaque-debug` ([#105])
- `mulx` feature ([#107])

[#105]: https://github.com/RustCrypto/universal-hashes/pull/105
[#107]: https://github.com/RustCrypto/universal-hashes/pull/107

## 0.4.3 (2020-12-08)
### Added
- CLMUL detection ([#92])

[#92]: https://github.com/RustCrypto/universal-hashes/pull/92

## 0.4.2 (2020-11-25)
### Added
- `KEY_SIZE` constant ([#82])

### Changed
- Bump `cfg-if` from v0.1 to v1.0.0 ([#86])

[#86]: https://github.com/RustCrypto/universal-hashes/pull/86
[#82]: https://github.com/RustCrypto/universal-hashes/pull/82

## 0.4.1 (2020-09-26)
### Changed
- Performance improvements ([#75])

[#75]: https://github.com/RustCrypto/universal-hashes/pull/75

## 0.4.0 (2020-06-06)
### Changed
- Bump `universal-hash` dependency to v0.4; MSRV 1.41 ([#52], [#57])
- Rename `result` methods to to `finalize` ([#56])

[#57]: https://github.com/RustCrypto/universal-hashes/pull/57
[#56]: https://github.com/RustCrypto/universal-hashes/pull/56
[#52]: https://github.com/RustCrypto/universal-hashes/pull/52

## 0.3.3 (2019-12-21)
### Changed
- Match ideal assembly implementation on x86/x86_64 ([#43], [#44])

[#43]: https://github.com/RustCrypto/universal-hashes/pull/43
[#44]: https://github.com/RustCrypto/universal-hashes/pull/44

## 0.3.2 (2019-12-05)
### Added
- Constant-time 32-bit software implementation ([#39])

### Changed
- Use `cfg-if` crate to reduce duplication ([#40])

[#39]: https://github.com/RustCrypto/universal-hashes/pull/39
[#40]: https://github.com/RustCrypto/universal-hashes/pull/40

## 0.3.1 (2019-11-14)
### Changed
- Upgrade to `zeroize` 1.0 ([#33])

[#33]: https://github.com/RustCrypto/universal-hashes/pull/33

## 0.3.0 (2019-10-05)
### Removed
- Remove `pub` from `field` module ([#28])

[#28]: https://github.com/RustCrypto/universal-hashes/pull/28

## 0.2.0 (2019-10-04)
### Changed
- Upgrade to `universal-hash` crate v0.3 ([#22])

[#22]: https://github.com/RustCrypto/universal-hashes/pull/22

## 0.1.1 (2019-10-01)
### Changed
- Upgrade to `zeroize` v1.0.0-pre ([#19])

[#19]: https://github.com/RustCrypto/universal-hashes/pull/19

## 0.1.0 (2019-09-19)
### Added
- Constant time software implementation ([#7])

### Changed
- Update to Rust 2018 edition ([#3])
- Use `UniversalHash` trait ([#6])
- Removed generics/traits from `field::Element` API ([#12])

### Removed
- `insecure-soft` cargo feature ([#7])

[#3]: https://github.com/RustCrypto/universal-hashes/pull/3
[#6]: https://github.com/RustCrypto/universal-hashes/pull/6
[#7]: https://github.com/RustCrypto/universal-hashes/pull/7
[#12]: https://github.com/RustCrypto/universal-hashes/pull/12

## 0.0.1 (2019-08-26)

- Initial release
