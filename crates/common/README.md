# rustfs-common

[![Crates.io](https://img.shields.io/crates/v/rustfs-common.svg)](https://crates.io/crates/rustfs-common)
[![Docs.rs](https://docs.rs/rustfs-common/badge.svg)](https://docs.rs/rustfs-common)
[![License](https://img.shields.io/crates/l/rustfs-common.svg)](../../LICENSE)

`rustfs-common` is a foundational crate for the RustFS project. It provides a collection of shared utilities, data
structures, and core functionalities used across various components of the RustFS ecosystem.

## Purpose

The primary goal of this crate is to centralize common code, reduce duplication, and ensure consistency. It includes
essential tools for:

* **Shared Data Structures:** Common types and models used by different services.
* **Core Utilities:** Helper functions for asynchronous operations, networking, and configuration.
* **Global State:** Provides mechanisms for managing shared application state.

## Installation

This crate is a core part of the RustFS workspace. To use it in another crate within the project, add it to your
`Cargo.toml`:

```toml
[dependencies]
rustfs-common = { version = "0.0.3" } # Adjust version/path as needed
```

## Usage

`rustfs-common` is intended for internal use by other crates within the RustFS project. It is not designed for
standalone use. Other components depend on it to access shared logic and data types.

**Example (Conceptual):**

```rust

```

## License

This project is licensed under the Apache License, Version 2.0. See the [`LICENSE`](../../LICENSE) file for details.