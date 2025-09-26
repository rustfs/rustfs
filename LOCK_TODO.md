# Lock Module TODO

- [x] Replace object batch operations in `SetDisks` with `BatchLockRequest` so write/delete flows acquire locks atomically.
- [x] Remove unused legacy namespace-lock helpers (`create_unique_clients`, unused commented LockGuard code) and ensure remaining call sites rely on FastLock directly.
