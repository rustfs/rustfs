@echo off
rem filepath: run.bat

if not defined SKIP_BUILD (
    cargo build -p rustfs --bins
)

set current_dir=%cd%

if not exist .\target\volume\test mkdir .\target\volume\test

if not defined RUST_LOG (
    set RUST_BACKTRACE=1
    set RUST_LOG=rustfs=debug,ecstore=debug,s3s=debug,iam=debug
)

rem set RUSTFS_ERASURE_SET_DRIVE_COUNT=5

rem set RUSTFS_STORAGE_CLASS_INLINE_BLOCK=512 KB

rem set RUSTFS_VOLUMES=.\target\volume\test{0...4}
set RUSTFS_VOLUMES=.\target\volume\test
set RUSTFS_ADDRESS=0.0.0.0:9000
set RUSTFS_CONSOLE_ENABLE=true
set RUSTFS_CONSOLE_ADDRESS=0.0.0.0:9002
rem set RUSTFS_SERVER_DOMAINS=localhost:9000

if not "%~1"=="" (
    set RUSTFS_VOLUMES=%~1
)

cargo run --bin rustfs