# Check if static files need to be downloaded
if (-not (Test-Path .\rustfs\static\index.html)) {
    Write-Host "Downloading rustfs-console-latest.zip"

    Invoke-WebRequest -Uri "https://dl.rustfs.com/artifacts/console/rustfs-console-latest.zip" -OutFile 'tempfile.zip'
    Expand-Archive -Path 'tempfile.zip' -DestinationPath '.\rustfs\static' -Force
    Remove-Item tempfile.zip
}

# Check if build should be skipped
if (-not $env:SKIP_BUILD) {
    cargo build -p rustfs --bins
}

$current_dir = Get-Location

# Create multiple test directories
$testDirs = @("test0", "test1", "test2", "test3", "test4")
foreach ($dir in $testDirs) {
    $path = Join-Path -Path ".\target\volume" -ChildPath $dir
    if (-not (Test-Path $path)) {
        New-Item -ItemType Directory -Path $path -Force | Out-Null
    }
}

# Set environment variables
if (-not $env:RUST_LOG) {
    $env:RUST_BACKTRACE = 1
    $env:RUST_LOG = "rustfs=debug,ecstore=debug,s3s=debug,iam=debug"
}

# The following environment variables are commented out, uncomment them if needed
# $env:RUSTFS_ERASURE_SET_DRIVE_COUNT = 5
# $env:RUSTFS_STORAGE_CLASS_INLINE_BLOCK = "512 KB"

$env:RUSTFS_VOLUMES = ".\target\volume\test{0...4}"
# $env:RUSTFS_VOLUMES = ".\target\volume\test"
$env:RUSTFS_ADDRESS = "127.0.0.1:9000"
$env:RUSTFS_CONSOLE_ENABLE = "true"
$env:RUSTFS_CONSOLE_ADDRESS = "127.0.0.1:9002"
# $env:RUSTFS_SERVER_DOMAINS = "localhost:9000"
# Change to the actual configuration file path, obs.example.toml is for reference only
$env:RUSTFS_OBS_CONFIG = ".\config\obs.example.toml"

# Check command line arguments
if ($args.Count -gt 0) {
    $env:RUSTFS_VOLUMES = $args[0]
}

# Run the program
cargo run --bin rustfs