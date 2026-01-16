# Generate SSE encryption key (32 bytes base64 encoded)
# For testing with __RUSTFS_SSE_SIMPLE_CMK environment variable
# Usage: .\generate-sse-keys.ps1

# Function to generate a random 256-bit key and encode it in base64
function Generate-Base64Key {
    # Generate 32 bytes (256 bits) of random data
    $bytes = New-Object byte[] 32
    $rng = [System.Security.Cryptography.RandomNumberGenerator]::Create()
    $rng.GetBytes($bytes)
    $rng.Dispose()
    
    # Convert to base64
    return [Convert]::ToBase64String($bytes)
}

# Generate key
$base64Key = Generate-Base64Key

# Output result
Write-Host ""
Write-Host "Generated SSE encryption key (32 bytes, base64 encoded):" -ForegroundColor Green
Write-Host ""
Write-Host $base64Key -ForegroundColor Yellow
Write-Host ""
Write-Host "You can use this in your environment variable:" -ForegroundColor Cyan
Write-Host "`$env:__RUSTFS_SSE_SIMPLE_CMK=`"$base64Key`"" -ForegroundColor White
Write-Host ""
