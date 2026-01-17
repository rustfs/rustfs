# Local KMS Key Decryption Tool

This directory contains Python scripts to decrypt RustFS Local KMS key files when you know the master key.

## Files

- `decrypt_local_kms_key.py` - Main decryption script
- `decrypt_local_kms_key_test.py` - Test suite and examples
- `decrypt_local_kms_key_README.md` - This file

## Requirements

Install the required Python package:

```bash
pip install cryptography
```

## Usage

### Basic Usage

```bash
python scripts/decrypt_local_kms_key.py <key_file_path> <master_key>
```

### Example

```bash
# Decrypt a key file with master key "my-secret-key"
python scripts/decrypt_local_kms_key.py target/kms-key-dir/test-key.key "my-secret-key"
```

use uv

```
uv run --with cryptography python scripts\decrypt_local_kms_key.py .\target\kms-key-dir\rustfs-master-key.key "my-secret-key"
```

### Output

The script will output:
- Key metadata (ID, algorithm, version, status, etc.)
- Decrypted key material in both hex and base64 formats
- Key length in bytes

Example output:
```
======================================================================
Successfully Decrypted Key File
======================================================================
Key ID:          test-key-001
Algorithm:       AES_256
Version:         1
Status:          Active
Usage:           EncryptDecrypt
Created At:      2024-01-17T10:00:00Z
Created By:      local-kms
Description:     Test key for encryption
Key Length:      32 bytes

Decrypted Key Material:
  Hex:           a1b2c3d4e5f6...
  Base64:        obPD1OX2...
======================================================================

⚠️  WARNING: The decrypted key material above is sensitive!
    Do not share it or store it in plain text.
```

## Testing

Run the test suite to verify the implementation:

```bash
python scripts/decrypt_local_kms_key_test.py
```

This will:
1. Test key derivation logic
2. Test encryption/decryption cycle
3. Show example key file format

## How It Works

### 1. Key Derivation

The script derives a 256-bit AES key from the master key string using:

```python
SHA256(master_key_string + "rustfs-kms-local")
```

This matches the Rust implementation in `crates/kms/src/backends/local.rs`:

```rust
let mut hasher = Sha256::new();
hasher.update(master_key.as_bytes());
hasher.update(b"rustfs-kms-local"); // Salt
let hash = hasher.finalize();
```

### 2. Decryption Process

1. Read the `.key` file (JSON format)
2. Extract `encrypted_key_material` (base64 string) and `nonce` (12 bytes)
3. Decode the base64 encrypted data
4. Decrypt using AES-256-GCM with the derived key and nonce
5. Return the plaintext key material

### 3. Key File Format

The `.key` files are JSON with the following structure:

```json
{
  "key_id": "test-key",
  "version": 1,
  "algorithm": "AES_256",
  "usage": "EncryptDecrypt",
  "status": "Active",
  "description": "Description",
  "metadata": {},
  "created_at": "2024-01-17T10:00:00Z",
  "rotated_at": null,
  "created_by": "local-kms",
  "encrypted_key_material": "base64_encoded_encrypted_bytes",
  "nonce": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
}
```

## Security Notes

⚠️ **Important Security Considerations:**

1. **Protect the Master Key**: The master key is the root of trust. Anyone with the master key can decrypt all keys.

2. **Sensitive Output**: The decrypted key material is highly sensitive. Do not:
   - Store it in plain text
   - Share it over insecure channels
   - Log it to files
   - Commit it to version control

3. **Use Cases**: This script is intended for:
   - Debugging and troubleshooting
   - Key recovery in emergencies
   - Migration to other KMS systems
   - Forensic analysis

4. **Production Use**: In production, keys should only be decrypted by the RustFS KMS service itself.

## Troubleshooting

### "Decryption failed. Wrong master key or corrupted file"

This error occurs when:
- The master key is incorrect
- The key file is corrupted
- The key file format doesn't match expectations

Verify that you're using the same master key that was configured when the key was created.

### "Invalid nonce length"

The nonce must be exactly 12 bytes for AES-GCM. If you see this error, the key file may be corrupted.

### "Missing required fields"

The key file must contain:
- `encrypted_key_material`
- `nonce`
- `key_id`
- `algorithm`

Check that the file is a valid RustFS Local KMS key file.

## Related Configuration

The master key is configured in RustFS via:

- Environment variable: `RUSTFS_KMS_LOCAL_MASTER_KEY`
- Config file: `kms.backend_config.master_key`

Example `.env`:
```bash
RUSTFS_KMS_BACKEND=local
RUSTFS_KMS_LOCAL_KEY_DIR=/path/to/key/dir
RUSTFS_KMS_LOCAL_MASTER_KEY=your-master-key-here
```

## License

Copyright 2024 RustFS Team

Licensed under the Apache License, Version 2.0.

