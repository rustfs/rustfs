#!/usr/bin/env python3
"""
Decrypt RustFS Local KMS Key File

This script decrypts a local KMS key file when you know the master key.
It replicates the key derivation and decryption logic from the Rust implementation.

Usage:
    python decrypt_local_kms_key.py <key_file_path> <master_key>

Example:
    python decrypt_local_kms_key.py target/kms-key-dir/test-key.key "my-master-key"

Requirements:
    pip install cryptography
"""

import sys
import json
import base64
import hashlib
from pathlib import Path
from cryptography.hazmat.primitives.ciphers.aead import AESGCM


def derive_master_key(master_key_str: str) -> bytes:
    """
    Derive a 256-bit AES key from the master key string.
    
    This replicates the Rust implementation:
    - SHA-256 hash of: master_key + salt("rustfs-kms-local")
    - Returns 32 bytes for AES-256
    
    Args:
        master_key_str: The master key string
        
    Returns:
        32-byte key for AES-256-GCM
    """
    hasher = hashlib.sha256()
    hasher.update(master_key_str.encode('utf-8'))
    hasher.update(b'rustfs-kms-local')  # Salt to prevent rainbow tables
    return hasher.digest()  # Returns 32 bytes


def decrypt_key_file(key_file_path: str, master_key: str) -> dict:
    """
    Decrypt a local KMS key file.
    
    Args:
        key_file_path: Path to the .key file
        master_key: The master key string used for encryption
        
    Returns:
        Dictionary containing the decrypted key material and metadata
        
    Raises:
        FileNotFoundError: If key file doesn't exist
        json.JSONDecodeError: If key file is not valid JSON
        ValueError: If decryption fails or file format is invalid
    """
    # Read the key file
    key_path = Path(key_file_path)
    if not key_path.exists():
        raise FileNotFoundError(f"Key file not found: {key_file_path}")
    
    with open(key_path, 'r', encoding='utf-8') as f:
        stored_key = json.load(f)
    
    # Validate required fields
    required_fields = ['encrypted_key_material', 'nonce', 'key_id', 'algorithm']
    missing_fields = [field for field in required_fields if field not in stored_key]
    if missing_fields:
        raise ValueError(f"Missing required fields: {missing_fields}")
    
    # Extract encrypted data
    encrypted_key_material_b64 = stored_key['encrypted_key_material']
    nonce = bytes(stored_key['nonce'])
    
    # Validate nonce length (must be 12 bytes for AES-GCM)
    if len(nonce) != 12:
        raise ValueError(f"Invalid nonce length: {len(nonce)}, expected 12 bytes")
    
    # Derive the AES key from master key
    aes_key = derive_master_key(master_key)
    
    # Decode the base64 encrypted key material
    try:
        encrypted_bytes = base64.b64decode(encrypted_key_material_b64)
    except Exception as e:
        raise ValueError(f"Failed to decode base64 encrypted key material: {e}")
    
    # Decrypt using AES-256-GCM
    try:
        aesgcm = AESGCM(aes_key)
        plaintext_key_material = aesgcm.decrypt(nonce, encrypted_bytes, None)
    except Exception as e:
        raise ValueError(f"Decryption failed. Wrong master key or corrupted file: {e}")
    
    # Return decrypted data with metadata
    return {
        'key_id': stored_key['key_id'],
        'algorithm': stored_key['algorithm'],
        'version': stored_key.get('version', 1),
        'status': stored_key.get('status'),
        'usage': stored_key.get('usage'),
        'created_at': stored_key.get('created_at'),
        'created_by': stored_key.get('created_by'),
        'description': stored_key.get('description'),
        'key_material_hex': plaintext_key_material.hex(),
        'key_material_base64': base64.b64encode(plaintext_key_material).decode('ascii'),
        'key_material_length': len(plaintext_key_material),
    }


def main():
    if len(sys.argv) != 3:
        print("Usage: python decrypt_local_kms_key.py <key_file_path> <master_key>")
        print()
        print("Example:")
        print('  python decrypt_local_kms_key.py target/kms-key-dir/test-key.key "my-master-key"')
        sys.exit(1)
    
    key_file_path = sys.argv[1]
    master_key = sys.argv[2]
    
    try:
        result = decrypt_key_file(key_file_path, master_key)
        
        print("=" * 70)
        print("Successfully Decrypted Key File")
        print("=" * 70)
        print(f"Key ID:          {result['key_id']}")
        print(f"Algorithm:       {result['algorithm']}")
        print(f"Version:         {result['version']}")
        print(f"Status:          {result['status']}")
        print(f"Usage:           {result['usage']}")
        print(f"Created At:      {result['created_at']}")
        print(f"Created By:      {result['created_by']}")
        print(f"Description:     {result['description']}")
        print(f"Key Length:      {result['key_material_length']} bytes")
        print()
        print("Decrypted Key Material:")
        print(f"  Hex:           {result['key_material_hex']}")
        print(f"  Base64:        {result['key_material_base64']}")
        print("=" * 70)
        
        # Security warning
        print()
        print("⚠️  WARNING: The decrypted key material above is sensitive!")
        print("    Do not share it or store it in plain text.")
        
    except FileNotFoundError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON format in key file: {e}", file=sys.stderr)
        sys.exit(1)
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()

