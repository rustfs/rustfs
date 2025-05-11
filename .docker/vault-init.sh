#!/bin/sh
# vault-init.sh - Initialize Vault for RustFS SSE-KMS

# Wait for Vault to start
until curl -s http://127.0.0.1:8200/v1/sys/health | grep "initialized" > /dev/null; do
    echo "Waiting for Vault to start..."
    sleep 1
done

# Set the Vault token
export VAULT_TOKEN="$VAULT_DEV_ROOT_TOKEN_ID"
export VAULT_ADDR="http://127.0.0.1:8200"

echo "Vault is running and initialized"

# Enable the Transit secrets engine (for encryption operations)
vault secrets enable transit
echo "Transit secrets engine enabled"

# Create a key for RustFS encryption
vault write -f transit/keys/rustfs-encryption-key
echo "Created rustfs-encryption-key"

# Create another key for RustFS with rotation capability
vault write -f transit/keys/rustfs-rotating-key
echo "Created rustfs-rotating-key"

# Set up key rotation policy
vault write transit/keys/rustfs-rotating-key/config auto_rotate_period="30d"
echo "Set up auto rotation for rustfs-rotating-key"

# Create a policy for RustFS to access these keys
cat > /tmp/rustfs-policy.hcl << EOF
# Policy for RustFS encryption operations
path "transit/encrypt/rustfs-encryption-key" {
  capabilities = ["create", "update"]
}

path "transit/decrypt/rustfs-encryption-key" {
  capabilities = ["create", "update"]
}

path "transit/encrypt/rustfs-rotating-key" {
  capabilities = ["create", "update"]
}

path "transit/decrypt/rustfs-rotating-key" {
  capabilities = ["create", "update"]
}
EOF

# Create the policy
vault policy write rustfs-encryption-policy /tmp/rustfs-policy.hcl
echo "Created rustfs-encryption-policy"

# Create a token for RustFS to use
RUSTFS_TOKEN=$(vault token create -policy=rustfs-encryption-policy -field=token)
echo "Created token for RustFS: $RUSTFS_TOKEN"

# Store the token for RustFS to use
echo "RUSTFS_KMS_VAULT_TOKEN=$RUSTFS_TOKEN" > /vault/config/rustfs-kms.env
echo "RUSTFS_KMS_VAULT_ENDPOINT=http://rustyvault:8200" >> /vault/config/rustfs-kms.env
echo "RUSTFS_KMS_VAULT_KEY_NAME=rustfs-encryption-key" >> /vault/config/rustfs-kms.env

echo "RustFS KMS configuration has been created"
echo "============================================"
echo "Vault is ready for use with RustFS SSE-KMS"
echo "============================================"

# Keep the container running
tail -f /dev/null