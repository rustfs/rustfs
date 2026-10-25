#!/bin/sh
set -eu
repo_root=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT HUP INT TERM
mkdir "$scratch/certs"
# A public root from webpki-roots 1.0.9, not a private key or active trust anchor.
distrusted="$repo_root/scripts/fixtures/distrusted-emsign-g3.pem"
openssl req -x509 -newkey rsa:2048 -nodes -days 1 -subj /CN=ca-bundle-test \
    -keyout "$scratch/key.pem" -out "$scratch/valid.pem" >/dev/null 2>&1
cp "$scratch/valid.pem" "$scratch/bundle.pem"
check() {
    sh "$repo_root/scripts/check_ca_bundle.sh" "$scratch/bundle.pem" "$scratch/certs"
}
reject() {
    if check > "$scratch/output" 2>&1; then
        echo "Expected CA rejection: $1" >&2
        exit 1
    fi
}
check
cat "$distrusted" >> "$scratch/bundle.pem"
reject 'G3 after a valid certificate in the bundle'
cp "$scratch/valid.pem" "$scratch/bundle.pem"
cp "$distrusted" "$scratch/certs/renamed-root.pem"
reject 'G3 in directory with an unrelated name'
rm "$scratch/certs/renamed-root.pem"
ln -s "$distrusted" "$scratch/certs/12345678.0"
reject 'G3 through a hashed trust-directory symlink'
rm "$scratch/certs/12345678.0"
# Equivalent PEM encodings must not defeat DER-based detection.
printf '%s\n' '-----BEGIN CERTIFICATE-----' > "$scratch/bundle.pem"
sed '/-----/d' "$distrusted" | tr -d '\n' >> "$scratch/bundle.pem"
printf '\n%s\n' '-----END CERTIFICATE-----' >> "$scratch/bundle.pem"
reject 'G3 with unwrapped PEM body'
printf '%s\n' '-----BEGIN CERTIFICATE-----' > "$scratch/bundle.pem"
reject 'truncated bundle'
printf '%s\n' '-----BEGIN CERTIFICATE-----' '-----END CERTIFICATE-----' > "$scratch/bundle.pem"
reject 'empty PEM body'
: > "$scratch/bundle.pem"
reject 'empty bundle'
cp "$scratch/valid.pem" "$scratch/bundle.pem"
check
printf '%s\n' 'CA bundle regression checks passed'
