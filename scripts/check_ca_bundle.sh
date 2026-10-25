#!/bin/sh
# Reject the distrusted emSign ECC Root CA - G3 in native trust stores.
set -eu

bundle=${1:?Usage: check_ca_bundle.sh BUNDLE CERT_DIRECTORY}
cert_directory=${2:?Usage: check_ca_bundle.sh BUNDLE CERT_DIRECTORY}
distrusted_sha256=86a1ecba089c4a8d3bbe2734c612ba341d813e043cf9e8a862cd5c57a36bbe6b
scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT HUP INT TERM

check_file() {
    # Hash DER, not subject text or PEM formatting. Also inspect hashed links:
    # native trust consumers may use the directory rather than the bundle.
    awk '
        /-----BEGIN CERTIFICATE-----/ { if (inside) exit 1; inside=1; cert=""; next }
        /-----END CERTIFICATE-----/ { if (!inside) exit 1; print cert; inside=0; next }
        inside { gsub(/[[:space:]]/, ""); cert=cert $0 }
        END { if (inside) exit 1 }
    ' "$1" > "$scratch/certificates"
    while IFS= read -r encoded; do
        printf '%s' "$encoded" | base64 -d > "$scratch/certificate.der"
        if [ ! -s "$scratch/certificate.der" ]; then
            echo "Empty certificate in $1" >&2
            return 1
        fi
        digest=$(sha256sum "$scratch/certificate.der")
        digest=${digest%% *}
        if [ "$digest" = "$distrusted_sha256" ]; then
            echo "Distrusted emSign ECC Root CA - G3 found in $1" >&2
            return 1
        fi
    done < "$scratch/certificates"
}

check_file "$bundle"
if [ ! -s "$scratch/certificates" ]; then
    echo "CA bundle contains no certificates: $bundle" >&2
    exit 1
fi
find -L "$cert_directory" -type f > "$scratch/files"
while IFS= read -r file; do
    check_file "$file"
done < "$scratch/files"
