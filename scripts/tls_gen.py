# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "cryptography>=45.0.0",
# ]
# ///

from __future__ import annotations

import argparse
import ipaddress
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID


DEFAULT_OUT_DIR = Path("target/tls")
SERVER_DNS_NAMES = ("localhost",)
SERVER_IP_ADDRESSES = ("127.0.0.1", "::1")
CLIENT_DNS_NAMES = ("rustfs-test-client",)
OUTPUT_FILES = (
    "rustfs_cert.pem",
    "rustfs_key.pem",
    "ca.crt",
    "public.crt",
    "client_ca.crt",
    "client_cert.pem",
    "client_key.pem",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate a full RustFS TLS bundle for local TLS and mTLS tests.",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=DEFAULT_OUT_DIR,
        help=f"output directory for generated PEM files (default: {DEFAULT_OUT_DIR})",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=365,
        help="certificate validity in days (default: 365)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="overwrite existing files in the output directory",
    )
    return parser.parse_args()


def build_subject(common_name: str) -> x509.Name:
    return x509.Name(
        [
            x509.NameAttribute(NameOID.COUNTRY_NAME, "US"),
            x509.NameAttribute(NameOID.ORGANIZATION_NAME, "RustFS"),
            x509.NameAttribute(NameOID.COMMON_NAME, common_name),
        ]
    )


def now_utc() -> datetime:
    return datetime.now(UTC)


def generate_private_key() -> rsa.RSAPrivateKey:
    return rsa.generate_private_key(public_exponent=65537, key_size=2048)


def build_ca_certificate(ca_key: rsa.RSAPrivateKey, days: int) -> x509.Certificate:
    subject = build_subject("RustFS Test CA")
    issued_at = now_utc() - timedelta(minutes=5)
    return (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(subject)
        .public_key(ca_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(issued_at)
        .not_valid_after(issued_at + timedelta(days=days))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .add_extension(
            x509.KeyUsage(
                digital_signature=False,
                content_commitment=False,
                key_encipherment=False,
                data_encipherment=False,
                key_agreement=False,
                key_cert_sign=True,
                crl_sign=True,
                encipher_only=False,
                decipher_only=False,
            ),
            critical=True,
        )
        .add_extension(x509.SubjectKeyIdentifier.from_public_key(ca_key.public_key()), critical=False)
        .sign(private_key=ca_key, algorithm=hashes.SHA256())
    )


def build_leaf_certificate(
    *,
    common_name: str,
    dns_names: tuple[str, ...],
    ip_addresses: tuple[str, ...],
    eku: x509.ObjectIdentifier,
    ca_cert: x509.Certificate,
    ca_key: rsa.RSAPrivateKey,
    leaf_key: rsa.RSAPrivateKey,
    days: int,
) -> x509.Certificate:
    issued_at = now_utc() - timedelta(minutes=5)
    san_entries: list[x509.GeneralName] = [x509.DNSName(name) for name in dns_names]
    san_entries.extend(x509.IPAddress(ipaddress.ip_address(ip)) for ip in ip_addresses)

    return (
        x509.CertificateBuilder()
        .subject_name(build_subject(common_name))
        .issuer_name(ca_cert.subject)
        .public_key(leaf_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(issued_at)
        .not_valid_after(issued_at + timedelta(days=days))
        .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
        .add_extension(
            x509.KeyUsage(
                digital_signature=True,
                content_commitment=False,
                key_encipherment=True,
                data_encipherment=False,
                key_agreement=False,
                key_cert_sign=False,
                crl_sign=False,
                encipher_only=False,
                decipher_only=False,
            ),
            critical=True,
        )
        .add_extension(x509.ExtendedKeyUsage([eku]), critical=False)
        .add_extension(x509.SubjectAlternativeName(san_entries), critical=False)
        .add_extension(x509.SubjectKeyIdentifier.from_public_key(leaf_key.public_key()), critical=False)
        .add_extension(x509.AuthorityKeyIdentifier.from_issuer_public_key(ca_key.public_key()), critical=False)
        .sign(private_key=ca_key, algorithm=hashes.SHA256())
    )


def cert_to_pem(cert: x509.Certificate) -> bytes:
    return cert.public_bytes(serialization.Encoding.PEM)


def key_to_pem(key: rsa.RSAPrivateKey) -> bytes:
    return key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


def ensure_writable(out_dir: Path, force: bool) -> None:
    existing = [out_dir / name for name in OUTPUT_FILES if (out_dir / name).exists()]
    if existing and not force:
        existing_list = ", ".join(path.name for path in existing)
        raise SystemExit(
            f"Refusing to overwrite existing files in {out_dir}: {existing_list}. "
            "Re-run with --force to replace them."
        )


def write_bundle(out_dir: Path, force: bool, days: int) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    ensure_writable(out_dir, force)

    ca_key = generate_private_key()
    ca_cert = build_ca_certificate(ca_key, days)

    server_key = generate_private_key()
    server_cert = build_leaf_certificate(
        common_name="localhost",
        dns_names=SERVER_DNS_NAMES,
        ip_addresses=SERVER_IP_ADDRESSES,
        eku=ExtendedKeyUsageOID.SERVER_AUTH,
        ca_cert=ca_cert,
        ca_key=ca_key,
        leaf_key=server_key,
        days=days,
    )

    client_key = generate_private_key()
    client_cert = build_leaf_certificate(
        common_name="rustfs-test-client",
        dns_names=CLIENT_DNS_NAMES,
        ip_addresses=(),
        eku=ExtendedKeyUsageOID.CLIENT_AUTH,
        ca_cert=ca_cert,
        ca_key=ca_key,
        leaf_key=client_key,
        days=days,
    )

    bundle = {
        "rustfs_cert.pem": cert_to_pem(server_cert),
        "rustfs_key.pem": key_to_pem(server_key),
        "ca.crt": cert_to_pem(ca_cert),
        "public.crt": cert_to_pem(ca_cert),
        "client_ca.crt": cert_to_pem(ca_cert),
        "client_cert.pem": cert_to_pem(client_cert),
        "client_key.pem": key_to_pem(client_key),
    }

    for name, content in bundle.items():
        (out_dir / name).write_bytes(content)


def main() -> int:
    args = parse_args()
    if args.days <= 0:
        print("--days must be a positive integer", file=sys.stderr)
        return 2

    try:
        write_bundle(args.out_dir, args.force, args.days)
    except SystemExit as exc:
        print(str(exc), file=sys.stderr)
        return 1

    print(f"Generated RustFS TLS bundle in {args.out_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
