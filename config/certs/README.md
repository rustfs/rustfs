## Certs

### Generate a self-signed certificate

```bash
openssl req -x509 -newkey rsa:2048 -keyout key.pem -out cert.pem -days 365 -nodes
```

### Generate a self-signed certificate with a specific subject

```bash
openssl req -x509 -newkey rsa:2048 -keyout key.pem -out cert.pem -days 365 -nodes \
  -subj "/C=US/ST=California/L=San Francisco/O=My Company/CN=mydomain.com"
```

### Generate a self-signed certificate with a specific subject and SAN

```bash
openssl req -x509 -newkey rsa:2048 -keyout key.pem -out cert.pem -days 365 -nodes \
  -subj "/C=US/ST=California/L=San Francisco/O=My Company/CN=mydomain.com" \
  -addext "subjectAltName=DNS:mydomain.com,DNS:www.mydomain.com"
```

### Generate a self-signed certificate with a specific subject and SAN (multiple SANs)

```bash
openssl req -x509 -newkey rsa:2048 -keyout key.pem -out cert.pem -days 365 -nodes \
  -subj "/C=US/ST=California/L=San Francisco/O=My Company/CN=mydomain.com" \
  -addext "subjectAltName=DNS:mydomain.com,DNS:www.mydomain.com,DNS:api.mydomain.com"
```

### TLS File

```text

 rustfs_tls_cert.pem api cert.pem

 rustfs_tls_key.pem api key.pem

 rustfs_console_tls_cert.pem console cert.pem

 rustfs_console_tls_key.pem console key.pem

```