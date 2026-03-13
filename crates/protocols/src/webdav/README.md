# WebDAV Protocol Gateway for RustFS

WebDAV (Web Distributed Authoring and Versioning) protocol implementation for RustFS, providing HTTP-based file access compatible with native OS file managers and WebDAV clients.

## Features

- HTTP/HTTPS WebDAV server with Basic authentication
- Full CRUD operations mapping to S3 storage backend
- Directory (bucket/prefix) creation and deletion
- File upload, download, and deletion
- Property queries (PROPFIND) for metadata
- TLS support with multi-certificate SNI
- Integration with RustFS IAM for access control

### Supported WebDAV Methods

| Method | Description | S3 Operation |
|--------|-------------|--------------|
| `PROPFIND` | List directory / Get metadata | ListObjects / HeadObject |
| `MKCOL` | Create directory | CreateBucket / PutObject (prefix) |
| `PUT` | Upload file | PutObject |
| `GET` | Download file | GetObject |
| `DELETE` | Delete file/directory | DeleteObject / DeleteBucket |
| `HEAD` | Get file metadata | HeadObject |
| `MOVE` | Move/rename file | CopyObject + DeleteObject |
| `COPY` | Copy file | CopyObject |

## Enable Feature

**WebDAV is opt-in and must be explicitly enabled.**

Build with WebDAV support:

```bash
cargo build --features webdav
```

Or enable all protocol features:

```bash
cargo build --features full
```

## Configuration

Configure WebDAV via environment variables:

| Variable | Description | Default |
|----------|-------------|---------|
| `RUSTFS_WEBDAV_ENABLE` | Enable WebDAV server | `false` |
| `RUSTFS_WEBDAV_ADDRESS` | Server bind address | `0.0.0.0:8080` |
| `RUSTFS_WEBDAV_TLS_ENABLED` | Enable TLS | `true` |
| `RUSTFS_WEBDAV_CERTS_DIR` | TLS certificate directory | - |
| `RUSTFS_WEBDAV_CA_FILE` | CA file for client verification | - |
| `RUSTFS_WEBDAV_MAX_BODY_SIZE` | Max upload size (bytes) | 5GB |
| `RUSTFS_WEBDAV_REQUEST_TIMEOUT` | Request timeout (seconds) | 300 |

## Quick Start

### Start Server

```bash
RUSTFS_WEBDAV_ENABLE=true \
RUSTFS_WEBDAV_ADDRESS=0.0.0.0:8080 \
RUSTFS_WEBDAV_TLS_ENABLED=false \
RUSTFS_ACCESS_KEY=rustfsadmin \
RUSTFS_SECRET_KEY=rustfsadmin \
./target/release/rustfs /path/to/data
```

### Test with curl

```bash
# List root (buckets)
curl -u rustfsadmin:rustfsadmin -X PROPFIND http://127.0.0.1:8080/ -H "Depth: 1"

# Create bucket
curl -u rustfsadmin:rustfsadmin -X MKCOL http://127.0.0.1:8080/mybucket/

# Upload file
curl -u rustfsadmin:rustfsadmin -T file.txt http://127.0.0.1:8080/mybucket/file.txt

# Download file
curl -u rustfsadmin:rustfsadmin http://127.0.0.1:8080/mybucket/file.txt

# Create subdirectory
curl -u rustfsadmin:rustfsadmin -X MKCOL http://127.0.0.1:8080/mybucket/subdir/

# Delete file
curl -u rustfsadmin:rustfsadmin -X DELETE http://127.0.0.1:8080/mybucket/file.txt

# Delete bucket
curl -u rustfsadmin:rustfsadmin -X DELETE http://127.0.0.1:8080/mybucket/
```

## Client Configuration

### Linux (GNOME Files / Nautilus)

1. Open Files application
2. Press `Ctrl+L` to show address bar
3. Enter: `dav://rustfsadmin:rustfsadmin@127.0.0.1:8080/`

### macOS Finder

1. Open Finder
2. Press `Cmd+K` (Connect to Server)
3. Enter: `http://rustfsadmin:rustfsadmin@127.0.0.1:8080/`

### Windows Explorer

1. Open This PC
2. Click "Map network drive"
3. Enter: `http://127.0.0.1:8080/`
4. Enter credentials when prompted

### VSCode (WebDAV Extension)

Install `jonpfote.webdav` extension and create `.code-workspace`:

```json
{
    "folders": [
        {
            "uri": "webdav://rustfs-local",
            "name": "RustFS WebDAV"
        }
    ],
    "settings": {
        "jonpfote.webdav-folders": {
            "rustfs-local": {
                "host": "127.0.0.1:8080",
                "ssl": false,
                "authtype": "basic",
                "username": "rustfsadmin",
                "password": "rustfsadmin"
            }
        }
    }
}
```

## Architecture

```
WebDAV Client (curl, Finder, Explorer, VSCode)
    │
    ▼ HTTP/HTTPS
┌───────────────────┐
│   WebDavServer    │  ← Hyper HTTP server + Basic Auth
└─────────┬─────────┘
          │
┌─────────▼─────────┐
│   WebDavDriver    │  ← DavFileSystem implementation
└─────────┬─────────┘
          │
┌─────────▼─────────┐
│  StorageBackend   │  ← S3 API operations
└─────────┬─────────┘
          │
┌─────────▼─────────┐
│     ECStore       │  ← Erasure coded storage
└───────────────────┘
```

### Key Components

- **config.rs** - WebDAV server configuration
- **server.rs** - Hyper HTTP server with TLS and Basic authentication
- **driver.rs** - DavFileSystem trait implementation mapping to S3

### Path Mapping

WebDAV paths are mapped to S3 buckets and objects:

```
WebDAV Path                    S3 Mapping
/                         →    List all buckets
/mybucket/                →    Bucket: mybucket
/mybucket/file.txt        →    Bucket: mybucket, Key: file.txt
/mybucket/dir/file.txt    →    Bucket: mybucket, Key: dir/file.txt
```

## License

Apache License 2.0
