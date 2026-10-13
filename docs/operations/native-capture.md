# Native capture Raft runtime

Use this when enabling, forming, restarting, or shutting down the native capture Raft runtime in a RustFS process.

Native capture is disabled by default. Enable it with `--native-capture-config /path/to/capture.json` or `RUSTFS_NATIVE_CAPTURE_CONFIG`. The configuration is strict JSON, limited to 64 KiB. Unknown fields are rejected.

```json
{
  "identity": {
    "cluster_id": [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
    "node_id": 0
  },
  "database": "/var/lib/rustfs/capture.redb",
  "reader_socket": "/run/rustfs/capture.sock",
  "peers": {
    "0": {"rpc_uri": "http://127.0.0.1:9000"}
  },
  "capture_binding": {
    "revision": 1,
    "binding_id": [2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2],
    "target_id": [3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3]
  },
  "mode": "create",
  "bootstrap": true
}
```

Choose deployment-specific nonzero identifiers. IDs above are examples. Each node has its own database and reader socket, a distinct node ID, and the same cluster identity and capture binding. All peers must use fixed ports. The local peer must match the authority used by the existing RustFS internal RPC Ping handshake. The runtime uses the existing RPC secret; the capture file contains no credentials.

Prepare the database and socket parent directories as the account that runs RustFS. The database parent must exist. The reader socket parent must be owned by that account and have no group or other permission bits. The reader refuses any existing socket pathname; stop its owner before removing a stale path, and do not remove a path belonging to another live process.

```sh
install -d -m 700 /var/lib/rustfs /run/rustfs
chmod 600 /etc/rustfs/capture.json
```

Run these commands with the deployment's service account or provision the directories with that account's ownership. The example configuration's `database` and `reader_socket` must point into these prepared directories. A source reader client connects to `/run/rustfs/capture.sock`, not the HTTP API port. Supply `RUSTFS_RPC_SECRET` through the service manager's protected environment or secret injection before launch; do not put a secret in the JSON, command line, or shell history.

For the example first formation configuration (`mode: create`, `bootstrap: true`), start the real executable on the matching fixed API port:

```sh
rustfs server --address 127.0.0.1:9000 --native-capture-config /etc/rustfs/capture.json /var/lib/rustfs/objects
```

After normal shutdown, retain the database, edit that same file to `mode: recover` and `bootstrap: false`, and run the same command. Do not delete the database to make recovery succeed. On a three-node deployment, use each node's configured local endpoint and object volumes in its launch command; its capture peer authority must match the existing internal RPC identity.

For first formation, use `create` only when the database does not exist. Set `bootstrap` to `true` only on the designated formation node. For three nodes, give every file the same peer map with all three node IDs and their actual RustFS HTTP endpoints; change each file's local node ID and private paths. Other nodes start without bootstrap and receive real Raft membership and replication.

For restart, change `mode` to `recover` and `bootstrap` to `false`. Recovery requires the retained database and its exact identity and binding. A missing database or an identity mismatch is an error. Recovery does not rebuild a lost database or apply membership changes from configuration. Preserve the database when replacing a process. Do not replace an occupied reader socket pathname while its owner is running.

The existing HTTP listener serves authenticated native Raft RPCs. The Unix source reader shares the owner's durable store and supports read-only event-page queries. Shutdown stops admission, waits for actual RPC and owner work, and joins the reader task before releasing storage. Startup failures after the owner is created also require this cleanup.

This process integration does not automatically capture S3 writes. There is no external command-submission endpoint. Production object publication and automatic Created events require the separate object-publication integration. Reader queries prove access to durable native source state; they do not by themselves prove S3 capture.
