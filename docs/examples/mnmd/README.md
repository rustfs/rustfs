# MNMD on Docker (4 nodes x 4 drives)

This folder contains a ready-to-use docker-compose.yml for a 4x4 MNMD deployment.

Highlights
- RUSTFS_VOLUMES uses 1..4 to match /data/rustfs1..4
- Uses Docker service names (rustfs-node1..4) — no hard-coded IPs
- Only node1 exposes 9000 externally
- Startup order via healthcheck + depends_on
- Optional wait-and-start.sh to gate app startup after container boot

Quick start
1) Prepare host directories (XFS):
   - Create /mnt/rustfs{1..4}_data{1..4}
   - Ensure each is mounted as XFS
   - Ensure the container user can write to them

2) Launch:
   - docker compose up -d

3) Verify:
   - docker ps (all 4 nodes up and healthy)
   - docker logs -f rustfs-node1
   - Access http://localhost:9000 (credentials as in compose)

Healthcheck fallback
If the image lacks `nc`, switch to:
- curl: `curl -fsS http://127.0.0.1:9000/ || exit 1`
- wget: `wget -qO- http://127.0.0.1:9000/ >/dev/null || exit 1`

Brace expansion fallback
If the server does not accept the {1...4} form in RUSTFS_VOLUMES, list all endpoints explicitly, e.g.:

```
RUSTFS_VOLUMES=http://rustfs-node1:9000/data/rustfs1 http://rustfs-node1:9000/data/rustfs2 http://rustfs-node1:9000/data/rustfs3 http://rustfs-node1:9000/data/rustfs4 \
http://rustfs-node2:9000/data/rustfs1 http://rustfs-node2:9000/data/rustfs2 http://rustfs-node2:9000/data/rustfs3 http://rustfs-node2:9000/data/rustfs4 \
http://rustfs-node3:9000/data/rustfs1 http://rustfs-node3:9000/data/rustfs2 http://rustfs-node3:9000/data/rustfs3 http://rustfs-node3:9000/data/rustfs4 \
http://rustfs-node4:9000/data/rustfs1 http://rustfs-node4:9000/data/rustfs2 http://rustfs-node4:9000/data/rustfs3 http://rustfs-node4:9000/data/rustfs4
```

---
