# MNMD Docker Troubleshooting Checklist

Preflight

- Directories: `sudo mkdir -p /mnt/rustfs{1..4}_data{1..4}`
- Filesystem: each `/mnt/rustfsX_dataY` must be XFS
    - Check: `findmnt -no FSTYPE /mnt/rustfs1_data1` -> `xfs`
- Permissions: ensure container user can write
    - Quick: `sudo chown -R 1000:1000 /mnt/rustfs*_data*` (or match the image user)
- SELinux (if enabled): add `:z` to bind mounts or adjust policy

Bring up (run from docs/examples/mnmd)

- `docker compose up -d`
- `docker ps` (4 containers up; wait for healthy)
- Logs: `docker logs -f rustfs-node1`

Entrypoint wrapper

- We mount `./wait-and-start.sh:/usr/local/bin/wait-and-start.sh:ro` and set entrypoint to
  `/bin/sh /usr/local/bin/wait-and-start.sh`
- No need to rely on the host execute bit; if you prefer, you can `chmod +x wait-and-start.sh` and set entrypoint
  directly to the script
- If the image doesn't have a default CMD (or ENTRYPOINT is replaced), we fall back to `RUSTFS_CMD` (default `rustfs`).
  Adjust it if your binary name differs.

In-container checks

- `docker exec -it rustfs-node1 sh -c 'ls -ld /data/rustfs*'`
- FS type reflects XFS:  
  `docker exec -it rustfs-node1 sh -c 'stat -f -c %T /data/rustfs1'` (expect `xfs`)

Network/DNS

- Service names resolve automatically on the Compose network:
  `docker exec -it rustfs-node2 getent hosts rustfs-node1`

If VolumeNotFound persists

- Confirm RUSTFS_VOLUMES uses `/data/rustfs{1...4}`, not `{0...4}`
- Consider replacing brace expansion with an explicit list of all 16 endpoints
- Ensure all 16 bind mounts exist and are writable

Healthcheck alternatives

- If `nc` missing, switch healthcheck to `curl` or `wget` as documented