# MNMD Docker Troubleshooting Checklist

Preflight
- Directories: `sudo mkdir -p /mnt/rustfs{1..4}_data{1..4}`
- Filesystem: each `/mnt/rustfsX_dataY` must be XFS  
  - Check: `findmnt -no FSTYPE /mnt/rustfs1_data1` -> `xfs`
- Permissions: ensure container user can write  
  - Quick: `sudo chown -R 1000:1000 /mnt/rustfs*_data*` (or match the image user)
- SELinux (if enabled): add `:z` to bind mounts or adjust policy

Bring up
- `docker compose up -d`
- `docker ps` (4 containers up; wait for healthy)
- Logs: `docker logs -f rustfs-node1`

In-container checks
- `docker exec -it rustfs-node1 sh -c 'ls -ld /data/rustfs*'`
- Check FS type (bind mount reflects XFS):  
  `docker exec -it rustfs-node1 sh -c 'stat -f -c %T /data/rustfs1'` (expect `xfs`)

Network/DNS
- Service names resolve automatically on the Compose network:
  `docker exec -it rustfs-node2 getent hosts rustfs-node1`

If VolumeNotFound persists
- Confirm RUSTFS_VOLUMES uses `/data/rustfs{1...4}`, not `{0...4}`
- Consider replacing brace expansion with an explicit list of all 16 endpoints
- Ensure all 16 bind mounts exist and are writable

Optional strong ordering
- Use `wait-and-start.sh` to gate app startup:
  - Mount the script into the container
  - Set entrypoint/command to run the script then exec the server binary

Healthcheck alternatives
- If `nc` missing, switch healthcheck to `curl` or `wget` as documented
