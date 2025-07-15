#!/bin/sh
# Run as root to fix directory permissions
chown -R 10001:10001 /var/tempo

# Use su-exec (a lightweight sudo/gosu alternative, commonly used in Alpine mirroring)
# Switch to user 10001 and execute the original command (CMD) passed to the script
# "$@" represents all parameters passed to this script, i.e. command in docker-compose
exec su-exec 10001:10001 /tempo "$@"