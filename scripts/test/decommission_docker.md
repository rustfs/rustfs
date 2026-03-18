# Local Docker Decommission Test

This setup simulates a two-pool RustFS cluster locally with Docker so decommission can be tested without relying on a remote environment.

## Topology

- One RustFS container
- Two pools
- Four disks per pool
- S3 API on `http://127.0.0.1:9100`
- Console on `http://127.0.0.1:9101`

Pool cmdlines used by admin decommission:

- `/data/pool0/disk{1...4}`
- `/data/pool1/disk{1...4}`

## Quick Start

1. Start the local cluster:
   `./scripts/test/decommission_docker.sh up`
2. Run the end-to-end smoke flow:
   `./scripts/test/decommission_docker.sh smoke`

## Manual Flow

1. Start the cluster:
   `./scripts/test/decommission_docker.sh up`
2. Prepare test data:
   `./scripts/test/decommission_validation.sh prepare rustfs-decom`
3. Start decommission on pool 1:
   `./scripts/test/decommission_validation.sh start rustfs-decom '/data/pool1/disk{1...4}'`
4. Wait for completion:
   `./scripts/test/decommission_validation.sh wait rustfs-decom '/data/pool1/disk{1...4}' 900`
5. Verify objects and versions:
   `./scripts/test/decommission_validation.sh verify rustfs-decom`

## Cleanup

- Stop containers:
  `./scripts/test/decommission_docker.sh down`
- Remove containers, volumes, generated data, and saved validation state:
  `./scripts/test/decommission_docker.sh reset`

## Notes

- This simulates pools, not separate physical nodes.
- It is good for validating decommission control flow and object migration behavior.
- It is not a substitute for a real distributed failure-injection test.
