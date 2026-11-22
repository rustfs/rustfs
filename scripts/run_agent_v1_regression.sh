#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

python -m unittest discover -s scripts -p 'test_agent_v1_*.py' -v
python scripts/verify_agent_v1_fixtures.py \
  --fixtures protocol/agent/v1/fixtures
python scripts/verify_agent_v1_fixtures.py \
  --schemas protocol/agent/v1 \
  --accept protocol/agent/v1/fixtures/diagnostic-scheduler/health-service-accept-vectors.json \
  --reject protocol/agent/v1/fixtures/diagnostic-scheduler/health-service-reject-vectors.json \
  --allow-schema-gaps protocol/agent/v1/schema-gap-allowlist.json
