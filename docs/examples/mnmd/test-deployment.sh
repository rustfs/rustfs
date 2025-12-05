#!/bin/bash
# Copyright 2024 RustFS Team
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# test-deployment.sh - Quick test script for MNMD deployment
# Usage: ./test-deployment.sh

set -e

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "========================================="
echo "RustFS MNMD Deployment Test"
echo "========================================="
echo ""

# Test 1: Check if all containers are running
echo "Test 1: Checking container status..."
RUNNING=$(docker-compose ps | grep -c "Up" || echo "0")
if [ "$RUNNING" -eq 4 ]; then
  echo -e "${GREEN}✓ All 4 containers are running${NC}"
else
  echo -e "${RED}✗ Only $RUNNING/4 containers are running${NC}"
  docker-compose ps
  exit 1
fi
echo ""

# Test 2: Check health status
echo "Test 2: Checking health status..."
HEALTHY=0
for node in rustfs-node1 rustfs-node2 rustfs-node3 rustfs-node4; do
  STATUS=$(docker inspect "$node" --format='{{.State.Health.Status}}' 2>/dev/null || echo "unknown")
  if [ "$STATUS" = "healthy" ]; then
    echo -e "  ${GREEN}✓ $node is healthy${NC}"
    HEALTHY=$((HEALTHY + 1))
  elif [ "$STATUS" = "starting" ]; then
    echo -e "  ${YELLOW}⚠ $node is starting (wait a moment)${NC}"
  else
    echo -e "  ${RED}✗ $node status: $STATUS${NC}"
  fi
done

if [ "$HEALTHY" -eq 4 ]; then
  echo -e "${GREEN}✓ All containers are healthy${NC}"
elif [ "$HEALTHY" -gt 0 ]; then
  echo -e "${YELLOW}⚠ $HEALTHY/4 containers are healthy (some may still be starting)${NC}"
else
  echo -e "${RED}✗ No containers are healthy${NC}"
  exit 1
fi
echo ""

# Test 3: Check API endpoints
echo "Test 3: Testing API endpoints..."
PORTS=(9000 9010 9020 9030)
API_SUCCESS=0
for port in "${PORTS[@]}"; do
  if curl -sf http://localhost:${port}/health >/dev/null 2>&1; then
    echo -e "  ${GREEN}✓ API on port $port is responding${NC}"
    API_SUCCESS=$((API_SUCCESS + 1))
  else
    echo -e "  ${RED}✗ API on port $port is not responding${NC}"
  fi
done

if [ "$API_SUCCESS" -eq 4 ]; then
  echo -e "${GREEN}✓ All API endpoints are working${NC}"
else
  echo -e "${YELLOW}⚠ $API_SUCCESS/4 API endpoints are working${NC}"
fi
echo ""

# Test 4: Check Console endpoints
echo "Test 4: Testing Console endpoints..."
CONSOLE_PORTS=(9001 9011 9021 9031)
CONSOLE_SUCCESS=0
for port in "${CONSOLE_PORTS[@]}"; do
  if curl -sf http://localhost:${port}/rustfs/console/health >/dev/null 2>&1; then
    echo -e "  ${GREEN}✓ Console on port $port is responding${NC}"
    CONSOLE_SUCCESS=$((CONSOLE_SUCCESS + 1))
  else
    echo -e "  ${RED}✗ Console on port $port is not responding${NC}"
  fi
done

if [ "$CONSOLE_SUCCESS" -eq 4 ]; then
  echo -e "${GREEN}✓ All Console endpoints are working${NC}"
else
  echo -e "${YELLOW}⚠ $CONSOLE_SUCCESS/4 Console endpoints are working${NC}"
fi
echo ""

# Test 5: Check inter-node connectivity
echo "Test 5: Testing inter-node connectivity..."
CONN_SUCCESS=0
for node in rustfs-node2 rustfs-node3 rustfs-node4; do
  if docker exec rustfs-node1 nc -z "$node" 9000 2>/dev/null; then
    echo -e "  ${GREEN}✓ node1 → $node connection OK${NC}"
    CONN_SUCCESS=$((CONN_SUCCESS + 1))
  else
    echo -e "  ${RED}✗ node1 → $node connection failed${NC}"
  fi
done

if [ "$CONN_SUCCESS" -eq 3 ]; then
  echo -e "${GREEN}✓ All inter-node connections are working${NC}"
else
  echo -e "${YELLOW}⚠ $CONN_SUCCESS/3 inter-node connections are working${NC}"
fi
echo ""

# Test 6: Verify data directories
echo "Test 6: Verifying data directories..."
DIR_SUCCESS=0
for i in {1..4}; do
  if docker exec rustfs-node1 test -d "/data/rustfs${i}"; then
    DIR_SUCCESS=$((DIR_SUCCESS + 1))
  else
    echo -e "  ${RED}✗ /data/rustfs${i} not found in node1${NC}"
  fi
done

if [ "$DIR_SUCCESS" -eq 4 ]; then
  echo -e "${GREEN}✓ All data directories exist${NC}"
else
  echo -e "${RED}✗ Only $DIR_SUCCESS/4 data directories exist${NC}"
fi
echo ""

# Summary
echo "========================================="
echo "Test Summary"
echo "========================================="
echo "Containers running: $RUNNING/4"
echo "Healthy containers: $HEALTHY/4"
echo "API endpoints: $API_SUCCESS/4"
echo "Console endpoints: $CONSOLE_SUCCESS/4"
echo "Inter-node connections: $CONN_SUCCESS/3"
echo "Data directories: $DIR_SUCCESS/4"
echo ""

TOTAL=$((RUNNING + HEALTHY + API_SUCCESS + CONSOLE_SUCCESS + CONN_SUCCESS + DIR_SUCCESS))
MAX_SCORE=23

if [ "$TOTAL" -eq "$MAX_SCORE" ]; then
  echo -e "${GREEN}✓ All tests passed! Deployment is working correctly.${NC}"
  exit 0
elif [ "$TOTAL" -ge 20 ]; then
  echo -e "${YELLOW}⚠ Most tests passed. Some components may still be starting up.${NC}"
  echo "  Try running this script again in a few moments."
  exit 0
else
  echo -e "${RED}✗ Some tests failed. Check the output above and logs for details.${NC}"
  echo "  Run 'docker-compose logs' for more information."
  exit 1
fi
