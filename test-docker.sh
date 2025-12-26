#!/bin/bash

set -e

# Cores para output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${YELLOW}🐳 Starting tests in Docker...${NC}"

echo -e "${YELLOW}📦 Building Docker image...${NC}"
docker build -f Dockerfile.test -t go-db-store-test .

echo -e "${YELLOW}🧪 Running tests...${NC}"
docker run --rm \
    -v "$(pwd):/app" \
    -e MONGODB_DOWNLOAD_URL="${MONGODB_DOWNLOAD_URL:-}" \
    go-db-store-test "$@"

echo -e "${GREEN}✅ Test execution completed!${NC}"