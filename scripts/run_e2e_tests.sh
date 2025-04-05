#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
set -e

# Ensure script is run from the project root
cd "$(dirname "$0")/.."

echo "--- Building Docker images ---"
docker-compose build server

echo "--- Starting Docker Compose services ---"
docker-compose up -d

# Simple wait for services to start - consider a more robust check later
echo "--- Waiting for services to initialize (10 seconds) ---"
sleep 10

echo "--- Running E2E tests ---"
# Run tests and capture exit code
go test ./... -tags=e2e -v -timeout 30s
TEST_EXIT_CODE=$?

echo "--- Stopping Docker Compose services ---"
docker-compose down -v

echo "--- E2E Test Run Complete ---"
exit $TEST_EXIT_CODE
