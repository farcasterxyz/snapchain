#!/bin/bash

# Builds and publishes image to Docker Hub for a single architecture.
# This is intended to be run by our GitHub Actions workflow.
#
# MUST be run from the root of the repository so the Docker build context is correct.
#
# You must `docker login ...` first so that we have the necessary permission to
# push the image layers + tags to Docker Hub.
#
# Environment variables:
#   ARCH - Target architecture (amd64 or arm64)

set -e

if [ -z "$ARCH" ]; then
  echo "Error: ARCH environment variable must be set (amd64 or arm64)"
  exit 1
fi

SNAPCHAIN_VERSION=$(awk -F '"' '/^version =/ {print $2}' Cargo.toml)

echo "Publishing $SNAPCHAIN_VERSION for linux/$ARCH"

# Build for single architecture using vanilla docker CLI
docker build -f Dockerfile \
  --platform "linux/${ARCH}" \
  -t farcasterxyz/snapchain:${SNAPCHAIN_VERSION}-${ARCH} \
  .

# Push the architecture-specific tag
docker push farcasterxyz/snapchain:${SNAPCHAIN_VERSION}-${ARCH}

echo "Successfully published farcasterxyz/snapchain:${SNAPCHAIN_VERSION}-${ARCH}"
