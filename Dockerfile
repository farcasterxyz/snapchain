FROM rust:1.83 AS builder

WORKDIR /usr/src/app

ARG ETH_SIGNATURE_VERIFIER_GIT_REPO_URL=https://github.com/CassOnMars/eth-signature-verifier.git
ENV ETH_SIGNATURE_VERIFIER_GIT_REPO_URL=$ETH_SIGNATURE_VERIFIER_GIT_REPO_URL
ENV RUST_BACKTRACE=1
RUN <<EOF
set -eu
apt-get update && apt-get install -y libclang-dev git libjemalloc-dev llvm-dev make protobuf-compiler libssl-dev openssh-client cmake
cd ..
git clone $ETH_SIGNATURE_VERIFIER_GIT_REPO_URL
EOF

# Unfortunately, we can't prefetch creates without including the source code,
# since the Cargo configuration references files in src.
# This means we'll re-fetch all crates every time the source code changes,
# which isn't ideal.
COPY Cargo.toml build.rs ./
COPY src ./src

ENV RUST_BACKTRACE=full
RUN cargo build --release --bins

## Pre-generate some configurations we can use
# TOOD: consider doing something different here
RUN target/release/setup_local_testnet

#################################################################################

FROM ubuntu:24.04

# Easier debugging within container
ARG GRPCURL_VERSION=1.9.1
ARG TARGETOS
ARG TARGETARCH
RUN <<EOF
  set -eu
  apt-get update && apt-get install -y curl
  curl -L https://github.com/fullstorydev/grpcurl/releases/download/v${GRPCURL_VERSION}/grpcurl_${GRPCURL_VERSION}_${TARGETOS}_${TARGETARCH}.deb > grpcurl.deb
  dpkg -i grpcurl.deb
  rm grpcurl.deb
  apt-get remove -y curl
  apt clean -y
EOF

WORKDIR /app
COPY --from=builder /usr/src/app/src/proto /app/proto
COPY --from=builder /usr/src/app/nodes /app/nodes
COPY --from=builder \
  /usr/src/app/target/release/snapchain \
  /usr/src/app/target/release/follow_blocks \
  /usr/src/app/target/release/setup_local_testnet \
  /usr/src/app/target/release/submit_message \
  /usr/src/app/target/release/perftest \
  /app/

ENV RUSTFLAGS="-Awarnings"
CMD ["./snapchain", "--id", "1"]
