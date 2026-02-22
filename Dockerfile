ARG MALACHITE_GIT_REPO_URL=https://github.com/informalsystems/malachite.git
ARG MALACHITE_GIT_REF=13bca14cd209d985c3adf101a02924acde8723a5
ARG ETH_SIGNATURE_VERIFIER_GIT_REPO_URL=https://github.com/CassOnMars/eth-signature-verifier.git
ARG ETH_SIGNATURE_VERIFIER_GIT_REF=8deb4a091982c345949dc66bf8684489d9f11889

FROM rust:1.89 AS chef
RUN cargo install cargo-chef
WORKDIR /usr/src/app

# Stage 1: Analyze dependencies and create a build recipe
FROM chef AS planner

ARG MALACHITE_GIT_REPO_URL
ARG MALACHITE_GIT_REF
ARG ETH_SIGNATURE_VERIFIER_GIT_REPO_URL
ARG ETH_SIGNATURE_VERIFIER_GIT_REF

ENV MALACHITE_GIT_REPO_URL=$MALACHITE_GIT_REPO_URL
ENV ETH_SIGNATURE_VERIFIER_GIT_REPO_URL=$ETH_SIGNATURE_VERIFIER_GIT_REPO_URL

RUN <<EOF
set -eu
cd ..
git clone $ETH_SIGNATURE_VERIFIER_GIT_REPO_URL
cd eth-signature-verifier
git checkout $ETH_SIGNATURE_VERIFIER_GIT_REF
cd ..
git clone $MALACHITE_GIT_REPO_URL
cd malachite
git checkout $MALACHITE_GIT_REF
EOF

COPY Cargo.lock Cargo.toml ./
COPY proto/Cargo.toml ./proto/Cargo.toml
# cargo-chef prepare needs the full source tree for `cargo metadata` to resolve
# binary targets (default-run, src/bin/*). The recipe.json output only captures
# dependency info, so the builder's `cook` layer stays cached even when source changes.
COPY src ./src
RUN cargo chef prepare --recipe-path recipe.json

# Stage 2: Build dependencies (cached until Cargo.lock/Cargo.toml change)
FROM chef AS builder

ARG MALACHITE_GIT_REPO_URL
ARG MALACHITE_GIT_REF
ARG ETH_SIGNATURE_VERIFIER_GIT_REPO_URL
ARG ETH_SIGNATURE_VERIFIER_GIT_REF

ENV MALACHITE_GIT_REPO_URL=$MALACHITE_GIT_REPO_URL
ENV ETH_SIGNATURE_VERIFIER_GIT_REPO_URL=$ETH_SIGNATURE_VERIFIER_GIT_REPO_URL
ENV RUST_BACKTRACE=1

RUN <<EOF
set -eu
apt-get update && apt-get install -y libclang-dev git libjemalloc-dev llvm-dev make protobuf-compiler libssl-dev openssh-client cmake
cd ..
git clone $ETH_SIGNATURE_VERIFIER_GIT_REPO_URL
cd eth-signature-verifier
git checkout $ETH_SIGNATURE_VERIFIER_GIT_REF
cd ..
git clone $MALACHITE_GIT_REPO_URL
cd malachite
git checkout $MALACHITE_GIT_REF
EOF

# Build only dependencies using the recipe (this layer is cached until
# Cargo.lock or Cargo.toml change, avoiding full recompilation on source changes)
COPY --from=planner /usr/src/app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json

# Build the actual source code (only this layer invalidates on src/ changes)
COPY Cargo.lock Cargo.toml ./
COPY proto ./proto
COPY src ./src

ENV RUST_BACKTRACE=full
RUN cargo build --release --bins

## Pre-generate some configurations we can use
# TODO: consider doing something different here
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
COPY --from=builder /usr/src/app/proto/definitions /app/proto
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
