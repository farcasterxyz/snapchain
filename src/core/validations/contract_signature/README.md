# Contract signature verification helper

This directory vendors the Solidity source and compiled bytecode for AmbireTech's
`ValidateSigOffchain` helper, used by [`../contract_signature.rs`](../contract_signature.rs)
to verify EOA / ERC-1271 / ERC-6492 signatures in a single `eth_call`.

## Provenance

- `Erc6492.sol` — copied verbatim from
  [`royal-markets/eth-signature-verifier@eb941d6`](https://github.com/royal-markets/eth-signature-verifier/blob/eb941d619a3c/contracts/Erc6492.sol),
  which extracted and lightly modified it from Reown's (formerly WalletConnect's)
  [`reown-com/erc6492`](https://github.com/reown-com/erc6492/blob/main/contracts/Erc6492.sol).
  The `UniversalSigValidator` and `ValidateSigOffchain` contracts are AmbireTech's
  reference implementation of [EIP-6492](https://eips.ethereum.org/EIPS/eip-6492).
  The file is byte-identical to both upstreams as of 2026-04-24.
- `ValidateSigOffchain.bytecode` — deterministic compile output of `Erc6492.sol`;
  see the build recipe below.

## Build recipe (bit-exact reproducible)

```
forge build --use 0.8.28
```

from this directory. Deterministic settings live in [`foundry.toml`](./foundry.toml):
solc 0.8.28, optimizer off, EVM version `cancun`, `bytecode_hash = "none"`,
`cbor_metadata = false`. These strip the trailing IPFS/CBOR metadata hash from the
produced bytecode so the output is bit-exact across machines and solc patch
releases.

## Verification

The expected SHA-256 of `ValidateSigOffchain.bytecode` is pinned in two places:

1. In the module-level doc comment of [`../contract_signature.rs`](../contract_signature.rs).
2. Enforced by the `verify-contract-signature-bytecode` CI job
   ([`../../../../../.github/workflows/verify-contract-signature-bytecode.yml`](../../../../../.github/workflows/verify-contract-signature-bytecode.yml)),
   which rebuilds from source on every PR that touches this directory and
   refuses to merge if the hash drifts.

To verify locally:

```
cd src/core/validations/contract_signature
forge build --use 0.8.28
python3 -c "import json,sys; d=json.load(open('out/Erc6492.sol/ValidateSigOffchain.json')); \
    bc=d['bytecode']['object']; sys.stdout.buffer.write(bytes.fromhex(bc[2:] if bc.startswith('0x') else bc))" \
    | shasum -a 256
diff <(python3 -c "...same as above...") ValidateSigOffchain.bytecode
```

(The CI job does the equivalent check non-interactively.)

## Why the bytecode is committed instead of produced via `build.rs`

Having `build.rs` invoke `forge` would add a foundry toolchain dependency to every
`cargo build` — CI runs, the Dockerfile's builder stage, and every contributor's
laptop. Committing the blob and gating drift via a single CI job keeps `cargo build`
free of foundry while preserving full transparency and tamper-evidence.
