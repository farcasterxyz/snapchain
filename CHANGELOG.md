# Changelog

All notable changes to this project will be documented in this file.

## [0.2.2] - 2025-03-31

### 🐛 Bug Fixes

- Return store errors on simulate and avoid logging (#338)
- Fix mempool size reported as 0 under load (#340)
- Potential fix for shards not starting consensus (#342)
- Stop tracking proposals that will never be used again (#341)

### ⚙️ Miscellaneous Tasks

- Add testnet configs (#339)

## [0.2.1] - 2025-03-26

### 🚀 Features

- Configurable validator sets (#279)
- Read node block pruning (#330)
- Add mempool size to getInfo (#333)
- Admin rpc to retry onchain events by fid and block range (#334)

### 🐛 Bug Fixes

- Auto reconnect to bootstrap peers if connection is lost (#335)

### ⚙️ Miscellaneous Tasks

- Change port to unique port in test_mempool_eviction (#332)

## [0.2.0] - 2025-03-21

### 🚀 Features

- Add virtual shard id to fids in the merkle trie to make re-sharding easier (#313)
- Switch event id to use block numbers instead of timestamps (#314)
- Add a timestamp index for block heights (#315)
- Add auth for rpc endpoints and cleanup admin rpc (#323)
- Validate for message network (#325)
- Disable mempool publish from non-validators nodes until after backfill (#328)
- Mainnet (#316)
- Add genesis block message (#329)

### 🐛 Bug Fixes

- Expedite processing of validator messages through mempool (#319)
- Add retries for onchain events (#318)
- Don't enforce block time validator is syncing (#317)
- Higher channel sizes, and remove read node expiry (#326)
- Fix mempool logging for link compact messages (#327)

### ⚙️ Miscellaneous Tasks

- Add `dump_wal` utility (#297)
- Add more logs for fname transfers (#322)
- Perf improvements (#320)

## [0.1.4] - 2025-03-16

### 🚀 Features

- Support event filters (#283)
- Add missing methods (#291)
- Upgrade to the latest version of malachite (#298)

### 🐛 Bug Fixes

- Fix read node config (#293)
- Fix host crash by restarting height if proposed value not found (#303)
- Fix flaky test due to consensus timeout (#304)
- Fix timeouts one final time (#305)
- Bump eth-signature-verifier (#307)

### ⚡ Performance

- Add more perf metrics (#296)
- Reduce malachite step timeouts to be in line with faster blocktimes (#299)
- Tune consensus timeouts (#302)
- Tune consensus timeouts and dynamically adjust commit delay for consistent block times (#308)
- Cache transaction on propose and validate to replay on commit (#309)

### ⚙️ Miscellaneous Tasks

- Fix Readme typos
- Clear Docker build cache

## [0.1.3] - 2025-03-04

### 🚀 Features

- Read node documentation and related fixes (#289)

### 🐛 Bug Fixes

- Remove stop id requirement for fnames (#270)
- Update rest types (#284)
- Update target_hash (#285)
- Update page token (#287)
- Name param should be string (#288)

### ⚙️ Miscellaneous Tasks

- Update README.md (#286)

## [0.1.2] - 2025-02-21

### 🚀 Features

- Enable http (#254)
- Upgrade malachite to latest commit (#266)

### 🐛 Bug Fixes

- Fix mempool infinitely rebroadcasting messages via gossip (#256)
- Parse fname server response correctly (#259)
- Block production fixes (#264)

## [0.1.1] - 2025-02-06
 - onchain events and fname bug fixes
 - add shard info to GetInfo

## [0.1.0] - 2025-02-05

- Initial testnet release of Snapchain

