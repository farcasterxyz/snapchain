# Changelog

All notable changes to this project will be documented in this file.

## [0.11.2] - 2026-01-08

### 🐛 Bug Fixes

- Recompute cached transaction if block events don't match (#741)
- Bump default ulimits for snapchain nodes (#743)

## [0.11.1] - 2026-01-05

### 🚀 Features

- Separate proto files into standalone crate (#729)

### 🐛 Bug Fixes

- Forward date the activation time in testnet for protocol v10 (#728)
- Support storage lends in replication (#731)
- Replication client checks to account for storage lend messages (#735)
- Enable manually triggering reingest for a heartbeat event (#737)

### ⚙️ Miscellaneous Tasks

- Add signer fid tag to metrics (#733)
- Take a replication snapshot if there are none (#734)

## [0.11.0] - 2025-12-03

### 🚀 Features

- Increase message size limit for username proofs (#722)

### 🐛 Bug Fixes

- Set correct ports for replication peer addresses (#724)

### ⚙️ Miscellaneous Tasks

- Add more information about lent and borrowed units to rpc (#717)
- Add more metrics to mempool (#718)
- Activate decentralized sync entrypoint (#723)

## [0.10.0] - 2025-10-22

### 🚀 Features

- Stop revoking all  messages signed by revoked signers (#714)

### 🐛 Bug Fixes

- Reset events sequence number on validate (#713)

## [0.9.2] - 2025-10-14

### 🐛 Bug Fixes

- Add http server support for storage lend messages (#709)

### ⚙️ Miscellaneous Tasks

- Send onchain events to shard 0 (#705)
- Downgrade failed onchain event merge log to warn (#710)

## [0.9.1] - 2025-10-09

### ⚙️ Miscellaneous Tasks

- Add bulk rpc for querying `LendStorage` messages (#702)
- Enable replication server by default (#703)
- Stop populating num_onchain_events in the GetInfo endpoint (#706)

## [0.9.0] - 2025-10-01

### 🚀 Features

- Implement pruning for storage lends (#683)
- Enable submitting storage lends via rpc (#688)
- Do a "fast" catchup on shard-0 by streaming blocks during replication (#693)
- Enable migrating onchain events to shard 0 and fix bugs with storage lending (#669)

### 🐛 Bug Fixes

- Make shard-0 block to Sep 3 2025 (#692)
- Update ETA constant for replication (#694)

### ⚙️ Miscellaneous Tasks

- Separate out ContactInfo and GossipMessage protos (#695)
- Increase minimum storage requirement for storage lending (#698)
- Set protocol release date to 10/8 (#699)

## [0.8.1] - 2025-09-23

### 🚀 Features

- Enable sending onchain events to shard 0 (#676)
- Configure or get (from public IP) announce_rpc_address (#681)
- Discover nodes to sync with using gossip p2p (#682)
- Ensure we have at least 65k FD limit to start replicator (#685)
- Pick shard-0 blocks for replicator (#687)
- Add progress for replication and other nice to haves (#689)

### 🐛 Bug Fixes

- Generate replicator snapshots every 8 hours (#680)
- Handle duplicates while inserting in the trie (#679)
- Dont create block_store in main() (#686)

### 🧪 Testing

- Add multi-page/rpc errors tests (#677)

### ⚙️ Miscellaneous Tasks

- Push protocol release out by 2 weeks (#690)

## [0.8.0] - 2025-09-16

### 🚀 Features

- Add trie paged iteration + remove attach_to_root (#660)
- Add trie-based replication server (#663)
- Add profile token user data type (#666)
- Add the ability to start a node via replication (#667)
- Support storage lending in the shard and block engines (#665)
- Add client manager + tests for replication rpc requests (#672)

### 🐛 Bug Fixes

- Hardcode the trie_branching_factor to 16 (#668)
- Update block event seqnum correctly if there are multiple block events in a single transaction (#673)
- Simplify the replication by removing trie/DB threads (#671)
- Rename replication rs files (#675)

### ⚙️ Miscellaneous Tasks

- Add chain tag to some onchain event metrics (#664)

## [0.7.0] - 2025-09-03

### 🚀 Features

- Produce transactions and events in shard 0 (#634)
- Allow attaching orphaned nodes to the merkle trie (#651)
- Read block events from shard 0 in other shards (#639)

## [0.6.0] - 2025-08-27

### 🐛 Bug Fixes

- Paged iterator doesn't go through sub-keys (#652)

### ⚙️ Miscellaneous Tasks

- Push EngineVersion::V8 activation date up a week (#655)
- Add metric for confirmed shard heights (#656)

## [0.5.1] - 2025-08-25

### 🚀 Features

- Add ability to batch insert into trie (#637)
- Create a new bulk-optimized DB option (#638)
- Allow conflict-free stores for replicator (#648)

### 🐛 Bug Fixes

- Don't return duplicate FIDs (#641)
- Fix consensus tests (#644)
- Fix build (#647)
- Correctly route fnames to all shards (#650)

## [0.5.0] - 2025-08-18

### 🚀 Features

- Add support for onchain events and verification messages in a single transaction (#608)
- Return account root hash for each fid in transaction (#621)
- Decouple shard 0 block production from other shards block production (#619)

### 🐛 Bug Fixes

- Update tagging instructions in readme
- Fix tests to increment and decrement hash values correctly (#613)
- Switch to polling getLogs for Base pro events to fix not picking up events (#616)
- Fix testnet version schedule to reflect when upgrade actually happened (#618)
- Handle replicator cursor gets stuck (#617)
- Add additional sync metrics to improve visibility (#631)
- Fix fname secondary DB index (#622)
- Don't run parallel migrations + quiet migration logs (#632)
- Supress version check log message (#633)

### 💼 Other

- Log flaky test failure (#628)

### ⚙️ Miscellaneous Tasks

- Add link store tests (#612)
- Enable new version on testnet (#636)
- Extract mempool polling logic into a module for sharing (#629)
- Pull engine metrics publishing out into a module for sharing (#630)

## [0.4.1] - 2025-08-01

### 🚀 Features

- Activate the storage unit redenomination a few days earlier on testnet (#577)
- Add endpoint for connected peers (#541)
- Add direct peers config for gossip (#590)
- Expose next engine version timestamp in GetInfo API (#595)
- Make snapshot metadata compatible with snapdown (#597)
- Add SubmitBulkMessages rpc/http API (#600)
- Port install and autoupgrade script from hubble (#596)
- Allow multi message simulation with dependent messages (#602)
- Add Grafana to install script (#605)

### 🐛 Bug Fixes

- Properly validate `targetFid` param in `linksByTargetFid` (#579)
- Fix broken storage unit related tests (#585)
- Resolve grpc and http api docs inconsistencies (#580)
- Make peer id legible in the currentPeers endpoint (#591)
- Cleanup dead code and comments in trie code (#594)
- Add progress info to snapshot download (#599)
- Suppress the "No valid cached transaction to apply" warning for read-only nodes (#603)

### ⚙️ Miscellaneous Tasks

- /v1/info response with values out of bounds (#575)
- Add more metrics for events emitted (#593)
- Add tests to ensure that snapchain validations are in sync with client validations (#598)
- Add cast store tests (#601)
- Add tests for the user data store (#604)
- Add reaction store tests (#607)
- Add verification store tests (#609)
- Add username proof store tests (#610)

## [0.4.0] - 2025-07-09

### 🚀 Features

- Storage redenomination (#570)

### 🐛 Bug Fixes

- Make GetOnChainSignersByFid endpoint only return active signers (#567)
- Check for timestamp being too far in the future (#569)
- Use correct rpc client for validating verifications (#571)

### ⚙️ Miscellaneous Tasks

- Bump protocol version for storage re-denomination (#573)

## [0.3.2] - 2025-07-02

### 🚀 Features

- Add event counts per event type to BlockConfirmed (#555)
- Add admin endpoint to retry fname transfers (#546)

### 🐛 Bug Fixes

- Fix pagination bug for certain api endpoints (#556)
- Populate event ids for merge failure events (#558)
- Calculate onchain events delay metric correctly (#565)

### ⚙️ Miscellaneous Tasks

- Emit block delay metric (#562)
- Record new onchain event metric (#564)

## [0.3.1] - 2025-06-23

### 🚀 Features

- Unified fid address type endpoint (#535)
- Add Block Confirmed event type as the first event emitted for a block (#549)

### 🐛 Bug Fixes

- Add tier subscriptions to http api (#537)
- Fix network check on verifications to support testnet (#536)
- Handle empty page tokens (#539)
- Restore fname tests using custom signer (#540)
- Show onchain event type for tier purchase in http api (#543)
- Mempool should not do duplicate checks for validator messages (#545)
- Make validation errors more specific (#548)

## [0.3.0] - 2025-06-09

### 🚀 Features

- Setup version schedule for next protocol release (#514)
- Support Basenames (#515)
- Support pro users (#516)
- Update protocol version in block header and validate it (#521)

### 🐛 Bug Fixes

- Fix flaky test due to timestamp conflict (#517)
- Handle fname transfers across shards (#522)
- Increase throttling of the event pruning job (#525)

### ⚡ Performance

- Fix perf test script (#507)

### ⚙️ Miscellaneous Tasks

- Make tier registry contract address configurable for testing (#524)
- Add chain as a tag to onchain events logs (#526)

## [0.2.22] - 2025-05-29

### 🐛 Bug Fixes

- Fix snapshot retry logic with more comprehensive error handling (#509)

## [0.2.21] - 2025-05-28

### 🚀 Features

- Implement `get_id_registry_on_chain_event_by_address` in http (#474)

### 🐛 Bug Fixes

- Log s3 errors better and retry on upload (#495)
- Register malachite consensus metrics in registry (#488)
- Handle historical bug where messages were not correctly revoked (#503)
- Retry when snapshot download fails (#504)

### ⚙️ Miscellaneous Tasks

- Add ability to upload snapshot just for 1 shard (#496)

## [0.2.20] - 2025-05-16

### 🐛 Bug Fixes

- Update tests that depend on hubs being up (#493)
- Process signer revoke messages as expected (#492)

## [0.2.19] - 2025-05-15

### 🐛 Bug Fixes

- Make sure to check key_type for signers (#484)
- Fix pagination for get_on_chain_signers_by_fid (#486)

## [0.2.18] - 2025-05-15

### 🐛 Bug Fixes

- Fix pagination for GetFids (#482)

## [0.2.17] - 2025-05-14

### 🚀 Features

- Implement eventById in the http server (#478)

### 🐛 Bug Fixes

- Send back CORS headers from the http server (#477)

## [0.2.16] - 2025-05-09

### 🐛 Bug Fixes

- `getidregistryonchaineventbyaddress` (#471)

## [0.2.15] - 2025-05-06

### ⚙️ Miscellaneous Tasks

- Add config option for forcing download from snapshot (#468)

## [0.2.14] - 2025-05-05

### 🐛 Bug Fixes

- Validate frame action body (#464)
- Handle frame actions in http server (#466)
- Read camelCase pageOption in http requests for backwards compatibility (#463)

## [0.2.13] - 2025-05-01

### 🚀 Features

- Add GetFids API (#407)
- Forward errors from mempool to client (#455)
- Add timestamp to hub event (#409)

### 🐛 Bug Fixes

- Prirotize data_bytes on message when both data and data_bytes are set (#454)
- Fix validateMessage http method (#460)
- Update eth-signature-verifier to use version with static build outputs (#459)

### 📚 Documentation

- Add a section on protocol upgrades (#377)

### ⚙️ Miscellaneous Tasks

- Update readme

## [0.2.12] - 2025-04-26

### 🐛 Bug Fixes

- Return empty `nextPageToken` for `PagedResponse` (#421)
- Make reaction type optional for `get_reactions_by_target` (#431)
- Implement /v1/events HTTP API endpoint (#451)

## [0.2.11] - 2025-04-25

### 🐛 Bug Fixes

- Tune gossip params (#433)
- Make the sleep in consensus critical path async (#436)
- Read nodes only attempt sync against connected peers (#446)

## [0.2.10] - 2025-04-23

### 🐛 Bug Fixes

- Increase output port capacity so we don't lose messages (#426)
- Increase sync timeout and let read nodes connect to more validators (#429)

### ⚙️ Miscellaneous Tasks

- Add logging for sync (#428)
- Add more logging for sync (#430)

## [0.2.9] - 2025-04-22

### 🐛 Bug Fixes

- Reduce read node sync timeout (#422)

## [0.2.8] - 2025-04-22

### 🐛 Bug Fixes

- Increase gossip connection limits  (#413)
- Stop dialing discovered peers by default (#419)

## [0.2.7] - 2025-04-22

### 🚀 Features

- Use public IP as announce address (#402)

### 🐛 Bug Fixes

- Parse pageToken as strings for http api (#398)
- Add `struct` for `LinksByFidRequest` in `http_server` (#399)
- Have username proof apis return proofs for fnames (#375)
- Allow http urls to be missing query parameters for optional fields (#408)

### 🚜 Refactor

- Change `UserNameProof` type to string (#400)
- Change expected return types for `SignerEventBody` (#410)

## [0.2.6] - 2025-04-18

### 🚀 Features

- Add submitMessage http endpoint (#386)

### 🐛 Bug Fixes

- Start subscribe rpc from latest event if from_id is not specified (#389)

### 📚 Documentation

- Make readability improvements (#378)

## [0.2.5] - 2025-04-16

### 🚀 Features

- Return block number as a field in Event (#359)

### 🐛 Bug Fixes

- Change query params to be backwards compatible with hubs (#363)

### ⚙️ Miscellaneous Tasks

- Add metrics for the number of concurrent submit_message requests (#371)
- Add technical spec to docs (#372)

## [0.2.4] - 2025-04-14

### 🚀 Features

- Add snapshot upload background job (#355)
- Verify commit signatures when processing decided values (#280)
- Docs (#360)
- Return error codes in standard format on submit (#369)

### 🐛 Bug Fixes

- Remove auth requirement for subscribe endpoint (#361)
- Keep subscribe shard index backwards compatible (#364)
- Reduce sync timeouts (#366)
- Retry connections to bootstrap peers (#367)
- Properly track stats on submit message failures (#370)

### ⚙️ Miscellaneous Tasks

- Enable snapshots for read nodes (#362)

## [0.2.3] - 2025-04-04

### 🚀 Features

- Support peer autodiscovery  (#337)
- Add a background job to prune events (#347)
- Rate limits in mempool (#348)

### 🐛 Bug Fixes

- Remove all stale proposals on decide (#345)
- Allow larger message size for link compact messages (#350)
- Ensure pruning jobs don't run into each other (#351)

### ⚙️ Miscellaneous Tasks

- Enable submit to read nodes (#349)
- Increase default mempool size (#352)

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

