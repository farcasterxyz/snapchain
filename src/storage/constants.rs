pub const PAGE_SIZE_MAX: usize = 1_000;

#[allow(dead_code)]
pub enum RootPrefix {
    Block = 1,
    Shard = 2,
    /* Used for multiple purposes, starts with a 4-byte fid */
    User = 3,
    /* Used to index casts by parent */
    CastsByParent = 4,
    /* Used to index casts by mention */
    CastsByMention = 5,
    /* Used to index links by target */
    LinksByTarget = 6,
    /* Used to index reactions by target  */
    ReactionsByTarget = 7,

    /* Merkle Trie Node */
    MerkleTrieNode = 8,

    /* Event log */
    HubEvents = 9,
    // /* The network ID that the rocksDB was created with */
    // Network = 10,

    // /* Used to store fname server name proofs */
    FNameUserNameProof = 11,

    // /* Used to store on chain events */
    OnChainEvent = 12,
    // /** DB Schema version used to manage migrations */
    DBSchemaVersion = 13,

    // /* Used to index verifications by address */
    VerificationByAddress = 14,

    /* Used to index fname username proofs by fid */
    FNameUserNameProofByFid = 15,

    /* Used to index user submitted username proofs */
    UserNameProofByName = 16,

    /* Used to maintain information about latest onchain events, fnames ingested */
    NodeLocalState = 17,

    /* Used to index blocks by timestamp */
    BlockIndex = 18,

    /* Used to index blocks events by seqnum */
    BlockEvent = 19,

    /* Merkle Trie Metadata. Reserved, not used right now */
    MerkleTrieMetadata = 20,

    /* Replication Bootstrap status */
    ReplicationBootstrapStatus = 21,

    LendStorageByRecipient = 22,

    /* Gasless-key nonces (user + app counters for KEY_ADD / KEY_REMOVE replay protection).
     * "Gasless" distinguishes these from on-chain signer events and from storage-layer "keys". */
    GaslessKey = 23,
}

/** Copied from the JS code */
#[repr(u8)]
pub enum UserPostfix {
    /* Message records (1-85) */
    CastMessage = 1,
    LinkMessage = 2,
    ReactionMessage = 3,
    VerificationMessage = 4,
    // Deprecated
    // SignerMessage = 5,
    UserDataMessage = 6,
    UsernameProofMessage = 7,
    LendStorageMessage = 8,

    // Add new message types here
    // NOTE: If you add a new message type, make sure that it is only used to store Message protobufs.
    // If you need to store an index, use one of the UserPostfix values below (>86).
    /** Index records (must be 86-255) */
    // Deprecated
    // BySigner = 86, // Index message by its signer

    /** CastStore add and remove sets */
    CastAdds = 87,
    CastRemoves = 88,

    /* LinkStore add and remove sets */
    LinkAdds = 89,
    LinkRemoves = 90,

    /** ReactionStore add and remove sets */
    ReactionAdds = 91,
    ReactionRemoves = 92,

    /** Verification add and remove sets */
    VerificationAdds = 93,
    VerificationRemoves = 94,

    /* Deprecated */
    // SignerAdds = 95,
    // SignerRemoves = 96,

    /* UserDataStore add set */
    UserDataAdds = 97,

    /* UserNameProof add set */
    UserNameProofAdds = 99,

    /* Link Compact State set */
    LinkCompactStateMessage = 100,

    LendStorages = 101,

    /* Gasless-key nonce counters, scoped under `RootPrefix::GaslessKey` */
    GaslessKeyUserNonce = 102, // per-FID user nonce for KEY_ADD + custody KEY_REMOVE
    GaslessKeyAppNonce = 103,  // per-AppFID nonce for self-revocation KEY_REMOVE

    /* Sliding-TTL last-used-at for gasless keys, scoped under `RootPrefix::GaslessKey`.
     * Per-(FID, public-key) timestamp; bumped on every validated use of a TTL'd key. */
    GaslessKeyLastUsedAt = 104,

    /* Off-chain signer record index, scoped under `RootPrefix::GaslessKey`.
     * Per-(FID, public-key) `GaslessKeyRecord` carrying scopes, ttl, request_fid. Populated by
     * KEY_ADD and deleted by KEY_REMOVE; read by scope enforcement and RPC. */
    GaslessKeyByFid = 105,

    /* Gasless-signer global-uniqueness index, scoped under `RootPrefix::GaslessKey`.
     * Per-public-key -> owning FID (4B BE). Populated by KEY_ADD and cleared by KEY_REMOVE;
     * read by `merge_key_add` to reject cross-FID reuse of the same Ed25519 public key as a
     * gasless signer. Scope is gasless-only — on-chain signer keys are not indexed here and
     * an on-chain signer may still coexist with a gasless entry under a different FID; this
     * index does not touch that relationship. The primary by-FID index has `[FID][PublicKey]`
     * ordering so it is not scannable by key — this secondary index is the only way to answer
     * "which FID currently holds this key as a gasless signer?" in O(1). */
    GaslessKeyByPublicKey = 106,

    /* Per-FID gasless-key count, scoped under `RootPrefix::GaslessKey`. Per-FID u32 big-endian
     * count incremented by KEY_ADD and decremented by KEY_REMOVE; used to enforce the per-FID
     * gasless-key cap (`MAX_GASLESS_KEYS_PER_FID`, NEYN-10579). On-chain signers are not
     * counted here — they have their own cap at the L2 KeyRegistry. Absent entry == 0;
     * decrementing to 0 deletes the entry to keep the index sparse. */
    GaslessKeyCountByFid = 107,
}

impl UserPostfix {
    pub fn as_u8(self) -> u8 {
        self as u8
    }
}
pub enum OnChainEventPostfix {
    OnChainEvents = 1,

    // Secondary indexes
    #[allow(dead_code)] // TODO
    SignerByFid = 51,

    #[allow(dead_code)] // TODO
    IdRegisterByFid = 52,

    #[allow(dead_code)] // TODO
    IdRegisterByCustodyAddress = 53,
}
