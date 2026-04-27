fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut builder = tonic_build::configure();

    // Custom type attributes required for malachite
    builder = builder
        .type_attribute("ShardHash", "#[derive(Eq, PartialOrd, Ord)]")
        .type_attribute("Height", "#[derive(Copy, Eq, PartialOrd, Ord)]")
        // TODO: this generates a lot of code, perhaps choose specific structures
        .type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]")
        // Used by `core::validations::key::MAX_KEY_ADD_SCOPES` to size the scopes cap against the
        // MessageType enum's actual variant count at build time.
        .enum_attribute("MessageType", "#[derive(variant_count::VariantCount)]");

    // TODO: auto-discover proto files
    builder.compile(
        &[
            "definitions/admin_rpc.proto",
            "definitions/blocks.proto",
            "definitions/rpc.proto",
            "definitions/message.proto",
            "definitions/onchain_event.proto",
            "definitions/hub_event.proto",
            "definitions/username_proof.proto",
            "definitions/sync_trie.proto",
            "definitions/node_state.proto",
            "definitions/gossip.proto",
            "definitions/request_response.proto",
            "definitions/replication.proto",
        ],
        &["definitions"],
    )?;

    Ok(())
}
