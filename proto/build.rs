fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut builder = tonic_build::configure();

    // Custom type attributes required for malachite
    builder = builder
        .type_attribute("ShardHash", "#[derive(Eq, PartialOrd, Ord)]")
        .type_attribute("Height", "#[derive(Copy, Eq, PartialOrd, Ord)]")
        // TODO: this generates a lot of code, perhaps choose specific structures
        .type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]");

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
            "definitions/hyper.proto",
            "definitions/node_state.proto",
            "definitions/gossip.proto",
            "definitions/request_response.proto",
            "definitions/replication.proto",
        ],
        &["definitions"],
    )?;

    Ok(())
}
