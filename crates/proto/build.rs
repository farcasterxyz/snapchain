fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut builder = tonic_build::configure();

    // Custom type attributes required for malachite
    builder = builder
        .type_attribute("ShardHash", "#[derive(Eq, PartialOrd, Ord)]")
        // Remove the Copy trait to avoid conflicts with prost's own Copy derive
        .type_attribute("Height", "#[derive(Eq, PartialOrd, Ord)]")
        // TODO: this generates a lot of code, perhaps choose specific structures
        .type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]");

    // Use compile_protos for newer version of tonic-build
    builder.compile_protos(
        &[
            "src/admin_rpc.proto",
            "src/blocks.proto",
            "src/rpc.proto",
            "src/message.proto",
            "src/onchain_event.proto",
            "src/hub_event.proto",
            "src/username_proof.proto",
            "src/sync_trie.proto",
            "src/node_state.proto",
            "src/gossip.proto",
            "src/request_response.proto",
        ],
        &["src"],
    )?;

    Ok(())
}
