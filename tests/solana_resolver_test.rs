use snapchain::connectors::onchain_events::SolanaNameResolver;
use snapchain::connectors::onchain_events::SolanaNameService;

/// Integration test that can be run manually against a live Solana RPC endpoint.
///
/// Set the following env vars before running to enable the test:
/// - `SOL_TEST_RPC_URL`: full RPC URL (e.g. https://api.mainnet-beta.solana.com)
/// - `SOL_TEST_NAME`: the `.sol` name to resolve (e.g. `bonfida.sol`)
/// - `SOL_TEST_EXPECTED_OWNER`: expected owner as a hex string (with or without 0x)
#[tokio::test]
async fn test_sol_resolver_with_public_rpc() {
    let rpc_url = match std::env::var("SOL_TEST_RPC_URL") {
        Ok(url) if !url.is_empty() => url,
        _ => return,
    };

    let (name, expected_owner_hex) = match (
        std::env::var("SOL_TEST_NAME"),
        std::env::var("SOL_TEST_EXPECTED_OWNER"),
    ) {
        (Ok(name), Ok(owner)) if !name.is_empty() && !owner.is_empty() => (name, owner),
        _ => return,
    };

    let mut owner_bytes = expected_owner_hex.trim_start_matches("0x").to_string();
    if owner_bytes.len() % 2 == 1 {
        owner_bytes = format!("0{}", owner_bytes);
    }
    let expected_owner =
        hex::decode(owner_bytes).expect("SOL_TEST_EXPECTED_OWNER must be valid hex bytes");

    let resolver: SolanaNameService =
        SolanaNameService::new(rpc_url).expect("invalid solana rpc url");
    let resolved_owner: Vec<u8> = resolver
        .resolve(name.clone())
        .await
        .expect("failed to resolve solana name");

    assert_eq!(
        resolved_owner, expected_owner,
        "resolved owner mismatch for {name}"
    );
}
