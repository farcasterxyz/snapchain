use snapchain::connectors::onchain_events::QnsResolver;
use snapchain::connectors::onchain_events::QuilibriumNameService;

/// Integration test that can be run manually against a live Quilibrium RPC endpoint.
///
/// Set the following env vars before running to enable the test:
/// - `QNS_TEST_RPC_URL`: full RPC URL (or leave empty to use the default public RPC)
/// - `QNS_TEST_NAME`: the `.q` name to resolve (e.g. `cassie.q`)
/// - `QNS_TEST_EXPECTED_OWNER`: expected owner as a hex string (with or without 0x)
#[tokio::test]
async fn test_qns_resolver_with_public_rpc() {
    let rpc_url = std::env::var("QNS_TEST_RPC_URL").unwrap_or_default();

    let (name, expected_owner_hex) = match (
        std::env::var("QNS_TEST_NAME"),
        std::env::var("QNS_TEST_EXPECTED_OWNER"),
    ) {
        (Ok(name), Ok(owner)) if !name.is_empty() && !owner.is_empty() => (name, owner),
        _ => return,
    };

    let mut owner_bytes = expected_owner_hex.trim_start_matches("0x").to_string();
    if owner_bytes.len() % 2 == 1 {
        owner_bytes = format!("0{}", owner_bytes);
    }
    let expected_owner =
        hex::decode(owner_bytes).expect("QNS_TEST_EXPECTED_OWNER must be valid hex bytes");

    let resolver: QuilibriumNameService =
        QuilibriumNameService::new(rpc_url).expect("invalid quilibrium rpc url");
    let resolved_owner: Vec<u8> = resolver
        .resolve(name.clone())
        .await
        .expect("failed to resolve quilibrium name");

    assert_eq!(
        resolved_owner, expected_owner,
        "resolved owner mismatch for {name}"
    );
}

/// Test that the resolver rejects names without the .q suffix
#[tokio::test]
async fn test_qns_resolver_rejects_invalid_suffix() {
    let rpc_url = std::env::var("QNS_TEST_RPC_URL").unwrap_or_default();

    let resolver: QuilibriumNameService =
        QuilibriumNameService::new(rpc_url).expect("invalid quilibrium rpc url");

    let result = resolver.resolve("test.eth".to_string()).await;
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        err.to_string().contains("must end with .q"),
        "expected invalid suffix error, got: {}",
        err
    );
}

/// Test that the resolver rejects names with empty labels
#[tokio::test]
async fn test_qns_resolver_rejects_empty_label() {
    let rpc_url = std::env::var("QNS_TEST_RPC_URL").unwrap_or_default();

    let resolver: QuilibriumNameService =
        QuilibriumNameService::new(rpc_url).expect("invalid quilibrium rpc url");

    let result = resolver.resolve(".q".to_string()).await;
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        err.to_string().contains("missing label"),
        "expected missing label error, got: {}",
        err
    );
}
