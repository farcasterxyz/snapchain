mod tests {
    use crate::core::validations::error::ValidationError;
    use crate::core::validations::verification::{validate_add_address, validate_fname_transfer};
    use crate::proto;
    use proto::{FnameTransfer, UserNameProof};

    #[test]
    fn test_validate_add_address_valid_eoa() {
        let add_address_body = &proto::VerificationAddAddressBody{
        address: hex::decode("91031dcfdea024b4d51e775486111d2b2a715871").unwrap(),
        claim_signature: hex::decode("b72c63d61f075b36fb66a9a867b50836cef19d653a3c09005628738677bcb25f25b6b6e6d2e1d69cd725327b3c020deef9e2575a22dc8ed08f88bc75718ce1cb1c").unwrap(),
        block_hash: hex::decode("d74860c4bbf574d5ad60f03a478a30f990e05ac723e138a5c860cdb3095f4296").unwrap(),
        verification_type: 0,
        chain_id: 0,
        protocol: 0,
      };

        let result = validate_add_address(add_address_body, 2, proto::FarcasterNetwork::Mainnet);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_add_address_valid_eoa_2() {
        let add_address_body = &proto::VerificationAddAddressBody{
        address: hex::decode("5eed8e690f36824f963498024249028d66961c0a").unwrap(),
        claim_signature: hex::decode("eaaa2438c2395a70415e520a5b3f94009ca691862e6c906b0b6d5170f1a148c8329aad093bd330e809089a3d4ca2678d817901be8031e3ba72ef19f9e809cf2700").unwrap(),
        block_hash: hex::decode("49d78d07b9e1caaf000da24b55efa6d293a128679fe070d527e5c45a862da9f9").unwrap(),
        verification_type: 0,
        chain_id: 0,
        protocol: 0,
      };

        let result =
            validate_add_address(add_address_body, 200739, proto::FarcasterNetwork::Mainnet);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_add_address_invalid_eoa_signature() {
        let add_address_body = &proto::VerificationAddAddressBody{
        address: hex::decode("91031dcfdea024b4d51e775486111d2b2a715871").unwrap(),
        claim_signature: hex::decode("a72c63d61f075b36fb66a9a867b50836cef19d653a3c09005628738677bcb25f25b6b6e6d2e1d69cd725327b3c020deef9e2575a22dc8ed08f88bc75718ce1cb1c").unwrap(),
        block_hash: hex::decode("d74860c4bbf574d5ad60f03a478a30f990e05ac723e138a5c860cdb3095f4296").unwrap(),
        verification_type: 0,
        chain_id: 0,
        protocol: 0,
      };

        let result = validate_add_address(add_address_body, 2, proto::FarcasterNetwork::Mainnet);
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_add_address_valid_solana() {
        let add_address_body = &proto::VerificationAddAddressBody{
        address: hex::decode("83f7335253bfaf321de49f25f6fd67fa8f1d0665b4cab33f67f7e4341bfd91d0").unwrap(),
        claim_signature: hex::decode("cff46a485d2cf233335ca91792e3da427c50d60fcd40422d6c05ba5d6aea57d8ca723867ebbc408687223b5b0942cde47d12a54c5354cdec1a9df410acc5f20e").unwrap(),
        block_hash: hex::decode("eb7710b438ee64db30eb21bc81f87dc6fccd9569bb63b8128507dcc34c4a72ce").unwrap(),
        verification_type: 0,
        chain_id: 0,
        protocol: 1,
      };

        let result = validate_add_address(add_address_body, 2, proto::FarcasterNetwork::Mainnet);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_add_address_invalid_solana_signature() {
        let add_address_body = &proto::VerificationAddAddressBody{
        address: hex::decode("83f7335253bfaf321de49f25f6fd67fa8f1d0665b4cab33f67f7e4341bfd91d0").unwrap(),
        claim_signature: hex::decode("aff46a485d2cf233335ca91792e3da427c50d60fcd40422d6c05ba5d6aea57d8ca723867ebbc408687223b5b0942cde47d12a54c5354cdec1a9df410acc5f20e").unwrap(),
        block_hash: hex::decode("eb7710b438ee64db30eb21bc81f87dc6fccd9569bb63b8128507dcc34c4a72ce").unwrap(),
        verification_type: 0,
        chain_id: 0,
        protocol: 1,
      };

        let result = validate_add_address(add_address_body, 2, proto::FarcasterNetwork::Mainnet);
        assert!(result.is_err());
    }

    #[test]
    fn test_fname_transfer_verify_valid_signature() {
        let transfer = &FnameTransfer{
            id: 1,
            from_fid: 0,
            proof: Some(UserNameProof{
                timestamp: 1628882891,
                name: "farcaster".into(),
                owner: hex::decode("8773442740c17c9d0f0b87022c722f9a136206ed").unwrap(),
                signature: hex::decode("b7181760f14eda0028e0b647ff15f45235526ced3b4ae07fcce06141b73d32960d3253776e62f761363fb8137087192047763f4af838950a96f3885f3c2289c41b").unwrap(),
                fid: 1,
                r#type: 1,
            })
        };
        let result = validate_fname_transfer(transfer);
        assert!(result.is_ok());
    }

    #[test]
    fn test_fname_transfer_verify_valid_signature_2() {
        let transfer = &FnameTransfer{
            id: 200739,
            from_fid: 200739,
            proof: Some(UserNameProof{
                timestamp: 1702071745,
                name: "pierre02100".into(),
                owner: hex::decode("59fe4ccccb6deefc78e274fa41ee4e107fac59ae").unwrap(),
                signature: hex::decode("69c07157eebc605777b20d18222c5a642b138d932adfd3a0ebdc771edeb1f50b7ad6191ee0b4479141f1284baafcc43551e018eccd9f66eb30f1c2643418918a1b").unwrap(),
                fid: 200739,
                r#type: 1,
            })
        };
        let result = validate_fname_transfer(transfer);
        assert!(result.is_ok());
    }

    #[test]
    fn test_fname_transfer_verify_wrong_address_for_signature_fails() {
        let transfer = &FnameTransfer{
            id: 1,
            from_fid: 1,
            proof: Some(UserNameProof{
                timestamp: 1628882891,
                name: "farcaster".into(),
                owner: hex::decode("8773442740c17c9d0f0b87022c722f9a136206ed").unwrap(),
                signature: hex::decode("a7181760f14eda0028e0b647ff15f45235526ced3b4ae07fcce06141b73d32960d3253776e62f761363fb8137087192047763f4af838950a96f3885f3c2289c41b").unwrap(),
                fid: 1,
                r#type: 1,
            })
        };
        let result = validate_fname_transfer(transfer);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), ValidationError::InvalidSignature);
    }

    #[test]
    fn test_fname_transfer_verify_invalid_signature_fails() {
        let transfer = &FnameTransfer{
      id: 1,
      from_fid: 1,
      proof: Some(UserNameProof{
        timestamp: 1628882891,
        name: "farcaster".into(),
        owner: hex::decode("8773442740c17c9d0f0b87022c722f9a136206ed").unwrap(),
        signature: hex::decode("181760f14eda0028e0b647ff15f45235526ced3b4ae07fcce06141b73d32960d3253776e62f761363fb8137087192047763f4af838950a96f3885f3c2289c41b").unwrap(),
        fid: 1,
        r#type: 1,
      })
    };
        let result = validate_fname_transfer(transfer);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), ValidationError::InvalidSignature);
    }
}
