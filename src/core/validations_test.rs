mod tests {
    use crate::core::util::calculate_message_hash;
    use crate::core::validations::{
        validate_add_address, validate_fname_transfer, validate_message, ValidationError,
    };
    use crate::proto;
    use crate::storage::store::test_helper;
    use crate::utils::factory::{messages_factory, time};
    use prost::Message;
    use proto::{FnameTransfer, UserNameProof};

    fn assert_validation_error(msg: &proto::Message, expected_error: ValidationError) {
        let result = validate_message(msg);
        assert!(result.is_err());
        assert_eq!(result.err().unwrap(), expected_error);
    }

    fn assert_valid(msg: &proto::Message) {
        let result = validate_message(msg);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validates_data_bytes() {
        let mut msg = messages_factory::casts::create_cast_add(1234, "test", None, None);
        assert_valid(&msg);

        // Set data and data_bytes to None
        msg.data = None;
        msg.data_bytes = None;

        assert_validation_error(&msg, ValidationError::MissingData);

        msg.data_bytes = Some(vec![]);
        assert_validation_error(&msg, ValidationError::MissingData);

        // when data bytes is too large
        msg.data_bytes = Some(vec![0; 2049]);
        assert_validation_error(&msg, ValidationError::InvalidDataLength);

        // When valid
        let mut msg = messages_factory::casts::create_cast_add(1234, "test", None, None);
        // Valid data, but empty data_bytes
        msg.data_bytes = None;
        assert_valid(&msg);

        // Valid data_bytes, but empty data
        msg.data_bytes = Some(msg.data.as_ref().unwrap().encode_to_vec());
        msg.data = None;
        assert_valid(&msg);
    }

    fn valid_message() -> proto::Message {
        messages_factory::casts::create_cast_add(1234, "test", None, None)
    }

    #[test]
    fn test_validates_hash_scheme() {
        let mut msg = valid_message();
        assert_valid(&msg);

        msg.hash_scheme = 0;
        assert_validation_error(&msg, ValidationError::InvalidHashScheme);

        msg.hash_scheme = 2;
        assert_validation_error(&msg, ValidationError::InvalidHashScheme);
    }

    #[test]
    fn test_validates_hash() {
        let timestamp = time::farcaster_time();
        let mut msg = valid_message();
        assert_valid(&msg);

        msg.data.as_mut().unwrap().timestamp = timestamp + 10;
        assert_validation_error(&msg, ValidationError::InvalidHash);

        msg.hash = vec![];
        assert_validation_error(&msg, ValidationError::InvalidHash);

        msg.hash = vec![0; 20];
        assert_validation_error(&msg, ValidationError::InvalidHash);
    }

    #[test]
    fn validates_signature_scheme() {
        let mut msg = valid_message();
        assert_valid(&msg);

        msg.signature_scheme = 0;
        assert_validation_error(&msg, ValidationError::InvalidSignatureScheme);

        msg.signature_scheme = 2;
        assert_validation_error(&msg, ValidationError::InvalidSignatureScheme);
    }

    #[test]
    fn validates_signature() {
        let timestamp = time::farcaster_time();
        let mut msg = valid_message();
        assert_valid(&msg);

        // Change the data so the signature becomes invalid
        msg.data.as_mut().unwrap().timestamp = timestamp + 10;
        msg.hash = calculate_message_hash(&msg.data.as_ref().unwrap().encode_to_vec()); // Ensure hash is valid
        assert_validation_error(&msg, ValidationError::InvalidSignature);

        msg.signature = vec![];
        assert_validation_error(&msg, ValidationError::MissingSignature);

        msg.signature = vec![0; 64];
        assert_validation_error(&msg, ValidationError::InvalidSignature);

        msg = valid_message();
        msg.signer = vec![];

        assert_validation_error(&msg, ValidationError::MissingOrInvalidSigner);

        msg.signer = test_helper::generate_signer()
            .verifying_key()
            .to_bytes()
            .to_vec();
        assert_validation_error(&msg, ValidationError::InvalidSignature);
    }

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
            from_fid: 1,
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
