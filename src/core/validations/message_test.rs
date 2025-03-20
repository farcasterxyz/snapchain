mod tests {
    use crate::core::util::calculate_message_hash;
    use crate::core::validations::error::ValidationError;
    use crate::core::validations::message::validate_message;
    use crate::proto;
    use crate::proto::FarcasterNetwork;
    use crate::storage::store::test_helper;
    use crate::utils::factory::{messages_factory, time};
    use ed25519_dalek::Signer;
    use prost::Message;

    fn assert_validation_error(msg: &proto::Message, expected_error: ValidationError) {
        let result = validate_message(msg, FarcasterNetwork::Testnet);
        assert!(result.is_err());
        assert_eq!(result.err().unwrap(), expected_error);
    }

    fn assert_valid(msg: &proto::Message) {
        let result = validate_message(msg, FarcasterNetwork::Testnet);
        assert!(result.is_ok());
    }

    fn assert_mutated_valid(msg: &mut proto::Message) {
        // Recalculate hash and signature based on the new data
        msg.hash = calculate_message_hash(&msg.data.as_ref().unwrap().encode_to_vec());
        let signer = test_helper::generate_signer();
        msg.signer = signer.verifying_key().to_bytes().to_vec();
        msg.signature = signer.sign(&msg.hash).to_bytes().to_vec();
        let result = validate_message(msg, FarcasterNetwork::Testnet);
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
    fn test_validates_network() {
        let mut msg = valid_message();
        assert_valid(&msg);

        msg.data.as_mut().unwrap().network = FarcasterNetwork::None as i32;
        assert_eq!(
            validate_message(&msg, FarcasterNetwork::Testnet).unwrap_err(),
            ValidationError::InvalidNetwork
        );

        // When network is mainnet, other networks are not allowed
        msg.data.as_mut().unwrap().network = FarcasterNetwork::Testnet as i32;
        assert_eq!(
            validate_message(&msg, FarcasterNetwork::Mainnet).unwrap_err(),
            ValidationError::InvalidNetwork
        );

        msg.data.as_mut().unwrap().network = FarcasterNetwork::Devnet as i32;
        assert_eq!(
            validate_message(&msg, FarcasterNetwork::Mainnet).unwrap_err(),
            ValidationError::InvalidNetwork
        );

        msg.data.as_mut().unwrap().network = FarcasterNetwork::None as i32;
        assert_eq!(
            validate_message(&msg, FarcasterNetwork::Mainnet).unwrap_err(),
            ValidationError::InvalidNetwork
        );

        // mainnet is valid
        msg.data.as_mut().unwrap().network = FarcasterNetwork::Mainnet as i32;
        assert_mutated_valid(&mut msg);

        // other networks on testnet/devnet are valid
        msg.data.as_mut().unwrap().network = FarcasterNetwork::Testnet as i32;
        assert_mutated_valid(&mut msg);
        msg.data.as_mut().unwrap().network = FarcasterNetwork::Devnet as i32;
        assert_mutated_valid(&mut msg);
        msg.data.as_mut().unwrap().network = FarcasterNetwork::Mainnet as i32;
        assert_mutated_valid(&mut msg);
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
}
