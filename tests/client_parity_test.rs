use prost::Message;
use serde::{Deserialize, Serialize};
use snapchain::core::util::FarcasterTime;
use snapchain::core::validations::message::validate_message;
use snapchain::proto;
use snapchain::version::version::EngineVersion;
use std::fs;
use std::path::Path;

#[derive(Debug, Deserialize, Serialize)]
struct TestInput {
    message_file: String,
    is_valid: bool,
}

#[derive(Debug)]
struct TestCase {
    message: proto::Message,
    message_file: String,
    is_valid: bool,
}

fn load_test_data() -> Result<Vec<TestCase>, Box<dyn std::error::Error>> {
    let base_dir = "tests/client_parity_tests";
    let test_data_path = "tests/client_parity_tests/test_inputs.json";

    let data = fs::read_to_string(test_data_path)?;
    let test_inputs: Vec<TestInput> = serde_json::from_str(&data)?;

    let mut test_cases = Vec::new();
    for input in test_inputs {
        let file_path = Path::new(base_dir).join(&input.message_file);
        let message_bytes = fs::read(&file_path)
            .map_err(|e| format!("Failed to read {}: {}", input.message_file, e))?;

        let message = proto::Message::decode(message_bytes.as_slice()).unwrap();

        test_cases.push(TestCase {
            message,
            message_file: input.message_file,
            is_valid: input.is_valid,
        });
    }

    Ok(test_cases)
}

#[test]
fn test_validations_parity() {
    let test_cases = load_test_data().expect("Failed to load test data");

    for test_case in test_cases {
        let current_time = FarcasterTime::current();
        let engine_version = EngineVersion::latest();

        let validation_result = validate_message(
            &test_case.message,
            proto::FarcasterNetwork::Testnet,
            true,
            &current_time,
            engine_version,
        );

        let is_valid = validation_result.is_ok();

        assert_eq!(
            is_valid, test_case.is_valid,
            "Test case {}: expected is_valid={}, got is_valid={}. Raw validation result: {:#?}",
            test_case.message_file, test_case.is_valid, is_valid, validation_result
        );
    }
}
