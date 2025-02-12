use clap::Parser;
use ed25519_dalek::{SecretKey, SigningKey};
use hex::FromHex;
use snapchain::proto::hub_service_client::HubServiceClient;
use snapchain::utils::cli::compose_message;
use snapchain::utils::cli::send_message;

#[derive(Parser)]
struct Cli {
    #[arg(long)]
    addr: String,
    #[arg(long, default_value = "100")]
    count: usize,
}

#[tokio::main]
async fn main() {
    let args = Cli::parse();

    // feel free to specify your own key
    let private_key = SigningKey::from_bytes(
        &SecretKey::from_hex("1000000000000000000000000000000000000000000000000000000000000000")
            .unwrap(),
    );

    let mut client = HubServiceClient::connect(args.addr).await.unwrap();
    let count = args.count;

    let mut success = 0;
    for i in 1..count {
        let resp = send_message(
            &mut client,
            &compose_message(
                i as u64,
                format!("Test message: {}", i).as_str(),
                None,
                Some(&private_key),
            ),
        )
        .await;

        if resp.is_ok() {
            success += 1;
        }
    }

    println!("Submitted {} messages, {} succeeded", count, success);
}
