use clap::Parser;
use ed25519_dalek::{SecretKey, SigningKey};
use hex::FromHex;
use snapchain::proto::admin_service_client::AdminServiceClient;
use snapchain::proto::hub_service_client::HubServiceClient;
use snapchain::proto::{IdRegisterEventType, SignerEventType};
use snapchain::utils::cli::compose_message;
use snapchain::utils::cli::send_message;
use snapchain::utils::factory::{address, events_factory};
use tonic::Request;

#[derive(Parser)]
struct Cli {
    #[arg(long)]
    addr: String,
    #[arg(long, default_value = "100")]
    count: usize,
    /// Skip FID registration (use if FIDs are already registered)
    #[arg(long)]
    skip_registration: bool,
    /// Use a specific FID instead of auto-incrementing
    #[arg(long)]
    fid: Option<u64>,
    /// Admin authentication (format: username:password)
    #[arg(long, default_value = "admin:password")]
    admin_auth: String,
}

async fn register_fid(
    admin_client: &mut AdminServiceClient<tonic::transport::Channel>,
    fid: u64,
    signer: &SigningKey,
    auth: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("Registering FID {}...", fid);

    // Generate a random address for this FID
    let address = address::generate_random_address();

    // Create the onchain events needed for FID registration
    let events = vec![
        // 1. Storage/rent event
        events_factory::create_rent_event(fid, None, None, false),
        // 2. ID registration event
        events_factory::create_id_register_event(
            fid,
            IdRegisterEventType::Register,
            address.clone(),
            None,
        ),
        // 3. Signer event
        events_factory::create_signer_event(fid, signer.clone(), SignerEventType::Add, None, None),
    ];

    // Submit each event
    for (i, event) in events.iter().enumerate() {
        match snapchain::utils::cli::send_on_chain_event(
            admin_client,
            event,
            Some(auth.to_string()),
        )
        .await
        {
            Ok(_) => {
                println!("  ✓ Submitted event {} of {}", i + 1, events.len());
            }
            Err(e) => {
                eprintln!("  ✗ Failed to submit event {}: {}", i + 1, e);
                return Err(e);
            }
        }
    }

    println!("  ✓ FID {} registered successfully", fid);
    Ok(())
}

#[tokio::main]
async fn main() {
    let args = Cli::parse();

    // feel free to specify your own key
    let private_key = SigningKey::from_bytes(
        &SecretKey::from_hex("1000000000000000000000000000000000000000000000000000000000000000")
            .unwrap(),
    );

    let mut client = HubServiceClient::connect(args.addr.clone()).await.unwrap();
    let count = args.count;

    // Register FIDs unless skipped
    if !args.skip_registration {
        println!("Connecting to admin service for FID registration...");
        match AdminServiceClient::connect(args.addr.clone()).await {
            Ok(mut admin_client) => {
                // Register FIDs 1 through count-1 (since loop is 1..count)
                for i in 1..count {
                    if let Err(e) =
                        register_fid(&mut admin_client, i as u64, &private_key, &args.admin_auth)
                            .await
                    {
                        eprintln!("Failed to register FID {}: {}", i, e);
                        eprintln!("Continuing with message submission anyway...");
                        break;
                    }
                }
                println!("FID registration completed!\n");
            }
            Err(e) => {
                eprintln!("Could not connect to admin service: {}", e);
                eprintln!(
                    "Skipping FID registration. Messages may fail if FIDs aren't registered."
                );
            }
        }
    }

    let mut success = 0;
    let message_count = if args.fid.is_some() { 1 } else { count - 1 };
    println!("Sending {} messages...", message_count);

    if let Some(custom_fid) = args.fid {
        // Send message to specific FID
        let resp = send_message(
            &mut client,
            &compose_message(
                custom_fid,
                "Hello from custom FID!",
                None,
                Some(&private_key),
            ),
            Some(args.admin_auth.clone()),
        )
        .await;

        if resp.is_ok() {
            success += 1;
            println!("  ✓ Message sent successfully to FID {}", custom_fid);
        } else {
            eprintln!("  ✗ Message failed: {:?}", resp.err());
        }
    } else {
        // Original logic for auto-incrementing FIDs - start from 1, not 0
        for i in 1..count {
            let resp = send_message(
                &mut client,
                &compose_message(
                    i as u64,
                    format!("Test message: {}", i).as_str(),
                    None,
                    Some(&private_key),
                ),
                Some(args.admin_auth.clone()),
            )
            .await;

            if resp.is_ok() {
                success += 1;
                println!("  ✓ Message {} sent successfully", i);
            } else {
                eprintln!("  ✗ Message {} failed: {:?}", i, resp.err());
            }
        }
    }

    println!(
        "\nSubmitted {} messages, {} succeeded",
        message_count, success
    );
}
