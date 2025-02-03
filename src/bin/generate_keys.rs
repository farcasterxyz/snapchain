use libp2p::identity::ed25519::SecretKey;

#[tokio::main]
async fn main() {
    let secret_key = SecretKey::generate();
    let keypair = libp2p::identity::ed25519::Keypair::from(secret_key.clone());
    println!("Private key: {} (DO NOT SHARE)", hex::encode(secret_key));
    println!("Public key : {}", hex::encode(keypair.public().to_bytes()));
}
