//! Boots the `snapchain` binary as a single-node devnet validator and as a read node, waits for
//! its HTTP API to answer, then stops it with SIGINT and checks that it exits cleanly.
//!
//! Every other test drives the library directly, so this is the only test that runs `main` and
//! the node's startup wiring.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream, UdpSocket};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use libp2p::identity::ed25519::{Keypair, SecretKey};

const STARTUP_TIMEOUT: Duration = Duration::from_secs(60);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);

fn free_tcp_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn free_udp_port() -> u16 {
    UdpSocket::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

struct NodePorts {
    rpc: u16,
    http: u16,
    gossip: u16,
}

fn write_config(dir: &Path, ports: &NodePorts, read_node: bool) -> std::path::PathBuf {
    let secret_key = SecretKey::generate();
    let public_key = hex::encode(Keypair::from(secret_key.clone()).public().to_bytes());
    let config = format!(
        r#"
rpc_address = "127.0.0.1:{rpc}"
http_address = "127.0.0.1:{http}"
rocksdb_dir = "{db_dir}"
read_node = {read_node}

[statsd]
prefix = "snapchain_test"
addr = "127.0.0.1:8125"

[gossip]
address = "/ip4/127.0.0.1/udp/{gossip}/quic-v1"

[consensus]
private_key = "{private_key}"
shard_ids = [1]
num_shards = 1
validator_sets = [{{ effective_at = 0, validator_public_keys = ["{public_key}"], shard_ids = [1] }}]

[snapshot]
backup_dir = "{backup_dir}"
snapshot_download_dir = "{snapshot_dir}"
load_db_from_snapshot = false
"#,
        rpc = ports.rpc,
        http = ports.http,
        gossip = ports.gossip,
        db_dir = dir.join(".rocks").display(),
        backup_dir = dir.join(".rocks.backup").display(),
        snapshot_dir = dir.join(".rocks.snapshot").display(),
        private_key = hex::encode(&secret_key),
    );
    let path = dir.join("snapchain.toml");
    std::fs::write(&path, config).unwrap();
    path
}

fn http_get_status(port: u16, path: &str) -> Option<u16> {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).ok()?;
    stream.set_read_timeout(Some(Duration::from_secs(5))).ok()?;
    write!(
        stream,
        "GET {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n"
    )
    .ok()?;
    let mut response = String::new();
    stream.read_to_string(&mut response).ok()?;
    response.split_whitespace().nth(1)?.parse().ok()
}

fn node_log(dir: &Path) -> String {
    let log = std::fs::read_to_string(dir.join("node.log")).unwrap_or_default();
    let tail: Vec<&str> = log.lines().rev().take(40).collect();
    tail.into_iter().rev().collect::<Vec<_>>().join("\n")
}

fn wait_until_serving(child: &mut Child, dir: &Path, http_port: u16) {
    let deadline = Instant::now() + STARTUP_TIMEOUT;
    while Instant::now() < deadline {
        if let Some(status) = child.try_wait().unwrap() {
            panic!(
                "node exited during startup with {status}\n{}",
                node_log(dir)
            );
        }
        if http_get_status(http_port, "/v1/info") == Some(200) {
            return;
        }
        std::thread::sleep(Duration::from_millis(250));
    }
    let _ = child.kill();
    panic!(
        "node did not serve /v1/info within {STARTUP_TIMEOUT:?}\n{}",
        node_log(dir)
    );
}

// SIGINT rather than kill(): `main` returns on Ctrl-C, so the process exits normally and
// flushes its coverage profile.
fn interrupt_and_wait(child: &mut Child, dir: &Path) {
    let status = Command::new("kill")
        .args(["-INT", &child.id().to_string()])
        .status()
        .unwrap();
    assert!(status.success(), "failed to send SIGINT");

    let deadline = Instant::now() + SHUTDOWN_TIMEOUT;
    while Instant::now() < deadline {
        if let Some(status) = child.try_wait().unwrap() {
            assert!(
                status.success(),
                "node exited with {status}\n{}",
                node_log(dir)
            );
            return;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    let _ = child.kill();
    panic!(
        "node did not exit within {SHUTDOWN_TIMEOUT:?} of SIGINT\n{}",
        node_log(dir)
    );
}

fn boot_and_stop(read_node: bool) {
    let dir = tempfile::tempdir().unwrap();
    let ports = NodePorts {
        rpc: free_tcp_port(),
        http: free_tcp_port(),
        gossip: free_udp_port(),
    };
    let config_path = write_config(dir.path(), &ports, read_node);
    let log = std::fs::File::create(dir.path().join("node.log")).unwrap();

    let mut child = Command::new(env!("CARGO_BIN_EXE_snapchain"))
        .arg("--config-path")
        .arg(&config_path)
        .stdout(log.try_clone().unwrap())
        .stderr(log)
        .stdin(Stdio::null())
        .spawn()
        .unwrap();

    wait_until_serving(&mut child, dir.path(), ports.http);
    interrupt_and_wait(&mut child, dir.path());
}

#[test]
fn validator_node_starts_serves_and_stops_on_sigint() {
    boot_and_stop(false);
}

#[test]
fn read_node_starts_serves_and_stops_on_sigint() {
    boot_and_stop(true);
}
