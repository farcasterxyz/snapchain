//! Expose the `snapchain-proto` crate version to the CLI's source as a compile-time env var so
//! `fc --version` can show which proto version was baked into the binary. We parse the proto
//! crate's `Cargo.toml` directly to avoid pulling in a TOML parser as a build dep.

use std::fs;
use std::path::PathBuf;

fn main() {
    let proto_manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("cli/ has a workspace parent")
        .join("proto")
        .join("Cargo.toml");

    let manifest = fs::read_to_string(&proto_manifest)
        .unwrap_or_else(|e| panic!("read {}: {}", proto_manifest.display(), e));

    // Pluck the first top-level `version = "..."` after `[package]`.
    let in_package = manifest.split("[package]").nth(1).unwrap_or("");
    let version = in_package
        .lines()
        .find_map(|l| {
            let l = l.trim();
            if l.starts_with('[') {
                return Some(Err(()));
            }
            l.strip_prefix("version").and_then(|rest| {
                rest.trim_start()
                    .strip_prefix('=')
                    .and_then(|v| v.trim().strip_prefix('"'))
                    .and_then(|v| v.strip_suffix('"'))
                    .map(|v| Ok(v.to_string()))
            })
        })
        .and_then(Result::ok)
        .expect("snapchain-proto Cargo.toml: could not find [package] version");

    println!("cargo:rustc-env=SNAPCHAIN_PROTO_VERSION={}", version);
    println!("cargo:rerun-if-changed={}", proto_manifest.display());
}
