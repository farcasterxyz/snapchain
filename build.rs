use glob;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Ensure build.rs reruns when proto files change
    println!("cargo:rerun-if-changed=src/proto");

    let mut builder = tonic_build::configure();

    // Custom type attributes required for malachite
    builder = builder
        .type_attribute("ShardHash", "#[derive(Eq, PartialOrd, Ord)]")
        .type_attribute("Height", "#[derive(Copy, Eq, PartialOrd, Ord)]")
        // TODO: this generates a lot of code, perhaps choose specific structures
        .type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]");

    // Dynamically discover proto files
    let mut proto_files: Vec<_> = glob::glob("src/proto/*.proto")?
        .filter_map(Result::ok)
        .collect();

    proto_files.sort(); // stable deterministic order

    if proto_files.is_empty() {
        return Err("No .proto files found under src/proto; check your layout/checkout".into());
    }

    // This helps catch accidental additions in CI logs.
    for path in &proto_files {
        println!("cargo:warning=Compiling proto: {}", path.display());
    }

    builder.compile(
        &proto_files.iter().map(|p| p.as_path()).collect::<Vec<_>>(),
        &["src/proto"],
    )?;

    Ok(())
}
