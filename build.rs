use walkdir::WalkDir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut builder = tonic_build::configure();
    builder = builder
        .type_attribute("ShardHash", "#[derive(Eq, PartialOrd, Ord)]")
        .type_attribute("Height", "#[derive(Copy, Eq, PartialOrd, Ord)]")
        .type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]");
    let proto_files: Vec<String> = WalkDir::new("src/proto")
        .into_iter()
        .filter_map(|entry| {
            let entry = entry.ok()?;
            if entry.path().extension()? == "proto" {
                Some(entry.path().to_str()?.to_string())
            } else {
                None
            }
        })
        .collect();
    if proto_files.is_empty() {
        return Err("No .proto files found in src/proto".into());
    }
    builder.compile(&proto_files, &["src/proto"])?;

    Ok(())
}
