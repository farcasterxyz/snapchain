use clap::{Parser, Subcommand};
use serde_json;
use snapchain::hyper::{HyperDiffReport, CAPABILITY_HYPER};

#[derive(Parser)]
#[command(name = "snapchain-hyper", about = "Hyper pipeline utilities")]
struct HyperCli {
    #[command(subcommand)]
    command: HyperCommand,
}

#[derive(Subcommand)]
enum HyperCommand {
    /// Compare the canonical and hyper state for a specific block.
    Diff {
        #[arg(long)]
        block: u64,
    },
    /// Walk the latest N blocks to ensure the hyper store is populated.
    Audit {
        #[arg(long, default_value_t = 100)]
        latest: u64,
    },
    /// Stream hyper metrics that will be surfaced once the pipeline lands.
    Metrics {
        #[arg(long, default_value_t = false)]
        tail: bool,
    },
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = HyperCli::parse();

    match cli.command {
        HyperCommand::Diff { block } => {
            let report = HyperDiffReport {
                block_id: block,
                legacy_state_root: None,
                hyper_state_root: Vec::new(),
                retained_message_delta: 0,
                notes: vec![format!(
                    "Hyper diff CLI scaffolding active. Enable {} and implement the dual pipeline to populate this report.",
                    CAPABILITY_HYPER
                )],
            };
            println!("{}", serde_json::to_string_pretty(&report)?);
        }
        HyperCommand::Audit { latest } => {
            println!(
                "hyper audit placeholder: would scan the latest {} canonical blocks once the hyper pipeline is available.",
                latest
            );
        }
        HyperCommand::Metrics { tail } => {
            println!(
                "hyper metrics placeholder (tail = {}). Configure {} once hyper nodes negotiate the new capability.",
                tail, CAPABILITY_HYPER
            );
        }
    }

    Ok(())
}
