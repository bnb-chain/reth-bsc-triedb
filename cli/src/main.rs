use clap::{Parser, Subcommand};
use eyre::Result;
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

mod generate_storage_root;

use generate_storage_root::GenerateStorageRootArgs;

/// Rust Ethereum TrieDB CLI tools
#[derive(Debug, Parser)]
#[command(name = "triedb-cli")]
#[command(about = "CLI tools for rust-eth-triedb", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Generate storage root from a directory
    GenerateStorageRoot(GenerateStorageRootArgs),
}

fn main() -> Result<()> {
    // Initialize tracing with default level if RUST_LOG is not set
    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info"));
    
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(env_filter)
        .init();

    let cli = Cli::parse();

    match cli.command {
        Commands::GenerateStorageRoot(args) => {
            if let Err(e) = args.execute() {
                eprintln!("Error: {}", e);
                std::process::exit(1);
            }
        }
    }

    Ok(())
}

