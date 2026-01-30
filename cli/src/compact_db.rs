use clap::Args;
use eyre::{eyre, Result};
use rust_eth_triedb_pathdb::{PathDB, PathProviderConfig, PathProviderManager};
use std::path::PathBuf;
use tracing::info;

/// Run a full RocksDB compaction for a PathDB directory.
#[derive(Debug, Args)]
pub struct CompactDbArgs {
    /// Path to the RocksDB database directory.
    #[arg(long)]
    pub path: PathBuf,
}

impl CompactDbArgs {
    pub fn execute(self) -> Result<()> {
        if !self.path.exists() {
            return Err(eyre!("path does not exist: {}", self.path.display()));
        }

        let mut config = PathProviderConfig::default();
        // Avoid accidentally creating an empty database when the path is wrong.
        config.create_if_missing = false;

        let started = std::time::Instant::now();
        info!(path = %self.path.display(), "Opening PathDB for compaction");
        let db = PathDB::new(
            self.path
                .to_str()
                .ok_or_else(|| eyre!("invalid utf-8 path: {}", self.path.display()))?,
            config,
        )?;

        info!(path = %self.path.display(), "Starting full compaction");
        db.compact()?;
        info!(
            path = %self.path.display(),
            elapsed_ms = started.elapsed().as_millis(),
            "Compaction finished"
        );

        Ok(())
    }
}

