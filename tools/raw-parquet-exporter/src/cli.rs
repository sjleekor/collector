use std::path::PathBuf;

use clap::{Parser, Subcommand, ValueEnum};

use crate::config::Priority;
use crate::db::validate_pg_snapshot_id;

#[derive(Debug, Parser)]
#[command(
    author,
    version,
    about = "Export raw PostgreSQL tables to a Parquet lake"
)]
pub struct Cli {
    #[arg(long, default_value = "info", global = true)]
    pub log_level: String,

    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Debug, Subcommand)]
pub enum Commands {
    /// Build and print export jobs without writing data files.
    Plan(PlanArgs),
    /// Export raw_id_range data files partitioned by bsns_year/reprt_code.
    Export(ExportArgs),
    /// Validate a completed export manifest against Parquet metadata.
    Validate(ValidateArgs),
    /// Compare source PostgreSQL rows with exported Parquet samples.
    ValidateSamples(ValidateSamplesArgs),
    /// Resume a failed export run. Phase 2+.
    Resume(ResumeArgs),
    /// Hold an exported PostgreSQL snapshot open; print its id on the first line.
    SnapshotHold(SnapshotHoldArgs),
}

#[derive(Debug, Clone, Parser)]
pub struct PlanArgs {
    #[arg(long, default_value = "config/export_tables.toml")]
    pub config: PathBuf,

    #[arg(long, default_value = "config/local.example.toml")]
    pub runtime: PathBuf,

    #[arg(long, value_delimiter = ',')]
    pub tables: Vec<String>,

    #[arg(long, value_parser = Priority::parse)]
    pub priority: Option<Priority>,

    #[arg(long)]
    pub dry_run: bool,

    #[arg(long)]
    pub offline: bool,

    #[arg(long, value_enum, default_value_t = PlanFormat::Text)]
    pub format: PlanFormat,

    #[arg(long)]
    pub max_db_connections: Option<u16>,

    #[arg(long)]
    pub writer_workers: Option<u16>,

    #[arg(long)]
    pub chunk_rows: Option<i64>,

    #[arg(long)]
    pub since_date: Option<String>,

    #[arg(long)]
    pub until_date: Option<String>,

    #[arg(long)]
    pub snapshot_date: Option<String>,
}

#[derive(Debug, Clone, Parser)]
pub struct ExportArgs {
    #[arg(long, default_value = "config/export_tables.toml")]
    pub config: PathBuf,

    #[arg(long, default_value = "config/local.example.toml")]
    pub runtime: PathBuf,

    #[arg(long, value_delimiter = ',')]
    pub tables: Vec<String>,

    #[arg(long, value_parser = Priority::parse)]
    pub priority: Option<Priority>,

    #[arg(long)]
    pub dry_run: bool,

    #[arg(long)]
    pub chunk_rows: Option<i64>,

    #[arg(long)]
    pub start_raw_id: Option<i64>,

    #[arg(long)]
    pub all_chunks: bool,

    #[arg(long, default_value_t = 65_536)]
    pub batch_rows: usize,

    #[arg(long)]
    pub max_rows_per_file: Option<u64>,

    #[arg(long)]
    pub snapshot_date: Option<String>,

    #[arg(long)]
    pub since_date: Option<String>,

    #[arg(long)]
    pub until_date: Option<String>,

    #[arg(long)]
    pub force: bool,

    /// Read every query from this exported PostgreSQL snapshot (see `snapshot-hold`).
    #[arg(long, value_parser = validate_pg_snapshot_id)]
    pub pg_snapshot: Option<String>,
}

#[derive(Debug, Clone, Parser)]
pub struct ValidateArgs {
    #[arg(long)]
    pub manifest: PathBuf,
}

#[derive(Debug, Clone, Parser)]
pub struct ValidateSamplesArgs {
    #[arg(long)]
    pub manifest: PathBuf,

    #[arg(long, default_value = "config/local.example.toml")]
    pub runtime: PathBuf,

    #[arg(long, value_delimiter = ',')]
    pub raw_ids: Vec<i64>,
}

#[derive(Debug, Clone, Parser)]
pub struct ResumeArgs {
    #[arg(long, default_value = "config/export_tables.toml")]
    pub config: PathBuf,

    #[arg(long, default_value = "config/local.example.toml")]
    pub runtime: PathBuf,

    #[arg(long)]
    pub checkpoint: PathBuf,

    /// Must equal the snapshot the checkpoint was written on.
    #[arg(long, value_parser = validate_pg_snapshot_id)]
    pub pg_snapshot: Option<String>,
}

#[derive(Debug, Clone, Parser)]
pub struct SnapshotHoldArgs {
    #[arg(long, default_value = "config/local.example.toml")]
    pub runtime: PathBuf,

    /// Safety cap: release the snapshot after this long even if nobody asked.
    /// A leaked holder would otherwise pin the vacuum horizon indefinitely.
    #[arg(long, default_value_t = 14_400)]
    pub max_hold_seconds: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum PlanFormat {
    Text,
    Json,
}
