use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use parquet::file::reader::{FileReader, SerializedFileReader};
use serde::{Deserialize, Serialize};

use crate::error::{ExporterError, Result};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExportManifest {
    pub run_id: String,
    pub created_at_unix_seconds: u64,
    pub source: ManifestSource,
    pub table: ManifestTable,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestSource {
    pub name: String,
    pub schema: String,
    pub snapshot_date: String,
    pub snapshot_policy: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestTable {
    pub name: String,
    #[serde(default)]
    pub schema: Option<ManifestSchema>,
    pub rows_exported: u64,
    pub files: Vec<ManifestFile>,
    pub extract_predicate: String,
    pub min_raw_id: Option<i64>,
    pub max_raw_id: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ManifestSchema {
    pub hash_algorithm: String,
    pub hash: String,
    pub columns: Vec<ManifestColumn>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ManifestColumn {
    pub name: String,
    pub ordinal_position: i32,
    pub nullable: bool,
    pub pg_type: String,
    pub pg_data_type: String,
    pub pg_udt_name: String,
    pub arrow_type: String,
    pub column_default: Option<String>,
    pub numeric_precision: Option<i32>,
    pub numeric_scale: Option<i32>,
    pub datetime_precision: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestFile {
    pub path: PathBuf,
    pub rows: u64,
    pub bytes: u64,
    pub partition_values: HashMap<String, String>,
    pub min_raw_id: Option<i64>,
    pub max_raw_id: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExportCheckpoint {
    pub version: u32,
    #[serde(default = "default_checkpoint_strategy")]
    pub strategy: String,
    pub run_id: String,
    pub completed: bool,
    pub table: String,
    pub source: ManifestSource,
    pub extract_predicate: String,
    pub extract_start_raw_id: i64,
    pub final_exclusive_end: i64,
    pub next_raw_id: i64,
    pub chunk_rows: i64,
    #[serde(default)]
    pub date_column: Option<String>,
    #[serde(default)]
    pub date_start: Option<String>,
    #[serde(default)]
    pub date_final_exclusive_end: Option<String>,
    #[serde(default)]
    pub date_next_start: Option<String>,
    pub batch_rows: usize,
    pub max_rows_per_file: Option<u64>,
    pub chunks_planned: u64,
    pub chunks_completed: u64,
    pub rows_exported: u64,
    pub files: Vec<ManifestFile>,
    #[serde(default)]
    pub schema: Option<ManifestSchema>,
    pub partitions: Vec<CheckpointPartition>,
    pub manifest_file: Option<PathBuf>,
    pub updated_at_unix_seconds: u64,
}

fn default_checkpoint_strategy() -> String {
    "raw_id_range".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointPartition {
    pub partition_values: HashMap<String, String>,
    pub next_part_number: u32,
}

#[derive(Debug, Clone, Serialize)]
pub struct ValidationReport {
    pub manifest: PathBuf,
    pub table: String,
    pub manifest_rows: u64,
    pub parquet_rows: u64,
    pub files_checked: usize,
    pub passed: bool,
}

pub fn new_run_id(table: &str) -> String {
    let unix = unix_seconds_now();
    format!("{table}-{unix}-{}", std::process::id())
}

pub fn unix_seconds_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

pub fn write_manifest(path: &Path, manifest: &ExportManifest) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|source| ExporterError::CreateDir {
            path: parent.to_path_buf(),
            source,
        })?;
    }
    let json = serde_json::to_string_pretty(manifest)?;
    fs::write(path, json).map_err(|source| ExporterError::WriteFile {
        path: path.to_path_buf(),
        source,
    })
}

pub fn write_checkpoint(path: &Path, checkpoint: &ExportCheckpoint) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|source| ExporterError::CreateDir {
            path: parent.to_path_buf(),
            source,
        })?;
    }
    let json = serde_json::to_string_pretty(checkpoint)?;
    fs::write(path, json).map_err(|source| ExporterError::WriteFile {
        path: path.to_path_buf(),
        source,
    })
}

pub fn read_checkpoint(path: &Path) -> Result<ExportCheckpoint> {
    let text = fs::read_to_string(path).map_err(|source| ExporterError::ReadFile {
        path: path.to_path_buf(),
        source,
    })?;
    serde_json::from_str(&text).map_err(ExporterError::from)
}

pub fn read_manifest(path: &Path) -> Result<ExportManifest> {
    let text = fs::read_to_string(path).map_err(|source| ExporterError::ReadFile {
        path: path.to_path_buf(),
        source,
    })?;
    serde_json::from_str(&text).map_err(ExporterError::from)
}

pub fn validate_manifest(path: &Path) -> Result<ValidationReport> {
    let manifest = read_manifest(path)?;
    let mut parquet_rows = 0_u64;

    for file in &manifest.table.files {
        let parquet_file =
            std::fs::File::open(&file.path).map_err(|source| ExporterError::ReadFile {
                path: file.path.clone(),
                source,
            })?;
        let reader = SerializedFileReader::new(parquet_file)?;
        let rows = reader.metadata().file_metadata().num_rows();
        if rows < 0 {
            return Err(ExporterError::InvalidData(format!(
                "negative row count in parquet metadata for {}",
                file.path.display()
            )));
        }
        parquet_rows += rows as u64;
    }

    Ok(ValidationReport {
        manifest: path.to_path_buf(),
        table: manifest.table.name,
        manifest_rows: manifest.table.rows_exported,
        parquet_rows,
        files_checked: manifest.table.files.len(),
        passed: parquet_rows == manifest.table.rows_exported,
    })
}
