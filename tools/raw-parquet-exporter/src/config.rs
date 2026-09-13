use std::collections::HashSet;
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use crate::error::{ExporterError, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum Priority {
    P0,
    P1,
    P2,
}

impl Priority {
    pub fn parse(value: &str) -> std::result::Result<Self, String> {
        value.parse()
    }
}

impl FromStr for Priority {
    type Err = String;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        match value.trim().to_ascii_uppercase().as_str() {
            "P0" => Ok(Self::P0),
            "P1" => Ok(Self::P1),
            "P2" => Ok(Self::P2),
            other => Err(format!("expected P0, P1, or P2, got `{other}`")),
        }
    }
}

impl<'de> Deserialize<'de> for Priority {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        value.parse().map_err(serde::de::Error::custom)
    }
}

impl fmt::Display for Priority {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::P0 => f.write_str("P0"),
            Self::P1 => f.write_str("P1"),
            Self::P2 => f.write_str("P2"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExtractStrategy {
    RawIdRange,
    DateMonth,
    FullTable,
    SnapshotItems,
    EmptyTable,
}

impl fmt::Display for ExtractStrategy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RawIdRange => f.write_str("raw_id_range"),
            Self::DateMonth => f.write_str("date_month"),
            Self::FullTable => f.write_str("full_table"),
            Self::SnapshotItems => f.write_str("snapshot_items"),
            Self::EmptyTable => f.write_str("empty_table"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExportConfig {
    #[serde(default)]
    pub defaults: Defaults,
    #[serde(default)]
    pub tables: Vec<TableConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Defaults {
    #[serde(default = "default_compression")]
    pub compression: String,
    #[serde(default = "default_row_group_rows")]
    pub row_group_rows: u64,
    #[serde(default = "default_target_file_bytes")]
    pub target_file_bytes: u64,
    #[serde(default = "default_db_read_connections")]
    pub db_read_connections: u16,
    #[serde(default = "default_writer_workers")]
    pub writer_workers: u16,
}

impl Default for Defaults {
    fn default() -> Self {
        Self {
            compression: default_compression(),
            row_group_rows: default_row_group_rows(),
            target_file_bytes: default_target_file_bytes(),
            db_read_connections: default_db_read_connections(),
            writer_workers: default_writer_workers(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableConfig {
    pub name: String,
    pub priority: Priority,
    pub extract_strategy: ExtractStrategy,
    #[serde(default)]
    pub extract_key: Option<String>,
    #[serde(default)]
    pub date_column: Option<String>,
    #[serde(default)]
    pub chunk_rows: Option<i64>,
    #[serde(default)]
    pub output_partitions: Vec<String>,
    #[serde(default)]
    pub order_by: Vec<String>,
    #[serde(default)]
    pub jsonb_columns: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeConfig {
    #[serde(default)]
    pub source: SourceConfig,
    #[serde(default)]
    pub output: OutputConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceConfig {
    #[serde(default = "default_source_name")]
    pub name: String,
    #[serde(default = "default_dsn_env")]
    pub dsn_env: String,
    #[serde(default = "default_schema")]
    pub schema: String,
    #[serde(default = "default_read_only")]
    pub read_only: bool,
}

impl Default for SourceConfig {
    fn default() -> Self {
        Self {
            name: default_source_name(),
            dsn_env: default_dsn_env(),
            schema: default_schema(),
            read_only: default_read_only(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutputConfig {
    #[serde(default = "default_output_root")]
    pub root: PathBuf,
    #[serde(default = "default_snapshot_date")]
    pub snapshot_date: String,
    #[serde(default = "default_tmp_root")]
    pub tmp_root: PathBuf,
}

impl Default for OutputConfig {
    fn default() -> Self {
        Self {
            root: default_output_root(),
            snapshot_date: default_snapshot_date(),
            tmp_root: default_tmp_root(),
        }
    }
}

pub fn load_export_config(path: impl AsRef<Path>) -> Result<ExportConfig> {
    let path = resolve_config_path(path.as_ref());
    let text = fs::read_to_string(&path).map_err(|source| ExporterError::ReadFile {
        path: path.clone(),
        source,
    })?;
    let config: ExportConfig =
        toml::from_str(&text).map_err(|source| ExporterError::ParseToml {
            path: path.clone(),
            source,
        })?;
    config.validate()?;
    Ok(config)
}

pub fn load_runtime_config(path: impl AsRef<Path>) -> Result<RuntimeConfig> {
    let path = resolve_config_path(path.as_ref());
    let text = fs::read_to_string(&path).map_err(|source| ExporterError::ReadFile {
        path: path.clone(),
        source,
    })?;
    toml::from_str(&text).map_err(|source| ExporterError::ParseToml { path, source })
}

pub fn resolve_config_path(path: &Path) -> PathBuf {
    if path.exists() || path.is_absolute() {
        return path.to_path_buf();
    }

    let tool_relative = Path::new("tools/raw-parquet-exporter").join(path);
    if tool_relative.exists() {
        return tool_relative;
    }

    path.to_path_buf()
}

impl ExportConfig {
    pub fn validate(&self) -> Result<()> {
        if self.tables.is_empty() {
            return Err(ExporterError::InvalidConfig(
                "at least one [[tables]] entry is required".to_string(),
            ));
        }

        let mut seen = HashSet::new();
        for table in &self.tables {
            validate_simple_identifier(&table.name)?;
            if !seen.insert(table.name.as_str()) {
                return Err(ExporterError::InvalidConfig(format!(
                    "duplicate table config `{}`",
                    table.name
                )));
            }

            match table.extract_strategy {
                ExtractStrategy::RawIdRange => {
                    let key = table.extract_key.as_deref().ok_or_else(|| {
                        ExporterError::InvalidConfig(format!(
                            "{} uses raw_id_range but extract_key is missing",
                            table.name
                        ))
                    })?;
                    validate_simple_identifier(key)?;
                    let chunk_rows = table
                        .chunk_rows
                        .unwrap_or(self.defaults.row_group_rows as i64);
                    if chunk_rows <= 0 {
                        return Err(ExporterError::InvalidConfig(format!(
                            "{} chunk_rows must be positive",
                            table.name
                        )));
                    }
                }
                ExtractStrategy::DateMonth => {
                    let column = table.date_column.as_deref().ok_or_else(|| {
                        ExporterError::InvalidConfig(format!(
                            "{} uses date_month but date_column is missing",
                            table.name
                        ))
                    })?;
                    validate_simple_identifier(column)?;
                }
                ExtractStrategy::FullTable
                | ExtractStrategy::SnapshotItems
                | ExtractStrategy::EmptyTable => {}
            }

            for column in &table.order_by {
                validate_simple_identifier(column)?;
            }
            for column in &table.jsonb_columns {
                validate_simple_identifier(column)?;
            }
        }

        Ok(())
    }

    pub fn selected_tables(
        &self,
        names: &[String],
        priority: Option<Priority>,
    ) -> Result<Vec<TableConfig>> {
        let requested: HashSet<&str> = names.iter().map(String::as_str).collect();
        let mut selected = Vec::new();

        for table in &self.tables {
            if !requested.is_empty() && !requested.contains(table.name.as_str()) {
                continue;
            }
            if let Some(priority) = priority {
                if table.priority != priority {
                    continue;
                }
            }
            selected.push(table.clone());
        }

        if !requested.is_empty() {
            let configured: HashSet<&str> = self
                .tables
                .iter()
                .map(|table| table.name.as_str())
                .collect();
            let missing: Vec<&str> = requested.difference(&configured).copied().collect();
            if !missing.is_empty() {
                return Err(ExporterError::InvalidConfig(format!(
                    "unknown table(s): {}",
                    missing.join(", ")
                )));
            }
        }

        Ok(selected)
    }
}

pub fn validate_simple_identifier(identifier: &str) -> Result<()> {
    let mut chars = identifier.chars();
    let Some(first) = chars.next() else {
        return Err(ExporterError::InvalidIdentifier(identifier.to_string()));
    };

    if !(first == '_' || first.is_ascii_alphabetic()) {
        return Err(ExporterError::InvalidIdentifier(identifier.to_string()));
    }

    if !chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric()) {
        return Err(ExporterError::InvalidIdentifier(identifier.to_string()));
    }

    Ok(())
}

fn default_compression() -> String {
    "zstd".to_string()
}

fn default_row_group_rows() -> u64 {
    131_072
}

fn default_target_file_bytes() -> u64 {
    536_870_912
}

fn default_db_read_connections() -> u16 {
    2
}

fn default_writer_workers() -> u16 {
    4
}

fn default_source_name() -> String {
    "local_mydb".to_string()
}

fn default_dsn_env() -> String {
    "DB_DSN".to_string()
}

fn default_schema() -> String {
    "public".to_string()
}

fn default_read_only() -> bool {
    true
}

fn default_output_root() -> PathBuf {
    PathBuf::from("data_lake/raw_postgres")
}

fn default_snapshot_date() -> String {
    "2026-06-19".to_string()
}

fn default_tmp_root() -> PathBuf {
    PathBuf::from("data_lake/_tmp/raw_export")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_priority_case_insensitively() {
        assert_eq!("p0".parse::<Priority>().unwrap(), Priority::P0);
        assert!("p3".parse::<Priority>().is_err());
    }

    #[test]
    fn rejects_invalid_table_identifier() {
        let config = r#"
            [[tables]]
            name = "bad-name"
            priority = "P0"
            extract_strategy = "full_table"
        "#;
        let parsed: ExportConfig = toml::from_str(config).unwrap();

        assert!(parsed.validate().is_err());
    }

    #[test]
    fn requires_extract_key_for_raw_id_range() {
        let config = r#"
            [[tables]]
            name = "dart_xbrl_fact_raw"
            priority = "P0"
            extract_strategy = "raw_id_range"
        "#;
        let parsed: ExportConfig = toml::from_str(config).unwrap();

        assert!(parsed.validate().is_err());
    }
}
