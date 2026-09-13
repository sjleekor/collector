use std::path::PathBuf;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum ExporterError {
    #[error("failed to read {path}: {source}")]
    ReadFile {
        path: PathBuf,
        source: std::io::Error,
    },

    #[error("failed to write {path}: {source}")]
    WriteFile {
        path: PathBuf,
        source: std::io::Error,
    },

    #[error("failed to create directory {path}: {source}")]
    CreateDir {
        path: PathBuf,
        source: std::io::Error,
    },

    #[error("failed to rename {from} to {to}: {source}")]
    RenameFile {
        from: PathBuf,
        to: PathBuf,
        source: std::io::Error,
    },

    #[error("failed to stat {path}: {source}")]
    Metadata {
        path: PathBuf,
        source: std::io::Error,
    },

    #[error("failed to remove {path}: {source}")]
    RemoveFile {
        path: PathBuf,
        source: std::io::Error,
    },

    #[error("failed to parse TOML config {path}: {source}")]
    ParseToml {
        path: PathBuf,
        source: toml::de::Error,
    },

    #[error("failed to serialize plan JSON: {0}")]
    SerializeJson(#[from] serde_json::Error),

    #[error("invalid config: {0}")]
    InvalidConfig(String),

    #[error("invalid identifier `{0}`")]
    InvalidIdentifier(String),

    #[error("database error: {0}")]
    Database(#[from] tokio_postgres::Error),

    #[error("arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),

    #[error("invalid integer value for {name}: {source}")]
    InvalidInteger {
        name: &'static str,
        source: std::num::ParseIntError,
    },

    #[error("invalid data: {0}")]
    InvalidData(String),

    #[error("command is not implemented yet: {0}")]
    NotImplemented(&'static str),
}

pub type Result<T> = std::result::Result<T, ExporterError>;
