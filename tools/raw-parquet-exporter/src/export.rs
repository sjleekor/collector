use std::collections::{HashMap, HashSet};
use std::fs;
use std::fs::File;
use std::path::{Path, PathBuf};

use chrono::{Datelike, Duration, NaiveDate};
use futures_util::{pin_mut, TryStreamExt};
use parquet::arrow::ArrowWriter;
use serde::{Deserialize, Serialize};
use tokio_postgres::Row;

use crate::config::{Defaults, ExtractStrategy, RuntimeConfig, TableConfig};
use crate::db::{quote_ident, ColumnInfo, Db, TableBounds};
use crate::error::{ExporterError, Result};
use crate::manifest::{
    new_run_id, unix_seconds_now, write_checkpoint, write_manifest, CheckpointPartition,
    ExportCheckpoint, ExportManifest, ManifestFile, ManifestSchema, ManifestSource, ManifestTable,
};
use crate::parquet_writer::{new_arrow_writer, RecordBatchBuilder};
use crate::schema::build_manifest_schema;

#[derive(Debug, Clone)]
pub struct DartXbrlExportOptions {
    pub chunk_rows: i64,
    pub start_raw_id: Option<i64>,
    pub all_chunks: bool,
    pub batch_rows: usize,
    pub max_rows_per_file: Option<u64>,
    pub dry_run: bool,
    pub force: bool,
    pub resume: Option<ResumeRequest>,
}

#[derive(Debug, Clone)]
pub struct DateMonthExportOptions {
    pub since_date: Option<String>,
    pub until_date: Option<String>,
    pub batch_rows: usize,
    pub max_rows_per_file: Option<u64>,
    pub dry_run: bool,
    pub force: bool,
    pub resume: Option<ResumeRequest>,
}

#[derive(Debug, Clone)]
pub struct FullTableExportOptions {
    pub batch_rows: usize,
    pub max_rows_per_file: Option<u64>,
    pub dry_run: bool,
    pub force: bool,
}

#[derive(Debug, Clone)]
pub struct SnapshotItemsExportOptions {
    pub batch_rows: usize,
    pub max_rows_per_file: Option<u64>,
    pub dry_run: bool,
    pub force: bool,
}

#[derive(Debug, Clone)]
pub struct SchemaOnlyExportOptions {
    pub dry_run: bool,
    pub force: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ResumeRequest {
    pub checkpoint_file: PathBuf,
    pub checkpoint: ExportCheckpoint,
}

#[derive(Debug, Clone, Serialize)]
pub struct ExportResult {
    pub run_id: String,
    pub dry_run: bool,
    pub table: String,
    pub extract_predicate: String,
    pub chunks_planned: u64,
    pub chunks_exported: u64,
    pub rows_exported: u64,
    pub parquet_files: Vec<PathBuf>,
    pub manifest_file: Option<PathBuf>,
    pub checkpoint_file: Option<PathBuf>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum PartitionKey {
    Unpartitioned,
    ColumnValues(Vec<(String, String)>),
    BsnsYearReprtCode { bsns_year: i32, reprt_code: String },
    YearMonth { year: i32, month: u32 },
}

impl PartitionKey {
    fn from_bsns_reprt_row(
        row: &Row,
        bsns_year_index: usize,
        reprt_code_index: usize,
    ) -> Result<Self> {
        let bsns_year: i32 = row.try_get(bsns_year_index)?;
        let reprt_code: String = row.try_get(reprt_code_index)?;
        Ok(Self::BsnsYearReprtCode {
            bsns_year,
            reprt_code,
        })
    }

    fn from_date32_days(days_since_epoch: i32) -> Result<Self> {
        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).ok_or_else(|| {
            ExporterError::InvalidData("failed to construct unix epoch date".to_string())
        })?;
        let date = epoch
            .checked_add_signed(Duration::days(days_since_epoch as i64))
            .ok_or_else(|| {
                ExporterError::InvalidData(format!(
                    "date32 value {days_since_epoch} is outside supported range"
                ))
            })?;
        Ok(Self::YearMonth {
            year: date.year(),
            month: date.month(),
        })
    }

    fn partition_values(&self) -> HashMap<String, String> {
        match self {
            Self::Unpartitioned => HashMap::new(),
            Self::ColumnValues(values) => values.iter().cloned().collect(),
            Self::BsnsYearReprtCode {
                bsns_year,
                reprt_code,
            } => HashMap::from([
                ("bsns_year".to_string(), bsns_year.to_string()),
                ("reprt_code".to_string(), reprt_code.clone()),
            ]),
            Self::YearMonth { year, month } => HashMap::from([
                ("year".to_string(), year.to_string()),
                ("month".to_string(), format!("{month:02}")),
            ]),
        }
    }

    fn from_partition_values(values: &HashMap<String, String>) -> Result<Self> {
        if values.is_empty() {
            return Ok(Self::Unpartitioned);
        }

        if values.len() == 2 && values.contains_key("year") && values.contains_key("month") {
            let year = values
                .get("year")
                .expect("checked year key")
                .parse::<i32>()
                .map_err(|source| ExporterError::InvalidInteger {
                    name: "year",
                    source,
                })?;
            let month = values
                .get("month")
                .expect("checked month key")
                .parse::<u32>()
                .map_err(|source| ExporterError::InvalidInteger {
                    name: "month",
                    source,
                })?;
            return Ok(Self::YearMonth { year, month });
        }

        if values.len() == 2
            && values.contains_key("bsns_year")
            && values.contains_key("reprt_code")
        {
            let bsns_year = values
                .get("bsns_year")
                .expect("checked bsns_year key")
                .parse::<i32>()
                .map_err(|source| ExporterError::InvalidInteger {
                    name: "bsns_year",
                    source,
                })?;
            let reprt_code = values
                .get("reprt_code")
                .expect("checked reprt_code key")
                .clone();
            return Ok(Self::BsnsYearReprtCode {
                bsns_year,
                reprt_code,
            });
        }

        let mut generic_values = values
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect::<Vec<_>>();
        generic_values.sort_by(|left, right| left.0.cmp(&right.0));
        Ok(Self::ColumnValues(generic_values))
    }

    fn path_segments(&self) -> Vec<String> {
        match self {
            Self::Unpartitioned => Vec::new(),
            Self::ColumnValues(values) => values
                .iter()
                .map(|(key, value)| format!("{key}={}", encode_partition_value(value)))
                .collect(),
            Self::BsnsYearReprtCode {
                bsns_year,
                reprt_code,
            } => vec![
                format!("bsns_year={bsns_year}"),
                format!("reprt_code={}", encode_partition_value(reprt_code)),
            ],
            Self::YearMonth { year, month } => {
                vec![format!("year={year}"), format!("month={month:02}")]
            }
        }
    }
}

struct FullTableExportContext<'a> {
    db: &'a Db,
    runtime: &'a RuntimeConfig,
    defaults: &'a Defaults,
    table: &'a TableConfig,
    columns: &'a [ColumnInfo],
    run_id: &'a str,
    partition_indices: Vec<usize>,
    options: &'a FullTableExportOptions,
}

struct SnapshotItemsExportContext<'a> {
    db: &'a Db,
    runtime: &'a RuntimeConfig,
    defaults: &'a Defaults,
    table: &'a TableConfig,
    columns: &'a [ColumnInfo],
    run_id: &'a str,
    snapshot_date_index: usize,
    options: &'a SnapshotItemsExportOptions,
}

struct OpenPartitionWriter {
    final_dir: PathBuf,
    tmp_dir: PathBuf,
    part_number: u32,
    writer: Option<ArrowWriter<File>>,
    batch_builder: Option<RecordBatchBuilder>,
    rows: u64,
    min_raw_id: Option<i64>,
    max_raw_id: Option<i64>,
    partition_values: HashMap<String, String>,
    columns: Vec<ColumnInfo>,
    compression: String,
    row_group_rows: usize,
    batch_rows: usize,
    files: Vec<ManifestFile>,
    track_raw_id_bounds: bool,
}

struct PartitionWriterOptions<'a> {
    runtime: &'a RuntimeConfig,
    defaults: &'a Defaults,
    columns: &'a [ColumnInfo],
    run_id: &'a str,
    table: &'a str,
    batch_rows: usize,
    starting_part_number: u32,
    track_raw_id_bounds: bool,
}

struct ClosedPartitionWriter {
    next_part_number: u32,
    files: Vec<ManifestFile>,
}

impl OpenPartitionWriter {
    fn try_new(options: &PartitionWriterOptions<'_>, key: &PartitionKey) -> Result<Self> {
        let final_dir = data_partition_dir_path(options.runtime, options.table, key);
        let tmp_dir = tmp_partition_dir_path(options.runtime, options.run_id, options.table, key);

        fs::create_dir_all(&tmp_dir).map_err(|source| ExporterError::CreateDir {
            path: tmp_dir.clone(),
            source,
        })?;
        fs::create_dir_all(&final_dir).map_err(|source| ExporterError::CreateDir {
            path: final_dir.clone(),
            source,
        })?;

        let mut writer = Self {
            final_dir,
            tmp_dir,
            part_number: options.starting_part_number,
            writer: None,
            batch_builder: None,
            rows: 0,
            min_raw_id: None,
            max_raw_id: None,
            partition_values: key.partition_values(),
            columns: options.columns.to_vec(),
            compression: options.defaults.compression.clone(),
            row_group_rows: options.defaults.row_group_rows as usize,
            batch_rows: options.batch_rows,
            files: Vec::new(),
            track_raw_id_bounds: options.track_raw_id_bounds,
        };
        writer.open_current_part()?;
        Ok(writer)
    }

    fn append_row(&mut self, row: &Row, raw_id: i64, max_rows_per_file: Option<u64>) -> Result<()> {
        self.open_current_part()?;
        self.batch_builder
            .as_mut()
            .expect("partition writer must have an active batch builder")
            .append_row(row)?;
        self.rows += 1;
        if self.track_raw_id_bounds {
            self.min_raw_id = Some(self.min_raw_id.map_or(raw_id, |min| min.min(raw_id)));
            self.max_raw_id = Some(self.max_raw_id.map_or(raw_id, |max| max.max(raw_id)));
        }

        if self
            .batch_builder
            .as_ref()
            .is_some_and(|builder| builder.row_count() >= self.batch_rows)
        {
            self.flush()?;
        }
        if max_rows_per_file.is_some_and(|max_rows| self.rows >= max_rows) {
            self.finish_current_part()?;
        }
        Ok(())
    }

    fn open_current_part(&mut self) -> Result<()> {
        if self.writer.is_some() {
            return Ok(());
        }
        let tmp_file = self.current_tmp_file();
        let file = fs::File::create(&tmp_file).map_err(|source| ExporterError::WriteFile {
            path: tmp_file,
            source,
        })?;
        let batch_builder = RecordBatchBuilder::try_new(&self.columns, self.batch_rows)?;
        let writer = new_arrow_writer(
            file,
            batch_builder.schema(),
            &self.compression,
            self.row_group_rows,
        )?;

        self.batch_builder = Some(batch_builder);
        self.writer = Some(writer);
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        if self
            .batch_builder
            .as_ref()
            .is_none_or(RecordBatchBuilder::is_empty)
        {
            return Ok(());
        }
        let batch = self
            .batch_builder
            .as_mut()
            .expect("partition writer must have an active batch builder")
            .finish(self.batch_rows)?;
        self.writer
            .as_mut()
            .expect("partition writer must have an active parquet writer")
            .write(&batch)?;
        Ok(())
    }

    fn finish_current_part(&mut self) -> Result<()> {
        if self.writer.is_none() {
            return Ok(());
        }

        self.flush()?;
        let tmp_file = self.current_tmp_file();
        let final_file = self.current_final_file();
        let writer = self
            .writer
            .take()
            .expect("partition writer must have an active parquet writer");
        writer.close()?;
        self.batch_builder = None;
        fs::rename(&tmp_file, &final_file).map_err(|source| ExporterError::RenameFile {
            from: tmp_file.clone(),
            to: final_file.clone(),
            source,
        })?;

        let bytes = fs::metadata(&final_file)
            .map_err(|source| ExporterError::Metadata {
                path: final_file.clone(),
                source,
            })?
            .len();

        self.files.push(ManifestFile {
            path: final_file,
            rows: self.rows,
            bytes,
            partition_values: self.partition_values.clone(),
            min_raw_id: self.min_raw_id,
            max_raw_id: self.max_raw_id,
        });
        self.part_number += 1;
        self.rows = 0;
        self.min_raw_id = None;
        self.max_raw_id = None;
        Ok(())
    }

    fn close(mut self) -> Result<ClosedPartitionWriter> {
        self.finish_current_part()?;
        Ok(ClosedPartitionWriter {
            next_part_number: self.part_number,
            files: self.files,
        })
    }

    fn current_final_file(&self) -> PathBuf {
        self.final_dir.join(part_file_name(self.part_number))
    }

    fn current_tmp_file(&self) -> PathBuf {
        self.tmp_dir
            .join(format!("{}.tmp", part_file_name(self.part_number)))
    }
}

struct ChunkExportContext<'a> {
    db: &'a Db,
    runtime: &'a RuntimeConfig,
    defaults: &'a Defaults,
    table: &'a TableConfig,
    columns: &'a [ColumnInfo],
    run_id: &'a str,
    raw_id_index: usize,
    bsns_year_index: usize,
    reprt_code_index: usize,
    options: &'a DartXbrlExportOptions,
}

struct DateMonthExportContext<'a> {
    db: &'a Db,
    runtime: &'a RuntimeConfig,
    defaults: &'a Defaults,
    table: &'a TableConfig,
    columns: &'a [ColumnInfo],
    run_id: &'a str,
    date_column: &'a str,
    date_column_index: usize,
    options: &'a DateMonthExportOptions,
}

struct ChunkExportResult {
    rows_exported: u64,
    files: Vec<ManifestFile>,
    partition_next_parts: HashMap<PartitionKey, u32>,
}

struct CheckpointWriteState<'a> {
    completed: bool,
    run_id: &'a str,
    table: &'a TableConfig,
    source: &'a ManifestSource,
    schema: &'a ManifestSchema,
    extract_predicate: &'a str,
    extract_start_raw_id: i64,
    final_exclusive_end: i64,
    next_raw_id: i64,
    options: &'a DartXbrlExportOptions,
    chunks_planned: u64,
    chunks_completed: u64,
    rows_exported: u64,
    files: &'a [ManifestFile],
    partition_next_parts: &'a HashMap<PartitionKey, u32>,
    manifest_file: Option<PathBuf>,
}

struct DateCheckpointWriteState<'a> {
    completed: bool,
    run_id: &'a str,
    table: &'a TableConfig,
    source: &'a ManifestSource,
    schema: &'a ManifestSchema,
    extract_predicate: &'a str,
    date_column: &'a str,
    date_start: NaiveDate,
    date_final_exclusive_end: NaiveDate,
    date_next_start: NaiveDate,
    options: &'a DateMonthExportOptions,
    chunks_planned: u64,
    chunks_completed: u64,
    rows_exported: u64,
    files: &'a [ManifestFile],
    partition_next_parts: &'a HashMap<PartitionKey, u32>,
    manifest_file: Option<PathBuf>,
}

struct ZeroRowManifestRequest<'a> {
    runtime: &'a RuntimeConfig,
    table: &'a TableConfig,
    source: &'a ManifestSource,
    schema: &'a ManifestSchema,
    run_id: String,
    predicate: String,
    dry_run: bool,
    force: bool,
    chunks_planned: u64,
    chunks_exported: u64,
}

pub async fn export_raw_id_partitioned_table(
    db: &Db,
    runtime: &RuntimeConfig,
    defaults: &Defaults,
    table: &TableConfig,
    columns: &[ColumnInfo],
    bounds: &TableBounds,
    options: &DartXbrlExportOptions,
) -> Result<ExportResult> {
    if table.extract_strategy != ExtractStrategy::RawIdRange {
        return Err(ExporterError::InvalidConfig(
            "raw-id partitioned export requires extract_strategy=raw_id_range".to_string(),
        ));
    }
    if table.output_partitions != ["bsns_year".to_string(), "reprt_code".to_string()] {
        return Err(ExporterError::InvalidConfig(format!(
            "{} export currently requires output_partitions = [\"bsns_year\", \"reprt_code\"]",
            table.name
        )));
    }
    if options.chunk_rows <= 0 {
        return Err(ExporterError::InvalidConfig(
            "chunk_rows must be positive".to_string(),
        ));
    }
    if options.batch_rows == 0 {
        return Err(ExporterError::InvalidConfig(
            "batch_rows must be positive".to_string(),
        ));
    }
    if options.max_rows_per_file == Some(0) {
        return Err(ExporterError::InvalidConfig(
            "max_rows_per_file must be positive".to_string(),
        ));
    }

    let (source_min, source_max) = match bounds {
        TableBounds::RawId { min, max } => (*min, *max),
        _ => {
            return Err(ExporterError::InvalidConfig(
                "raw-id partitioned export requires raw_id bounds".to_string(),
            ))
        }
    };
    let Some(source_min) = source_min else {
        return Err(ExporterError::InvalidConfig(format!(
            "{} has no rows to export",
            table.name
        )));
    };
    let Some(source_max) = source_max else {
        return Err(ExporterError::InvalidConfig(format!(
            "{} has no rows to export",
            table.name
        )));
    };

    let source_exclusive_end = source_max.saturating_add(1);
    let source = ManifestSource {
        name: runtime.source.name.clone(),
        schema: runtime.source.schema.clone(),
        snapshot_date: runtime.output.snapshot_date.clone(),
        snapshot_policy: "per_chunk_read_committed".to_string(),
    };
    let manifest_schema = build_manifest_schema(columns)?;
    let (
        run_id,
        extract_start_raw_id,
        mut chunk_start,
        final_exclusive_end,
        planned_chunks,
        mut chunks_exported,
        mut rows_exported,
        mut files,
        mut partition_next_parts,
        checkpoint_file,
    ) = if let Some(resume) = &options.resume {
        validate_resume_checkpoint(
            &resume.checkpoint,
            runtime,
            table,
            source_exclusive_end,
            options,
            &manifest_schema,
        )?;
        cleanup_resume_outputs(runtime, table, &resume.checkpoint)?;
        (
            resume.checkpoint.run_id.clone(),
            resume.checkpoint.extract_start_raw_id,
            resume.checkpoint.next_raw_id,
            resume.checkpoint.final_exclusive_end,
            resume.checkpoint.chunks_planned,
            resume.checkpoint.chunks_completed,
            resume.checkpoint.rows_exported,
            resume.checkpoint.files.clone(),
            partition_next_parts_from_checkpoint(&resume.checkpoint)?,
            resume.checkpoint_file.clone(),
        )
    } else {
        let start = options.start_raw_id.unwrap_or(source_min);
        if start < source_min || start > source_max {
            return Err(ExporterError::InvalidConfig(format!(
                "start_raw_id {start} is outside source bounds {source_min}..={source_max}"
            )));
        }
        let final_exclusive_end = resolve_final_exclusive_end(start, source_max, options);
        let planned_chunks = planned_chunk_count(start, final_exclusive_end, options.chunk_rows);
        let run_id = new_run_id(&table.name);
        (
            run_id.clone(),
            start,
            start,
            final_exclusive_end,
            planned_chunks,
            0,
            0,
            Vec::new(),
            HashMap::new(),
            checkpoint_file_path(runtime, &run_id),
        )
    };
    let predicate = format!("raw_id >= {extract_start_raw_id} AND raw_id < {final_exclusive_end}");

    if options.dry_run {
        return Ok(ExportResult {
            run_id,
            dry_run: true,
            table: table.name.clone(),
            extract_predicate: predicate,
            chunks_planned: planned_chunks,
            chunks_exported: 0,
            rows_exported: 0,
            parquet_files: Vec::new(),
            manifest_file: None,
            checkpoint_file: options
                .resume
                .as_ref()
                .map(|resume| resume.checkpoint_file.clone()),
        });
    }

    let table_output_dir = table_output_dir(runtime, &table.name);
    if options.resume.is_none() && table_output_dir.exists() {
        if options.force {
            fs::remove_dir_all(&table_output_dir).map_err(|source| ExporterError::RemoveFile {
                path: table_output_dir.clone(),
                source,
            })?;
        } else if contains_parquet_files(&table_output_dir)? {
            return Err(ExporterError::InvalidConfig(format!(
                "output directory already contains parquet files: {} (use --force to overwrite)",
                table_output_dir.display()
            )));
        }
    }

    let raw_id_index = column_index(columns, "raw_id")?;
    let bsns_year_index = column_index(columns, "bsns_year")?;
    let reprt_code_index = column_index(columns, "reprt_code")?;
    let chunk_context = ChunkExportContext {
        db,
        runtime,
        defaults,
        table,
        columns,
        run_id: &run_id,
        raw_id_index,
        bsns_year_index,
        reprt_code_index,
        options,
    };

    write_progress_checkpoint(
        &checkpoint_file,
        CheckpointWriteState {
            completed: false,
            run_id: &run_id,
            table,
            source: &source,
            schema: &manifest_schema,
            extract_predicate: &predicate,
            extract_start_raw_id,
            final_exclusive_end,
            next_raw_id: chunk_start,
            options,
            chunks_planned: planned_chunks,
            chunks_completed: chunks_exported,
            rows_exported,
            files: &files,
            partition_next_parts: &partition_next_parts,
            manifest_file: None,
        },
    )?;

    while chunk_start < final_exclusive_end {
        let chunk_exclusive_end =
            next_chunk_exclusive_end(chunk_start, final_exclusive_end, options.chunk_rows);
        let chunk_result = export_raw_id_range(
            &chunk_context,
            chunk_start,
            chunk_exclusive_end,
            &partition_next_parts,
        )
        .await?;
        rows_exported += chunk_result.rows_exported;
        chunks_exported += 1;
        files.extend(chunk_result.files);
        for (key, next_part_number) in chunk_result.partition_next_parts {
            partition_next_parts.insert(key, next_part_number);
        }
        tracing::info!(
            chunk = chunks_exported,
            planned_chunks,
            start_raw_id = chunk_start,
            exclusive_end = chunk_exclusive_end,
            rows = chunk_result.rows_exported,
            "exported raw_id chunk"
        );
        chunk_start = chunk_exclusive_end;
        write_progress_checkpoint(
            &checkpoint_file,
            CheckpointWriteState {
                completed: false,
                run_id: &run_id,
                table,
                source: &source,
                schema: &manifest_schema,
                extract_predicate: &predicate,
                extract_start_raw_id,
                final_exclusive_end,
                next_raw_id: chunk_start,
                options,
                chunks_planned: planned_chunks,
                chunks_completed: chunks_exported,
                rows_exported,
                files: &files,
                partition_next_parts: &partition_next_parts,
                manifest_file: None,
            },
        )?;
    }

    files.sort_by(|left, right| left.path.cmp(&right.path));
    let parquet_files = files
        .iter()
        .map(|file| file.path.clone())
        .collect::<Vec<_>>();
    let min_raw_id = files.iter().filter_map(|file| file.min_raw_id).min();
    let max_raw_id = files.iter().filter_map(|file| file.max_raw_id).max();

    let manifest_file = manifest_file_path(runtime, &table.name);
    let manifest = ExportManifest {
        run_id: run_id.clone(),
        created_at_unix_seconds: unix_seconds_now(),
        source: source.clone(),
        table: ManifestTable {
            name: table.name.clone(),
            schema: Some(manifest_schema.clone()),
            rows_exported,
            files,
            extract_predicate: predicate.clone(),
            min_raw_id,
            max_raw_id,
        },
    };
    write_manifest(&manifest_file, &manifest)?;
    write_progress_checkpoint(
        &checkpoint_file,
        CheckpointWriteState {
            completed: true,
            run_id: &run_id,
            table,
            source: &source,
            schema: &manifest_schema,
            extract_predicate: &predicate,
            extract_start_raw_id,
            final_exclusive_end,
            next_raw_id: final_exclusive_end,
            options,
            chunks_planned: planned_chunks,
            chunks_completed: chunks_exported,
            rows_exported,
            files: &manifest.table.files,
            partition_next_parts: &partition_next_parts,
            manifest_file: Some(manifest_file.clone()),
        },
    )?;

    Ok(ExportResult {
        run_id,
        dry_run: false,
        table: table.name.clone(),
        extract_predicate: predicate,
        chunks_planned: planned_chunks,
        chunks_exported,
        rows_exported,
        parquet_files,
        manifest_file: Some(manifest_file),
        checkpoint_file: Some(checkpoint_file),
    })
}

pub async fn export_date_month_partitioned_table(
    db: &Db,
    runtime: &RuntimeConfig,
    defaults: &Defaults,
    table: &TableConfig,
    columns: &[ColumnInfo],
    bounds: &TableBounds,
    options: &DateMonthExportOptions,
) -> Result<ExportResult> {
    if table.extract_strategy != ExtractStrategy::DateMonth {
        return Err(ExporterError::InvalidConfig(
            "date-month export requires extract_strategy=date_month".to_string(),
        ));
    }
    if table.output_partitions
        != [
            "year(trade_date)".to_string(),
            "month(trade_date)".to_string(),
        ]
    {
        return Err(ExporterError::InvalidConfig(format!(
            "{} export currently requires output_partitions = [\"year(trade_date)\", \"month(trade_date)\"]",
            table.name
        )));
    }
    if options.batch_rows == 0 {
        return Err(ExporterError::InvalidConfig(
            "batch_rows must be positive".to_string(),
        ));
    }
    if options.max_rows_per_file == Some(0) {
        return Err(ExporterError::InvalidConfig(
            "max_rows_per_file must be positive".to_string(),
        ));
    }

    let date_column = table.date_column.as_deref().ok_or_else(|| {
        ExporterError::InvalidConfig(format!(
            "{} uses date_month but date_column is missing",
            table.name
        ))
    })?;
    let manifest_schema = build_manifest_schema(columns)?;
    let source = ManifestSource {
        name: runtime.source.name.clone(),
        schema: runtime.source.schema.clone(),
        snapshot_date: runtime.output.snapshot_date.clone(),
        snapshot_policy: "per_month_read_committed".to_string(),
    };
    let (
        run_id,
        first_start,
        final_exclusive_end,
        next_start,
        planned_chunks,
        mut chunks_exported,
        mut rows_exported,
        mut files,
        mut partition_next_parts,
        checkpoint_file,
        predicate,
    ) = if let Some(resume) = &options.resume {
        validate_date_month_resume_checkpoint(
            &resume.checkpoint,
            runtime,
            table,
            bounds,
            date_column,
            options,
            &manifest_schema,
        )?;
        cleanup_resume_outputs(runtime, table, &resume.checkpoint)?;
        let first_start = checkpoint_date(&resume.checkpoint.date_start, "date_start")?;
        let final_exclusive_end = checkpoint_date(
            &resume.checkpoint.date_final_exclusive_end,
            "date_final_exclusive_end",
        )?;
        let next_start = checkpoint_date(&resume.checkpoint.date_next_start, "date_next_start")?;
        (
            resume.checkpoint.run_id.clone(),
            first_start,
            final_exclusive_end,
            next_start,
            resume.checkpoint.chunks_planned,
            resume.checkpoint.chunks_completed,
            resume.checkpoint.rows_exported,
            resume.checkpoint.files.clone(),
            partition_next_parts_from_checkpoint(&resume.checkpoint)?,
            resume.checkpoint_file.clone(),
            resume.checkpoint.extract_predicate.clone(),
        )
    } else {
        let ranges = date_month_ranges(bounds, options)?;
        if ranges.is_empty() {
            let run_id = new_run_id(&table.name);
            let predicate = format!("{date_column} has no source rows");
            return export_zero_row_manifest(ZeroRowManifestRequest {
                runtime,
                table,
                source: &source,
                schema: &manifest_schema,
                run_id,
                predicate,
                dry_run: options.dry_run,
                force: options.force,
                chunks_planned: 0,
                chunks_exported: 0,
            });
        }
        let Some((first_start, _)) = ranges.first() else {
            return Err(ExporterError::InvalidConfig(format!(
                "{} has no date_month ranges to export",
                table.name
            )));
        };
        let Some((_, final_exclusive_end)) = ranges.last() else {
            return Err(ExporterError::InvalidConfig(format!(
                "{} has no date_month ranges to export",
                table.name
            )));
        };
        let predicate = format!(
            "{date_column} >= DATE '{}' AND {date_column} < DATE '{}'",
            first_start, final_exclusive_end
        );
        let run_id = new_run_id(&table.name);
        (
            run_id.clone(),
            *first_start,
            *final_exclusive_end,
            *first_start,
            ranges.len() as u64,
            0,
            0,
            Vec::new(),
            HashMap::new(),
            checkpoint_file_path(runtime, &run_id),
            predicate,
        )
    };

    if options.dry_run {
        return Ok(ExportResult {
            run_id,
            dry_run: true,
            table: table.name.clone(),
            extract_predicate: predicate,
            chunks_planned: planned_chunks,
            chunks_exported: 0,
            rows_exported: 0,
            parquet_files: Vec::new(),
            manifest_file: None,
            checkpoint_file: None,
        });
    }

    let table_output_dir = table_output_dir(runtime, &table.name);
    if options.resume.is_none() && table_output_dir.exists() {
        if options.force {
            fs::remove_dir_all(&table_output_dir).map_err(|source| ExporterError::RemoveFile {
                path: table_output_dir.clone(),
                source,
            })?;
        } else if contains_parquet_files(&table_output_dir)? {
            return Err(ExporterError::InvalidConfig(format!(
                "output directory already contains parquet files: {} (use --force to overwrite)",
                table_output_dir.display()
            )));
        }
    }

    let date_column_index = column_index(columns, date_column)?;
    let context = DateMonthExportContext {
        db,
        runtime,
        defaults,
        table,
        columns,
        run_id: &run_id,
        date_column,
        date_column_index,
        options,
    };

    write_date_month_checkpoint(
        &checkpoint_file,
        DateCheckpointWriteState {
            completed: false,
            run_id: &run_id,
            table,
            source: &source,
            schema: &manifest_schema,
            extract_predicate: &predicate,
            date_column,
            date_start: first_start,
            date_final_exclusive_end: final_exclusive_end,
            date_next_start: next_start,
            options,
            chunks_planned: planned_chunks,
            chunks_completed: chunks_exported,
            rows_exported,
            files: &files,
            partition_next_parts: &partition_next_parts,
            manifest_file: None,
        },
    )?;

    let ranges = date_month_ranges_from(next_start, final_exclusive_end)?;
    for (start_date, exclusive_end) in ranges {
        let chunk_result =
            export_date_range(&context, start_date, exclusive_end, &partition_next_parts).await?;
        rows_exported += chunk_result.rows_exported;
        chunks_exported += 1;
        files.extend(chunk_result.files);
        for (key, next_part_number) in chunk_result.partition_next_parts {
            partition_next_parts.insert(key, next_part_number);
        }
        tracing::info!(
            chunk = chunks_exported,
            planned_chunks,
            %start_date,
            %exclusive_end,
            rows = chunk_result.rows_exported,
            "exported date_month chunk"
        );
        let next_start = exclusive_end;
        write_date_month_checkpoint(
            &checkpoint_file,
            DateCheckpointWriteState {
                completed: false,
                run_id: &run_id,
                table,
                source: &source,
                schema: &manifest_schema,
                extract_predicate: &predicate,
                date_column,
                date_start: first_start,
                date_final_exclusive_end: final_exclusive_end,
                date_next_start: next_start,
                options,
                chunks_planned: planned_chunks,
                chunks_completed: chunks_exported,
                rows_exported,
                files: &files,
                partition_next_parts: &partition_next_parts,
                manifest_file: None,
            },
        )?;
    }

    files.sort_by(|left, right| left.path.cmp(&right.path));
    let parquet_files = files
        .iter()
        .map(|file| file.path.clone())
        .collect::<Vec<_>>();
    let manifest_file = manifest_file_path(runtime, &table.name);
    let manifest = ExportManifest {
        run_id: run_id.clone(),
        created_at_unix_seconds: unix_seconds_now(),
        source: source.clone(),
        table: ManifestTable {
            name: table.name.clone(),
            schema: Some(manifest_schema.clone()),
            rows_exported,
            files,
            extract_predicate: predicate.clone(),
            min_raw_id: None,
            max_raw_id: None,
        },
    };
    write_manifest(&manifest_file, &manifest)?;
    write_date_month_checkpoint(
        &checkpoint_file,
        DateCheckpointWriteState {
            completed: true,
            run_id: &run_id,
            table,
            source: &source,
            schema: &manifest_schema,
            extract_predicate: &predicate,
            date_column,
            date_start: first_start,
            date_final_exclusive_end: final_exclusive_end,
            date_next_start: final_exclusive_end,
            options,
            chunks_planned: planned_chunks,
            chunks_completed: chunks_exported,
            rows_exported,
            files: &manifest.table.files,
            partition_next_parts: &partition_next_parts,
            manifest_file: Some(manifest_file.clone()),
        },
    )?;

    Ok(ExportResult {
        run_id,
        dry_run: false,
        table: table.name.clone(),
        extract_predicate: predicate,
        chunks_planned: planned_chunks,
        chunks_exported,
        rows_exported,
        parquet_files,
        manifest_file: Some(manifest_file),
        checkpoint_file: Some(checkpoint_file),
    })
}

pub async fn export_full_table(
    db: &Db,
    runtime: &RuntimeConfig,
    defaults: &Defaults,
    table: &TableConfig,
    columns: &[ColumnInfo],
    bounds: &TableBounds,
    options: &FullTableExportOptions,
) -> Result<ExportResult> {
    if table.extract_strategy != ExtractStrategy::FullTable {
        return Err(ExporterError::InvalidConfig(
            "full-table export requires extract_strategy=full_table".to_string(),
        ));
    }
    if !matches!(bounds, TableBounds::FullTable) {
        return Err(ExporterError::InvalidConfig(
            "full-table export requires full_table bounds".to_string(),
        ));
    }
    if options.batch_rows == 0 {
        return Err(ExporterError::InvalidConfig(
            "batch_rows must be positive".to_string(),
        ));
    }
    if options.max_rows_per_file == Some(0) {
        return Err(ExporterError::InvalidConfig(
            "max_rows_per_file must be positive".to_string(),
        ));
    }

    let run_id = new_run_id(&table.name);
    let predicate = "full table".to_string();
    let source = ManifestSource {
        name: runtime.source.name.clone(),
        schema: runtime.source.schema.clone(),
        snapshot_date: runtime.output.snapshot_date.clone(),
        snapshot_policy: "full_table_read_committed".to_string(),
    };
    let manifest_schema = build_manifest_schema(columns)?;
    let partition_indices = full_table_partition_indices(table, columns)?;

    if options.dry_run {
        return Ok(ExportResult {
            run_id,
            dry_run: true,
            table: table.name.clone(),
            extract_predicate: predicate,
            chunks_planned: 1,
            chunks_exported: 0,
            rows_exported: 0,
            parquet_files: Vec::new(),
            manifest_file: None,
            checkpoint_file: None,
        });
    }

    let output_dir = table_output_dir(runtime, &table.name);
    if output_dir.exists() {
        if options.force {
            fs::remove_dir_all(&output_dir).map_err(|source| ExporterError::RemoveFile {
                path: output_dir.clone(),
                source,
            })?;
        } else if contains_parquet_files(&output_dir)? {
            return Err(ExporterError::InvalidConfig(format!(
                "output directory already contains parquet files: {} (use --force to overwrite)",
                output_dir.display()
            )));
        }
    }

    let context = FullTableExportContext {
        db,
        runtime,
        defaults,
        table,
        columns,
        run_id: &run_id,
        partition_indices,
        options,
    };
    let chunk_result = export_full_table_rows(&context).await?;
    let mut files = chunk_result.files;
    files.sort_by(|left, right| left.path.cmp(&right.path));
    let parquet_files = files
        .iter()
        .map(|file| file.path.clone())
        .collect::<Vec<_>>();
    let rows_exported = chunk_result.rows_exported;

    let manifest_file = manifest_file_path(runtime, &table.name);
    let manifest = ExportManifest {
        run_id: run_id.clone(),
        created_at_unix_seconds: unix_seconds_now(),
        source,
        table: ManifestTable {
            name: table.name.clone(),
            schema: Some(manifest_schema),
            rows_exported,
            files,
            extract_predicate: predicate.clone(),
            min_raw_id: None,
            max_raw_id: None,
        },
    };
    write_manifest(&manifest_file, &manifest)?;

    Ok(ExportResult {
        run_id,
        dry_run: false,
        table: table.name.clone(),
        extract_predicate: predicate,
        chunks_planned: 1,
        chunks_exported: 1,
        rows_exported,
        parquet_files,
        manifest_file: Some(manifest_file),
        checkpoint_file: None,
    })
}

pub async fn export_snapshot_items(
    db: &Db,
    runtime: &RuntimeConfig,
    defaults: &Defaults,
    table: &TableConfig,
    columns: &[ColumnInfo],
    bounds: &TableBounds,
    options: &SnapshotItemsExportOptions,
) -> Result<ExportResult> {
    if table.extract_strategy != ExtractStrategy::SnapshotItems {
        return Err(ExporterError::InvalidConfig(
            "snapshot-items export requires extract_strategy=snapshot_items".to_string(),
        ));
    }
    if !matches!(bounds, TableBounds::SnapshotItems) {
        return Err(ExporterError::InvalidConfig(
            "snapshot-items export requires snapshot_items bounds".to_string(),
        ));
    }
    if table.output_partitions != ["snapshot_date(as_of_date)".to_string()] {
        return Err(ExporterError::InvalidConfig(format!(
            "{} snapshot_items export currently requires output_partitions = [\"snapshot_date(as_of_date)\"]",
            table.name
        )));
    }
    if table.name != "stock_master_snapshot_items" {
        return Err(ExporterError::InvalidConfig(format!(
            "snapshot_items export currently supports stock_master_snapshot_items, got {}",
            table.name
        )));
    }
    if options.batch_rows == 0 {
        return Err(ExporterError::InvalidConfig(
            "batch_rows must be positive".to_string(),
        ));
    }
    if options.max_rows_per_file == Some(0) {
        return Err(ExporterError::InvalidConfig(
            "max_rows_per_file must be positive".to_string(),
        ));
    }
    let _ = column_index(columns, "snapshot_id")?;
    for column in &table.order_by {
        let _ = column_index(columns, column)?;
    }

    let run_id = new_run_id(&table.name);
    let predicate =
        "stock_master_snapshot_items joined to stock_master_snapshot by snapshot_id".to_string();
    let source = ManifestSource {
        name: runtime.source.name.clone(),
        schema: runtime.source.schema.clone(),
        snapshot_date: runtime.output.snapshot_date.clone(),
        snapshot_policy: "snapshot_items_read_committed".to_string(),
    };
    let manifest_schema = build_manifest_schema(columns)?;

    if options.dry_run {
        return Ok(ExportResult {
            run_id,
            dry_run: true,
            table: table.name.clone(),
            extract_predicate: predicate,
            chunks_planned: 1,
            chunks_exported: 0,
            rows_exported: 0,
            parquet_files: Vec::new(),
            manifest_file: None,
            checkpoint_file: None,
        });
    }

    let output_dir = table_output_dir(runtime, &table.name);
    if output_dir.exists() {
        if options.force {
            fs::remove_dir_all(&output_dir).map_err(|source| ExporterError::RemoveFile {
                path: output_dir.clone(),
                source,
            })?;
        } else if contains_parquet_files(&output_dir)? {
            return Err(ExporterError::InvalidConfig(format!(
                "output directory already contains parquet files: {} (use --force to overwrite)",
                output_dir.display()
            )));
        }
    }

    let context = SnapshotItemsExportContext {
        db,
        runtime,
        defaults,
        table,
        columns,
        run_id: &run_id,
        snapshot_date_index: columns.len(),
        options,
    };
    let chunk_result = export_snapshot_item_rows(&context).await?;
    let mut files = chunk_result.files;
    files.sort_by(|left, right| left.path.cmp(&right.path));
    let parquet_files = files
        .iter()
        .map(|file| file.path.clone())
        .collect::<Vec<_>>();
    let rows_exported = chunk_result.rows_exported;

    let manifest_file = manifest_file_path(runtime, &table.name);
    let manifest = ExportManifest {
        run_id: run_id.clone(),
        created_at_unix_seconds: unix_seconds_now(),
        source,
        table: ManifestTable {
            name: table.name.clone(),
            schema: Some(manifest_schema),
            rows_exported,
            files,
            extract_predicate: predicate.clone(),
            min_raw_id: None,
            max_raw_id: None,
        },
    };
    write_manifest(&manifest_file, &manifest)?;

    Ok(ExportResult {
        run_id,
        dry_run: false,
        table: table.name.clone(),
        extract_predicate: predicate,
        chunks_planned: 1,
        chunks_exported: 1,
        rows_exported,
        parquet_files,
        manifest_file: Some(manifest_file),
        checkpoint_file: None,
    })
}

pub fn export_empty_table(
    runtime: &RuntimeConfig,
    table: &TableConfig,
    columns: &[ColumnInfo],
    bounds: &TableBounds,
    options: &SchemaOnlyExportOptions,
) -> Result<ExportResult> {
    if table.extract_strategy != ExtractStrategy::EmptyTable {
        return Err(ExporterError::InvalidConfig(
            "schema-only export requires extract_strategy=empty_table".to_string(),
        ));
    }
    if !matches!(bounds, TableBounds::EmptyTable) {
        return Err(ExporterError::InvalidConfig(
            "schema-only export requires empty_table bounds".to_string(),
        ));
    }

    let run_id = new_run_id(&table.name);
    let predicate = "empty table schema-only".to_string();
    if options.dry_run {
        return Ok(ExportResult {
            run_id,
            dry_run: true,
            table: table.name.clone(),
            extract_predicate: predicate,
            chunks_planned: 0,
            chunks_exported: 0,
            rows_exported: 0,
            parquet_files: Vec::new(),
            manifest_file: None,
            checkpoint_file: None,
        });
    }

    let output_dir = table_output_dir(runtime, &table.name);
    if output_dir.exists() {
        if options.force {
            fs::remove_dir_all(&output_dir).map_err(|source| ExporterError::RemoveFile {
                path: output_dir.clone(),
                source,
            })?;
        } else if contains_parquet_files(&output_dir)? {
            return Err(ExporterError::InvalidConfig(format!(
                "output directory already contains parquet files: {} (use --force to overwrite)",
                output_dir.display()
            )));
        }
    }

    let source = ManifestSource {
        name: runtime.source.name.clone(),
        schema: runtime.source.schema.clone(),
        snapshot_date: runtime.output.snapshot_date.clone(),
        snapshot_policy: "schema_only".to_string(),
    };
    let manifest_schema = build_manifest_schema(columns)?;
    let manifest_file = manifest_file_path(runtime, &table.name);
    let manifest = ExportManifest {
        run_id: run_id.clone(),
        created_at_unix_seconds: unix_seconds_now(),
        source,
        table: ManifestTable {
            name: table.name.clone(),
            schema: Some(manifest_schema),
            rows_exported: 0,
            files: Vec::new(),
            extract_predicate: predicate.clone(),
            min_raw_id: None,
            max_raw_id: None,
        },
    };
    write_manifest(&manifest_file, &manifest)?;

    Ok(ExportResult {
        run_id,
        dry_run: false,
        table: table.name.clone(),
        extract_predicate: predicate,
        chunks_planned: 0,
        chunks_exported: 0,
        rows_exported: 0,
        parquet_files: Vec::new(),
        manifest_file: Some(manifest_file),
        checkpoint_file: None,
    })
}

async fn export_raw_id_range(
    context: &ChunkExportContext<'_>,
    start: i64,
    exclusive_end: i64,
    partition_next_parts: &HashMap<PartitionKey, u32>,
) -> Result<ChunkExportResult> {
    let sql = build_select_sql(
        context.runtime.source.schema.as_str(),
        context.table,
        context.columns,
        start,
        exclusive_end,
    )?;
    let stream = context
        .db
        .client()
        .query_raw(sql.as_str(), std::iter::empty::<&str>())
        .await?;
    pin_mut!(stream);

    let mut rows_exported = 0_u64;
    let mut writers: HashMap<PartitionKey, OpenPartitionWriter> = HashMap::new();
    while let Some(row) = stream.try_next().await? {
        let raw_id: i64 = row.try_get(context.raw_id_index)?;
        let key = PartitionKey::from_bsns_reprt_row(
            &row,
            context.bsns_year_index,
            context.reprt_code_index,
        )?;
        if !writers.contains_key(&key) {
            let starting_part_number = partition_next_parts.get(&key).copied().unwrap_or(0);
            let writer_options = PartitionWriterOptions {
                runtime: context.runtime,
                defaults: context.defaults,
                columns: context.columns,
                run_id: context.run_id,
                table: &context.table.name,
                batch_rows: context.options.batch_rows,
                starting_part_number,
                track_raw_id_bounds: true,
            };
            let writer = OpenPartitionWriter::try_new(&writer_options, &key)?;
            writers.insert(key.clone(), writer);
        }
        let writer = writers
            .get_mut(&key)
            .expect("partition writer must exist after insertion");
        writer.append_row(&row, raw_id, context.options.max_rows_per_file)?;
        rows_exported += 1;
    }

    let mut files = Vec::new();
    let mut partition_next_parts = HashMap::new();
    for (key, writer) in writers {
        let closed_writer = writer.close()?;
        partition_next_parts.insert(key, closed_writer.next_part_number);
        files.extend(closed_writer.files);
    }

    Ok(ChunkExportResult {
        rows_exported,
        files,
        partition_next_parts,
    })
}

async fn export_date_range(
    context: &DateMonthExportContext<'_>,
    start_date: NaiveDate,
    exclusive_end: NaiveDate,
    partition_next_parts: &HashMap<PartitionKey, u32>,
) -> Result<ChunkExportResult> {
    let sql = build_date_select_sql(
        context.runtime.source.schema.as_str(),
        context.table,
        context.columns,
        context.date_column,
        start_date,
        exclusive_end,
    )?;
    let stream = context
        .db
        .client()
        .query_raw(sql.as_str(), std::iter::empty::<&str>())
        .await?;
    pin_mut!(stream);

    let mut rows_exported = 0_u64;
    let mut writers: HashMap<PartitionKey, OpenPartitionWriter> = HashMap::new();
    while let Some(row) = stream.try_next().await? {
        let date32_days: i32 = row.try_get(context.date_column_index)?;
        let key = PartitionKey::from_date32_days(date32_days)?;
        if !writers.contains_key(&key) {
            let starting_part_number = partition_next_parts.get(&key).copied().unwrap_or(0);
            let writer_options = PartitionWriterOptions {
                runtime: context.runtime,
                defaults: context.defaults,
                columns: context.columns,
                run_id: context.run_id,
                table: &context.table.name,
                batch_rows: context.options.batch_rows,
                starting_part_number,
                track_raw_id_bounds: false,
            };
            let writer = OpenPartitionWriter::try_new(&writer_options, &key)?;
            writers.insert(key.clone(), writer);
        }
        let writer = writers
            .get_mut(&key)
            .expect("partition writer must exist after insertion");
        writer.append_row(&row, date32_days as i64, context.options.max_rows_per_file)?;
        rows_exported += 1;
    }

    let mut files = Vec::new();
    let mut partition_next_parts = HashMap::new();
    for (key, writer) in writers {
        let closed_writer = writer.close()?;
        partition_next_parts.insert(key, closed_writer.next_part_number);
        files.extend(closed_writer.files);
    }

    Ok(ChunkExportResult {
        rows_exported,
        files,
        partition_next_parts,
    })
}

async fn export_full_table_rows(context: &FullTableExportContext<'_>) -> Result<ChunkExportResult> {
    let sql = build_full_table_select_sql(
        context.runtime.source.schema.as_str(),
        context.table,
        context.columns,
    )?;
    let stream = context
        .db
        .client()
        .query_raw(sql.as_str(), std::iter::empty::<&str>())
        .await?;
    pin_mut!(stream);

    let writer_options = PartitionWriterOptions {
        runtime: context.runtime,
        defaults: context.defaults,
        columns: context.columns,
        run_id: context.run_id,
        table: &context.table.name,
        batch_rows: context.options.batch_rows,
        starting_part_number: 0,
        track_raw_id_bounds: false,
    };
    let mut writers: HashMap<PartitionKey, OpenPartitionWriter> = HashMap::new();
    let mut rows_exported = 0_u64;
    while let Some(row) = stream.try_next().await? {
        let key = full_table_partition_key(&row, context.columns, &context.partition_indices)?;
        if !writers.contains_key(&key) {
            let writer = OpenPartitionWriter::try_new(&writer_options, &key)?;
            writers.insert(key.clone(), writer);
        }
        writers
            .get_mut(&key)
            .expect("full-table writer must exist after insertion")
            .append_row(&row, 0, context.options.max_rows_per_file)?;
        rows_exported += 1;
    }

    let mut files = Vec::new();
    for (_, writer) in writers {
        files.extend(writer.close()?.files);
    }

    Ok(ChunkExportResult {
        rows_exported,
        files,
        partition_next_parts: HashMap::new(),
    })
}

async fn export_snapshot_item_rows(
    context: &SnapshotItemsExportContext<'_>,
) -> Result<ChunkExportResult> {
    let sql = build_snapshot_items_select_sql(
        context.runtime.source.schema.as_str(),
        context.table,
        context.columns,
    )?;
    let stream = context
        .db
        .client()
        .query_raw(sql.as_str(), std::iter::empty::<&str>())
        .await?;
    pin_mut!(stream);

    let writer_options = PartitionWriterOptions {
        runtime: context.runtime,
        defaults: context.defaults,
        columns: context.columns,
        run_id: context.run_id,
        table: &context.table.name,
        batch_rows: context.options.batch_rows,
        starting_part_number: 0,
        track_raw_id_bounds: false,
    };
    let mut writers: HashMap<PartitionKey, OpenPartitionWriter> = HashMap::new();
    let mut rows_exported = 0_u64;
    while let Some(row) = stream.try_next().await? {
        let key = snapshot_item_partition_key(&row, context.snapshot_date_index)?;
        if !writers.contains_key(&key) {
            let writer = OpenPartitionWriter::try_new(&writer_options, &key)?;
            writers.insert(key.clone(), writer);
        }
        writers
            .get_mut(&key)
            .expect("snapshot item writer must exist after insertion")
            .append_row(&row, 0, context.options.max_rows_per_file)?;
        rows_exported += 1;
    }

    let mut files = Vec::new();
    for (_, writer) in writers {
        files.extend(writer.close()?.files);
    }

    Ok(ChunkExportResult {
        rows_exported,
        files,
        partition_next_parts: HashMap::new(),
    })
}

fn write_progress_checkpoint(path: &Path, state: CheckpointWriteState<'_>) -> Result<()> {
    let checkpoint = ExportCheckpoint {
        version: 1,
        strategy: "raw_id_range".to_string(),
        run_id: state.run_id.to_string(),
        completed: state.completed,
        table: state.table.name.clone(),
        source: state.source.clone(),
        extract_predicate: state.extract_predicate.to_string(),
        extract_start_raw_id: state.extract_start_raw_id,
        final_exclusive_end: state.final_exclusive_end,
        next_raw_id: state.next_raw_id,
        chunk_rows: state.options.chunk_rows,
        date_column: None,
        date_start: None,
        date_final_exclusive_end: None,
        date_next_start: None,
        batch_rows: state.options.batch_rows,
        max_rows_per_file: state.options.max_rows_per_file,
        chunks_planned: state.chunks_planned,
        chunks_completed: state.chunks_completed,
        rows_exported: state.rows_exported,
        files: state.files.to_vec(),
        schema: Some(state.schema.clone()),
        partitions: checkpoint_partitions(state.partition_next_parts),
        manifest_file: state.manifest_file,
        updated_at_unix_seconds: unix_seconds_now(),
    };
    write_checkpoint(path, &checkpoint)
}

fn write_date_month_checkpoint(path: &Path, state: DateCheckpointWriteState<'_>) -> Result<()> {
    let checkpoint = ExportCheckpoint {
        version: 1,
        strategy: "date_month".to_string(),
        run_id: state.run_id.to_string(),
        completed: state.completed,
        table: state.table.name.clone(),
        source: state.source.clone(),
        extract_predicate: state.extract_predicate.to_string(),
        extract_start_raw_id: 0,
        final_exclusive_end: 0,
        next_raw_id: 0,
        chunk_rows: 0,
        date_column: Some(state.date_column.to_string()),
        date_start: Some(state.date_start.to_string()),
        date_final_exclusive_end: Some(state.date_final_exclusive_end.to_string()),
        date_next_start: Some(state.date_next_start.to_string()),
        batch_rows: state.options.batch_rows,
        max_rows_per_file: state.options.max_rows_per_file,
        chunks_planned: state.chunks_planned,
        chunks_completed: state.chunks_completed,
        rows_exported: state.rows_exported,
        files: state.files.to_vec(),
        schema: Some(state.schema.clone()),
        partitions: checkpoint_partitions(state.partition_next_parts),
        manifest_file: state.manifest_file,
        updated_at_unix_seconds: unix_seconds_now(),
    };
    write_checkpoint(path, &checkpoint)
}

fn export_zero_row_manifest(request: ZeroRowManifestRequest<'_>) -> Result<ExportResult> {
    if request.dry_run {
        return Ok(ExportResult {
            run_id: request.run_id,
            dry_run: true,
            table: request.table.name.clone(),
            extract_predicate: request.predicate,
            chunks_planned: request.chunks_planned,
            chunks_exported: 0,
            rows_exported: 0,
            parquet_files: Vec::new(),
            manifest_file: None,
            checkpoint_file: None,
        });
    }

    let output_dir = table_output_dir(request.runtime, &request.table.name);
    if output_dir.exists() {
        if request.force {
            fs::remove_dir_all(&output_dir).map_err(|source| ExporterError::RemoveFile {
                path: output_dir.clone(),
                source,
            })?;
        } else if contains_parquet_files(&output_dir)? {
            return Err(ExporterError::InvalidConfig(format!(
                "output directory already contains parquet files: {} (use --force to overwrite)",
                output_dir.display()
            )));
        }
    }

    let manifest_file = manifest_file_path(request.runtime, &request.table.name);
    let manifest = ExportManifest {
        run_id: request.run_id.clone(),
        created_at_unix_seconds: unix_seconds_now(),
        source: request.source.clone(),
        table: ManifestTable {
            name: request.table.name.clone(),
            schema: Some(request.schema.clone()),
            rows_exported: 0,
            files: Vec::new(),
            extract_predicate: request.predicate.clone(),
            min_raw_id: None,
            max_raw_id: None,
        },
    };
    write_manifest(&manifest_file, &manifest)?;

    Ok(ExportResult {
        run_id: request.run_id,
        dry_run: false,
        table: request.table.name.clone(),
        extract_predicate: request.predicate,
        chunks_planned: request.chunks_planned,
        chunks_exported: request.chunks_exported,
        rows_exported: 0,
        parquet_files: Vec::new(),
        manifest_file: Some(manifest_file),
        checkpoint_file: None,
    })
}

fn checkpoint_partitions(
    partition_next_parts: &HashMap<PartitionKey, u32>,
) -> Vec<CheckpointPartition> {
    let mut partitions = partition_next_parts
        .iter()
        .map(|(key, next_part_number)| CheckpointPartition {
            partition_values: key.partition_values(),
            next_part_number: *next_part_number,
        })
        .collect::<Vec<_>>();
    partitions.sort_by_key(|partition| partition_sort_key(&partition.partition_values));
    partitions
}

fn partition_sort_key(values: &HashMap<String, String>) -> String {
    let mut pairs = values
        .iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>();
    pairs.sort();
    pairs.join("/")
}

fn partition_next_parts_from_checkpoint(
    checkpoint: &ExportCheckpoint,
) -> Result<HashMap<PartitionKey, u32>> {
    let mut partition_next_parts = HashMap::new();
    for partition in &checkpoint.partitions {
        let key = PartitionKey::from_partition_values(&partition.partition_values)?;
        partition_next_parts.insert(key, partition.next_part_number);
    }
    Ok(partition_next_parts)
}

fn validate_resume_checkpoint(
    checkpoint: &ExportCheckpoint,
    runtime: &RuntimeConfig,
    table: &TableConfig,
    source_exclusive_end: i64,
    options: &DartXbrlExportOptions,
    current_schema: &ManifestSchema,
) -> Result<()> {
    if checkpoint.version != 1 {
        return Err(ExporterError::InvalidConfig(format!(
            "unsupported checkpoint version {}; expected 1",
            checkpoint.version
        )));
    }
    if checkpoint.strategy != "raw_id_range" {
        return Err(ExporterError::InvalidConfig(format!(
            "checkpoint strategy `{}` cannot be resumed by raw_id_range exporter",
            checkpoint.strategy
        )));
    }
    if checkpoint.completed {
        return Err(ExporterError::InvalidConfig(format!(
            "checkpoint for run {} is already completed",
            checkpoint.run_id
        )));
    }
    if checkpoint.table != table.name {
        return Err(ExporterError::InvalidConfig(format!(
            "checkpoint table `{}` does not match selected table `{}`",
            checkpoint.table, table.name
        )));
    }
    if checkpoint.source.name != runtime.source.name
        || checkpoint.source.schema != runtime.source.schema
        || checkpoint.source.snapshot_date != runtime.output.snapshot_date
    {
        return Err(ExporterError::InvalidConfig(
            "checkpoint source does not match runtime source".to_string(),
        ));
    }
    if checkpoint.chunk_rows != options.chunk_rows
        || checkpoint.batch_rows != options.batch_rows
        || checkpoint.max_rows_per_file != options.max_rows_per_file
    {
        return Err(ExporterError::InvalidConfig(
            "checkpoint export options do not match resume options".to_string(),
        ));
    }
    if checkpoint.final_exclusive_end > source_exclusive_end {
        return Err(ExporterError::InvalidConfig(format!(
            "checkpoint final_exclusive_end {} exceeds current source bound {}",
            checkpoint.final_exclusive_end, source_exclusive_end
        )));
    }
    if checkpoint.next_raw_id < checkpoint.extract_start_raw_id
        || checkpoint.next_raw_id > checkpoint.final_exclusive_end
    {
        return Err(ExporterError::InvalidConfig(format!(
            "checkpoint next_raw_id {} is outside export range {}..{}",
            checkpoint.next_raw_id, checkpoint.extract_start_raw_id, checkpoint.final_exclusive_end
        )));
    }
    if let Some(checkpoint_schema) = &checkpoint.schema {
        if checkpoint_schema.hash != current_schema.hash {
            return Err(ExporterError::InvalidConfig(format!(
                "checkpoint schema hash {} does not match current schema hash {}",
                checkpoint_schema.hash, current_schema.hash
            )));
        }
    }
    Ok(())
}

fn validate_date_month_resume_checkpoint(
    checkpoint: &ExportCheckpoint,
    runtime: &RuntimeConfig,
    table: &TableConfig,
    bounds: &TableBounds,
    date_column: &str,
    options: &DateMonthExportOptions,
    current_schema: &ManifestSchema,
) -> Result<()> {
    if checkpoint.version != 1 {
        return Err(ExporterError::InvalidConfig(format!(
            "unsupported checkpoint version {}; expected 1",
            checkpoint.version
        )));
    }
    if checkpoint.strategy != "date_month" {
        return Err(ExporterError::InvalidConfig(format!(
            "checkpoint strategy `{}` cannot be resumed by date_month exporter",
            checkpoint.strategy
        )));
    }
    if checkpoint.completed {
        return Err(ExporterError::InvalidConfig(format!(
            "checkpoint for run {} is already completed",
            checkpoint.run_id
        )));
    }
    if checkpoint.table != table.name {
        return Err(ExporterError::InvalidConfig(format!(
            "checkpoint table `{}` does not match selected table `{}`",
            checkpoint.table, table.name
        )));
    }
    if checkpoint.source.name != runtime.source.name
        || checkpoint.source.schema != runtime.source.schema
        || checkpoint.source.snapshot_date != runtime.output.snapshot_date
    {
        return Err(ExporterError::InvalidConfig(
            "checkpoint source does not match runtime source".to_string(),
        ));
    }
    if checkpoint.date_column.as_deref() != Some(date_column) {
        return Err(ExporterError::InvalidConfig(format!(
            "checkpoint date_column {:?} does not match table date_column `{date_column}`",
            checkpoint.date_column
        )));
    }
    if checkpoint.batch_rows != options.batch_rows
        || checkpoint.max_rows_per_file != options.max_rows_per_file
    {
        return Err(ExporterError::InvalidConfig(
            "checkpoint export options do not match resume options".to_string(),
        ));
    }
    if let Some(checkpoint_schema) = &checkpoint.schema {
        if checkpoint_schema.hash != current_schema.hash {
            return Err(ExporterError::InvalidConfig(format!(
                "checkpoint schema hash {} does not match current schema hash {}",
                checkpoint_schema.hash, current_schema.hash
            )));
        }
    }

    let date_start = checkpoint_date(&checkpoint.date_start, "date_start")?;
    let final_exclusive_end = checkpoint_date(
        &checkpoint.date_final_exclusive_end,
        "date_final_exclusive_end",
    )?;
    let date_next_start = checkpoint_date(&checkpoint.date_next_start, "date_next_start")?;
    if date_start >= final_exclusive_end {
        return Err(ExporterError::InvalidConfig(format!(
            "checkpoint date range is empty: {date_start}..{final_exclusive_end}"
        )));
    }
    if date_next_start < date_start || date_next_start > final_exclusive_end {
        return Err(ExporterError::InvalidConfig(format!(
            "checkpoint date_next_start {date_next_start} is outside export range {date_start}..{final_exclusive_end}"
        )));
    }

    let TableBounds::Date { min: _, max } = bounds else {
        return Err(ExporterError::InvalidConfig(
            "date_month resume requires date bounds".to_string(),
        ));
    };
    if let Some(max) = max.as_deref() {
        let max_month = parse_year_month(max)?;
        let source_final_exclusive_end = month_start(next_month(max_month))?;
        if final_exclusive_end > source_final_exclusive_end {
            return Err(ExporterError::InvalidConfig(format!(
                "checkpoint final date {final_exclusive_end} exceeds current source bound {source_final_exclusive_end}"
            )));
        }
    }

    Ok(())
}

fn cleanup_resume_outputs(
    runtime: &RuntimeConfig,
    table: &TableConfig,
    checkpoint: &ExportCheckpoint,
) -> Result<()> {
    let keep_files = checkpoint
        .files
        .iter()
        .map(|file| file.path.clone())
        .collect::<HashSet<_>>();
    remove_uncheckpointed_parquet_files(&table_output_dir(runtime, &table.name), &keep_files)?;

    let tmp_dir = runtime.output.tmp_root.join(&checkpoint.run_id);
    if tmp_dir.exists() {
        fs::remove_dir_all(&tmp_dir).map_err(|source| ExporterError::RemoveFile {
            path: tmp_dir,
            source,
        })?;
    }
    Ok(())
}

fn remove_uncheckpointed_parquet_files(
    path: &PathBuf,
    keep_files: &HashSet<PathBuf>,
) -> Result<()> {
    if !path.exists() {
        return Ok(());
    }

    for entry in fs::read_dir(path).map_err(|source| ExporterError::ReadFile {
        path: path.clone(),
        source,
    })? {
        let entry = entry.map_err(|source| ExporterError::ReadFile {
            path: path.clone(),
            source,
        })?;
        let child_path = entry.path();
        let file_type = entry
            .file_type()
            .map_err(|source| ExporterError::Metadata {
                path: child_path.clone(),
                source,
            })?;
        if file_type.is_dir() {
            remove_uncheckpointed_parquet_files(&child_path, keep_files)?;
        } else if child_path.extension().is_some_and(|ext| ext == "parquet")
            && !keep_files.contains(&child_path)
        {
            fs::remove_file(&child_path).map_err(|source| ExporterError::RemoveFile {
                path: child_path,
                source,
            })?;
        }
    }
    Ok(())
}

fn checkpoint_file_path(runtime: &RuntimeConfig, run_id: &str) -> PathBuf {
    runtime
        .output
        .root
        .join(format!("snapshot_date={}", runtime.output.snapshot_date))
        .join(format!("source={}", runtime.source.name))
        .join("_manifests")
        .join("checkpoints")
        .join(format!("{run_id}.json"))
}

fn resolve_final_exclusive_end(
    start: i64,
    source_max: i64,
    options: &DartXbrlExportOptions,
) -> i64 {
    let source_exclusive_end = source_max.saturating_add(1);
    if options.all_chunks {
        source_exclusive_end
    } else {
        next_chunk_exclusive_end(start, source_exclusive_end, options.chunk_rows)
    }
}

fn next_chunk_exclusive_end(chunk_start: i64, final_exclusive_end: i64, chunk_rows: i64) -> i64 {
    chunk_start
        .saturating_add(chunk_rows)
        .min(final_exclusive_end)
}

fn planned_chunk_count(start: i64, final_exclusive_end: i64, chunk_rows: i64) -> u64 {
    if start >= final_exclusive_end {
        return 0;
    }

    let total_rows = final_exclusive_end.saturating_sub(start) as u64;
    total_rows.div_ceil(chunk_rows as u64)
}

fn date_month_ranges(
    bounds: &TableBounds,
    options: &DateMonthExportOptions,
) -> Result<Vec<(NaiveDate, NaiveDate)>> {
    let TableBounds::Date { min, max } = bounds else {
        return Err(ExporterError::InvalidConfig(
            "date-month export requires date bounds".to_string(),
        ));
    };
    let Some(min) = min.as_deref() else {
        return Ok(Vec::new());
    };
    let Some(max) = max.as_deref() else {
        return Ok(Vec::new());
    };

    let mut start_month = parse_year_month(min)?;
    let max_month = parse_year_month(max)?;
    if let Some(since_date) = &options.since_date {
        start_month = start_month.max(parse_year_month(since_date)?);
    }
    let mut end_month = max_month;
    if let Some(until_date) = &options.until_date {
        end_month = end_month.min(parse_year_month(until_date)?);
    }
    if start_month > end_month {
        return Ok(Vec::new());
    }

    let mut ranges = Vec::new();
    let mut current = start_month;
    while current <= end_month {
        let next = next_month(current);
        ranges.push((month_start(current)?, month_start(next)?));
        current = next;
    }
    Ok(ranges)
}

fn date_month_ranges_from(
    start: NaiveDate,
    final_exclusive_end: NaiveDate,
) -> Result<Vec<(NaiveDate, NaiveDate)>> {
    if start > final_exclusive_end {
        return Err(ExporterError::InvalidConfig(format!(
            "date_next_start {start} is after final exclusive end {final_exclusive_end}"
        )));
    }
    let mut ranges = Vec::new();
    let mut current = start;
    while current < final_exclusive_end {
        let current_month = (current.year(), current.month());
        let next = month_start(next_month(current_month))?.min(final_exclusive_end);
        ranges.push((current, next));
        current = next;
    }
    Ok(ranges)
}

fn parse_year_month(date: &str) -> Result<(i32, u32)> {
    let mut parts = date.split('-');
    let year = parts
        .next()
        .ok_or_else(|| invalid_date(date))?
        .parse::<i32>()
        .map_err(|_| invalid_date(date))?;
    let month = parts
        .next()
        .ok_or_else(|| invalid_date(date))?
        .parse::<u32>()
        .map_err(|_| invalid_date(date))?;
    if !(1..=12).contains(&month) {
        return Err(invalid_date(date));
    }
    Ok((year, month))
}

fn month_start((year, month): (i32, u32)) -> Result<NaiveDate> {
    NaiveDate::from_ymd_opt(year, month, 1).ok_or_else(|| {
        ExporterError::InvalidConfig(format!("invalid year/month: {year:04}-{month:02}"))
    })
}

fn checkpoint_date(value: &Option<String>, field_name: &'static str) -> Result<NaiveDate> {
    let value = value.as_deref().ok_or_else(|| {
        ExporterError::InvalidConfig(format!("checkpoint is missing {field_name}"))
    })?;
    NaiveDate::parse_from_str(value, "%Y-%m-%d").map_err(|source| {
        ExporterError::InvalidConfig(format!(
            "checkpoint {field_name} value `{value}` is not YYYY-MM-DD: {source}"
        ))
    })
}

fn next_month((year, month): (i32, u32)) -> (i32, u32) {
    if month == 12 {
        (year + 1, 1)
    } else {
        (year, month + 1)
    }
}

fn invalid_date(date: &str) -> ExporterError {
    ExporterError::InvalidConfig(format!(
        "expected date formatted as YYYY-MM or YYYY-MM-DD, got `{date}`"
    ))
}

fn build_select_sql(
    schema: &str,
    table: &TableConfig,
    columns: &[ColumnInfo],
    start: i64,
    exclusive_end: i64,
) -> Result<String> {
    let select_list = columns
        .iter()
        .map(select_expr)
        .collect::<Result<Vec<_>>>()?
        .join(", ");
    let extract_key = table.extract_key.as_deref().unwrap_or("raw_id");
    Ok(format!(
        "SELECT {select_list} FROM {schema}.{table} WHERE {key} >= {start} AND {key} < {exclusive_end} ORDER BY {key}",
        schema = quote_ident(schema)?,
        table = quote_ident(&table.name)?,
        key = quote_ident(extract_key)?,
    ))
}

fn build_date_select_sql(
    schema: &str,
    table: &TableConfig,
    columns: &[ColumnInfo],
    date_column: &str,
    start_date: NaiveDate,
    exclusive_end: NaiveDate,
) -> Result<String> {
    let select_list = columns
        .iter()
        .map(select_expr)
        .collect::<Result<Vec<_>>>()?
        .join(", ");
    Ok(format!(
        "SELECT {select_list} FROM {schema}.{table} WHERE {date_column} >= DATE '{start_date}' AND {date_column} < DATE '{exclusive_end}' ORDER BY {date_column}",
        schema = quote_ident(schema)?,
        table = quote_ident(&table.name)?,
        date_column = quote_ident(date_column)?,
    ))
}

fn build_full_table_select_sql(
    schema: &str,
    table: &TableConfig,
    columns: &[ColumnInfo],
) -> Result<String> {
    let select_list = columns
        .iter()
        .map(select_expr)
        .collect::<Result<Vec<_>>>()?
        .join(", ");
    let mut sql = format!(
        "SELECT {select_list} FROM {schema}.{table}",
        schema = quote_ident(schema)?,
        table = quote_ident(&table.name)?,
    );
    if !table.order_by.is_empty() {
        let order_by = table
            .order_by
            .iter()
            .map(|column| quote_ident(column))
            .collect::<Result<Vec<_>>>()?
            .join(", ");
        sql.push_str(" ORDER BY ");
        sql.push_str(&order_by);
    }
    Ok(sql)
}

fn build_snapshot_items_select_sql(
    schema: &str,
    table: &TableConfig,
    columns: &[ColumnInfo],
) -> Result<String> {
    let select_list = columns
        .iter()
        .map(|column| select_expr_with_alias(column, Some("i")))
        .collect::<Result<Vec<_>>>()?
        .join(", ");
    let mut order_by = vec![
        format!("s.{}", quote_ident("as_of_date")?),
        format!("i.{}", quote_ident("snapshot_id")?),
    ];
    for column in &table.order_by {
        if column == "snapshot_id" {
            continue;
        }
        order_by.push(format!("i.{}", quote_ident(column)?));
    }
    Ok(format!(
        "SELECT {select_list}, (s.{as_of_date} - DATE '1970-01-01')::int4 AS {snapshot_date_partition} FROM {schema}.{items_table} AS i JOIN {schema}.{snapshot_table} AS s ON i.{snapshot_id} = s.{snapshot_id} ORDER BY {order_by}",
        as_of_date = quote_ident("as_of_date")?,
        snapshot_date_partition = quote_ident("__snapshot_date_partition")?,
        schema = quote_ident(schema)?,
        items_table = quote_ident(&table.name)?,
        snapshot_table = quote_ident("stock_master_snapshot")?,
        snapshot_id = quote_ident("snapshot_id")?,
        order_by = order_by.join(", "),
    ))
}

fn select_expr(column: &ColumnInfo) -> Result<String> {
    select_expr_with_alias(column, None)
}

fn select_expr_with_alias(column: &ColumnInfo, table_alias: Option<&str>) -> Result<String> {
    let name = quote_ident(&column.column_name)?;
    let qualified_name = if let Some(table_alias) = table_alias {
        format!("{table_alias}.{name}")
    } else {
        name.clone()
    };
    let expr = match column.udt_name.as_str() {
        "jsonb" | "numeric" | "uuid" => format!("{qualified_name}::text"),
        "date" => format!("({qualified_name} - DATE '1970-01-01')::int4"),
        "timestamptz" => {
            format!("floor(extract(epoch from {qualified_name}) * 1000000)::bigint")
        }
        "timestamp" => {
            format!(
                "floor(extract(epoch from ({qualified_name} AT TIME ZONE 'UTC')) * 1000000)::bigint"
            )
        }
        "int8" | "int4" | "bool" | "text" | "varchar" | "bpchar" => qualified_name,
        other => {
            return Err(ExporterError::InvalidData(format!(
                "unsupported PostgreSQL type for {}: {}",
                column.column_name, other
            )))
        }
    };
    Ok(format!("{expr} AS {name}"))
}

fn data_partition_dir_path(runtime: &RuntimeConfig, table: &str, key: &PartitionKey) -> PathBuf {
    let mut path = table_output_dir(runtime, table);
    for segment in key.path_segments() {
        path = path.join(segment);
    }
    path
}

fn table_output_dir(runtime: &RuntimeConfig, table: &str) -> PathBuf {
    runtime
        .output
        .root
        .join(format!("snapshot_date={}", runtime.output.snapshot_date))
        .join(format!("source={}", runtime.source.name))
        .join(table)
        .join("schema_version=1")
}

fn tmp_partition_dir_path(
    runtime: &RuntimeConfig,
    run_id: &str,
    table: &str,
    key: &PartitionKey,
) -> PathBuf {
    let mut path = runtime.output.tmp_root.join(run_id).join(table);
    for segment in key.path_segments() {
        path = path.join(segment);
    }
    path
}

fn part_file_name(part_number: u32) -> String {
    format!("part-{part_number:06}.parquet")
}

fn manifest_file_path(runtime: &RuntimeConfig, table: &str) -> PathBuf {
    runtime
        .output
        .root
        .join(format!("snapshot_date={}", runtime.output.snapshot_date))
        .join(format!("source={}", runtime.source.name))
        .join("_manifests")
        .join("table_manifests")
        .join(format!("{table}.json"))
}

fn column_index(columns: &[ColumnInfo], name: &str) -> Result<usize> {
    columns
        .iter()
        .position(|column| column.column_name == name)
        .ok_or_else(|| ExporterError::InvalidConfig(format!("required column `{name}` is missing")))
}

fn full_table_partition_indices(table: &TableConfig, columns: &[ColumnInfo]) -> Result<Vec<usize>> {
    let mut seen = HashSet::new();
    table
        .output_partitions
        .iter()
        .map(|partition| {
            if quote_ident(partition).is_err() {
                return Err(ExporterError::InvalidConfig(format!(
                    "{} full_table output partition `{partition}` must be a source column name; expressions are not supported yet",
                    table.name
                )));
            }
            if !seen.insert(partition.as_str()) {
                return Err(ExporterError::InvalidConfig(format!(
                    "{} has duplicate output partition `{partition}`",
                    table.name
                )));
            }
            column_index(columns, partition)
        })
        .collect()
}

fn full_table_partition_key(
    row: &Row,
    columns: &[ColumnInfo],
    partition_indices: &[usize],
) -> Result<PartitionKey> {
    if partition_indices.is_empty() {
        return Ok(PartitionKey::Unpartitioned);
    }

    let values = partition_indices
        .iter()
        .map(|index| {
            let column = &columns[*index];
            Ok((
                column.column_name.clone(),
                row_partition_value(row, *index, column)?,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(PartitionKey::ColumnValues(values))
}

fn snapshot_item_partition_key(row: &Row, snapshot_date_index: usize) -> Result<PartitionKey> {
    let snapshot_date_days: i32 = row.try_get(snapshot_date_index)?;
    Ok(PartitionKey::ColumnValues(vec![(
        "snapshot_date".to_string(),
        date32_days_to_string(snapshot_date_days)?,
    )]))
}

fn row_partition_value(row: &Row, index: usize, column: &ColumnInfo) -> Result<String> {
    match column.udt_name.as_str() {
        "int8" | "timestamptz" | "timestamp" => {
            let value: Option<i64> = row.try_get(index)?;
            Ok(value
                .map(|value| value.to_string())
                .unwrap_or_else(null_partition_value))
        }
        "int4" => {
            let value: Option<i32> = row.try_get(index)?;
            Ok(value
                .map(|value| value.to_string())
                .unwrap_or_else(null_partition_value))
        }
        "bool" => {
            let value: Option<bool> = row.try_get(index)?;
            Ok(value
                .map(|value| value.to_string())
                .unwrap_or_else(null_partition_value))
        }
        "date" => {
            let value: Option<i32> = row.try_get(index)?;
            value
                .map(date32_days_to_string)
                .transpose()
                .map(|value| value.unwrap_or_else(null_partition_value))
        }
        "jsonb" | "numeric" | "text" | "varchar" | "bpchar" | "uuid" => {
            let value: Option<String> = row.try_get(index)?;
            Ok(value.unwrap_or_else(null_partition_value))
        }
        other => Err(ExporterError::InvalidData(format!(
            "unsupported PostgreSQL type for output partition {}: {}",
            column.column_name, other
        ))),
    }
}

fn date32_days_to_string(days_since_epoch: i32) -> Result<String> {
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).ok_or_else(|| {
        ExporterError::InvalidData("failed to construct unix epoch date".to_string())
    })?;
    let date = epoch
        .checked_add_signed(Duration::days(days_since_epoch as i64))
        .ok_or_else(|| {
            ExporterError::InvalidData(format!(
                "date32 value {days_since_epoch} is outside supported range"
            ))
        })?;
    Ok(date.to_string())
}

fn null_partition_value() -> String {
    "__null__".to_string()
}

fn encode_partition_value(value: &str) -> String {
    if value.is_empty() {
        return "__empty__".to_string();
    }

    value
        .chars()
        .map(|ch| match ch {
            'A'..='Z' | 'a'..='z' | '0'..='9' | '_' | '-' | '.' => ch,
            _ => '_',
        })
        .collect()
}

fn contains_parquet_files(path: &PathBuf) -> Result<bool> {
    if !path.exists() {
        return Ok(false);
    }
    for entry in fs::read_dir(path).map_err(|source| ExporterError::ReadFile {
        path: path.clone(),
        source,
    })? {
        let entry = entry.map_err(|source| ExporterError::ReadFile {
            path: path.clone(),
            source,
        })?;
        let child_path = entry.path();
        let file_type = entry
            .file_type()
            .map_err(|source| ExporterError::Metadata {
                path: child_path.clone(),
                source,
            })?;
        if file_type.is_dir() {
            if contains_parquet_files(&child_path)? {
                return Ok(true);
            }
        } else if child_path.extension().is_some_and(|ext| ext == "parquet") {
            return Ok(true);
        }
    }
    Ok(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_column(
        table_name: &str,
        column_name: &str,
        ordinal_position: i32,
        data_type: &str,
        udt_name: &str,
    ) -> ColumnInfo {
        ColumnInfo {
            table_name: table_name.to_string(),
            column_name: column_name.to_string(),
            ordinal_position,
            is_nullable: false,
            data_type: data_type.to_string(),
            udt_name: udt_name.to_string(),
            column_default: None,
            numeric_precision: None,
            numeric_scale: None,
            datetime_precision: None,
        }
    }

    #[test]
    fn casts_json_numeric_and_date_for_select() {
        let json_col = ColumnInfo {
            table_name: "t".to_string(),
            column_name: "raw_payload".to_string(),
            ordinal_position: 1,
            is_nullable: false,
            data_type: "jsonb".to_string(),
            udt_name: "jsonb".to_string(),
            column_default: None,
            numeric_precision: None,
            numeric_scale: None,
            datetime_precision: None,
        };
        assert_eq!(
            select_expr(&json_col).unwrap(),
            "\"raw_payload\"::text AS \"raw_payload\""
        );
    }

    #[test]
    fn partition_path_uses_empty_sentinel() {
        assert_eq!(encode_partition_value(""), "__empty__");
        assert_eq!(encode_partition_value("11011"), "11011");
        assert_eq!(encode_partition_value("a/b"), "a_b");
    }

    #[test]
    fn part_file_names_are_zero_padded() {
        assert_eq!(part_file_name(0), "part-000000.parquet");
        assert_eq!(part_file_name(12), "part-000012.parquet");
    }

    #[test]
    fn single_chunk_range_stops_after_chunk_rows() {
        let options = DartXbrlExportOptions {
            chunk_rows: 1_000,
            start_raw_id: Some(7_500_001),
            all_chunks: false,
            batch_rows: 1_000,
            max_rows_per_file: None,
            dry_run: false,
            force: false,
            resume: None,
        };
        let final_end = resolve_final_exclusive_end(7_500_001, 8_500_000, &options);

        assert_eq!(final_end, 7_501_001);
        assert_eq!(planned_chunk_count(7_500_001, final_end, 1_000), 1);
    }

    #[test]
    fn all_chunks_range_extends_to_source_max() {
        let options = DartXbrlExportOptions {
            chunk_rows: 1_000,
            start_raw_id: Some(7_500_001),
            all_chunks: true,
            batch_rows: 1_000,
            max_rows_per_file: None,
            dry_run: false,
            force: false,
            resume: None,
        };
        let final_end = resolve_final_exclusive_end(7_500_001, 7_502_500, &options);

        assert_eq!(final_end, 7_502_501);
        assert_eq!(planned_chunk_count(7_500_001, final_end, 1_000), 3);
        assert_eq!(
            next_chunk_exclusive_end(7_502_001, final_end, 1_000),
            7_502_501
        );
    }

    #[test]
    fn date32_partition_key_uses_year_month() {
        let key = PartitionKey::from_date32_days(13_760).unwrap();

        assert_eq!(
            key.path_segments(),
            vec!["year=2007".to_string(), "month=09".to_string()]
        );
        assert_eq!(
            key.partition_values(),
            HashMap::from([
                ("year".to_string(), "2007".to_string()),
                ("month".to_string(), "09".to_string())
            ])
        );
    }

    #[test]
    fn unpartitioned_key_has_no_partition_path_or_values() {
        let key = PartitionKey::Unpartitioned;

        assert_eq!(key.path_segments(), Vec::<String>::new());
        assert_eq!(key.partition_values(), HashMap::new());
        assert_eq!(
            PartitionKey::from_partition_values(&HashMap::new()).unwrap(),
            PartitionKey::Unpartitioned
        );
    }

    #[test]
    fn generic_column_partition_key_preserves_configured_path_order() {
        let key = PartitionKey::ColumnValues(vec![
            ("bsns_year".to_string(), "2024".to_string()),
            ("reprt_code".to_string(), "11011/extra".to_string()),
        ]);

        assert_eq!(
            key.path_segments(),
            vec![
                "bsns_year=2024".to_string(),
                "reprt_code=11011_extra".to_string()
            ]
        );
        assert_eq!(
            key.partition_values(),
            HashMap::from([
                ("bsns_year".to_string(), "2024".to_string()),
                ("reprt_code".to_string(), "11011/extra".to_string())
            ])
        );
    }

    #[test]
    fn date32_days_format_as_snapshot_partition_date() {
        assert_eq!(date32_days_to_string(20_454).unwrap(), "2026-01-01");
    }

    #[test]
    fn full_table_select_quotes_order_by_and_casts_string_like_types() {
        let table = TableConfig {
            name: "stock_master_snapshot".to_string(),
            priority: crate::config::Priority::P1,
            extract_strategy: ExtractStrategy::FullTable,
            extract_key: None,
            date_column: None,
            chunk_rows: None,
            output_partitions: Vec::new(),
            order_by: vec!["as_of_date".to_string(), "snapshot_id".to_string()],
            jsonb_columns: Vec::new(),
        };
        let columns = vec![
            ColumnInfo {
                table_name: table.name.clone(),
                column_name: "snapshot_id".to_string(),
                ordinal_position: 1,
                is_nullable: false,
                data_type: "uuid".to_string(),
                udt_name: "uuid".to_string(),
                column_default: None,
                numeric_precision: None,
                numeric_scale: None,
                datetime_precision: None,
            },
            ColumnInfo {
                table_name: table.name.clone(),
                column_name: "raw_payload".to_string(),
                ordinal_position: 2,
                is_nullable: true,
                data_type: "jsonb".to_string(),
                udt_name: "jsonb".to_string(),
                column_default: None,
                numeric_precision: None,
                numeric_scale: None,
                datetime_precision: None,
            },
        ];

        let sql = build_full_table_select_sql("public", &table, &columns).unwrap();

        assert_eq!(
            sql,
            "SELECT \"snapshot_id\"::text AS \"snapshot_id\", \"raw_payload\"::text AS \"raw_payload\" FROM \"public\".\"stock_master_snapshot\" ORDER BY \"as_of_date\", \"snapshot_id\""
        );
    }

    #[test]
    fn full_table_partition_indices_reject_expression_partitions() {
        let table = TableConfig {
            name: "stock_master_snapshot_items".to_string(),
            priority: crate::config::Priority::P1,
            extract_strategy: ExtractStrategy::FullTable,
            extract_key: None,
            date_column: None,
            chunk_rows: None,
            output_partitions: vec!["snapshot_date(as_of_date)".to_string()],
            order_by: Vec::new(),
            jsonb_columns: Vec::new(),
        };
        let columns = vec![ColumnInfo {
            table_name: table.name.clone(),
            column_name: "as_of_date".to_string(),
            ordinal_position: 1,
            is_nullable: false,
            data_type: "date".to_string(),
            udt_name: "date".to_string(),
            column_default: None,
            numeric_precision: None,
            numeric_scale: None,
            datetime_precision: None,
        }];

        assert!(full_table_partition_indices(&table, &columns).is_err());
    }

    #[test]
    fn snapshot_items_select_joins_snapshot_and_appends_partition_date() {
        let table = TableConfig {
            name: "stock_master_snapshot_items".to_string(),
            priority: crate::config::Priority::P1,
            extract_strategy: ExtractStrategy::SnapshotItems,
            extract_key: None,
            date_column: None,
            chunk_rows: None,
            output_partitions: vec!["snapshot_date(as_of_date)".to_string()],
            order_by: vec![
                "snapshot_id".to_string(),
                "market".to_string(),
                "ticker".to_string(),
            ],
            jsonb_columns: Vec::new(),
        };
        let columns = vec![
            test_column(
                "stock_master_snapshot_items",
                "snapshot_id",
                1,
                "uuid",
                "uuid",
            ),
            test_column("stock_master_snapshot_items", "ticker", 2, "text", "text"),
            test_column("stock_master_snapshot_items", "market", 3, "text", "text"),
        ];

        let sql = build_snapshot_items_select_sql("public", &table, &columns).unwrap();

        assert_eq!(
            sql,
            "SELECT i.\"snapshot_id\"::text AS \"snapshot_id\", i.\"ticker\" AS \"ticker\", i.\"market\" AS \"market\", (s.\"as_of_date\" - DATE '1970-01-01')::int4 AS \"__snapshot_date_partition\" FROM \"public\".\"stock_master_snapshot_items\" AS i JOIN \"public\".\"stock_master_snapshot\" AS s ON i.\"snapshot_id\" = s.\"snapshot_id\" ORDER BY s.\"as_of_date\", i.\"snapshot_id\", i.\"market\", i.\"ticker\""
        );
    }

    #[test]
    fn date_month_ranges_follow_month_boundaries() {
        let options = DateMonthExportOptions {
            since_date: Some("2007-09".to_string()),
            until_date: Some("2007-10-15".to_string()),
            batch_rows: 1_000,
            max_rows_per_file: None,
            dry_run: false,
            force: false,
            resume: None,
        };
        let ranges = date_month_ranges(
            &TableBounds::Date {
                min: Some("2007-06-05".to_string()),
                max: Some("2007-12-28".to_string()),
            },
            &options,
        )
        .unwrap();

        assert_eq!(ranges.len(), 2);
        assert_eq!(ranges[0].0.to_string(), "2007-09-01");
        assert_eq!(ranges[0].1.to_string(), "2007-10-01");
        assert_eq!(ranges[1].0.to_string(), "2007-10-01");
        assert_eq!(ranges[1].1.to_string(), "2007-11-01");
    }

    #[test]
    fn date_month_ranges_are_empty_when_source_has_no_rows() {
        let options = DateMonthExportOptions {
            since_date: None,
            until_date: None,
            batch_rows: 1_000,
            max_rows_per_file: None,
            dry_run: false,
            force: false,
            resume: None,
        };
        let ranges = date_month_ranges(
            &TableBounds::Date {
                min: None,
                max: None,
            },
            &options,
        )
        .unwrap();

        assert!(ranges.is_empty());
    }
}
