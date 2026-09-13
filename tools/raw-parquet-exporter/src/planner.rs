use std::cmp;
use std::collections::HashMap;

use serde::Serialize;

use crate::config::{Defaults, ExtractStrategy, Priority, RuntimeConfig, TableConfig};
use crate::db::{ColumnInfo, TableBounds};
use crate::error::{ExporterError, Result};
use crate::schema::{arrow_type_for_pg, pg_type_display};

#[derive(Debug, Clone)]
pub struct PlanOptions {
    pub chunk_rows_override: Option<i64>,
    pub max_db_connections_override: Option<u16>,
    pub writer_workers_override: Option<u16>,
    pub since_date: Option<String>,
    pub until_date: Option<String>,
    pub snapshot_date_override: Option<String>,
    pub offline: bool,
    pub dry_run: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct ExportPlan {
    pub source_name: String,
    pub source_schema: String,
    pub snapshot_date: String,
    pub dry_run: bool,
    pub offline: bool,
    pub defaults: PlanDefaults,
    pub tables: Vec<TablePlan>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct PlanDefaults {
    pub compression: String,
    pub row_group_rows: u64,
    pub target_file_bytes: u64,
    pub db_read_connections: u16,
    pub writer_workers: u16,
}

#[derive(Debug, Clone, Serialize)]
pub struct TablePlan {
    pub table: String,
    pub priority: Priority,
    pub extract_strategy: ExtractStrategy,
    pub output_partitions: Vec<String>,
    pub order_by: Vec<String>,
    pub columns: Vec<PlanColumn>,
    pub jobs: Vec<ExportJob>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct PlanColumn {
    pub name: String,
    pub ordinal_position: i32,
    pub nullable: bool,
    pub pg_type: String,
    pub arrow_type: String,
    pub jsonb_text_preserved: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct ExportJob {
    pub job_id: String,
    pub table: String,
    pub extract_predicate: String,
    pub output_partitions: Vec<String>,
    pub order_by: Vec<String>,
    pub expected_min_key: Option<String>,
    pub expected_max_key: Option<String>,
    pub notes: Vec<String>,
}

pub fn build_plan(
    defaults: &Defaults,
    runtime: &RuntimeConfig,
    tables: &[TableConfig],
    columns_by_table: &HashMap<String, Vec<ColumnInfo>>,
    bounds_by_table: &HashMap<String, TableBounds>,
    options: &PlanOptions,
) -> Result<ExportPlan> {
    let mut warnings = Vec::new();
    if options.offline {
        warnings
            .push("offline mode: database schema and min/max bounds were not queried".to_string());
    }

    let mut table_plans = Vec::new();
    for table in tables {
        let columns = columns_by_table
            .get(&table.name)
            .map(|columns| plan_columns(columns, &table.jsonb_columns))
            .unwrap_or_default();

        let mut table_warnings = Vec::new();
        if columns.is_empty() {
            table_warnings.push(
                "schema columns are unavailable; run without --offline to introspect PostgreSQL"
                    .to_string(),
            );
        }

        let bounds = bounds_by_table.get(&table.name);
        let jobs = build_jobs(table, bounds, defaults, options, &mut table_warnings)?;
        table_plans.push(TablePlan {
            table: table.name.clone(),
            priority: table.priority,
            extract_strategy: table.extract_strategy,
            output_partitions: table.output_partitions.clone(),
            order_by: table.order_by.clone(),
            columns,
            jobs,
            warnings: table_warnings,
        });
    }

    Ok(ExportPlan {
        source_name: runtime.source.name.clone(),
        source_schema: runtime.source.schema.clone(),
        snapshot_date: options
            .snapshot_date_override
            .clone()
            .unwrap_or_else(|| runtime.output.snapshot_date.clone()),
        dry_run: options.dry_run,
        offline: options.offline,
        defaults: PlanDefaults {
            compression: defaults.compression.clone(),
            row_group_rows: defaults.row_group_rows,
            target_file_bytes: defaults.target_file_bytes,
            db_read_connections: options
                .max_db_connections_override
                .unwrap_or(defaults.db_read_connections),
            writer_workers: options
                .writer_workers_override
                .unwrap_or(defaults.writer_workers),
        },
        tables: table_plans,
        warnings,
    })
}

impl ExportPlan {
    pub fn to_text(&self) -> String {
        let mut out = String::new();
        out.push_str(&format!(
            "source={} schema={} snapshot_date={} dry_run={} offline={}\n",
            self.source_name, self.source_schema, self.snapshot_date, self.dry_run, self.offline
        ));
        out.push_str(&format!(
            "defaults: compression={} row_group_rows={} target_file_bytes={} db_read_connections={} writer_workers={}\n",
            self.defaults.compression,
            self.defaults.row_group_rows,
            self.defaults.target_file_bytes,
            self.defaults.db_read_connections,
            self.defaults.writer_workers
        ));

        for warning in &self.warnings {
            out.push_str(&format!("warning: {warning}\n"));
        }

        for table in &self.tables {
            out.push_str(&format!(
                "\n[{}] priority={} strategy={} columns={} jobs={}\n",
                table.table,
                table.priority,
                table.extract_strategy,
                table.columns.len(),
                table.jobs.len()
            ));
            if !table.output_partitions.is_empty() {
                out.push_str(&format!(
                    "  partitions: {}\n",
                    table.output_partitions.join(", ")
                ));
            }
            if !table.order_by.is_empty() {
                out.push_str(&format!("  order_by: {}\n", table.order_by.join(", ")));
            }
            for warning in &table.warnings {
                out.push_str(&format!("  warning: {warning}\n"));
            }
            for job in &table.jobs {
                out.push_str(&format!("  - {}: {}\n", job.job_id, job.extract_predicate));
            }
        }

        out
    }
}

fn plan_columns(columns: &[ColumnInfo], jsonb_columns: &[String]) -> Vec<PlanColumn> {
    columns
        .iter()
        .map(|column| PlanColumn {
            name: column.column_name.clone(),
            ordinal_position: column.ordinal_position,
            nullable: column.is_nullable,
            pg_type: pg_type_display(column),
            arrow_type: arrow_type_for_pg(column),
            jsonb_text_preserved: jsonb_columns.contains(&column.column_name)
                || column.udt_name == "jsonb",
        })
        .collect()
}

fn build_jobs(
    table: &TableConfig,
    bounds: Option<&TableBounds>,
    defaults: &Defaults,
    options: &PlanOptions,
    warnings: &mut Vec<String>,
) -> Result<Vec<ExportJob>> {
    match table.extract_strategy {
        ExtractStrategy::RawIdRange => {
            build_raw_id_jobs(table, bounds, defaults, options, warnings)
        }
        ExtractStrategy::DateMonth => build_date_month_jobs(table, bounds, options, warnings),
        ExtractStrategy::FullTable => Ok(vec![ExportJob {
            job_id: format!("{}:full", table.name),
            table: table.name.clone(),
            extract_predicate: "TRUE".to_string(),
            output_partitions: table.output_partitions.clone(),
            order_by: table.order_by.clone(),
            expected_min_key: None,
            expected_max_key: None,
            notes: vec!["single full-table scan".to_string()],
        }]),
        ExtractStrategy::SnapshotItems => Ok(vec![ExportJob {
            job_id: format!("{}:snapshot-items", table.name),
            table: table.name.clone(),
            extract_predicate:
                "snapshot_id joined to stock_master_snapshot to derive snapshot_date".to_string(),
            output_partitions: table.output_partitions.clone(),
            order_by: table.order_by.clone(),
            expected_min_key: None,
            expected_max_key: None,
            notes: vec!["single snapshot-items scan partitioned by snapshot_date".to_string()],
        }]),
        ExtractStrategy::EmptyTable => Ok(Vec::new()),
    }
}

fn build_raw_id_jobs(
    table: &TableConfig,
    bounds: Option<&TableBounds>,
    defaults: &Defaults,
    options: &PlanOptions,
    warnings: &mut Vec<String>,
) -> Result<Vec<ExportJob>> {
    let extract_key = table.extract_key.as_deref().ok_or_else(|| {
        ExporterError::InvalidConfig(format!(
            "{} uses raw_id_range but extract_key is missing",
            table.name
        ))
    })?;
    let chunk_rows = options
        .chunk_rows_override
        .or(table.chunk_rows)
        .unwrap_or(defaults.row_group_rows as i64);
    if chunk_rows <= 0 {
        return Err(ExporterError::InvalidConfig(format!(
            "{} chunk_rows must be positive",
            table.name
        )));
    }

    let Some(TableBounds::RawId { min, max }) = bounds else {
        warnings.push("raw_id bounds are unavailable; emitted placeholder job".to_string());
        return Ok(vec![ExportJob {
            job_id: format!("{}:raw-id:offline", table.name),
            table: table.name.clone(),
            extract_predicate: format!("{extract_key} range unavailable"),
            output_partitions: table.output_partitions.clone(),
            order_by: table.order_by.clone(),
            expected_min_key: None,
            expected_max_key: None,
            notes: vec!["run without --offline to query min/max extract key".to_string()],
        }]);
    };

    let (Some(min), Some(max)) = (*min, *max) else {
        warnings.push("source table is empty".to_string());
        return Ok(Vec::new());
    };

    let mut jobs = Vec::new();
    let mut start = min;
    while start <= max {
        let exclusive_end = cmp::min(start.saturating_add(chunk_rows), max.saturating_add(1));
        let inclusive_end = exclusive_end.saturating_sub(1);
        jobs.push(ExportJob {
            job_id: format!("{}:raw-id:{}-{}", table.name, start, inclusive_end),
            table: table.name.clone(),
            extract_predicate: format!(
                "{extract_key} >= {start} AND {extract_key} < {exclusive_end}"
            ),
            output_partitions: table.output_partitions.clone(),
            order_by: table.order_by.clone(),
            expected_min_key: Some(start.to_string()),
            expected_max_key: Some(inclusive_end.to_string()),
            notes: Vec::new(),
        });

        if exclusive_end <= start {
            break;
        }
        start = exclusive_end;
    }

    Ok(jobs)
}

fn build_date_month_jobs(
    table: &TableConfig,
    bounds: Option<&TableBounds>,
    options: &PlanOptions,
    warnings: &mut Vec<String>,
) -> Result<Vec<ExportJob>> {
    let date_column = table.date_column.as_deref().ok_or_else(|| {
        ExporterError::InvalidConfig(format!(
            "{} uses date_month but date_column is missing",
            table.name
        ))
    })?;

    let Some(TableBounds::Date { min, max }) = bounds else {
        warnings.push("date bounds are unavailable; emitted placeholder job".to_string());
        return Ok(vec![ExportJob {
            job_id: format!("{}:date-month:offline", table.name),
            table: table.name.clone(),
            extract_predicate: format!("{date_column} month range unavailable"),
            output_partitions: table.output_partitions.clone(),
            order_by: table.order_by.clone(),
            expected_min_key: None,
            expected_max_key: None,
            notes: vec!["run without --offline to query min/max date".to_string()],
        }]);
    };

    let (Some(min), Some(max)) = (min.as_deref(), max.as_deref()) else {
        warnings.push("source table is empty".to_string());
        return Ok(Vec::new());
    };

    let mut start_month = parse_year_month(min)?;
    let max_month = parse_year_month(max)?;
    if let Some(since_date) = &options.since_date {
        start_month = cmp::max(start_month, parse_year_month(since_date)?);
    }
    let mut end_month = max_month;
    if let Some(until_date) = &options.until_date {
        end_month = cmp::min(end_month, parse_year_month(until_date)?);
    }

    if start_month > end_month {
        warnings.push("date filters produced no monthly jobs".to_string());
        return Ok(Vec::new());
    }

    let mut jobs = Vec::new();
    let mut current = start_month;
    while current <= end_month {
        let next = next_month(current);
        let start_date = format!("{:04}-{:02}-01", current.0, current.1);
        let next_date = format!("{:04}-{:02}-01", next.0, next.1);
        jobs.push(ExportJob {
            job_id: format!("{}:{:04}-{:02}", table.name, current.0, current.1),
            table: table.name.clone(),
            extract_predicate: format!(
                "{date_column} >= DATE '{start_date}' AND {date_column} < DATE '{next_date}'"
            ),
            output_partitions: table.output_partitions.clone(),
            order_by: table.order_by.clone(),
            expected_min_key: Some(start_date),
            expected_max_key: Some(format!("{:04}-{:02}", current.0, current.1)),
            notes: Vec::new(),
        });
        current = next;
    }

    Ok(jobs)
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

fn invalid_date(date: &str) -> ExporterError {
    ExporterError::InvalidConfig(format!(
        "expected date formatted as YYYY-MM-DD, got `{date}`"
    ))
}

fn next_month((year, month): (i32, u32)) -> (i32, u32) {
    if month == 12 {
        (year + 1, 1)
    } else {
        (year, month + 1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Defaults, SourceConfig};

    fn table(strategy: ExtractStrategy) -> TableConfig {
        TableConfig {
            name: "daily_ohlcv".to_string(),
            priority: Priority::P0,
            extract_strategy: strategy,
            extract_key: None,
            date_column: None,
            chunk_rows: None,
            output_partitions: Vec::new(),
            order_by: Vec::new(),
            jsonb_columns: Vec::new(),
        }
    }

    fn runtime() -> RuntimeConfig {
        RuntimeConfig {
            source: SourceConfig::default(),
            output: Default::default(),
        }
    }

    fn options() -> PlanOptions {
        PlanOptions {
            chunk_rows_override: None,
            max_db_connections_override: None,
            writer_workers_override: None,
            since_date: None,
            until_date: None,
            snapshot_date_override: None,
            offline: false,
            dry_run: true,
        }
    }

    #[test]
    fn builds_raw_id_chunks() {
        let mut table = table(ExtractStrategy::RawIdRange);
        table.name = "dart_xbrl_fact_raw".to_string();
        table.extract_key = Some("raw_id".to_string());
        table.chunk_rows = Some(1_000);
        let mut bounds = HashMap::new();
        bounds.insert(
            table.name.clone(),
            TableBounds::RawId {
                min: Some(1),
                max: Some(2_500),
            },
        );

        let plan = build_plan(
            &Defaults::default(),
            &runtime(),
            &[table],
            &HashMap::new(),
            &bounds,
            &options(),
        )
        .unwrap();

        assert_eq!(plan.tables[0].jobs.len(), 3);
        assert_eq!(
            plan.tables[0].jobs[0].extract_predicate,
            "raw_id >= 1 AND raw_id < 1001"
        );
    }

    #[test]
    fn builds_month_jobs_with_filters() {
        let mut table = table(ExtractStrategy::DateMonth);
        table.date_column = Some("trade_date".to_string());
        let mut bounds = HashMap::new();
        bounds.insert(
            table.name.clone(),
            TableBounds::Date {
                min: Some("2024-12-15".to_string()),
                max: Some("2025-02-20".to_string()),
            },
        );
        let mut options = options();
        options.since_date = Some("2025-01-01".to_string());

        let plan = build_plan(
            &Defaults::default(),
            &runtime(),
            &[table],
            &HashMap::new(),
            &bounds,
            &options,
        )
        .unwrap();

        assert_eq!(plan.tables[0].jobs.len(), 2);
        assert!(plan.tables[0].jobs[0]
            .extract_predicate
            .contains("DATE '2025-01-01'"));
    }
}
