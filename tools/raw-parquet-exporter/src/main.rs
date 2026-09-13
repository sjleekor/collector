use std::collections::HashMap;

use clap::Parser;
use raw_parquet_exporter::cli::{
    Cli, Commands, ExportArgs, PlanArgs, PlanFormat, ResumeArgs, ValidateSamplesArgs,
};
use raw_parquet_exporter::config::{load_export_config, load_runtime_config, ExtractStrategy};
use raw_parquet_exporter::db::{Db, TableBounds};
use raw_parquet_exporter::error::{ExporterError, Result};
use raw_parquet_exporter::export::{
    export_date_month_partitioned_table, export_empty_table, export_full_table,
    export_raw_id_partitioned_table, export_snapshot_items, DartXbrlExportOptions,
    DateMonthExportOptions, FullTableExportOptions, ResumeRequest, SchemaOnlyExportOptions,
    SnapshotItemsExportOptions,
};
use raw_parquet_exporter::manifest::{read_checkpoint, validate_manifest};
use raw_parquet_exporter::planner::{build_plan, PlanOptions};
use raw_parquet_exporter::sample_validate::validate_raw_id_samples;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("error: {error}");
        std::process::exit(1);
    }
}

async fn run() -> Result<()> {
    let _ = dotenvy::dotenv();
    let cli = Cli::parse();
    init_tracing(&cli.log_level);

    match cli.command {
        Commands::Plan(args) => run_plan(args).await,
        Commands::Export(args) => run_export(args).await,
        Commands::Validate(args) => {
            let report = validate_manifest(&args.manifest)?;
            println!("{}", serde_json::to_string_pretty(&report)?);
            Ok(())
        }
        Commands::ValidateSamples(args) => run_validate_samples(args).await,
        Commands::Resume(args) => run_resume(args).await,
    }
}

async fn run_validate_samples(args: ValidateSamplesArgs) -> Result<()> {
    let runtime_config = load_runtime_config(&args.runtime)?;
    let db = Db::connect(&runtime_config).await?;
    let report =
        validate_raw_id_samples(&db, &runtime_config, &args.manifest, &args.raw_ids).await?;
    println!("{}", serde_json::to_string_pretty(&report)?);
    if report.passed {
        Ok(())
    } else {
        Err(ExporterError::InvalidData(
            "sample validation found mismatches".to_string(),
        ))
    }
}

async fn run_export(args: ExportArgs) -> Result<()> {
    let export_config = load_export_config(&args.config)?;
    let mut runtime_config = load_runtime_config(&args.runtime)?;
    if let Some(snapshot_date) = args.snapshot_date.clone() {
        runtime_config.output.snapshot_date = snapshot_date;
    }

    let selected_tables = export_config.selected_tables(&args.tables, args.priority)?;
    if selected_tables.len() != 1 {
        return Err(ExporterError::InvalidConfig(
            "export currently supports exactly one selected table".to_string(),
        ));
    }
    let table = selected_tables[0].clone();

    let db = Db::connect(&runtime_config).await?;
    let table_names = vec![table.name.clone()];
    let columns_by_table = db
        .fetch_columns(&runtime_config.source.schema, &table_names)
        .await?;
    let columns = columns_by_table.get(&table.name).ok_or_else(|| {
        ExporterError::InvalidConfig(format!(
            "table `{}` was not found in schema `{}`",
            table.name, runtime_config.source.schema
        ))
    })?;
    let bounds = db
        .fetch_bounds(&runtime_config.source.schema, &table)
        .await?;

    let result = match table.extract_strategy {
        ExtractStrategy::RawIdRange => {
            let options = DartXbrlExportOptions {
                chunk_rows: args
                    .chunk_rows
                    .or(table.chunk_rows)
                    .unwrap_or(export_config.defaults.row_group_rows as i64),
                start_raw_id: args.start_raw_id,
                all_chunks: args.all_chunks,
                batch_rows: args.batch_rows,
                max_rows_per_file: args.max_rows_per_file,
                dry_run: args.dry_run,
                force: args.force,
                resume: None,
            };
            export_raw_id_partitioned_table(
                &db,
                &runtime_config,
                &export_config.defaults,
                &table,
                columns,
                &bounds,
                &options,
            )
            .await?
        }
        ExtractStrategy::DateMonth => {
            let options = DateMonthExportOptions {
                since_date: args.since_date,
                until_date: args.until_date,
                batch_rows: args.batch_rows,
                max_rows_per_file: args.max_rows_per_file,
                dry_run: args.dry_run,
                force: args.force,
                resume: None,
            };
            export_date_month_partitioned_table(
                &db,
                &runtime_config,
                &export_config.defaults,
                &table,
                columns,
                &bounds,
                &options,
            )
            .await?
        }
        ExtractStrategy::FullTable => {
            let options = FullTableExportOptions {
                batch_rows: args.batch_rows,
                max_rows_per_file: args.max_rows_per_file,
                dry_run: args.dry_run,
                force: args.force,
            };
            export_full_table(
                &db,
                &runtime_config,
                &export_config.defaults,
                &table,
                columns,
                &bounds,
                &options,
            )
            .await?
        }
        ExtractStrategy::EmptyTable => {
            let options = SchemaOnlyExportOptions {
                dry_run: args.dry_run,
                force: args.force,
            };
            export_empty_table(&runtime_config, &table, columns, &bounds, &options)?
        }
        ExtractStrategy::SnapshotItems => {
            let options = SnapshotItemsExportOptions {
                batch_rows: args.batch_rows,
                max_rows_per_file: args.max_rows_per_file,
                dry_run: args.dry_run,
                force: args.force,
            };
            export_snapshot_items(
                &db,
                &runtime_config,
                &export_config.defaults,
                &table,
                columns,
                &bounds,
                &options,
            )
            .await?
        }
    };
    println!("{}", serde_json::to_string_pretty(&result)?);
    Ok(())
}

async fn run_resume(args: ResumeArgs) -> Result<()> {
    let checkpoint = read_checkpoint(&args.checkpoint)?;
    let export_config = load_export_config(&args.config)?;
    let mut runtime_config = load_runtime_config(&args.runtime)?;
    runtime_config.output.snapshot_date = checkpoint.source.snapshot_date.clone();
    if checkpoint.source.name != runtime_config.source.name
        || checkpoint.source.schema != runtime_config.source.schema
    {
        return Err(ExporterError::InvalidConfig(
            "checkpoint source does not match runtime source".to_string(),
        ));
    }

    let table = export_config
        .tables
        .iter()
        .find(|table| table.name == checkpoint.table)
        .cloned()
        .ok_or_else(|| {
            ExporterError::InvalidConfig(format!(
                "checkpoint table `{}` was not found in export config",
                checkpoint.table
            ))
        })?;

    let db = Db::connect(&runtime_config).await?;
    let table_names = vec![table.name.clone()];
    let columns_by_table = db
        .fetch_columns(&runtime_config.source.schema, &table_names)
        .await?;
    let columns = columns_by_table.get(&table.name).ok_or_else(|| {
        ExporterError::InvalidConfig(format!(
            "table `{}` was not found in schema `{}`",
            table.name, runtime_config.source.schema
        ))
    })?;
    let bounds = db
        .fetch_bounds(&runtime_config.source.schema, &table)
        .await?;

    let checkpoint_file = args.checkpoint;
    let checkpoint_strategy = checkpoint.strategy.clone();
    let result = match checkpoint_strategy.as_str() {
        "raw_id_range" => {
            let options = DartXbrlExportOptions {
                chunk_rows: checkpoint.chunk_rows,
                start_raw_id: Some(checkpoint.extract_start_raw_id),
                all_chunks: true,
                batch_rows: checkpoint.batch_rows,
                max_rows_per_file: checkpoint.max_rows_per_file,
                dry_run: false,
                force: false,
                resume: Some(ResumeRequest {
                    checkpoint_file,
                    checkpoint,
                }),
            };
            export_raw_id_partitioned_table(
                &db,
                &runtime_config,
                &export_config.defaults,
                &table,
                columns,
                &bounds,
                &options,
            )
            .await?
        }
        "date_month" => {
            let options = DateMonthExportOptions {
                since_date: None,
                until_date: None,
                batch_rows: checkpoint.batch_rows,
                max_rows_per_file: checkpoint.max_rows_per_file,
                dry_run: false,
                force: false,
                resume: Some(ResumeRequest {
                    checkpoint_file,
                    checkpoint,
                }),
            };
            export_date_month_partitioned_table(
                &db,
                &runtime_config,
                &export_config.defaults,
                &table,
                columns,
                &bounds,
                &options,
            )
            .await?
        }
        other => {
            return Err(ExporterError::InvalidConfig(format!(
                "unsupported checkpoint strategy `{other}`"
            )))
        }
    };
    println!("{}", serde_json::to_string_pretty(&result)?);
    Ok(())
}

async fn run_plan(args: PlanArgs) -> Result<()> {
    let export_config = load_export_config(&args.config)?;
    let runtime_config = load_runtime_config(&args.runtime)?;
    let selected_tables = export_config.selected_tables(&args.tables, args.priority)?;
    if selected_tables.is_empty() {
        return Err(ExporterError::InvalidConfig(
            "no tables selected by --tables/--priority".to_string(),
        ));
    }

    let mut columns_by_table = HashMap::new();
    let mut bounds_by_table: HashMap<String, TableBounds> = HashMap::new();

    if !args.offline {
        let db = Db::connect(&runtime_config).await?;
        let table_names: Vec<String> = selected_tables
            .iter()
            .map(|table| table.name.clone())
            .collect();
        columns_by_table = db
            .fetch_columns(&runtime_config.source.schema, &table_names)
            .await?;

        for table in &selected_tables {
            if !columns_by_table.contains_key(&table.name) {
                return Err(ExporterError::InvalidConfig(format!(
                    "table `{}` was not found in schema `{}`",
                    table.name, runtime_config.source.schema
                )));
            }
            let bounds = db
                .fetch_bounds(&runtime_config.source.schema, table)
                .await?;
            bounds_by_table.insert(table.name.clone(), bounds);
        }
    }

    let options = PlanOptions {
        chunk_rows_override: args.chunk_rows,
        max_db_connections_override: args.max_db_connections,
        writer_workers_override: args.writer_workers,
        since_date: args.since_date,
        until_date: args.until_date,
        snapshot_date_override: args.snapshot_date,
        offline: args.offline,
        dry_run: true,
    };
    let plan = build_plan(
        &export_config.defaults,
        &runtime_config,
        &selected_tables,
        &columns_by_table,
        &bounds_by_table,
        &options,
    )?;

    match args.format {
        PlanFormat::Text => print!("{}", plan.to_text()),
        PlanFormat::Json => println!("{}", serde_json::to_string_pretty(&plan)?),
    }

    Ok(())
}

fn init_tracing(log_level: &str) {
    let filter = EnvFilter::try_new(log_level).unwrap_or_else(|_| EnvFilter::new("info"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .try_init();
}
