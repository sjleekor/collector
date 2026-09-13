use std::collections::{BTreeSet, HashMap, HashSet};
use std::fs::File;
use std::path::{Path, PathBuf};

use arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Decimal128Array, Int32Array, Int64Array,
    LargeStringArray, StringArray, TimestampMicrosecondArray,
};
use chrono::{Duration, NaiveDate};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde::Serialize;

use crate::config::RuntimeConfig;
use crate::db::{quote_ident, Db};
use crate::error::{ExporterError, Result};
use crate::manifest::{read_manifest, ExportManifest, ManifestColumn};

#[derive(Debug, Clone, Serialize)]
pub struct SampleValidationReport {
    pub manifest: PathBuf,
    pub table: String,
    pub key_column: String,
    pub requested_raw_ids: Vec<i64>,
    pub columns_checked: usize,
    pub compared_rows: usize,
    pub missing_in_source: Vec<i64>,
    pub missing_in_parquet: Vec<i64>,
    pub mismatches: Vec<SampleMismatch>,
    pub passed: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct SampleMismatch {
    pub raw_id: i64,
    pub column: String,
    pub source: Option<String>,
    pub parquet: Option<String>,
}

type SampleRow = HashMap<String, Option<String>>;

pub async fn validate_raw_id_samples(
    db: &Db,
    runtime: &RuntimeConfig,
    manifest_path: &Path,
    requested_raw_ids: &[i64],
) -> Result<SampleValidationReport> {
    let manifest = read_manifest(manifest_path)?;
    if manifest.source.name != runtime.source.name
        || manifest.source.schema != runtime.source.schema
    {
        return Err(ExporterError::InvalidConfig(format!(
            "manifest source {}.{} does not match runtime source {}.{}",
            manifest.source.name,
            manifest.source.schema,
            runtime.source.name,
            runtime.source.schema
        )));
    }

    let schema = manifest.table.schema.as_ref().ok_or_else(|| {
        ExporterError::InvalidConfig(format!(
            "manifest for {} does not include schema metadata",
            manifest.table.name
        ))
    })?;
    if !schema.columns.iter().any(|column| column.name == "raw_id") {
        return Err(ExporterError::InvalidConfig(format!(
            "{} sample validation currently requires a raw_id column",
            manifest.table.name
        )));
    }

    let raw_ids = resolve_raw_ids(&manifest, requested_raw_ids)?;
    let source_rows = fetch_source_rows(db, &runtime.source.schema, &manifest, &raw_ids).await?;
    let parquet_rows = fetch_parquet_rows(&manifest, &raw_ids)?;

    let mut missing_in_source = Vec::new();
    let mut missing_in_parquet = Vec::new();
    let mut mismatches = Vec::new();
    for raw_id in &raw_ids {
        let source = source_rows.get(raw_id);
        let parquet = parquet_rows.get(raw_id);
        match (source, parquet) {
            (None, None) => {
                missing_in_source.push(*raw_id);
                missing_in_parquet.push(*raw_id);
            }
            (None, Some(_)) => missing_in_source.push(*raw_id),
            (Some(_), None) => missing_in_parquet.push(*raw_id),
            (Some(source), Some(parquet)) => {
                for column in &schema.columns {
                    let source_value = source.get(&column.name).cloned().unwrap_or(None);
                    let parquet_value = parquet.get(&column.name).cloned().unwrap_or(None);
                    if source_value != parquet_value {
                        mismatches.push(SampleMismatch {
                            raw_id: *raw_id,
                            column: column.name.clone(),
                            source: source_value,
                            parquet: parquet_value,
                        });
                    }
                }
            }
        }
    }

    let compared_rows = raw_ids
        .iter()
        .filter(|raw_id| source_rows.contains_key(raw_id) && parquet_rows.contains_key(raw_id))
        .count();
    let passed =
        missing_in_source.is_empty() && missing_in_parquet.is_empty() && mismatches.is_empty();

    Ok(SampleValidationReport {
        manifest: manifest_path.to_path_buf(),
        table: manifest.table.name,
        key_column: "raw_id".to_string(),
        requested_raw_ids: raw_ids,
        columns_checked: schema.columns.len(),
        compared_rows,
        missing_in_source,
        missing_in_parquet,
        mismatches,
        passed,
    })
}

fn resolve_raw_ids(manifest: &ExportManifest, requested_raw_ids: &[i64]) -> Result<Vec<i64>> {
    let ids = if requested_raw_ids.is_empty() {
        let min = manifest
            .table
            .min_raw_id
            .or_else(|| {
                manifest
                    .table
                    .files
                    .iter()
                    .filter_map(|file| file.min_raw_id)
                    .min()
            })
            .ok_or_else(|| {
                ExporterError::InvalidConfig(format!(
                    "{} manifest has no raw_id bounds; pass --raw-ids explicitly",
                    manifest.table.name
                ))
            })?;
        let max = manifest
            .table
            .max_raw_id
            .or_else(|| {
                manifest
                    .table
                    .files
                    .iter()
                    .filter_map(|file| file.max_raw_id)
                    .max()
            })
            .ok_or_else(|| {
                ExporterError::InvalidConfig(format!(
                    "{} manifest has no raw_id bounds; pass --raw-ids explicitly",
                    manifest.table.name
                ))
            })?;
        vec![min, min + (max - min) / 2, max]
    } else {
        requested_raw_ids.to_vec()
    };

    let mut deduped = BTreeSet::new();
    for raw_id in ids {
        deduped.insert(raw_id);
    }
    Ok(deduped.into_iter().collect())
}

async fn fetch_source_rows(
    db: &Db,
    schema: &str,
    manifest: &ExportManifest,
    raw_ids: &[i64],
) -> Result<HashMap<i64, SampleRow>> {
    let table_schema = manifest.table.schema.as_ref().ok_or_else(|| {
        ExporterError::InvalidConfig(format!(
            "manifest for {} does not include schema metadata",
            manifest.table.name
        ))
    })?;
    let select_list = table_schema
        .columns
        .iter()
        .map(source_sample_expr)
        .collect::<Result<Vec<_>>>()?
        .join(", ");
    let sql = format!(
        "SELECT {raw_id} AS {sample_key}, {select_list} FROM {schema}.{table} WHERE {raw_id} = ANY($1::bigint[]) ORDER BY {raw_id}",
        raw_id = quote_ident("raw_id")?,
        sample_key = quote_ident("__sample_raw_id")?,
        schema = quote_ident(schema)?,
        table = quote_ident(&manifest.table.name)?,
    );
    let raw_ids_param = raw_ids.to_vec();
    let rows = db.client().query(&sql, &[&raw_ids_param]).await?;

    let mut by_raw_id = HashMap::new();
    for row in rows {
        let raw_id: i64 = row.try_get("__sample_raw_id")?;
        let mut values = HashMap::new();
        for column in &table_schema.columns {
            let value: Option<String> = row.try_get(column.name.as_str())?;
            values.insert(column.name.clone(), value);
        }
        by_raw_id.insert(raw_id, values);
    }
    Ok(by_raw_id)
}

fn fetch_parquet_rows(
    manifest: &ExportManifest,
    raw_ids: &[i64],
) -> Result<HashMap<i64, SampleRow>> {
    let table_schema = manifest.table.schema.as_ref().ok_or_else(|| {
        ExporterError::InvalidConfig(format!(
            "manifest for {} does not include schema metadata",
            manifest.table.name
        ))
    })?;
    let raw_id_targets = raw_ids.iter().copied().collect::<HashSet<_>>();
    let mut rows = HashMap::new();

    for manifest_file in &manifest.table.files {
        if !file_may_contain_raw_id(manifest_file.min_raw_id, manifest_file.max_raw_id, raw_ids) {
            continue;
        }
        let file = File::open(&manifest_file.path).map_err(|source| ExporterError::ReadFile {
            path: manifest_file.path.clone(),
            source,
        })?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)?
            .with_batch_size(8192)
            .build()?;

        for batch in reader {
            let batch = batch?;
            let raw_id_index = batch
                .schema()
                .index_of("raw_id")
                .map_err(ExporterError::from)?;
            let raw_id_array = batch
                .column(raw_id_index)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| {
                    ExporterError::InvalidData(
                        "Parquet raw_id column is not an Int64 array".to_string(),
                    )
                })?;

            for row_index in 0..batch.num_rows() {
                if raw_id_array.is_null(row_index) {
                    continue;
                }
                let raw_id = raw_id_array.value(row_index);
                if !raw_id_targets.contains(&raw_id) {
                    continue;
                }

                let mut values = HashMap::new();
                for column in &table_schema.columns {
                    let column_index = batch.schema().index_of(&column.name)?;
                    let value = parquet_value_as_sample_string(
                        batch.column(column_index),
                        row_index,
                        column,
                    )?;
                    values.insert(column.name.clone(), value);
                }
                rows.insert(raw_id, values);
            }
        }
    }

    Ok(rows)
}

fn file_may_contain_raw_id(
    min_raw_id: Option<i64>,
    max_raw_id: Option<i64>,
    raw_ids: &[i64],
) -> bool {
    let (Some(min_raw_id), Some(max_raw_id)) = (min_raw_id, max_raw_id) else {
        return true;
    };
    raw_ids
        .iter()
        .any(|raw_id| *raw_id >= min_raw_id && *raw_id <= max_raw_id)
}

fn source_sample_expr(column: &ManifestColumn) -> Result<String> {
    let name = quote_ident(&column.name)?;
    let expr = match column.pg_udt_name.as_str() {
        "date" => format!("{name}::text"),
        "timestamptz" => format!("floor(extract(epoch from {name}) * 1000000)::bigint::text"),
        "timestamp" => {
            format!(
                "floor(extract(epoch from ({name} AT TIME ZONE 'UTC')) * 1000000)::bigint::text"
            )
        }
        _ => format!("{name}::text"),
    };
    Ok(format!("{expr} AS {name}"))
}

fn parquet_value_as_sample_string(
    array: &ArrayRef,
    row_index: usize,
    column: &ManifestColumn,
) -> Result<Option<String>> {
    if array.is_null(row_index) {
        return Ok(None);
    }
    let value = match column.pg_udt_name.as_str() {
        "int8" => downcast_value::<Int64Array>(array, &column.name)?
            .value(row_index)
            .to_string(),
        "int4" => downcast_value::<Int32Array>(array, &column.name)?
            .value(row_index)
            .to_string(),
        "bool" => downcast_value::<BooleanArray>(array, &column.name)?
            .value(row_index)
            .to_string(),
        "date" => {
            let days = downcast_value::<Date32Array>(array, &column.name)?.value(row_index);
            date32_days_to_string(days)?
        }
        "timestamptz" | "timestamp" => {
            downcast_value::<TimestampMicrosecondArray>(array, &column.name)?
                .value(row_index)
                .to_string()
        }
        "numeric" => {
            let value = downcast_value::<Decimal128Array>(array, &column.name)?.value(row_index);
            decimal128_to_string(value, column.numeric_scale.unwrap_or(0))
        }
        "jsonb" | "text" | "varchar" | "bpchar" | "uuid" => {
            string_array_value(array, row_index, &column.name)?
        }
        other => {
            return Err(ExporterError::InvalidData(format!(
                "unsupported PostgreSQL type for sample validation {}: {}",
                column.name, other
            )))
        }
    };
    Ok(Some(value))
}

fn downcast_value<'a, T: 'static>(array: &'a ArrayRef, column_name: &str) -> Result<&'a T> {
    array.as_any().downcast_ref::<T>().ok_or_else(|| {
        ExporterError::InvalidData(format!(
            "Parquet column {column_name} has an unexpected Arrow type"
        ))
    })
}

fn string_array_value(array: &ArrayRef, row_index: usize, column_name: &str) -> Result<String> {
    if let Some(array) = array.as_any().downcast_ref::<LargeStringArray>() {
        return Ok(array.value(row_index).to_string());
    }
    if let Some(array) = array.as_any().downcast_ref::<StringArray>() {
        return Ok(array.value(row_index).to_string());
    }
    Err(ExporterError::InvalidData(format!(
        "Parquet column {column_name} is not a UTF8 array"
    )))
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

fn decimal128_to_string(value: i128, scale: i32) -> String {
    if scale <= 0 {
        return value.to_string();
    }

    let sign = if value < 0 { "-" } else { "" };
    let absolute = value.abs();
    let factor = 10_i128.pow(scale as u32);
    let integer = absolute / factor;
    let fraction = absolute % factor;
    format!("{sign}{integer}.{fraction:0width$}", width = scale as usize)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_decimal128_with_fixed_scale() {
        assert_eq!(decimal128_to_string(0, 4), "0.0000");
        assert_eq!(decimal128_to_string(477_870_000_000, 4), "47787000.0000");
        assert_eq!(decimal128_to_string(-12345, 2), "-123.45");
    }

    #[test]
    fn resolves_default_raw_ids_from_manifest_bounds() {
        let manifest = ExportManifest {
            run_id: "run".to_string(),
            created_at_unix_seconds: 0,
            source: crate::manifest::ManifestSource {
                name: "local".to_string(),
                schema: "public".to_string(),
                snapshot_date: "2026-06-19".to_string(),
                snapshot_policy: "test".to_string(),
            },
            table: crate::manifest::ManifestTable {
                name: "t".to_string(),
                schema: None,
                rows_exported: 100,
                files: Vec::new(),
                extract_predicate: "raw_id >= 1 AND raw_id < 101".to_string(),
                min_raw_id: Some(1),
                max_raw_id: Some(100),
            },
        };

        assert_eq!(resolve_raw_ids(&manifest, &[]).unwrap(), vec![1, 50, 100]);
        assert_eq!(resolve_raw_ids(&manifest, &[7, 7, 3]).unwrap(), vec![3, 7]);
    }
}
