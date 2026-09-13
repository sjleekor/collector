use crate::db::ColumnInfo;
use crate::error::{ExporterError, Result};
use crate::manifest::{ManifestColumn, ManifestSchema};

use sha2::{Digest, Sha256};

pub fn pg_type_display(column: &ColumnInfo) -> String {
    match (
        column.udt_name.as_str(),
        column.numeric_precision,
        column.numeric_scale,
    ) {
        ("numeric", Some(precision), Some(scale)) => format!("numeric({precision},{scale})"),
        _ => column.data_type.clone(),
    }
}

pub fn arrow_type_for_pg(column: &ColumnInfo) -> String {
    match column.udt_name.as_str() {
        "int8" => "Int64".to_string(),
        "int4" => "Int32".to_string(),
        "bool" => "Boolean".to_string(),
        "date" => "Date32".to_string(),
        "timestamptz" => "Timestamp(Microsecond, UTC)".to_string(),
        "timestamp" => "Timestamp(Microsecond, None)".to_string(),
        "numeric" => {
            let precision = column.numeric_precision.unwrap_or(38);
            let scale = column.numeric_scale.unwrap_or(10);
            format!("Decimal128({precision},{scale})")
        }
        "jsonb" | "uuid" | "text" | "varchar" | "bpchar" => "LargeUtf8".to_string(),
        _ => "LargeUtf8".to_string(),
    }
}

pub fn build_manifest_schema(columns: &[ColumnInfo]) -> Result<ManifestSchema> {
    let manifest_columns = columns
        .iter()
        .map(manifest_column_for_pg)
        .collect::<Vec<_>>();
    let canonical = serde_json::to_vec(&manifest_columns).map_err(ExporterError::from)?;
    let mut hasher = Sha256::new();
    hasher.update(canonical);
    let hash = hex_encode(&hasher.finalize());

    Ok(ManifestSchema {
        hash_algorithm: "sha256".to_string(),
        hash,
        columns: manifest_columns,
    })
}

pub fn manifest_column_for_pg(column: &ColumnInfo) -> ManifestColumn {
    ManifestColumn {
        name: column.column_name.clone(),
        ordinal_position: column.ordinal_position,
        nullable: column.is_nullable,
        pg_type: pg_type_display(column),
        pg_data_type: column.data_type.clone(),
        pg_udt_name: column.udt_name.clone(),
        arrow_type: arrow_type_for_pg(column),
        column_default: column.column_default.clone(),
        numeric_precision: column.numeric_precision,
        numeric_scale: column.numeric_scale,
        datetime_precision: column.datetime_precision,
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    fn column(udt_name: &str, data_type: &str) -> ColumnInfo {
        ColumnInfo {
            table_name: "t".to_string(),
            column_name: "c".to_string(),
            ordinal_position: 1,
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
    fn maps_jsonb_to_large_utf8() {
        assert_eq!(arrow_type_for_pg(&column("jsonb", "jsonb")), "LargeUtf8");
    }

    #[test]
    fn maps_uuid_to_large_utf8() {
        assert_eq!(arrow_type_for_pg(&column("uuid", "uuid")), "LargeUtf8");
    }

    #[test]
    fn maps_timestamptz_to_utc_timestamp() {
        assert_eq!(
            arrow_type_for_pg(&column("timestamptz", "timestamp with time zone")),
            "Timestamp(Microsecond, UTC)"
        );
    }

    #[test]
    fn manifest_schema_hash_is_stable_for_same_columns() {
        let columns = vec![column("int8", "bigint"), column("jsonb", "jsonb")];

        let left = build_manifest_schema(&columns).unwrap();
        let right = build_manifest_schema(&columns).unwrap();

        assert_eq!(left.hash_algorithm, "sha256");
        assert_eq!(left.hash.len(), 64);
        assert_eq!(left.hash, right.hash);
        assert_eq!(left.columns[1].arrow_type, "LargeUtf8");
    }
}
