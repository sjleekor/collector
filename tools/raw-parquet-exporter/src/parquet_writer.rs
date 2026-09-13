use std::fs::File;
use std::sync::Arc;

use arrow::array::builder::{
    BooleanBuilder, Date32Builder, Decimal128Builder, Int32Builder, Int64Builder,
    LargeStringBuilder, TimestampMicrosecondBuilder,
};
use arrow::array::{ArrayRef, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use tokio_postgres::Row;

use crate::db::ColumnInfo;
use crate::error::{ExporterError, Result};

#[derive(Debug, Clone)]
pub enum ColumnKind {
    Int64,
    Int32,
    Boolean,
    Date32,
    TimestampMicrosUtc,
    Decimal128 { precision: u8, scale: i8 },
    LargeUtf8,
}

impl ColumnKind {
    fn from_column(column: &ColumnInfo) -> Result<Self> {
        match column.udt_name.as_str() {
            "int8" => Ok(Self::Int64),
            "int4" => Ok(Self::Int32),
            "bool" => Ok(Self::Boolean),
            "date" => Ok(Self::Date32),
            "timestamptz" => Ok(Self::TimestampMicrosUtc),
            "timestamp" => Ok(Self::TimestampMicrosUtc),
            "numeric" => {
                let precision = column.numeric_precision.ok_or_else(|| {
                    ExporterError::InvalidData(format!(
                        "numeric column {} has no precision",
                        column.column_name
                    ))
                })? as u8;
                let scale = column.numeric_scale.unwrap_or(0) as i8;
                Ok(Self::Decimal128 { precision, scale })
            }
            "jsonb" | "text" | "varchar" | "bpchar" | "uuid" => Ok(Self::LargeUtf8),
            other => Err(ExporterError::InvalidData(format!(
                "unsupported PostgreSQL type for {}: {}",
                column.column_name, other
            ))),
        }
    }

    fn data_type(&self) -> DataType {
        match self {
            Self::Int64 => DataType::Int64,
            Self::Int32 => DataType::Int32,
            Self::Boolean => DataType::Boolean,
            Self::Date32 => DataType::Date32,
            Self::TimestampMicrosUtc => {
                DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into()))
            }
            Self::Decimal128 { precision, scale } => DataType::Decimal128(*precision, *scale),
            Self::LargeUtf8 => DataType::LargeUtf8,
        }
    }
}

enum ArrayBuilder {
    Int64(Int64Builder),
    Int32(Int32Builder),
    Boolean(BooleanBuilder),
    Date32(Date32Builder),
    TimestampMicrosUtc(TimestampMicrosecondBuilder),
    Decimal128 {
        builder: Decimal128Builder,
        precision: u8,
        scale: i8,
    },
    LargeUtf8(LargeStringBuilder),
}

impl ArrayBuilder {
    fn new(kind: &ColumnKind, capacity: usize) -> Result<Self> {
        Ok(match kind {
            ColumnKind::Int64 => Self::Int64(Int64Builder::with_capacity(capacity)),
            ColumnKind::Int32 => Self::Int32(Int32Builder::with_capacity(capacity)),
            ColumnKind::Boolean => Self::Boolean(BooleanBuilder::with_capacity(capacity)),
            ColumnKind::Date32 => Self::Date32(Date32Builder::with_capacity(capacity)),
            ColumnKind::TimestampMicrosUtc => Self::TimestampMicrosUtc(
                TimestampMicrosecondBuilder::with_capacity(capacity).with_timezone("+00:00"),
            ),
            ColumnKind::Decimal128 { precision, scale } => Self::Decimal128 {
                builder: Decimal128Builder::with_capacity(capacity)
                    .with_precision_and_scale(*precision, *scale)?,
                precision: *precision,
                scale: *scale,
            },
            ColumnKind::LargeUtf8 => Self::LargeUtf8(LargeStringBuilder::with_capacity(
                capacity,
                capacity.saturating_mul(64),
            )),
        })
    }

    fn append_row_value(&mut self, row: &Row, index: usize, column_name: &str) -> Result<()> {
        match self {
            Self::Int64(builder) => append_option(builder, row.try_get(index)?),
            Self::Int32(builder) => append_option(builder, row.try_get(index)?),
            Self::Boolean(builder) => {
                let value: Option<bool> = row.try_get(index)?;
                match value {
                    Some(value) => builder.append_value(value),
                    None => builder.append_null(),
                }
            }
            Self::Date32(builder) => append_option(builder, row.try_get(index)?),
            Self::TimestampMicrosUtc(builder) => append_option(builder, row.try_get(index)?),
            Self::Decimal128 { builder, scale, .. } => {
                let value: Option<String> = row.try_get(index)?;
                match value {
                    Some(value) => builder.append_value(parse_decimal_to_i128(&value, *scale)?),
                    None => builder.append_null(),
                }
            }
            Self::LargeUtf8(builder) => {
                let value: Option<String> = row.try_get(index)?;
                match value {
                    Some(value) => builder.append_value(value),
                    None => builder.append_null(),
                }
            }
        }
        tracing::trace!(column = column_name, "appended row value");
        Ok(())
    }

    fn finish(&mut self) -> Result<ArrayRef> {
        Ok(match self {
            Self::Int64(builder) => Arc::new(builder.finish()) as ArrayRef,
            Self::Int32(builder) => Arc::new(builder.finish()) as ArrayRef,
            Self::Boolean(builder) => Arc::new(builder.finish()) as ArrayRef,
            Self::Date32(builder) => Arc::new(builder.finish()) as ArrayRef,
            Self::TimestampMicrosUtc(builder) => Arc::new(builder.finish()) as ArrayRef,
            Self::Decimal128 {
                builder,
                precision,
                scale,
            } => Arc::new(
                builder
                    .finish()
                    .with_precision_and_scale(*precision, *scale)?,
            ) as ArrayRef,
            Self::LargeUtf8(builder) => Arc::new(builder.finish()) as ArrayRef,
        })
    }
}

pub struct RecordBatchBuilder {
    schema: SchemaRef,
    columns: Vec<(String, ColumnKind, ArrayBuilder)>,
    row_count: usize,
}

impl RecordBatchBuilder {
    pub fn try_new(columns: &[ColumnInfo], capacity: usize) -> Result<Self> {
        let mut fields = Vec::with_capacity(columns.len());
        let mut builders = Vec::with_capacity(columns.len());

        for column in columns {
            let kind = ColumnKind::from_column(column)?;
            fields.push(Field::new(
                column.column_name.clone(),
                kind.data_type(),
                column.is_nullable,
            ));
            let builder = ArrayBuilder::new(&kind, capacity)?;
            builders.push((column.column_name.clone(), kind, builder));
        }

        Ok(Self {
            schema: Arc::new(Schema::new(fields)),
            columns: builders,
            row_count: 0,
        })
    }

    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    pub fn row_count(&self) -> usize {
        self.row_count
    }

    pub fn is_empty(&self) -> bool {
        self.row_count == 0
    }

    pub fn append_row(&mut self, row: &Row) -> Result<()> {
        for (index, (column_name, _, builder)) in self.columns.iter_mut().enumerate() {
            builder.append_row_value(row, index, column_name)?;
        }
        self.row_count += 1;
        Ok(())
    }

    pub fn finish(&mut self, capacity: usize) -> Result<RecordBatch> {
        let arrays = self
            .columns
            .iter_mut()
            .map(|(_, _, builder)| builder.finish())
            .collect::<Result<Vec<_>>>()?;
        let batch = RecordBatch::try_new(self.schema.clone(), arrays)?;

        for (_, kind, builder) in &mut self.columns {
            *builder = ArrayBuilder::new(kind, capacity)?;
        }
        self.row_count = 0;

        Ok(batch)
    }
}

pub fn new_arrow_writer(
    file: File,
    schema: SchemaRef,
    compression: &str,
    row_group_rows: usize,
) -> Result<ArrowWriter<File>> {
    let compression = match compression.to_ascii_lowercase().as_str() {
        "zstd" => Compression::ZSTD(ZstdLevel::default()),
        "uncompressed" | "none" => Compression::UNCOMPRESSED,
        other => {
            return Err(ExporterError::InvalidConfig(format!(
                "unsupported compression `{other}`; use zstd or uncompressed"
            )))
        }
    };
    let props = WriterProperties::builder()
        .set_compression(compression)
        .set_max_row_group_size(row_group_rows)
        .build();

    ArrowWriter::try_new(file, schema, Some(props)).map_err(ExporterError::from)
}

fn append_option<T>(
    builder: &mut arrow::array::builder::PrimitiveBuilder<T>,
    value: Option<T::Native>,
) where
    T: arrow::datatypes::ArrowPrimitiveType,
{
    match value {
        Some(value) => builder.append_value(value),
        None => builder.append_null(),
    }
}

pub fn parse_decimal_to_i128(value: &str, scale: i8) -> Result<i128> {
    if scale < 0 {
        return Err(ExporterError::InvalidData(format!(
            "negative decimal scale is unsupported: {scale}"
        )));
    }
    let scale = scale as usize;
    let value = value.trim();
    if value.is_empty() {
        return Err(ExporterError::InvalidData(
            "cannot parse empty decimal".to_string(),
        ));
    }
    if value.eq_ignore_ascii_case("nan") {
        return Err(ExporterError::InvalidData(
            "cannot encode numeric NaN as Decimal128".to_string(),
        ));
    }

    let (negative, unsigned) = match value.as_bytes()[0] {
        b'-' => (true, &value[1..]),
        b'+' => (false, &value[1..]),
        _ => (false, value),
    };
    let mut parts = unsigned.split('.');
    let int_part = parts.next().unwrap_or_default();
    let frac_part = parts.next().unwrap_or_default();
    if parts.next().is_some() {
        return Err(ExporterError::InvalidData(format!(
            "invalid decimal value `{value}`"
        )));
    }
    if !int_part.chars().all(|ch| ch.is_ascii_digit())
        || !frac_part.chars().all(|ch| ch.is_ascii_digit())
    {
        return Err(ExporterError::InvalidData(format!(
            "invalid decimal value `{value}`"
        )));
    }
    if frac_part.len() > scale && frac_part[scale..].chars().any(|ch| ch != '0') {
        return Err(ExporterError::InvalidData(format!(
            "decimal `{value}` has more than {scale} non-zero fractional digits"
        )));
    }

    let factor = 10_i128.pow(scale as u32);
    let int_value = if int_part.is_empty() {
        0
    } else {
        int_part.parse::<i128>().map_err(|source| {
            ExporterError::InvalidData(format!("invalid decimal integer part `{value}`: {source}"))
        })?
    };
    let mut frac = frac_part.chars().take(scale).collect::<String>();
    while frac.len() < scale {
        frac.push('0');
    }
    let frac_value = if frac.is_empty() {
        0
    } else {
        frac.parse::<i128>().map_err(|source| {
            ExporterError::InvalidData(format!(
                "invalid decimal fractional part `{value}`: {source}"
            ))
        })?
    };

    let scaled = int_value
        .checked_mul(factor)
        .and_then(|value| value.checked_add(frac_value))
        .ok_or_else(|| ExporterError::InvalidData(format!("decimal `{value}` overflows i128")))?;

    Ok(if negative { -scaled } else { scaled })
}

#[cfg(test)]
mod tests {
    use super::parse_decimal_to_i128;

    #[test]
    fn parses_decimal_with_fixed_scale() {
        assert_eq!(parse_decimal_to_i128("123.45", 4).unwrap(), 1_234_500);
        assert_eq!(parse_decimal_to_i128("-0.0100", 4).unwrap(), -100);
        assert_eq!(parse_decimal_to_i128("42", 4).unwrap(), 420_000);
    }

    #[test]
    fn rejects_precision_loss() {
        assert!(parse_decimal_to_i128("1.00001", 4).is_err());
        assert_eq!(parse_decimal_to_i128("1.00000", 4).unwrap(), 10_000);
    }
}
