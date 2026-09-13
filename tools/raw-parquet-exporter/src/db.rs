use std::collections::HashMap;
use std::env;

use serde::Serialize;
use tokio_postgres::{Client, Config as PgConfig, NoTls};

use crate::config::{validate_simple_identifier, ExtractStrategy, RuntimeConfig, TableConfig};
use crate::error::{ExporterError, Result};

pub struct Db {
    client: Client,
}

#[derive(Debug, Clone, Serialize)]
pub struct ColumnInfo {
    pub table_name: String,
    pub column_name: String,
    pub ordinal_position: i32,
    pub is_nullable: bool,
    pub data_type: String,
    pub udt_name: String,
    pub column_default: Option<String>,
    pub numeric_precision: Option<i32>,
    pub numeric_scale: Option<i32>,
    pub datetime_precision: Option<i32>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TableBounds {
    RawId {
        min: Option<i64>,
        max: Option<i64>,
    },
    Date {
        min: Option<String>,
        max: Option<String>,
    },
    FullTable,
    SnapshotItems,
    EmptyTable,
}

impl Db {
    pub async fn connect(runtime: &RuntimeConfig) -> Result<Self> {
        let (client, connection) = connect_postgres(runtime).await?;
        tokio::spawn(async move {
            if let Err(error) = connection.await {
                tracing::error!(%error, "postgres connection task failed");
            }
        });

        apply_session_settings(&client, runtime.source.read_only).await?;
        Ok(Self { client })
    }

    pub fn client(&self) -> &Client {
        &self.client
    }

    pub async fn fetch_columns(
        &self,
        schema: &str,
        table_names: &[String],
    ) -> Result<HashMap<String, Vec<ColumnInfo>>> {
        validate_simple_identifier(schema)?;
        let rows = self
            .client
            .query(
                "
                SELECT
                    table_name,
                    column_name,
                    ordinal_position,
                    is_nullable,
                    data_type,
                    udt_name,
                    column_default,
                    numeric_precision,
                    numeric_scale,
                    datetime_precision
                FROM information_schema.columns
                WHERE table_schema = $1
                  AND table_name = ANY($2)
                ORDER BY table_name, ordinal_position
                ",
                &[&schema, &table_names],
            )
            .await?;

        let mut by_table: HashMap<String, Vec<ColumnInfo>> = HashMap::new();
        for row in rows {
            let is_nullable: String = row.get("is_nullable");
            let info = ColumnInfo {
                table_name: row.get("table_name"),
                column_name: row.get("column_name"),
                ordinal_position: row.get("ordinal_position"),
                is_nullable: is_nullable == "YES",
                data_type: row.get("data_type"),
                udt_name: row.get("udt_name"),
                column_default: row.get("column_default"),
                numeric_precision: row.get("numeric_precision"),
                numeric_scale: row.get("numeric_scale"),
                datetime_precision: row.get("datetime_precision"),
            };
            by_table
                .entry(info.table_name.clone())
                .or_default()
                .push(info);
        }

        Ok(by_table)
    }

    pub async fn fetch_bounds(&self, schema: &str, table: &TableConfig) -> Result<TableBounds> {
        validate_simple_identifier(schema)?;
        validate_simple_identifier(&table.name)?;

        match table.extract_strategy {
            ExtractStrategy::RawIdRange => {
                let key = table.extract_key.as_deref().ok_or_else(|| {
                    ExporterError::InvalidConfig(format!(
                        "{} uses raw_id_range but extract_key is missing",
                        table.name
                    ))
                })?;
                validate_simple_identifier(key)?;
                let sql = format!(
                    "SELECT min({key})::bigint AS min_key, max({key})::bigint AS max_key FROM {schema}.{table}",
                    key = quote_ident(key)?,
                    schema = quote_ident(schema)?,
                    table = quote_ident(&table.name)?,
                );
                let row = self.client.query_one(&sql, &[]).await?;
                Ok(TableBounds::RawId {
                    min: row.get("min_key"),
                    max: row.get("max_key"),
                })
            }
            ExtractStrategy::DateMonth => {
                let column = table.date_column.as_deref().ok_or_else(|| {
                    ExporterError::InvalidConfig(format!(
                        "{} uses date_month but date_column is missing",
                        table.name
                    ))
                })?;
                validate_simple_identifier(column)?;
                let sql = format!(
                    "SELECT min({column})::text AS min_date, max({column})::text AS max_date FROM {schema}.{table}",
                    column = quote_ident(column)?,
                    schema = quote_ident(schema)?,
                    table = quote_ident(&table.name)?,
                );
                let row = self.client.query_one(&sql, &[]).await?;
                Ok(TableBounds::Date {
                    min: row.get("min_date"),
                    max: row.get("max_date"),
                })
            }
            ExtractStrategy::FullTable => Ok(TableBounds::FullTable),
            ExtractStrategy::SnapshotItems => Ok(TableBounds::SnapshotItems),
            ExtractStrategy::EmptyTable => Ok(TableBounds::EmptyTable),
        }
    }
}

pub fn quote_ident(identifier: &str) -> Result<String> {
    validate_simple_identifier(identifier)?;
    Ok(format!("\"{}\"", identifier.replace('"', "\"\"")))
}

async fn connect_postgres(
    runtime: &RuntimeConfig,
) -> Result<(
    Client,
    tokio_postgres::Connection<tokio_postgres::Socket, tokio_postgres::tls::NoTlsStream>,
)> {
    let dsn_env = runtime.source.dsn_env.trim();
    if !dsn_env.is_empty() {
        if let Ok(dsn) = env::var(dsn_env) {
            if !dsn.trim().is_empty() {
                return tokio_postgres::connect(dsn.trim(), NoTls)
                    .await
                    .map_err(ExporterError::from);
            }
        }
    }

    let mut config = PgConfig::new();
    config.host(env::var("DB_HOST").unwrap_or_else(|_| "localhost".to_string()));
    let port = match env::var("DB_PORT") {
        Ok(value) if !value.trim().is_empty() => {
            value
                .parse::<u16>()
                .map_err(|source| ExporterError::InvalidInteger {
                    name: "DB_PORT",
                    source,
                })?
        }
        _ => 5432,
    };
    config.port(port);
    config.dbname(env::var("DB_NAME").unwrap_or_else(|_| "krx_data".to_string()));
    config.user(env::var("DB_USER").unwrap_or_else(|_| "krx_user".to_string()));
    if let Ok(password) = env::var("DB_PASSWORD") {
        if !password.is_empty() {
            config.password(password);
        }
    }

    config.connect(NoTls).await.map_err(ExporterError::from)
}

async fn apply_session_settings(client: &Client, read_only: bool) -> Result<()> {
    client
        .batch_execute(
            "
            SET statement_timeout = '6h';
            SET idle_in_transaction_session_timeout = '30min';
            SET DateStyle = ISO;
            SET IntervalStyle = iso_8601;
            ",
        )
        .await?;

    if read_only {
        client
            .batch_execute("SET default_transaction_read_only = on;")
            .await?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quotes_valid_identifier() {
        assert_eq!(
            quote_ident("dart_xbrl_fact_raw").unwrap(),
            "\"dart_xbrl_fact_raw\""
        );
    }

    #[test]
    fn rejects_identifier_expression() {
        assert!(quote_ident("year(trade_date)").is_err());
    }
}
