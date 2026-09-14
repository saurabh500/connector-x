//! Explicit bridge-backed SQL Server source. Ordinary routing still uses Tiberius.

mod connection;
mod conversion;
mod errors;
mod metadata;
mod writer;

pub use crate::sources::mssql::{FloatN, IntN, MsSQLTypeSystem};
use crate::{
    constants::DB_BUFFER_SIZE,
    data_order::DataOrder,
    errors::ConnectorXError,
    sources::{PartitionParser, Produce, Source, SourcePartition},
    sql::{count_query, get_partition_range_query, CXQuery},
};
use anyhow::anyhow;
use bb8::{Pool, PooledConnection};
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
pub use connection::{mssql_config, ConnectionManager};
use conversion::{FromValue, Value};
pub use errors::MsSQLBridgeSourceError;
use mssql_tiberius_bridge::{
    writer::{ColumnMetadata, RowWriter, TdsDataType},
    Client,
};
use rust_decimal::Decimal;
use sqlparser::dialect::MsSqlDialect;
use std::{convert::TryFrom, sync::Arc};
use tokio::runtime::{Handle, Runtime};
use url::Url;
use uuid_old::Uuid;
use writer::ValueWriter;

type Result<T> = std::result::Result<T, MsSQLBridgeSourceError>;

#[derive(Clone, Debug, PartialEq)]
struct ColumnSignature {
    name: String,
    ty: TdsDataType,
    nullable: bool,
    precision: Option<u8>,
    scale: Option<u8>,
}

impl From<&ColumnMetadata> for ColumnSignature {
    fn from(col: &ColumnMetadata) -> Self {
        Self {
            name: col.column_name.clone(),
            ty: col.data_type,
            nullable: col.is_nullable(),
            precision: col.get_precision(),
            scale: col.get_scale(),
        }
    }
}

fn check_schema(actual: &[ColumnSignature], expected: &[ColumnSignature]) -> Result<()> {
    if actual != expected {
        return Err(MsSQLBridgeSourceError::Conversion(
            "result-set schema changed during SQL Server read".into(),
        ));
    }
    Ok(())
}

pub struct MsSQLBridgeSource {
    rt: Arc<Runtime>,
    pool: Pool<ConnectionManager>,
    origin_query: Option<String>,
    queries: Vec<CXQuery<String>>,
    columns: Vec<ColumnSignature>,
    schema: Vec<MsSQLTypeSystem>,
}

impl MsSQLBridgeSource {
    pub fn new(rt: Arc<Runtime>, conn: &str, nconn: usize) -> Result<Self> {
        let size = u32::try_from(nconn)
            .ok()
            .filter(|n| *n > 0)
            .ok_or_else(|| anyhow!("SQL Server connection count must be between 1 and u32::MAX"))?;
        let manager = ConnectionManager::new(mssql_config(&Url::parse(conn)?)?);
        let pool = rt.block_on(Pool::builder().max_size(size).build(manager))?;
        Ok(Self {
            rt,
            pool,
            origin_query: None,
            queries: vec![],
            columns: vec![],
            schema: vec![],
        })
    }
}

async fn count_rows(conn: &mut Client, query: &str) -> Result<usize> {
    let row = conn
        .query_first(query, &[])
        .await?
        .ok_or(MsSQLBridgeSourceError::GetNRowsFailed)?;
    let n = row
        .try_get::<i32, _>(0usize)?
        .ok_or(MsSQLBridgeSourceError::GetNRowsFailed)?;
    usize::try_from(n).map_err(|_| MsSQLBridgeSourceError::GetNRowsFailed)
}

/// Explicit bridge range discovery; the existing public partition route is unchanged.
pub fn get_partition_range(conn: &Url, query: &str, col: &str) -> Result<(i64, i64)> {
    let rt = Runtime::new().map_err(anyhow::Error::from)?;
    let config = mssql_config(conn)?;
    let query =
        get_partition_range_query(query, col, &MsSqlDialect {}).map_err(ConnectorXError::from)?;
    rt.block_on(async {
        let mut client = Client::connect(&config).await?;
        let row = client
            .query_first(query.as_str(), &[])
            .await?
            .ok_or_else(|| anyhow!("SQL Server returned no partition range"))?;
        use mssql_tiberius_bridge::ColumnType;
        let ty = row
            .columns()
            .first()
            .ok_or_else(|| anyhow!("missing range column type"))?
            .column_type();
        if !matches!(
            ty,
            ColumnType::Int1
                | ColumnType::Int2
                | ColumnType::Int4
                | ColumnType::Int8
                | ColumnType::Float4
                | ColumnType::Float8
        ) {
            return Err(anyhow!("Partition can only be done on int or float columns").into());
        }
        let min = row
            .raw_value(0)
            .ok_or_else(|| anyhow!("missing minimum column"))?;
        let max = row
            .raw_value(1)
            .ok_or_else(|| anyhow!("missing maximum column"))?;
        Ok((conversion::range_value(min)?, conversion::range_value(max)?))
    })
}

impl Source for MsSQLBridgeSource {
    const DATA_ORDERS: &'static [DataOrder] = &[DataOrder::RowMajor];
    type Partition = MsSQLBridgeSourcePartition;
    type TypeSystem = MsSQLTypeSystem;
    type Error = MsSQLBridgeSourceError;

    fn set_data_order(&mut self, order: DataOrder) -> Result<()> {
        if !matches!(order, DataOrder::RowMajor) {
            return Err(ConnectorXError::UnsupportedDataOrder(order).into());
        }
        Ok(())
    }
    fn set_queries<Q: ToString>(&mut self, queries: &[CXQuery<Q>]) {
        self.queries = queries.iter().map(|q| q.map(Q::to_string)).collect();
    }
    fn set_origin_query(&mut self, query: Option<String>) {
        self.origin_query = query;
    }

    fn fetch_metadata(&mut self) -> Result<()> {
        let query = self
            .queries
            .first()
            .ok_or_else(|| anyhow!("SQL Server requires a query"))?;
        let mut conn = self.rt.block_on(self.pool.get())?;
        let (columns, schema) = self.rt.block_on(async {
            if !conn.start_query(query.as_str(), &[]).await? {
                return Err(anyhow!("SQL Server returned no columns").into());
            }
            let mapped = (|| {
                let metadata = conn.query_metadata()?;
                if metadata.is_empty() {
                    return Err(anyhow!("SQL Server returned no columns").into());
                }
                let columns = metadata.iter().map(ColumnSignature::from).collect();
                let schema = metadata
                    .iter()
                    .map(MsSQLTypeSystem::try_from)
                    .collect::<Result<Vec<_>>>()?;
                Ok::<_, MsSQLBridgeSourceError>((columns, schema))
            })();
            let closed = conn.close_query().await;
            let metadata = mapped?;
            closed?;
            Ok::<_, MsSQLBridgeSourceError>(metadata)
        })?;
        self.columns = columns;
        self.schema = schema;
        Ok(())
    }
    fn result_rows(&mut self) -> Result<Option<usize>> {
        match &self.origin_query {
            Some(q) => {
                let query = count_query(&CXQuery::Naked(q.clone()), &MsSqlDialect {})?;
                let mut conn = self.rt.block_on(self.pool.get())?;
                Ok(Some(
                    self.rt.block_on(count_rows(&mut conn, query.as_str()))?,
                ))
            }
            None => Ok(None),
        }
    }
    fn names(&self) -> Vec<String> {
        self.columns.iter().map(|c| c.name.clone()).collect()
    }
    fn schema(&self) -> Vec<MsSQLTypeSystem> {
        self.schema.clone()
    }
    fn partition(self) -> Result<Vec<Self::Partition>> {
        let Self {
            queries,
            pool,
            rt,
            columns,
            ..
        } = self;
        Ok(queries
            .into_iter()
            .map(|query| MsSQLBridgeSourcePartition {
                pool: pool.clone(),
                rt: rt.clone(),
                query,
                columns: columns.clone(),
                nrows: 0,
            })
            .collect())
    }
}

pub struct MsSQLBridgeSourcePartition {
    pool: Pool<ConnectionManager>,
    rt: Arc<Runtime>,
    query: CXQuery<String>,
    columns: Vec<ColumnSignature>,
    nrows: usize,
}

impl SourcePartition for MsSQLBridgeSourcePartition {
    type TypeSystem = MsSQLTypeSystem;
    type Parser<'a> = MsSQLBridgeSourceParser<'a>;
    type Error = MsSQLBridgeSourceError;

    fn result_rows(&mut self) -> Result<()> {
        let query = count_query(&self.query, &MsSqlDialect {})?;
        let mut conn = self.rt.block_on(self.pool.get())?;
        self.nrows = self.rt.block_on(count_rows(&mut conn, query.as_str()))?;
        Ok(())
    }
    fn parser(&mut self) -> Result<Self::Parser<'_>> {
        let mut conn = self.rt.block_on(self.pool.get())?;
        if !self
            .rt
            .block_on(conn.start_query(self.query.as_str(), &[]))?
        {
            return Err(anyhow!("SQL Server returned no columns").into());
        }
        check_schema(&conn.query_schema()?, &self.columns)?;
        Ok(MsSQLBridgeSourceParser {
            rt: self.rt.handle(),
            conn,
            columns: &self.columns,
            batch: Batch::default(),
            current_row: 0,
            current_col: 0,
        })
    }
    fn nrows(&self) -> usize {
        self.nrows
    }
    fn ncols(&self) -> usize {
        self.columns.len()
    }
}

// A narrow seam for testing refill boundaries without simulating SQL or the wire protocol.
trait QueryRows {
    async fn read<W: RowWriter + Send>(
        &mut self,
        writer: &mut W,
    ) -> mssql_tiberius_bridge::Result<bool>;
    async fn advance(&mut self) -> mssql_tiberius_bridge::Result<bool>;
    fn query_schema(&self) -> Result<Vec<ColumnSignature>>;
}

impl QueryRows for Client {
    async fn read<W: RowWriter + Send>(
        &mut self,
        writer: &mut W,
    ) -> mssql_tiberius_bridge::Result<bool> {
        self.next_row_into(writer).await
    }
    async fn advance(&mut self) -> mssql_tiberius_bridge::Result<bool> {
        self.next_result().await
    }
    fn query_schema(&self) -> Result<Vec<ColumnSignature>> {
        Ok(self
            .query_metadata()?
            .iter()
            .map(ColumnSignature::from)
            .collect())
    }
}

#[derive(Default)]
struct Batch {
    rows: Vec<Vec<Value>>,
    len: usize,
    finished: bool,
    failure: Option<String>,
}

impl Batch {
    async fn refill<R: QueryRows>(
        &mut self,
        rows: &mut R,
        schema: &[ColumnSignature],
    ) -> Result<()> {
        if let Some(error) = &self.failure {
            return Err(MsSQLBridgeSourceError::Conversion(format!(
                "SQL Server read already failed: {error}"
            )));
        }
        self.len = 0;
        if self.finished {
            return Ok(());
        }
        let fetched = self.read_batch(rows, schema).await;
        if let Err(error) = &fetched {
            self.rows.clear();
            self.len = 0;
            self.failure = Some(error.to_string());
        }
        fetched
    }

    async fn read_batch<R: QueryRows>(
        &mut self,
        rows: &mut R,
        schema: &[ColumnSignature],
    ) -> Result<()> {
        for index in 0..DB_BUFFER_SIZE {
            if self.rows.len() == index {
                self.rows.push(Vec::with_capacity(schema.len()));
            }
            loop {
                let mut writer = ValueWriter::new(&mut self.rows[index], schema.len());
                let result = rows.read(&mut writer).await;
                if writer.finish(result)? {
                    break;
                }
                if !rows.advance().await? {
                    self.finished = true;
                    return Ok(());
                }
                check_schema(&rows.query_schema()?, schema)?;
            }
            self.len += 1;
        }
        Ok(())
    }
}

pub struct MsSQLBridgeSourceParser<'a> {
    rt: &'a Handle,
    conn: PooledConnection<'a, ConnectionManager>,
    columns: &'a [ColumnSignature],
    batch: Batch,
    current_col: usize,
    current_row: usize,
}

impl MsSQLBridgeSourceParser<'_> {
    fn next_loc(&mut self) -> Result<(usize, usize)> {
        if self.columns.is_empty() || self.current_row >= self.batch.len {
            return Err(anyhow!("SQL Server parser has no buffered value").into());
        }
        let ret = (self.current_row, self.current_col);
        self.current_row += (self.current_col + 1) / self.columns.len();
        self.current_col = (self.current_col + 1) % self.columns.len();
        Ok(ret)
    }
}

impl<'a> PartitionParser<'a> for MsSQLBridgeSourceParser<'a> {
    type TypeSystem = MsSQLTypeSystem;
    type Error = MsSQLBridgeSourceError;

    fn fetch_next(&mut self) -> Result<(usize, bool)> {
        if self.current_col != 0 {
            return Err(anyhow!("SQL Server refill requires a fully consumed row").into());
        }
        let remaining = self.batch.len - self.current_row;
        if remaining > 0 {
            return Ok((remaining, self.batch.finished));
        }
        let result = self
            .rt
            .block_on(self.batch.refill(&mut *self.conn, self.columns));
        self.current_row = 0;
        result?;
        Ok((self.batch.len, self.batch.finished))
    }
}

macro_rules! impl_produce {
    ($($t:ty,)+) => {$(
        impl<'r, 'a> Produce<'r, $t> for MsSQLBridgeSourceParser<'a> {
            type Error = MsSQLBridgeSourceError;
            fn produce(&'r mut self) -> Result<$t> {
                let (row, col) = self.next_loc()?;
                <$t>::from_value(&self.batch.rows[row][col])?
                    .ok_or_else(|| anyhow!("SQL Server NULL at non-nullable position ({row}, {col})").into())
            }
        }
        impl<'r, 'a> Produce<'r, Option<$t>> for MsSQLBridgeSourceParser<'a> {
            type Error = MsSQLBridgeSourceError;
            fn produce(&'r mut self) -> Result<Option<$t>> {
                let (row, col) = self.next_loc()?;
                <$t>::from_value(&self.batch.rows[row][col])
            }
        }
    )+};
}

impl_produce!(
    u8,
    i16,
    i32,
    i64,
    IntN,
    f32,
    f64,
    FloatN,
    bool,
    &'r str,
    &'r [u8],
    Uuid,
    Decimal,
    NaiveDateTime,
    NaiveDate,
    NaiveTime,
    DateTime<Utc>,
);

#[cfg(test)]
mod tests;
