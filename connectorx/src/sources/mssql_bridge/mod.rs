//! SQL Server compatibility source using ordinary bridge rows and metadata events.

mod connection;
mod conversion;
mod errors;
mod metadata;

pub use crate::sources::mssql::{FloatN, IntN, MsSQLTypeSystem};
use crate::{
    constants::DB_BUFFER_SIZE,
    data_order::DataOrder,
    errors::ConnectorXError,
    sources::{PartitionParser, Produce, Source, SourcePartition},
    sql::{count_query, get_partition_range_query, CXQuery},
    utils::DummyBox,
};
use anyhow::anyhow;
use bb8::{Pool, PooledConnection};
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
pub use connection::{mssql_config, ConnectionManager};
use conversion::FromRow;
pub use errors::MsSQLBridgeSourceError;
use futures::StreamExt;
use mssql_tiberius_bridge::{Client, Column, ColumnType, QueryItem, QueryStream, Row};
use owning_ref::OwningHandle;
use rust_decimal::Decimal;
use sqlparser::dialect::MsSqlDialect;
use std::{convert::TryFrom, sync::Arc};
use tokio::runtime::{Handle, Runtime};
use url::Url;
use uuid_old::Uuid;

type Result<T> = std::result::Result<T, MsSQLBridgeSourceError>;
type Conn<'a> = PooledConnection<'a, ConnectionManager>;

fn check_schema(actual: &[Column], expected: &[Column]) -> Result<()> {
    let same = actual.len() == expected.len()
        && actual.iter().zip(expected).all(|(a, b)| {
            a.name() == b.name()
                && a.column_type() == b.column_type()
                && a.nullable() == b.nullable()
                && a.byte_length() == b.byte_length()
                && a.precision() == b.precision()
                && a.scale() == b.scale()
        });
    if !same {
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
    columns: Vec<Column>,
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
        .query(query, &[])
        .await?
        .into_row()
        .await?
        .ok_or(MsSQLBridgeSourceError::GetNRowsFailed)?;
    let n = i32::from_row(&row, 0)?.ok_or(MsSQLBridgeSourceError::GetNRowsFailed)?;
    usize::try_from(n).map_err(|_| MsSQLBridgeSourceError::GetNRowsFailed)
}

pub fn get_partition_range(conn: &Url, query: &str, col: &str) -> Result<(i64, i64)> {
    let rt = Runtime::new().map_err(anyhow::Error::from)?;
    let config = mssql_config(conn)?;
    let query = get_partition_range_query(query, col, &MsSqlDialect {})?;
    rt.block_on(async {
        let mut client = Client::connect(&config).await?;
        let row = client
            .query(query.as_str(), &[])
            .await?
            .into_row()
            .await?
            .ok_or_else(|| anyhow!("SQL Server returned no partition range"))?;
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
        log::debug!(target: "connectorx::mssql_backend", "MSSQL metadata backend: mssql-tds");
        let query = self
            .queries
            .first()
            .ok_or_else(|| anyhow!("SQL Server requires a query"))?;
        let mut conn = self.rt.block_on(self.pool.get())?;
        let columns = self.rt.block_on(async {
            let mut stream = conn.query(query.as_str(), &[]).await?;
            let columns = stream
                .columns()
                .await?
                .filter(|columns| !columns.is_empty())
                .ok_or_else(|| anyhow!("SQL Server returned no columns"))?
                .to_vec();
            while let Some(item) = stream.next().await {
                if let QueryItem::Metadata(metadata) = item? {
                    check_schema(metadata.columns(), &columns)?;
                }
            }
            Ok::<_, MsSQLBridgeSourceError>(columns)
        })?;
        self.schema = columns
            .iter()
            .map(MsSQLTypeSystem::try_from)
            .collect::<Result<_>>()?;
        self.columns = columns;
        Ok(())
    }
    fn result_rows(&mut self) -> Result<Option<usize>> {
        match &self.origin_query {
            Some(q) => {
                log::debug!(target: "connectorx::mssql_backend", "MSSQL count backend: mssql-tds");
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
        self.columns.iter().map(|c| c.name().to_owned()).collect()
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
    columns: Vec<Column>,
    nrows: usize,
}

impl SourcePartition for MsSQLBridgeSourcePartition {
    type TypeSystem = MsSQLTypeSystem;
    type Parser<'a> = MsSQLBridgeSourceParser<'a>;
    type Error = MsSQLBridgeSourceError;
    fn result_rows(&mut self) -> Result<()> {
        log::debug!(target: "connectorx::mssql_backend", "MSSQL partition count backend: mssql-tds");
        let query = count_query(&self.query, &MsSqlDialect {})?;
        let mut conn = self.rt.block_on(self.pool.get())?;
        self.nrows = self.rt.block_on(count_rows(&mut conn, query.as_str()))?;
        Ok(())
    }
    fn parser(&mut self) -> Result<Self::Parser<'_>> {
        log::debug!(target: "connectorx::mssql_backend", "MSSQL partition backend: mssql-tds");
        let conn = self.rt.block_on(self.pool.get())?;
        // As in the legacy source, the stable boxed owner outlives its borrowing stream.
        let items = OwningHandle::try_new(Box::new(conn), |conn: *const Conn<'_>| unsafe {
            let conn = &mut *(conn as *mut Conn<'_>);
            self.rt
                .block_on(conn.query(self.query.as_str(), &[]))
                .map(DummyBox)
        })?;
        Ok(MsSQLBridgeSourceParser {
            rt: self.rt.handle(),
            items,
            columns: &self.columns,
            rowbuf: Vec::with_capacity(DB_BUFFER_SIZE),
            current_row: 0,
            current_col: 0,
            finished: false,
            failure: None,
        })
    }
    fn nrows(&self) -> usize {
        self.nrows
    }
    fn ncols(&self) -> usize {
        self.columns.len()
    }
}

pub struct MsSQLBridgeSourceParser<'a> {
    rt: &'a Handle,
    items: OwningHandle<Box<Conn<'a>>, DummyBox<QueryStream<'a>>>,
    columns: &'a [Column],
    rowbuf: Vec<Row>,
    current_row: usize,
    current_col: usize,
    finished: bool,
    failure: Option<String>,
}

impl MsSQLBridgeSourceParser<'_> {
    fn next_loc(&mut self) -> Result<(usize, usize)> {
        if self.columns.is_empty() || self.current_row >= self.rowbuf.len() {
            return Err(anyhow!("SQL Server parser has no buffered value").into());
        }
        let location = (self.current_row, self.current_col);
        self.current_row += (self.current_col + 1) / self.columns.len();
        self.current_col = (self.current_col + 1) % self.columns.len();
        Ok(location)
    }
    fn refill(&mut self) -> Result<()> {
        self.rowbuf.clear();
        self.current_row = 0;
        // Preserve the legacy source's bounded item loop and per-item runtime entry.
        for _ in 0..DB_BUFFER_SIZE {
            match self.rt.block_on(self.items.next()).transpose()? {
                Some(QueryItem::Metadata(metadata)) => {
                    check_schema(metadata.columns(), self.columns)?
                }
                Some(QueryItem::Row(row)) => self.rowbuf.push(row),
                None => {
                    self.finished = true;
                    break;
                }
            }
        }
        Ok(())
    }
}

impl<'a> PartitionParser<'a> for MsSQLBridgeSourceParser<'a> {
    type TypeSystem = MsSQLTypeSystem;
    type Error = MsSQLBridgeSourceError;
    fn fetch_next(&mut self) -> Result<(usize, bool)> {
        if let Some(error) = &self.failure {
            return Err(anyhow!("SQL Server read already failed: {error}").into());
        }
        if self.current_col != 0 {
            return Err(anyhow!("SQL Server refill requires a fully consumed row").into());
        }
        let remaining = self.rowbuf.len() - self.current_row;
        if remaining > 0 {
            return Ok((remaining, self.finished));
        }
        if self.finished {
            return Ok((0, true));
        }
        if let Err(error) = self.refill() {
            self.rowbuf.clear();
            self.failure = Some(error.to_string());
            return Err(error);
        }
        Ok((self.rowbuf.len(), self.finished))
    }
}

macro_rules! impl_produce {
    ($($t:ty,)+) => {$(
        impl<'r, 'a> Produce<'r, $t> for MsSQLBridgeSourceParser<'a> {
            type Error = MsSQLBridgeSourceError;
            fn produce(&'r mut self) -> Result<$t> {
                let (row, col) = self.next_loc()?;
                <$t>::from_row(&self.rowbuf[row], col)?.ok_or_else(|| anyhow!("SQL Server NULL at non-nullable position ({row}, {col})").into())
            }
        }
        impl<'r, 'a> Produce<'r, Option<$t>> for MsSQLBridgeSourceParser<'a> {
            type Error = MsSQLBridgeSourceError;
            fn produce(&'r mut self) -> Result<Option<$t>> {
                let (row, col) = self.next_loc()?;
                <$t>::from_row(&self.rowbuf[row], col)
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
