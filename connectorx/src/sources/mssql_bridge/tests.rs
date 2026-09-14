use super::*;
use std::collections::VecDeque;

fn schema() -> Vec<ColumnSignature> {
    vec![ColumnSignature {
        name: "n".into(),
        ty: TdsDataType::Int4,
        nullable: false,
        precision: None,
        scale: None,
    }]
}

enum Event {
    Row(i32),
    Boundary(Vec<ColumnSignature>),
    Error,
    Incomplete,
    Eof,
}

struct Rows {
    events: VecDeque<Event>,
    schema: Vec<ColumnSignature>,
    reads: usize,
    advances: usize,
}

impl Rows {
    fn new(events: impl IntoIterator<Item = Event>) -> Self {
        Self {
            events: events.into_iter().collect(),
            schema: schema(),
            reads: 0,
            advances: 0,
        }
    }
}

impl QueryRows for Rows {
    async fn read<W: RowWriter + Send>(
        &mut self,
        writer: &mut W,
    ) -> mssql_tiberius_bridge::Result<bool> {
        self.reads += 1;
        match self.events.front() {
            Some(Event::Boundary(_)) | Some(Event::Eof) => Ok(false),
            _ => match self.events.pop_front().expect("unexpected read beyond EOF") {
                Event::Row(n) => {
                    writer.write_i32(0, n);
                    writer.end_row();
                    Ok(true)
                }
                Event::Incomplete => {
                    writer.write_i32(0, 1);
                    Ok(true)
                }
                Event::Error => Err(mssql_tiberius_bridge::Error::Tds(
                    mssql_tiberius_bridge::writer::TdsError::UsageError(
                        "injected read error".into(),
                    ),
                )),
                _ => unreachable!(),
            },
        }
    }
    async fn advance(&mut self) -> mssql_tiberius_bridge::Result<bool> {
        self.advances += 1;
        match self.events.pop_front().expect("advance past EOF") {
            Event::Boundary(schema) => {
                self.schema = schema;
                Ok(true)
            }
            Event::Eof => Ok(false),
            _ => panic!("advance called before boundary"),
        }
    }
    fn query_schema(&self) -> Result<Vec<ColumnSignature>> {
        Ok(self.schema.clone())
    }
}

#[test]
fn refill_is_32_rows_on_each_call_reuses_storage_and_stops_at_eof() {
    assert_eq!(DB_BUFFER_SIZE, 32, "this test pins the actual refill size");
    let rt = Runtime::new().unwrap();
    let mut rows = Rows::new((0..65).map(Event::Row).chain([Event::Eof]));
    let mut batch = Batch::default();
    rt.block_on(batch.refill(&mut rows, &schema())).unwrap();
    assert_eq!((batch.len, rows.reads, batch.finished), (32, 32, false));
    let storage: Vec<_> = batch.rows.iter().map(Vec::as_ptr).collect();
    rt.block_on(batch.refill(&mut rows, &schema())).unwrap();
    assert_eq!((batch.len, rows.reads, batch.finished), (32, 64, false));
    assert_eq!(
        storage,
        batch.rows.iter().map(Vec::as_ptr).collect::<Vec<_>>()
    );
    assert_eq!(i32::from_value(&batch.rows[0][0]).unwrap(), Some(32));
    rt.block_on(batch.refill(&mut rows, &schema())).unwrap();
    assert_eq!((batch.len, rows.reads, batch.finished), (1, 66, true));
    assert_eq!(i32::from_value(&batch.rows[0][0]).unwrap(), Some(64));
    for _ in 0..2 {
        rt.block_on(batch.refill(&mut rows, &schema())).unwrap();
        assert_eq!((batch.len, rows.reads, rows.advances), (0, 66, 1));
    }
}

#[test]
fn empty_results_and_matching_result_sets_are_explicitly_advanced() {
    let rt = Runtime::new().unwrap();
    let mut rows = Rows::new([
        Event::Boundary(schema()),
        Event::Row(1),
        Event::Boundary(schema()),
        Event::Boundary(schema()),
        Event::Row(2),
        Event::Eof,
    ]);
    let mut batch = Batch::default();
    rt.block_on(batch.refill(&mut rows, &schema())).unwrap();
    assert_eq!((batch.len, batch.finished, rows.advances), (2, true, 4));
    assert_eq!(i32::from_value(&batch.rows[1][0]).unwrap(), Some(2));
}

#[test]
fn changed_schema_discards_partial_batch_and_failure_stays_latched() {
    let rt = Runtime::new().unwrap();
    let mut changed = schema();
    changed[0].ty = TdsDataType::Flt8;
    let mut rows = Rows::new([
        Event::Row(1),
        Event::Boundary(changed),
        Event::Row(2),
        Event::Eof,
    ]);
    let mut batch = Batch::default();
    assert!(rt.block_on(batch.refill(&mut rows, &schema())).is_err());
    assert!(batch.rows.is_empty());
    assert_eq!(batch.len, 0);
    let reads = rows.reads;
    for _ in 0..2 {
        assert!(rt.block_on(batch.refill(&mut rows, &schema())).is_err());
        assert_eq!(rows.reads, reads);
    }
}

#[test]
fn trailing_read_error_and_incomplete_row_never_become_eof() {
    let rt = Runtime::new().unwrap();
    for event in [Event::Error, Event::Incomplete] {
        let mut rows = Rows::new([Event::Row(1), event, Event::Eof]);
        let mut batch = Batch::default();
        assert!(rt.block_on(batch.refill(&mut rows, &schema())).is_err());
        assert_eq!(batch.len, 0);
        assert!(batch.rows.is_empty());
        assert!(!batch.finished);
        let reads = rows.reads;
        assert!(rt.block_on(batch.refill(&mut rows, &schema())).is_err());
        assert_eq!(rows.reads, reads);
    }
}

#[test]
fn schema_check_includes_names_nullability_precision_and_scale() {
    let expected = schema();
    check_schema(&expected, &expected).unwrap();
    for index in 0..4 {
        let mut changed = expected.clone();
        match index {
            0 => changed[0].name = "different".into(),
            1 => changed[0].nullable = true,
            2 => changed[0].precision = Some(10),
            _ => changed[0].scale = Some(2),
        }
        assert!(check_schema(&changed, &expected).is_err());
    }
    assert!(check_schema(&[], &expected).is_err());
}

#[test]
fn invalid_pool_sizes_fail_before_connecting() {
    let rt = Arc::new(Runtime::new().unwrap());
    assert!(MsSQLBridgeSource::new(rt.clone(), "mssql://localhost/db?encrypt=false", 0).is_err());
    if usize::BITS > 32 {
        assert!(
            MsSQLBridgeSource::new(rt, "mssql://localhost/db?encrypt=false", usize::MAX).is_err()
        );
    }
}

#[test]
#[ignore = "requires explicitly supplied MSSQL_URL; read-only queries, no fixture creation"]
fn live_source_counts_ranges_metadata_and_parser() {
    let uri = std::env::var("MSSQL_URL").expect("supply MSSQL_URL explicitly");
    let rt = Arc::new(Runtime::new().unwrap());
    let query = "SELECT n FROM (VALUES (1), (2)) AS t(n)";
    let mut source = MsSQLBridgeSource::new(rt, &uri, 1).unwrap();
    source.set_queries(&[CXQuery::naked(query)]);
    source.set_origin_query(Some(query.into()));
    source.fetch_metadata().unwrap();
    assert_eq!(source.names(), ["n"]);
    assert_eq!(source.result_rows().unwrap(), Some(2));
    assert_eq!(
        get_partition_range(&Url::parse(&uri).unwrap(), query, "n").unwrap(),
        (1, 2)
    );
    let mut partitions = source.partition().unwrap();
    partitions[0].result_rows().unwrap();
    assert_eq!(partitions[0].nrows(), 2);
    let mut parser = partitions[0].parser().unwrap();
    assert_eq!(parser.fetch_next().unwrap(), (2, true));
    assert_eq!(parser.parse::<i32>().unwrap(), 1);
    assert_eq!(parser.parse::<i32>().unwrap(), 2);
    for _ in 0..2 {
        assert_eq!(parser.fetch_next().unwrap(), (0, true));
    }
}

#[test]
#[ignore = "requires explicitly supplied MSSQL_URL; read-only pool lifecycle queries"]
fn live_pool_reuses_clean_connections_and_discards_pending_queries() {
    use bb8::ManageConnection;
    let uri = std::env::var("MSSQL_URL").expect("supply MSSQL_URL explicitly");
    let manager = ConnectionManager::new(mssql_config(&Url::parse(&uri).unwrap()).unwrap());
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let pool = Pool::builder()
            .max_size(1)
            .build(manager.clone())
            .await
            .unwrap();
        {
            let mut conn = pool.get().await.unwrap();
            for _ in 0..2 {
                assert!(!manager.has_broken(&mut conn));
                assert!(conn
                    .start_query("SELECT 1 UNION ALL SELECT 2", &[])
                    .await
                    .unwrap());
                assert!(manager.has_broken(&mut conn));
                conn.close_query().await.unwrap();
                assert!(!manager.has_broken(&mut conn));
                assert_eq!(
                    conn.query_first("SELECT 42", &[])
                        .await
                        .unwrap()
                        .unwrap()
                        .try_get::<i32, _>(0usize)
                        .unwrap(),
                    Some(42)
                );
            }
            conn.start_query("SELECT 1 UNION ALL SELECT 2", &[])
                .await
                .unwrap();
            assert!(manager.has_broken(&mut conn));
        }
        let mut conn = pool.get().await.unwrap();
        manager.is_valid(&mut conn).await.unwrap();
        assert!(!manager.has_broken(&mut conn));
    });
}

#[cfg(feature = "dst_arrow")]
#[test]
fn legacy_and_bridge_transport_types_and_conversions_remain_available() {
    use crate::transports::{
        MsSQLArrowStreamTransport, MsSQLArrowTransport, MsSQLBridgeArrowStreamTransport,
        MsSQLBridgeArrowTransport,
    };
    use crate::typesystem::{Transport, TypeConversion};
    fn arrow<T: Transport<S = crate::sources::mssql::MsSQLSource>>() {}
    fn bridge<T: Transport<S = MsSQLBridgeSource>>() {}
    arrow::<MsSQLArrowTransport>();
    arrow::<MsSQLArrowStreamTransport>();
    bridge::<MsSQLBridgeArrowTransport>();
    bridge::<MsSQLBridgeArrowStreamTransport>();
    let id = Uuid::from_bytes([0xab; 16]);
    assert_eq!(
        <MsSQLArrowTransport as TypeConversion<Uuid, String>>::convert(id),
        <MsSQLBridgeArrowTransport as TypeConversion<Uuid, String>>::convert(id),
    );
    let decimal = Decimal::new(123456, 3);
    assert_eq!(
        <MsSQLArrowTransport as TypeConversion<Decimal, f64>>::convert(decimal),
        <MsSQLBridgeArrowTransport as TypeConversion<Decimal, f64>>::convert(decimal),
    );
}

#[cfg(feature = "dst_arrow")]
#[test]
#[ignore = "requires explicitly supplied MSSQL_URL; read-only Arrow and streaming query"]
fn live_both_arrow_bindings_preserve_decimal_output() {
    use crate::arrow_batch_iter::{ArrowBatchIter, RecordBatchIterator};
    use crate::destinations::{
        arrow::ArrowDestination, arrowstream::ArrowDestination as StreamDestination,
    };
    use crate::prelude::Dispatcher;
    use crate::transports::{MsSQLBridgeArrowStreamTransport, MsSQLBridgeArrowTransport};
    use arrow::{array::Decimal128Array, datatypes::DataType};
    let uri = std::env::var("MSSQL_URL").expect("supply MSSQL_URL explicitly");
    let query = [CXQuery::naked(
        "SELECT CAST(1.25 AS decimal(10,2)) AS amount",
    )];
    let source = MsSQLBridgeSource::new(Arc::new(Runtime::new().unwrap()), &uri, 1).unwrap();
    let mut destination = ArrowDestination::new();
    Dispatcher::<_, _, MsSQLBridgeArrowTransport>::new(source, &mut destination, &query, None)
        .run()
        .unwrap();
    let materialized = destination.arrow().unwrap();
    let source = MsSQLBridgeSource::new(Arc::new(Runtime::new().unwrap()), &uri, 1).unwrap();
    let mut stream = ArrowBatchIter::<_, MsSQLBridgeArrowStreamTransport>::new(
        source,
        StreamDestination::new_with_batch_size(1),
        None,
        &query,
    )
    .unwrap();
    stream.prepare();
    let streamed: Vec<_> = std::iter::from_fn(|| stream.next_batch()).collect();
    let schema = materialized[0].schema();
    assert_eq!(schema.fields().len(), 1);
    assert_eq!(schema.field(0).name(), "amount");
    assert_eq!(schema.field(0).data_type(), &DataType::Decimal128(38, 10));
    // Finalizing an exact-full stream batch can emit a valid empty batch.
    for batches in [&materialized, &streamed] {
        let mut values = vec![];
        let mut total_rows = 0;
        for batch in batches {
            assert_eq!(batch.schema(), schema);
            total_rows += batch.num_rows();
            let decimals = batch
                .column(0)
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .unwrap();
            values.extend(decimals.iter());
        }
        assert_eq!(total_rows, 1);
        assert_eq!(values, [Some(12_500_000_000)]);
    }
    assert!(stream.next_batch().is_none());
    assert!(stream.next_batch().is_none());
}
