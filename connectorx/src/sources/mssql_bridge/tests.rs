use super::*;

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
fn live_pool_validates_and_reuses_connections_after_partial_reads() {
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
                {
                    let mut items = conn.query_compat("SELECT 1 UNION ALL SELECT 2", &[]);
                    assert!(matches!(
                        items.next().await.unwrap().unwrap(),
                        QueryItem::Metadata(_)
                    ));
                    assert!(matches!(
                        items.next().await.unwrap().unwrap(),
                        QueryItem::Row(_)
                    ));
                }
                assert!(!manager.has_broken(&mut conn));
                manager.is_valid(&mut conn).await.unwrap();
                assert_eq!(
                    conn.query_compat("SELECT 42", &[])
                        .into_row()
                        .await
                        .unwrap()
                        .unwrap()
                        .try_get::<i32, _>(0usize)
                        .unwrap(),
                    Some(42)
                );
            }
            let mut items = conn.query_compat("SELECT 1 UNION ALL SELECT 2", &[]);
            assert!(matches!(
                items.next().await.unwrap().unwrap(),
                QueryItem::Metadata(_)
            ));
        }
        let mut conn = pool.get().await.unwrap();
        manager.is_valid(&mut conn).await.unwrap();
        assert!(!manager.has_broken(&mut conn));
    });
}

#[test]
#[ignore = "requires explicitly supplied MSSQL_URL; read-only streaming and error cases"]
fn live_row_stream_refills_boundaries_and_trailing_errors() {
    assert_eq!(DB_BUFFER_SIZE, 32);
    let uri = std::env::var("MSSQL_URL").expect("supply MSSQL_URL explicitly");
    let rt = Arc::new(Runtime::new().unwrap());
    let query = "SELECT CAST(value AS int) AS n FROM GENERATE_SERIES(0,64)";
    let mut source = MsSQLBridgeSource::new(rt.clone(), &uri, 1).unwrap();
    source.set_queries(&[CXQuery::naked(query)]);
    source.fetch_metadata().unwrap();
    let mut partitions = source.partition().unwrap();
    {
        let mut parser = partitions[0].parser().unwrap();
        let mut expected = 0;
        // Metadata occupies the first item in the unchanged legacy 32-item loop.
        for (rows, finished) in [(31, false), (32, false), (2, true)] {
            assert_eq!(parser.fetch_next().unwrap(), (rows, finished));
            assert!(parser.rowbuf.len() <= 32);
            for _ in 0..rows {
                assert_eq!(parser.parse::<i32>().unwrap(), expected);
                expected += 1;
            }
        }
        assert_eq!(expected, 65);
        for _ in 0..2 {
            assert_eq!(parser.fetch_next().unwrap(), (0, true));
        }
    }
    partitions[0].query = CXQuery::naked(
        "SELECT CAST(value AS int) AS n FROM GENERATE_SERIES(0,0) WHERE 1=0; \
                 SELECT CAST(value AS int) AS n FROM GENERATE_SERIES(1,2); \
                 SELECT CAST(value AS int) AS n FROM GENERATE_SERIES(0,0) WHERE 1=0",
    );
    {
        let mut parser = partitions[0].parser().unwrap();
        assert_eq!(parser.fetch_next().unwrap(), (2, true));
        assert_eq!(parser.parse::<i32>().unwrap(), 1);
        assert_eq!(parser.parse::<i32>().unwrap(), 2);
    }
    partitions[0].query = CXQuery::naked(
        "SELECT CAST(value AS int) AS n FROM GENERATE_SERIES(0,64); \
                 RAISERROR('cx trailing error',16,1)",
    );
    {
        let mut parser = partitions[0].parser().unwrap();
        let mut expected = 0;
        // A fully buffered query would report the trailing error before either successful refill.
        for rows in [31, 32] {
            assert_eq!(parser.fetch_next().unwrap(), (rows, false));
            for _ in 0..rows {
                assert_eq!(parser.parse::<i32>().unwrap(), expected);
                expected += 1;
            }
        }
        assert_eq!(expected, 63);
        assert!(parser.fetch_next().is_err());
        assert!(parser.rowbuf.is_empty());
        assert!(parser
            .fetch_next()
            .unwrap_err()
            .to_string()
            .contains("already failed"));
    }
    for query in [
                "SELECT CAST(value AS int) AS n FROM GENERATE_SERIES(0,0); SELECT CAST(2 AS bigint) AS n",
                "SELECT CAST(value AS int) AS n FROM GENERATE_SERIES(0,0); RAISERROR('cx trailing error',16,1)",
            ] {
                partitions[0].query = CXQuery::naked(query);
                let mut parser = partitions[0].parser().unwrap();
                assert!(parser.fetch_next().is_err());
                assert!(parser.rowbuf.is_empty());
                for _ in 0..2 {
                    assert!(parser.fetch_next().unwrap_err().to_string().contains("already failed"));
                }
            }
    let config = mssql_config(&Url::parse(&uri).unwrap()).unwrap();
    rt.block_on(async {
        let mut conn = Client::connect(&config).await.unwrap();
        assert!(conn
            .query_compat("SELECT 1 AS n WHERE 1=0; SELECT 2 AS n", &[])
            .into_row()
            .await
            .unwrap()
            .is_none());
        assert!(conn
            .query_compat("SELECT 1 AS n; RAISERROR('cx trailing error',16,1)", &[])
            .into_row()
            .await
            .is_err());
    });
    for query in [
        "SELECT 1 AS n; SELECT CAST(2 AS bigint) AS n",
        "SELECT 1 AS n WHERE 1=0; SELECT CAST(2 AS bigint) AS n WHERE 1=0",
        "SELECT 1 AS n; RAISERROR('cx trailing error',16,1)",
    ] {
        let mut source = MsSQLBridgeSource::new(rt.clone(), &uri, 1).unwrap();
        source.set_queries(&[CXQuery::naked(query)]);
        assert!(source.fetch_metadata().is_err());
    }
    partitions[0].query = CXQuery::naked("SELECT missing_column FROM (VALUES (1)) AS t(n)");
    assert!(partitions[0].parser().is_err());
    partitions[0].query = CXQuery::naked("RAISERROR('cx initial error',16,1)");
    assert!(partitions[0].parser().is_err());
    let mut source = MsSQLBridgeSource::new(rt, &uri, 1).unwrap();
    source.set_queries(&[CXQuery::naked("SELECT CAST(1 AS int) AS n WHERE 1=0")]);
    source.fetch_metadata().unwrap();
    assert_eq!(source.names(), ["n"]);
    assert_eq!(source.schema().len(), 1);
    let mut partitions = source.partition().unwrap();
    let mut parser = partitions[0].parser().unwrap();
    for _ in 0..2 {
        assert_eq!(parser.fetch_next().unwrap(), (0, true));
    }
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

#[test]
#[ignore = "requires explicitly supplied MSSQL_URL; read-only supported-type conversions"]
fn live_ordinary_row_supported_types_and_nulls() {
    let uri = std::env::var("MSSQL_URL").expect("supply MSSQL_URL explicitly");
    let query = "SELECT \
        CAST(255 AS tinyint) AS a, CAST(-12 AS smallint) AS b, CAST(-1234 AS int) AS c, \
        CAST(9000000000 AS bigint) AS d, CAST(1.25 AS real) AS e, CAST(-4.5 AS float) AS f, \
        CAST(1 AS bit) AS g, CAST(NCHAR(937) AS nvarchar(10)) AS h, CAST('text' AS varchar(10)) AS i, \
        CAST(0x010002 AS varbinary(10)) AS j, CAST('00112233-4455-6677-8899-aabbccddeeff' AS uniqueidentifier) AS k, \
        CAST(-1.25 AS decimal(10,2)) AS l, CAST(-1.25 AS money) AS m, CAST(-2.5 AS smallmoney) AS n, \
        CAST('2024-02-29' AS date) AS o, CAST('12:34:56.1234567' AS time(7)) AS p, \
        CAST('2024-02-29T12:34:56.1234567' AS datetime2(7)) AS q, \
        CAST('2024-02-29T12:34:56.003' AS datetime) AS r, \
        CAST('2024-02-29T12:34:00' AS smalldatetime) AS s, \
        CAST('2024-02-29T12:34:56.1234567+05:30' AS datetimeoffset(7)) AS t, \
        CAST(NULL AS int) AS u, CAST(NULL AS nvarchar(10)) AS v, CAST(NULL AS decimal(10,2)) AS w";
    let mut source = MsSQLBridgeSource::new(Arc::new(Runtime::new().unwrap()), &uri, 1).unwrap();
    source.set_queries(&[CXQuery::naked(query)]);
    source.fetch_metadata().unwrap();
    assert_eq!(source.schema().len(), 23);
    let mut partitions = source.partition().unwrap();
    {
        let mut parser = partitions[0].parser().unwrap();
        assert_eq!(parser.fetch_next().unwrap(), (1, true));
        assert_eq!(parser.parse::<u8>().unwrap(), 255);
        assert!(
            parser.fetch_next().is_err(),
            "partial rows cannot be refilled"
        );
        assert_eq!(parser.parse::<i16>().unwrap(), -12);
        assert_eq!(parser.parse::<i32>().unwrap(), -1234);
        assert_eq!(parser.parse::<i64>().unwrap(), 9_000_000_000);
        assert_eq!(parser.parse::<f32>().unwrap(), 1.25);
        assert_eq!(parser.parse::<f64>().unwrap(), -4.5);
        assert!(parser.parse::<bool>().unwrap());
        assert_eq!(parser.parse::<&str>().unwrap(), "\u{03a9}");
        assert_eq!(parser.parse::<&str>().unwrap(), "text");
        assert_eq!(parser.parse::<&[u8]>().unwrap(), &[1, 0, 2]);
        assert_eq!(
            parser.parse::<Uuid>().unwrap().to_string(),
            "00112233-4455-6677-8899-aabbccddeeff"
        );
        assert_eq!(parser.parse::<Decimal>().unwrap(), Decimal::new(-125, 2));
        assert_eq!(parser.parse::<f64>().unwrap(), -1.25);
        assert_eq!(parser.parse::<f32>().unwrap(), -2.5);
        let date = NaiveDate::from_ymd_opt(2024, 2, 29).unwrap();
        let time = NaiveTime::from_hms_nano_opt(12, 34, 56, 123_456_700).unwrap();
        assert_eq!(parser.parse::<NaiveDate>().unwrap(), date);
        assert_eq!(parser.parse::<NaiveTime>().unwrap(), time);
        assert_eq!(
            parser.parse::<NaiveDateTime>().unwrap(),
            date.and_time(time)
        );
        assert_eq!(
            parser.parse::<NaiveDateTime>().unwrap(),
            date.and_hms_nano_opt(12, 34, 56, 3_333_333).unwrap()
        );
        assert_eq!(
            parser.parse::<NaiveDateTime>().unwrap(),
            date.and_hms_opt(12, 34, 0).unwrap()
        );
        assert_eq!(
            parser.parse::<DateTime<Utc>>().unwrap(),
            date.and_hms_nano_opt(7, 4, 56, 123_456_700)
                .unwrap()
                .and_utc()
        );
        assert_eq!(parser.parse::<Option<i32>>().unwrap(), None);
        assert_eq!(parser.parse::<Option<&str>>().unwrap(), None);
        assert_eq!(parser.parse::<Option<Decimal>>().unwrap(), None);
        assert_eq!(parser.fetch_next().unwrap(), (0, true));
    }
    let mut parser = partitions[0].parser().unwrap();
    assert_eq!(parser.fetch_next().unwrap(), (1, true));
    assert!(
        parser.parse::<Option<bool>>().is_err(),
        "non-null type mismatch must not become NULL"
    );
}

#[test]
#[ignore = "requires explicitly supplied MSSQL_URL; fixed-width metadata and padding"]
fn live_fixed_width_types_with_nulls_and_empty_metadata() {
    let uri = std::env::var("MSSQL_URL").expect("supply MSSQL_URL explicitly");
    let rt = Arc::new(Runtime::new().unwrap());
    for shape in ["values", "nulls", "empty"] {
        let query = if shape == "nulls" {
            "SELECT CAST(NULL AS char(4)) AS c, CAST(NULL AS binary(4)) AS b, CAST(NULL AS smallmoney) AS m"
        } else if shape == "empty" {
            "SELECT CAST('xy' AS char(4)) AS c, CAST(0x0102 AS binary(4)) AS b, CAST(-2.5 AS smallmoney) AS m WHERE 1=0"
        } else {
            "SELECT CAST('xy' AS char(4)) AS c, CAST(0x0102 AS binary(4)) AS b, CAST(-2.5 AS smallmoney) AS m"
        };
        let mut source = MsSQLBridgeSource::new(rt.clone(), &uri, 1).unwrap();
        source.set_queries(&[CXQuery::naked(query)]);
        source.fetch_metadata().unwrap();
        assert_eq!(source.names(), ["c", "b", "m"]);
        assert!(matches!(source.schema()[0], MsSQLTypeSystem::Char(_)));
        assert!(matches!(source.schema()[1], MsSQLTypeSystem::Binary(_)));
        assert!(matches!(source.schema()[2], MsSQLTypeSystem::SmallMoney(_)));
        let mut partitions = source.partition().unwrap();
        let mut parser = partitions[0].parser().unwrap();
        assert_eq!(
            parser.fetch_next().unwrap(),
            (usize::from(shape != "empty"), true)
        );
        if shape != "empty" {
            assert_eq!(
                parser.parse::<Option<&str>>().unwrap(),
                (shape == "values").then_some("xy  ")
            );
            assert_eq!(
                parser.parse::<Option<&[u8]>>().unwrap(),
                (shape == "values").then_some(&[1, 2, 0, 0][..])
            );
            assert_eq!(
                parser.parse::<Option<f32>>().unwrap(),
                (shape == "values").then_some(-2.5)
            );
        }
        for _ in 0..2 {
            assert_eq!(parser.fetch_next().unwrap(), (0, true));
        }
    }
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
