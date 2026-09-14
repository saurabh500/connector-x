# MsSQL

```{note}
SQLServer does not need to specify protocol.
```

### MsSQL Connection
```{hint} 
if the user password has special characters, they need to be sanitized. example: `from urllib import parse; password = parse.quote_plus(password)`
```

```py
import connectorx as cx
conn = 'mssql://username:password@server:port/database?encrypt=true&trusted_connection=true'         # connection token
query = 'SELECT * FROM table'                                   # query string
cx.read_sql(conn, query)                                        # read data from MsSQL
```

### Connection Parameters
* By adding `trusted_connection=true` to connection uri parameter, windows authentication will be enabled. 
    * Example: `mssql://host:port/db?trusted_connection=true`
* By adding `encrypt=true` to connection uri parameter, SQLServer will use SSL encryption. 
    * Example: `mssql://host:port/db?encrypt=true&trusted_connection=true`
* By adding `trust_server_certificate=true` to connection uri parameter, the SQLServer certificate will not be validated and it is accepted as-is. 
    * Example: `mssql://host:port/db?trust_server_certificate=true&encrypt=true`
* By adding `trust_server_certificate_ca=/path/to/ca-cert.crt` to connection uri parameter, the SQLServer certificate will be validated against the given CA certificate in addition to the system-truststore.
    * Example: `mssql://host:port/db?encrypt=true&trust_server_certificate_ca=/path/to/ca-cert.crt`

### SQLServer-Pandas Type Mapping
| SQLServer Type  |      Pandas Type            |  Comment                           |
|:---------------:|:---------------------------:|:----------------------------------:|
| TINYINT         | int64, Int64(nullable)      |                                    |
| SMALLINT        | int64, Int64(nullable)      |                                    |
| INT             | int64, Int64(nullable)      |                                    |
| BIGINT          | int64, Int64(nullable)      |                                    |
| FLOAT           | float64                     |                                    |
| NUMERIC         | float64                     |                                    |
| DECIMAL         | float64                     | cannot support precision larger than 28                                   |
| BIT             | bool, boolean(nullable)     |                                    |
| VARCHAR         | object                      |                                    |
| CHAR            | object                      |                                    |
| TEXT            | object                      |                                    |
| NVARCHAR        | object                      |                                    |
| NCHAR           | object                      |                                    |
| NTEXT           | object                      |                                    |
| VARBINARY       | object                      |                                    |
| BINARY          | object                      |                                    |
| IMAGE           | object                      |                                    |
| DATETIME        | datetime64[ns]              |                                    |
| DATETIME2       | datetime64[ns]              |                                    |
| SMALLDATETIME   | datetime64[ns]              |                                    |
| DATE            | datetime64[ns]              |                                    |
| DATETIMEOFFSET  | datetime64[ns]              |                                    |
| TIME            | object                      |                                    |
| UNIQUEIDENTIFIER| object                      |                                    |

### Opt-in SQL Server backend

The `src_mssql` feature compiles both the original Tiberius source and
`connectorx::sources::mssql_bridge::MsSQLBridgeSource`. **Tiberius remains the
default.** Set `CONNECTORX_MSSQL_BACKEND=mssql-tds` to opt in to the bridge-backed
source, or `CONNECTORX_MSSQL_BACKEND=tiberius` to select the original source.
Only these exact lowercase values are accepted. An unset variable means
Tiberius; an empty, invalid, or non-Unicode value is a configuration error before
SQL Server range discovery, metadata, or data connections. Other databases
ignore this variable. A build without `src_mssql` reports that the selected
backend is not compiled rather than falling back.

There is no Python argument, URI parameter, or protocol selector for the backend.
One selection is captured at operation entry, including automatic partition
range discovery, counts, metadata, and all partitions. This applies to Pandas,
Arrow, Arrow streaming, and downstream Polars wrappers, as well as standalone
`partition_sql` and `get_meta`. An existing stream keeps its original source
even if the environment changes; a subsequent operation can select another
backend. Avoid changing process environment concurrently with new operations.

Rust `SourceConn` fields and constructors are unchanged and do not read the
selector. Public execution wrappers (`partition`, `get_col_range`,
`get_part_query`, `get_arrow`, and `new_record_batch_iter`) resolve it for each
call, including manually constructed `SourceConn` values. C++ Arrow calls use
these same wrappers. Federated reads capture one selection for all SQL Server
connections before rewriting the query and reuse it across remote fragments.
Bindings that combine partitioning and
execution use the internal `ResolvedSource` context to share one snapshot.
Explicitly constructed source objects continue to use their own backend.

`get_arrow::try_new_record_batch_iter` is a fallible Rust constructor. Python
uses the resolved fallible path, so selection, source creation, and metadata
errors during construction become Python errors. The legacy Rust factory
retains its infallible signature and panics on construction errors. **This does
not redesign midstream error handling:** producer failures may still panic
under the existing `RecordBatchIterator` contract.

For routing diagnostics, `RUST_LOG=connectorx::mssql_backend=debug` logs the
selected backend and the backend used for range, metadata, count, and partition
work. These messages do not include connection strings or SQL.

The bridge source uses the existing SQL Server type system and transport
conversions. Explicit bindings are `MsSQLBridgeArrowTransport`,
`MsSQLBridgeArrowStreamTransport`, and the Python crate's
`pandas::MsSQLBridgePandasTransport`. SQL partition rewriting uses the same
SQL Server dialect for both backends.

This fork pins the unreleased bridge revision
`1f74ea89da85dc4ea08498c521f8b070cc9cc484` and depends on
[bridge PR 126](https://github.com/saurabh500/mssql-tiberius-bridge/pull/126).
Published bridge 0.1.0 does not expose metadata for empty rowsets; this approved
commit dependency changes the bridge's `query`/`simple_query` to return a
borrowed Tiberius-style `QueryStream`, exposes metadata for empty rowsets, and
corrects fixed-width CHAR, BINARY, and SMALLMONEY metadata mappings.
The separate fast-path bridge PR 124 is not a dependency of this layer.
Replace the git dependency with a released version before product shipping.
ConnectorX depends only on the bridge, not directly on `mssql-tds`, and does not
enable the bridge's Arrow feature.

The experimental source requires explicit `encrypt=true` or `encrypt=false`.
The latter can still negotiate TLS when required by the server. Missing or
unrecognized encryption settings and `trust_server_certificate_ca` are errors:
the native driver's login-only TLS mode and certificate pinning are not
equivalents of the legacy unencrypted default and custom CA validation.
URI values are decoded once. With a named instance, an explicit port takes
precedence; without a port, SQL Browser resolves the instance.

This bridge version does not forward native integrated-authentication features.
The bridge backend therefore rejects `trusted_connection=true`; the original
Tiberius backend and its authentication features are unchanged.

The native driver's default initial-connect retry and idle-recovery policy is
retained; the ordinary bridge API has no retry-count setter. Existing bb8
connection-attempt and replacement behavior is also retained. ConnectorX itself
does not replay failed SQL or switch backends after an error. Dead connections
are discarded. Dropping a stream between yielded items leaves unread results
for the next real query to drain. Checkout validation executes and drains
`SELECT 1`; it does not use the bridge's cached `ping`.

This compatibility layer follows the legacy `query(...).await`,
`stream.columns().await`, and `stream.into_row().await` call sequence. It uses
ordinary owned bridge `Row` values and metadata events, retaining the legacy
bounded 32-item refill loop and per-item runtime
entry. It does not use custom row writers or the separate fast-path APIs.
Metadata and scalar helpers consume events to EOF, discarding unneeded rows
rather than collecting entire results, so trailing errors are reported.
Result-set boundaries include empty rowsets. Changes in exposed column names,
types, nullability, lengths, or scales are errors, not silently flattened data.
The bridge currently reports no precision metadata, so precision-only changes
cannot be checked. Failed refills discard partial rows and stay failed; EOF
stays EOF. New-backend datetimeoffset conversion normalizes to UTC; the legacy
Tiberius conversion is unchanged. No performance claim is made for this
compatibility implementation; measurements of the earlier fast-path experiment
do not describe this code.

The ignored `sources::mssql_bridge::tests::live_*` Rust tests require an
explicit `MSSQL_URL` and run read-only queries without creating fixtures.

### Performance (r5.4xlarge docker in another EC2 instance)

**Modin does not support read_sql on Mssql**

- Time chart, lower is better.

<p align="center"><img alt="time chart" src="https://raw.githubusercontent.com/sfu-db/connector-x/main/assets/mssql-time.png"/></p>

- Memory consumption chart, lower is better.

<p align="center"><img alt="memory chart" src="https://raw.githubusercontent.com/sfu-db/connector-x/main/assets/mssql-mem.png"/></p>

In conclusion, ConnectorX uses **3x** less memory and **14x** less time compared with Pandas.
