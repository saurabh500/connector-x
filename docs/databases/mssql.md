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
`d2e91bd4d75891acefe865c1a28c60abafab2bf4` and depends on
[bridge PR 124](https://github.com/saurabh500/mssql-tiberius-bridge/pull/124).
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

Native initial-connect retries and idle recovery are disabled with
`connect_retry_count(0)`. Existing bb8 connection-attempt and replacement
behavior is retained. ConnectorX does not replay failed SQL or switch backends
after an error. Unread or dead connections are discarded by the pool, and
checkout health checks query the server.

Rows are read incrementally in 32-row refills, reusing owned storage. Result-set
boundaries are explicit; incompatible column names, types, nullability,
precision, or scale are errors, not silently flattened data. Failed refills
discard partial rows and stay failed; EOF stays EOF. New-backend datetimeoffset
conversion normalizes to UTC; the legacy Tiberius conversion is unchanged.
Compatibility alone does not imply a performance gain.

The ignored `sources::mssql_bridge::tests::live_*` Rust tests require an
explicit `MSSQL_URL` and run read-only queries without creating fixtures.

### Same-wheel SF1 LINEITEM transfer check (2026-09-14)

One default-feature release wheel containing both backends was built from
runtime commit `2cff2bc010d43c7ce7da3955a6f941bf6ae35f81` using Rust 1.91.1.
Both methods used that same installed native extension and dependencies:
Python 3.12.10, NumPy 2.5.3, Pandas 2.3.3, and PyArrow 23.0.1 on Windows 11
(32 logical client CPUs). The session-owned SQL Server 2025 Docker container
was limited to **8 GiB and 4 CPUs**, bound to loopback, with `encrypt=true`
and explicit self-signed-certificate trust for both methods.

The workload was `SELECT * FROM lineitem`: all 16 columns and 6,001,215 rows
of SF1 data, returned as a fully materialized Pandas dataframe using four
automatic partitions on `l_orderkey`. This is a full-table transfer check,
not the TPC-H query suite or an official TPC result.

| Backend | Median read time | Measured range | Median process peak RSS |
|:--|--:|--:|--:|
| `tiberius` | 103.003 s | 102.219-139.469 s | 2.713 GiB |
| `mssql-tds` (bridge-backed) | 93.192 s | 91.659-93.780 s | 2.712 GiB |

The bridge had **9.5% lower median elapsed time** (about 1.105x the transfer
rate), with essentially unchanged peak memory in this run. The 139.469 s
Tiberius sample was retained. These are descriptive results from five
measured samples per backend on a shared host, not a general speedup promise
or a 20% qualification gate.

Each backend had two warmups followed by five measured reads. Workers ran
serially in fresh processes, alternating backend order deterministically by
round (not randomized), with warm server/OS caches and no cache flush.
Timing covered `cx.read_sql` through return of the complete dataframe;
imports, reference validation, and subsequent worker shutdown were excluded.
OS process peak RSS was captured immediately after the read, before validation.
All 14 samples passed schema, row-count, null, aggregate, and canonical
multiset-fingerprint checks against the preserved SF1 reference.

<details>
<summary>All 14 read times, including warmups (seconds)</summary>

| Round | Order | Tiberius | Bridge |
|:--|:--|--:|--:|
| Warmup 1 | Tiberius, bridge | 101.595 | 130.328 |
| Warmup 2 | Bridge, Tiberius | 101.145 | 90.840 |
| Measured 1 | Tiberius, bridge | 139.469 | 91.659 |
| Measured 2 | Bridge, Tiberius | 102.324 | 92.569 |
| Measured 3 | Tiberius, bridge | 103.003 | 93.192 |
| Measured 4 | Bridge, Tiberius | 102.219 | 93.780 |
| Measured 5 | Tiberius, bridge | 103.238 | 93.698 |

</details>

Wheel SHA256:
`08898c3e1c3208a7b8ddd7ba7f5967f672af38f1497c20adf41b3c926c49cc90`.
Installed native-extension SHA256 (also verified inside the wheel):
`9885578544cfd723b02cbaa8f8e9c3852de93bf1e5717f5bddb2b83dc73025e4`.

Separately, 16 small canonical Arrow workers passed correctness checks for
numeric, mixed, decimal/text, and wide-LOB data with one and four partitions.
Those are correctness smoke checks, not statistical performance measurements.
No SF10 result is claimed, and earlier direct-adapter measurements on other
hardware were not pooled with this bridge-backed comparison. Tiberius remains
the default, and the unreleased bridge pin still requires release qualification.

### Performance (r5.4xlarge docker in another EC2 instance)

**Modin does not support read_sql on Mssql**

- Time chart, lower is better.

<p align="center"><img alt="time chart" src="https://raw.githubusercontent.com/sfu-db/connector-x/main/assets/mssql-time.png"/></p>

- Memory consumption chart, lower is better.

<p align="center"><img alt="memory chart" src="https://raw.githubusercontent.com/sfu-db/connector-x/main/assets/mssql-mem.png"/></p>

In conclusion, ConnectorX uses **3x** less memory and **14x** less time compared with Pandas.
