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

### Experimental Rust bridge source

The `src_mssql` feature also compiles
`connectorx::sources::mssql_bridge::MsSQLBridgeSource`. This source must be
constructed explicitly in Rust. Normal Python, Rust router, C++, partition,
metadata, and Arrow streaming entrypoints still select the original Tiberius
source. No environment selector or automatic fallback is implemented here.

The bridge source uses the existing SQL Server type system and transport
conversions. Explicit bindings are `MsSQLBridgeArrowTransport`,
`MsSQLBridgeArrowStreamTransport`, and the Python crate's
`pandas::MsSQLBridgePandasTransport`. Its `get_partition_range` helper is also
explicit; it does not change the existing partition route.

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
No new performance claim is made by this integration.

The ignored `sources::mssql_bridge::tests::live_*` Rust tests require an
explicit `MSSQL_URL` and run read-only queries without creating fixtures.

### Performance (r5.4xlarge docker in another EC2 instance)

**Modin does not support read_sql on Mssql**

- Time chart, lower is better.

<p align="center"><img alt="time chart" src="https://raw.githubusercontent.com/sfu-db/connector-x/main/assets/mssql-time.png"/></p>

- Memory consumption chart, lower is better.

<p align="center"><img alt="memory chart" src="https://raw.githubusercontent.com/sfu-db/connector-x/main/assets/mssql-mem.png"/></p>

In conclusion, ConnectorX uses **3x** less memory and **14x** less time compared with Pandas.
