use crate::constants::CONNECTORX_PROTOCOL;
use crate::errors::{ConnectorXError, Result};
use crate::utils::remove_query_params;
use anyhow::anyhow;
use fehler::throws;
#[cfg(feature = "src_postgres")]
use redshift_iam::redshift_to_postgres;
use std::convert::TryFrom;
use std::ffi::OsStr;
use url::Url;

#[derive(Debug, Clone)]
pub enum SourceType {
    Postgres,
    SQLite,
    MySQL,
    MsSQL,
    Oracle,
    BigQuery,
    DuckDB,
    Trino,
    ClickHouse,
    Unknown,
}

#[derive(Debug, Clone)]
pub struct SourceConn {
    pub ty: SourceType,
    pub conn: Url,
    pub proto: String,
}

const MSSQL_BACKEND_ENV: &str = "CONNECTORX_MSSQL_BACKEND";

#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MsSQLBackend {
    Tiberius,
    MssqlTds,
}

impl MsSQLBackend {
    fn parse(value: Option<&OsStr>) -> Result<Self> {
        match value {
            None => Ok(Self::Tiberius),
            Some(value) if value == "tiberius" => Ok(Self::Tiberius),
            Some(value) if value == "mssql-tds" => Ok(Self::MssqlTds),
            _ => Err(
                anyhow!("CONNECTORX_MSSQL_BACKEND must be exactly 'tiberius' or 'mssql-tds'")
                    .into(),
            ),
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::Tiberius => "tiberius",
            Self::MssqlTds => "mssql-tds",
        }
    }
}

/// An operation's immutable connection and backend selection.
///
/// Shared with language bindings so partition discovery and execution cannot
/// resolve different backends. Construct a new context for each operation.
#[doc(hidden)]
pub struct ResolvedSource {
    source: SourceConn,
    mssql_backend: MsSQLBackend,
}

impl ResolvedSource {
    pub fn new(source: &SourceConn) -> Result<Self> {
        let mssql_backend = if matches!(source.ty, SourceType::MsSQL) {
            let backend = MsSQLBackend::parse(std::env::var_os(MSSQL_BACKEND_ENV).as_deref())?;
            if !cfg!(feature = "src_mssql") {
                return Err(anyhow!(
                    "MSSQL backend '{}' is not compiled; enable src_mssql",
                    backend.name()
                )
                .into());
            }
            log::debug!(target: "connectorx::mssql_backend", "MSSQL backend: {}", backend.name());
            backend
        } else {
            MsSQLBackend::Tiberius
        };
        Ok(Self {
            source: source.clone(),
            mssql_backend,
        })
    }

    #[cfg(feature = "fed_exec")]
    pub(crate) fn new_many(sources: Vec<&SourceConn>) -> Result<Vec<Self>> {
        let backend = sources
            .iter()
            .find(|source| matches!(source.ty, SourceType::MsSQL))
            .map(|source| Self::new(source))
            .transpose()?
            .map(|resolved| resolved.mssql_backend)
            .unwrap_or(MsSQLBackend::Tiberius);
        Ok(sources
            .into_iter()
            .map(|source| Self {
                source: source.clone(),
                mssql_backend: backend,
            })
            .collect())
    }

    pub fn source(&self) -> &SourceConn {
        &self.source
    }

    pub fn mssql_backend(&self) -> MsSQLBackend {
        self.mssql_backend
    }
}

impl TryFrom<&str> for SourceConn {
    type Error = ConnectorXError;

    fn try_from(conn: &str) -> Result<SourceConn> {
        let old_url = Url::parse(conn).map_err(|e| anyhow!("parse error: {}", e))?;

        // parse connectorx protocol
        let proto = match old_url.query_pairs().find(|p| p.0 == CONNECTORX_PROTOCOL) {
            Some((_, proto)) => proto.to_owned().to_string(),
            None => "binary".to_string(),
        };

        // create url by removing connectorx protocol
        let url = remove_query_params(&old_url, &[CONNECTORX_PROTOCOL]);

        // users from sqlalchemy may set engine in connection url (e.g. mssql+pymssql://...)
        // only for compatablility, we don't use the same engine
        match url.scheme().split('+').collect::<Vec<&str>>()[0] {
            "postgres" | "postgresql" => Ok(SourceConn::new(SourceType::Postgres, url, proto)),
            #[cfg(feature = "src_postgres")]
            "redshift-iam" => Ok(SourceConn::new(
                SourceType::Postgres,
                redshift_to_postgres(url),
                "cursor".to_string(),
            )),
            "sqlite" => Ok(SourceConn::new(SourceType::SQLite, url, proto)),
            "mysql" => Ok(SourceConn::new(SourceType::MySQL, url, proto)),
            "mssql" => Ok(SourceConn::new(SourceType::MsSQL, url, proto)),
            "oracle" => Ok(SourceConn::new(SourceType::Oracle, url, proto)),
            "bigquery" => Ok(SourceConn::new(SourceType::BigQuery, url, proto)),
            "duckdb" => Ok(SourceConn::new(SourceType::DuckDB, url, proto)),
            "trino" => Ok(SourceConn::new(SourceType::Trino, url, proto)),
            "clickhouse" => Ok(SourceConn::new(SourceType::ClickHouse, url, proto)),
            _ => Ok(SourceConn::new(SourceType::Unknown, url, proto)),
        }
    }
}

impl SourceConn {
    pub fn new(ty: SourceType, conn: Url, proto: String) -> Self {
        Self { ty, conn, proto }
    }
    pub fn set_protocol(&mut self, protocol: &str) {
        self.proto = protocol.to_string();
    }
}

#[throws(ConnectorXError)]
pub fn parse_source(conn: &str, protocol: Option<&str>) -> SourceConn {
    let mut source_conn = SourceConn::try_from(conn)?;
    match protocol {
        Some(p) => source_conn.set_protocol(p),
        None => {}
    }
    source_conn
}

#[cfg(test)]
mod tests {
    use super::{MsSQLBackend, ResolvedSource, SourceConn, SourceType, MSSQL_BACKEND_ENV};
    use std::convert::TryFrom;
    use std::ffi::{OsStr, OsString};
    use std::process::Command;

    #[test]
    fn backend_parser_is_exact_and_defaults_to_tiberius() {
        assert_eq!(MsSQLBackend::parse(None).unwrap(), MsSQLBackend::Tiberius);
        assert_eq!(
            MsSQLBackend::parse(Some(OsStr::new("tiberius"))).unwrap(),
            MsSQLBackend::Tiberius
        );
        assert_eq!(
            MsSQLBackend::parse(Some(OsStr::new("mssql-tds"))).unwrap(),
            MsSQLBackend::MssqlTds
        );
        for value in [
            "",
            "Tiberius",
            "MSSQL-TDS",
            " tiberius",
            "mssql-tds ",
            "binary",
            "secret-invalid",
        ] {
            let error = MsSQLBackend::parse(Some(OsStr::new(value)))
                .unwrap_err()
                .to_string();
            assert!(error.contains(MSSQL_BACKEND_ENV));
            assert!(!error.contains("secret-invalid"));
        }
    }

    #[test]
    fn backend_parser_rejects_non_unicode() {
        #[cfg(windows)]
        let value = {
            use std::os::windows::ffi::OsStringExt;
            OsString::from_wide(&[0xd800])
        };
        #[cfg(unix)]
        let value = {
            use std::os::unix::ffi::OsStringExt;
            OsString::from_vec(vec![0xff])
        };
        assert!(MsSQLBackend::parse(Some(&value)).is_err());
    }

    #[test]
    fn operation_selection_in_subprocesses() {
        for value in [
            None,
            Some("tiberius"),
            Some("mssql-tds"),
            Some(""),
            Some("invalid"),
        ] {
            let mut child = Command::new(std::env::current_exe().unwrap());
            child.args([
                "--exact",
                "source_router::tests::operation_selection_child",
                "--nocapture",
            ]);
            child.env("CX_SELECTOR_TEST_CHILD", "1");
            child.env_remove(MSSQL_BACKEND_ENV);
            if let Some(value) = value {
                child.env(MSSQL_BACKEND_ENV, value);
            }
            let output = child.output().unwrap();
            assert!(
                output.status.success(),
                "{}\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr),
            );
        }
    }

    #[test]
    fn operation_selection_child() {
        if std::env::var_os("CX_SELECTOR_TEST_CHILD").is_none() {
            return;
        }
        let conn = SourceConn {
            ty: SourceType::MsSQL,
            conn: "mssql://127.0.0.1:1/db".parse().unwrap(),
            proto: "binary".into(),
        };
        let expected = MsSQLBackend::parse(std::env::var_os(MSSQL_BACKEND_ENV).as_deref());
        let resolved = ResolvedSource::new(&conn);
        let part =
            crate::partition::PartitionQuery::new("SELECT 1 AS id", "id", Some(1), Some(2), 2);
        if expected.is_err() || !cfg!(feature = "src_mssql") {
            let error = resolved.err().unwrap().to_string();
            assert!(error.contains(if expected.is_err() {
                MSSQL_BACKEND_ENV
            } else {
                "not compiled"
            }));
            assert!(crate::partition::partition(&part, &conn).is_err());
            assert!(crate::partition::get_col_range(&conn, "SELECT 1 AS id", "id").is_err());
            assert!(crate::partition::get_part_query(&conn, "SELECT 1 AS id", "id", 1, 2).is_err());
            #[cfg(feature = "dst_arrow")]
            {
                let queries = [crate::sql::CXQuery::naked("SELECT 1 AS id")];
                assert!(crate::get_arrow::get_arrow(&conn, None, &queries, None).is_err());
                assert!(crate::get_arrow::try_new_record_batch_iter(
                    &conn, None, &queries, 10, None
                )
                .is_err());
            }
        } else {
            let resolved = resolved.unwrap();
            let original = expected.unwrap();
            assert_eq!(resolved.mssql_backend(), original);
            let queries = crate::partition::partition(&part, &conn).unwrap();
            assert_eq!(queries.len(), 2);
            // This subprocess runs only this test, so changing its environment is isolated.
            for next in ["mssql-tds", "tiberius"] {
                std::env::set_var(MSSQL_BACKEND_ENV, next);
                assert_eq!(
                    ResolvedSource::new(&conn).unwrap().mssql_backend().name(),
                    next
                );
                assert_eq!(resolved.mssql_backend(), original);
                assert_eq!(
                    crate::partition::partition_resolved(&part, &resolved)
                        .unwrap()
                        .iter()
                        .map(ToString::to_string)
                        .collect::<Vec<_>>(),
                    queries.iter().map(ToString::to_string).collect::<Vec<_>>(),
                );
            }
        }
        std::env::set_var(MSSQL_BACKEND_ENV, "invalid");
        let sqlite = SourceConn::new(
            SourceType::SQLite,
            "sqlite://unused".parse().unwrap(),
            "binary".into(),
        );
        assert!(ResolvedSource::new(&sqlite).is_ok());
        assert!(super::parse_source("mssql://127.0.0.1:1/db", None).is_ok());
        assert!(SourceConn::try_from("mssql://127.0.0.1:1/db").is_ok());
    }

    #[cfg(all(feature = "src_mssql", feature = "dst_arrow"))]
    #[test]
    #[ignore = "requires MSSQL_SELECTOR_TEST_URL; read-only queries"]
    fn live_public_wrappers() {
        assert!(std::env::var_os("MSSQL_SELECTOR_TEST_URL").is_some());
        for backend in [None, Some("tiberius"), Some("mssql-tds")] {
            let mut command = Command::new(std::env::current_exe().unwrap());
            command
                .args([
                    "--exact",
                    "source_router::tests::live_public_wrappers_child",
                    "--nocapture",
                ])
                .env("CX_SELECTOR_LIVE_CHILD", "1")
                .env("RUST_LOG", "connectorx::mssql_backend=debug")
                .env_remove(MSSQL_BACKEND_ENV);
            if let Some(backend) = backend {
                command.env(MSSQL_BACKEND_ENV, backend);
            }
            let result = command.output().unwrap();
            assert!(
                result.status.success(),
                "core live wrapper subprocess failed"
            );
            let trace = String::from_utf8(result.stderr).unwrap();
            let selected = backend.unwrap_or("tiberius");
            for stage in ["metadata", "range", "partition"] {
                assert!(trace.contains(&format!("MSSQL {stage} backend: {selected}")));
            }
            let other = if selected == "tiberius" {
                "mssql-tds"
            } else {
                "tiberius"
            };
            assert!(!trace.contains(&format!("backend: {other}")));
        }
    }

    #[cfg(all(feature = "src_mssql", feature = "dst_arrow"))]
    #[test]
    fn live_public_wrappers_child() {
        if std::env::var_os("CX_SELECTOR_LIVE_CHILD").is_none() {
            return;
        }
        env_logger::init();
        use crate::get_arrow::{get_arrow, get_arrow_resolved, new_record_batch_iter};
        use crate::partition::{partition_resolved, PartitionQuery};
        use crate::sql::CXQuery;

        let uri = std::env::var("MSSQL_SELECTOR_TEST_URL").unwrap();
        let parsed = SourceConn::try_from(uri.as_str()).unwrap();
        let sources = [
            SourceConn::new(SourceType::MsSQL, parsed.conn.clone(), "binary".into()),
            parsed,
            super::parse_source(&uri, None).unwrap(),
        ];
        let original = std::env::var_os(MSSQL_BACKEND_ENV);
        let query = "SELECT id FROM (VALUES (1), (2), (3)) AS data(id)";
        for source in sources {
            for _ in 0..2 {
                match &original {
                    Some(value) => std::env::set_var(MSSQL_BACKEND_ENV, value),
                    None => std::env::remove_var(MSSQL_BACKEND_ENV),
                }
                let queries = [CXQuery::naked(query)];
                let batches = get_arrow(&source, None, &queries, None)
                    .unwrap()
                    .arrow()
                    .unwrap();
                assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 3);
                let mut stream = new_record_batch_iter(&source, None, &queries, 2, None);
                let resolved = ResolvedSource::new(&source).unwrap();
                std::env::set_var(MSSQL_BACKEND_ENV, "invalid");
                let parts =
                    partition_resolved(&PartitionQuery::new(query, "id", None, None, 2), &resolved)
                        .unwrap();
                let batches = get_arrow_resolved(&resolved, None, &parts, None)
                    .unwrap()
                    .arrow()
                    .unwrap();
                assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 3);
                stream.prepare();
                let mut rows = 0;
                while let Some(batch) = stream.next_batch() {
                    rows += batch.num_rows();
                }
                assert_eq!(rows, 3);
                assert!(stream.next_batch().is_none());
                assert!(stream.next_batch().is_none());
                assert!(get_arrow(&source, None, &queries, None).is_err());
            }
        }
    }

    /// Removing the connectorx protocol must not re-encode the remaining parameters:
    /// sources that percent-decode the query would otherwise receive a literal `+`
    /// wherever the caller wrote a space.
    #[test]
    fn keeps_remaining_query_params_verbatim() {
        let source_conn = SourceConn::try_from(
            "postgresql://u:p@host:5432/db?options=-c%20statement_timeout%3D1s&cxprotocol=cursor",
        )
        .unwrap();

        assert_eq!(
            source_conn.conn.query(),
            Some("options=-c%20statement_timeout%3D1s")
        );
        assert_eq!(source_conn.proto, "cursor");
    }

    /// The query is left alone even when there is no protocol parameter to remove.
    #[test]
    fn leaves_the_query_alone_when_there_is_nothing_to_remove() {
        let source_conn = SourceConn::try_from("mysql://host:3306/db?a=x%20y&b=%2Fz").unwrap();

        assert_eq!(source_conn.conn.query(), Some("a=x%20y&b=%2Fz"));
        assert_eq!(source_conn.proto, "binary");
    }
}
