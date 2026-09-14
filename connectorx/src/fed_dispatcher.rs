use crate::{get_arrow::get_arrow_resolved, source_router::ResolvedSource};
use crate::{prelude::*, sql::CXQuery};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::prelude::*;
use fehler::throws;
use log::debug;
use rayon::prelude::*;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::sync::{mpsc::channel, Arc};

#[throws(ConnectorXOutError)]
pub fn run(
    sql: String,
    db_map: HashMap<String, String>,
    j4rs_base: Option<&str>,
    strategy: &str,
) -> Vec<RecordBatch> {
    debug!("federated input sql: {}", sql);
    let mut db_conn_map: HashMap<String, FederatedDataSourceInfo> = HashMap::new();
    for (k, v) in db_map.into_iter() {
        db_conn_map.insert(
            k,
            FederatedDataSourceInfo::new_from_conn_str(
                SourceConn::try_from(v.as_str())?,
                false,
                "",
                "",
            ),
        );
    }
    let resolved = resolve_connections(&db_conn_map)?;
    let fed_plan = rewrite_sql(sql.as_str(), &db_conn_map, j4rs_base, strategy)?;

    debug!("fetch queries from remote");
    let (sender, receiver) = channel();
    fed_plan.into_par_iter().enumerate().try_for_each_with(
        sender,
        |s, (i, p)| -> Result<(), ConnectorXOutError> {
            match p.db_name.as_str() {
                "LOCAL" => {
                    s.send((p.sql, None)).expect("send error local");
                }
                _ => {
                    debug!("start query {}: {}", i, p.sql);
                    let rbs = read_remote(&resolved[p.db_name.as_str()], &p.sql)?;

                    let provider = MemTable::try_new(rbs[0].schema(), vec![rbs])?;
                    s.send((p.db_alias, Some(Arc::new(provider))))
                        .expect(&format!("send error {}", i));
                    debug!("query {} finished", i);
                }
            }
            Ok(())
        },
    )?;

    let ctx = SessionContext::new();
    let mut alias_names: Vec<String> = vec![];
    let mut local_sql = String::new();
    receiver
        .iter()
        .try_for_each(|(alias, provider)| -> Result<(), ConnectorXOutError> {
            match provider {
                Some(p) => {
                    ctx.register_table(alias.as_str(), p)?;
                    alias_names.push(alias);
                }
                None => local_sql = alias,
            }

            Ok(())
        })?;

    debug!("\nexecute query final...\n{}\n", local_sql);
    let rt = Arc::new(tokio::runtime::Runtime::new().expect("Failed to create runtime"));
    // until datafusion fix the bug: https://github.com/apache/arrow-datafusion/issues/2147
    for alias in alias_names {
        local_sql = local_sql.replace(format!("\"{}\"", alias).as_str(), alias.as_str());
    }

    let df = rt.block_on(ctx.sql(local_sql.as_str()))?;
    rt.block_on(df.collect())?
}

fn resolve_connections<'a>(
    connections: &'a HashMap<String, FederatedDataSourceInfo<'_>>,
) -> Result<HashMap<&'a str, ResolvedSource>, ConnectorXOutError> {
    let (names, sources): (Vec<_>, Vec<_>) = connections
        .iter()
        .map(|(name, info)| (name.as_str(), info.conn_str_info.as_ref().unwrap()))
        .unzip();
    Ok(names
        .into_iter()
        .zip(ResolvedSource::new_many(sources)?)
        .collect())
}

fn read_remote(
    resolved: &ResolvedSource,
    sql: &str,
) -> Result<Vec<RecordBatch>, ConnectorXOutError> {
    let queries: Vec<_> = sql.split(';').map(CXQuery::naked).collect();
    Ok(get_arrow_resolved(resolved, None, &queries, None)?.arrow()?)
}

#[cfg(all(test, feature = "src_mssql"))]
mod tests {
    use super::*;
    use std::process::Command;

    #[test]
    #[ignore = "requires MSSQL_SELECTOR_TEST_URL; read-only remote fragments, no JVM"]
    fn live_federated_snapshot() {
        assert!(std::env::var_os("MSSQL_SELECTOR_TEST_URL").is_some());
        for backend in ["tiberius", "mssql-tds"] {
            let result = Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "fed_dispatcher::tests::federated_snapshot_child",
                    "--nocapture",
                ])
                .env("CX_FED_SELECTOR_CHILD", "1")
                .env("CONNECTORX_MSSQL_BACKEND", backend)
                .env("RUST_LOG", "connectorx::mssql_backend=debug")
                .output()
                .unwrap();
            assert!(
                result.status.success(),
                "federated snapshot subprocess failed"
            );
            let trace = String::from_utf8(result.stderr).unwrap();
            assert_eq!(
                trace.matches(&format!("MSSQL backend: {backend}")).count(),
                1
            );
            assert_eq!(
                trace
                    .matches(&format!("MSSQL metadata backend: {backend}"))
                    .count(),
                5
            );
            assert_eq!(
                trace
                    .matches(&format!("MSSQL partition backend: {backend}"))
                    .count(),
                9
            );
            let other = if backend == "tiberius" {
                "mssql-tds"
            } else {
                "tiberius"
            };
            assert!(!trace.contains(&format!("backend: {other}")));
        }
    }

    #[test]
    fn federated_snapshot_child() {
        if std::env::var_os("CX_FED_SELECTOR_CHILD").is_none() {
            return;
        }
        env_logger::init();
        let uri = std::env::var("MSSQL_SELECTOR_TEST_URL").unwrap();
        let mut connections = HashMap::new();
        for name in ["first", "second"] {
            connections.insert(
                name.to_string(),
                FederatedDataSourceInfo::new_from_conn_str(
                    SourceConn::try_from(uri.as_str()).unwrap(),
                    false,
                    "",
                    "",
                ),
            );
        }
        let other_uri = "postgres://localhost:1/unused?cxprotocol=cursor";
        connections.insert(
            "other".to_string(),
            FederatedDataSourceInfo::new_from_conn_str(
                SourceConn::try_from(other_uri).unwrap(),
                false,
                "",
                "",
            ),
        );
        let resolved = resolve_connections(&connections).unwrap();
        assert_eq!(resolved["other"].source().proto, "cursor");
        let first = read_remote(&resolved["first"], "SELECT 0 AS id").unwrap();
        assert_eq!(first.iter().map(|batch| batch.num_rows()).sum::<usize>(), 1);
        std::env::set_var("CONNECTORX_MSSQL_BACKEND", "invalid");
        for _ in 0..2 {
            resolved
                .par_iter()
                .filter(|(_, source)| matches!(source.source().ty, SourceType::MsSQL))
                .for_each(|(_, source)| {
                    let batches = read_remote(source, "SELECT 1 AS id;SELECT 2 AS id").unwrap();
                    let mut values: Vec<_> = batches
                        .iter()
                        .flat_map(|batch| {
                            batch
                                .column(0)
                                .as_any()
                                .downcast_ref::<arrow::array::Int64Array>()
                                .unwrap()
                                .values()
                                .to_vec()
                        })
                        .collect();
                    values.sort_unstable();
                    assert_eq!(values, [1, 2]);
                });
        }
        let non_mssql = HashMap::from([("other".to_string(), other_uri.to_string())]);
        let error = run(
            "SELECT 1".into(),
            non_mssql,
            Some("nonexistent-jvm-path"),
            "pushdown",
        )
        .unwrap_err();
        assert!(matches!(error, ConnectorXOutError::FileNotFoundError(_)));
        drop(resolved);
        let db_map = connections
            .into_keys()
            .map(|name| (name, uri.clone()))
            .collect();
        let error = run(
            "SELECT 1".into(),
            db_map,
            Some("nonexistent-jvm-path"),
            "pushdown",
        )
        .unwrap_err();
        assert!(error.to_string().contains("CONNECTORX_MSSQL_BACKEND"));
    }
}
