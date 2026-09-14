use thiserror::Error;

#[derive(Debug, Error)]
pub enum MsSQLBridgeSourceError {
    #[error("SQL Server configuration: {0}")]
    Configuration(String),
    #[error("SQL Server conversion: {0}")]
    Conversion(String),
    #[error("SQL Server failed to get the row count")]
    GetNRowsFailed,
    #[error(transparent)]
    Bridge(#[from] mssql_tiberius_bridge::Error),
    #[error(transparent)]
    Pool(#[from] bb8::RunError<mssql_tiberius_bridge::Error>),
    #[error(transparent)]
    Url(#[from] url::ParseError),
    #[error(transparent)]
    Utf8(#[from] std::str::Utf8Error),
    #[error(transparent)]
    Decode(#[from] std::string::FromUtf8Error),
    #[error(transparent)]
    ConnectorX(#[from] crate::errors::ConnectorXError),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}
