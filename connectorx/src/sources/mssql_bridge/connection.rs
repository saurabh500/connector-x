use super::MsSQLBridgeSourceError;
use async_trait::async_trait;
use bb8::ManageConnection;
use mssql_tiberius_bridge::{AuthMethod, Client, Config, EncryptionLevel};
use std::collections::HashMap;
use url::Url;
use urlencoding::decode;

/// Translate ConnectorX's URI without changing the legacy Tiberius configuration.
pub fn mssql_config(url: &Url) -> Result<Config, MsSQLBridgeSourceError> {
    let host = decode(url.host_str().unwrap_or("localhost"))?.into_owned();
    let parts: Vec<_> = host.split('\\').collect();
    if parts.len() > 2 || parts.iter().any(|part| part.is_empty()) {
        return Err(MsSQLBridgeSourceError::Configuration(
            "invalid host/instance".into(),
        ));
    }
    let mut config = Config::new();
    config.host(parts[0]);
    if let Some(port) = url.port() {
        config.port(port);
    } else if let Some(instance) = parts.get(1) {
        config.instance_name(*instance);
    }
    config.database(decode(url.path().strip_prefix('/').unwrap_or(url.path()))?.into_owned());
    config.authentication(AuthMethod::sql_server(
        decode(url.username())?.into_owned(),
        decode(url.password().unwrap_or(""))?.into_owned(),
    ));
    config.application_name("tiberius");
    let params: HashMap<String, String> = url.query_pairs().into_owned().collect();
    if params.get("trusted_connection").map(String::as_str) == Some("true") {
        return Err(MsSQLBridgeSourceError::Configuration(
            "this bridge version does not expose integrated-authentication features".into(),
        ));
    }
    if params
        .get("trust_server_certificate")
        .is_some_and(|v| v.eq_ignore_ascii_case("true"))
    {
        config.trust_cert();
    }
    if params.contains_key("trust_server_certificate_ca") {
        return Err(MsSQLBridgeSourceError::Configuration(
            "the bridge's certificate pin is not custom CA validation".into(),
        ));
    }
    config.encryption(match params.get("encrypt") {
        Some(v) if v.eq_ignore_ascii_case("true") => EncryptionLevel::Required,
        Some(v) if v.eq_ignore_ascii_case("false") => EncryptionLevel::Off,
        _ => return Err(MsSQLBridgeSourceError::Configuration(
            "specify encrypt=true or encrypt=false; mssql-tds has no equivalent of the legacy unencrypted default".into(),
        )),
    });
    // query_pairs has already decoded these values.
    if let Some(appname) = params.get("appname") {
        config.application_name(appname);
    }
    Ok(config)
}

#[derive(Clone)]
pub struct ConnectionManager {
    config: Config,
}

impl ConnectionManager {
    pub fn new(config: Config) -> Self {
        Self { config }
    }
}

#[async_trait]
impl ManageConnection for ConnectionManager {
    type Connection = Client;
    type Error = mssql_tiberius_bridge::Error;

    async fn connect(&self) -> Result<Client, Self::Error> {
        Client::connect(&self.config).await
    }

    async fn is_valid(
        &self,
        conn: &mut bb8::PooledConnection<'_, Self>,
    ) -> Result<(), Self::Error> {
        conn.query_compat("SELECT 1", &[])
            .into_row()
            .await
            .map(|_| ())
    }

    fn has_broken(&self, conn: &mut Client) -> bool {
        conn.is_connection_dead()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uri_decoding_and_explicit_port() {
        let url = Url::parse("mssql://user%40name:p%25ss@host%5Cinstance:1444/db%20name?encrypt=TRUE&appname=read%2520literal&trust_server_certificate=TRUE").unwrap();
        let config = mssql_config(&url).unwrap();
        assert_eq!(config.datasource_string(), "tcp:host,1444");
        let context = config.to_client_context();
        assert_eq!(context.connect_retry_count, 1);
        assert_eq!(context.user_name, "user@name");
        assert_eq!(context.password, "p%ss");
        assert_eq!(context.database, "db name");
        assert_eq!(context.application_name, "read%20literal");
        assert!(context.encryption_options.trust_server_certificate);
    }

    #[test]
    fn named_instance_uses_browser_only_without_port() {
        let config =
            mssql_config(&Url::parse("mssql://host%5Cinstance/db?encrypt=false").unwrap()).unwrap();
        assert_eq!(config.datasource_string(), "tcp:host\\instance");
    }

    #[test]
    fn unsupported_options_fail_instead_of_changing_tls() {
        for uri in [
            "mssql://localhost/db",
            "mssql://localhost/db?encrypt=",
            "mssql://localhost/db?encrypt=typo",
            "mssql://localhost/db?encrypt=true&trust_server_certificate_ca=ca.pem",
            "mssql://host%5C/db?encrypt=true",
            "mssql://localhost/db?encrypt=true&trusted_connection=true",
        ] {
            assert!(matches!(
                mssql_config(&Url::parse(uri).unwrap()),
                Err(MsSQLBridgeSourceError::Configuration(_))
            ));
        }
        for encrypt in ["true", "false", "TRUE", "FALSE"] {
            assert!(mssql_config(
                &Url::parse(&format!("mssql://localhost/db?encrypt={encrypt}")).unwrap()
            )
            .is_ok());
        }
    }
}
