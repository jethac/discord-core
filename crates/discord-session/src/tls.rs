//! Explicit TLS configuration shared by Discord transports.
//! Chooses a provider locally, so a host application enabling another provider cannot
//! cause rustls's automatic global-provider selection to panic.
use std::sync::{Arc, OnceLock};

pub fn config() -> Arc<rustls::ClientConfig> {
    static CONFIG: OnceLock<Arc<rustls::ClientConfig>> = OnceLock::new();
    CONFIG
        .get_or_init(|| {
            let roots =
                rustls::RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
            let provider = Arc::new(rustls::crypto::ring::default_provider());
            let config = rustls::ClientConfig::builder_with_provider(provider)
                .with_safe_default_protocol_versions()
                .expect("ring supports default TLS versions")
                .with_root_certificates(roots)
                .with_no_client_auth();
            Arc::new(config)
        })
        .clone()
}
pub fn connector() -> tokio_tungstenite::Connector {
    tokio_tungstenite::Connector::Rustls(config())
}
