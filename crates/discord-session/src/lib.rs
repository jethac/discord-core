//! Session credentials and call negotiation types shared with the media transport.
use serde::{Deserialize, Serialize};
use std::fmt;
use zeroize::Zeroizing;

/// Discord snowflake. Serialized as a string to preserve all 64 bits.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Id(pub u64);
impl fmt::Display for Id {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}
impl Serialize for Id {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.collect_str(self)
    }
}
impl<'de> Deserialize<'de> for Id {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Wire {
            Text(String),
            Number(u64),
        }
        let value = match Wire::deserialize(d)? {
            Wire::Text(s) => s.parse().map_err(serde::de::Error::custom)?,
            Wire::Number(n) => n,
        };
        if value == 0 {
            return Err(serde::de::Error::custom("zero snowflake"));
        }
        Ok(Self(value))
    }
}

/// Errors never contain credentials or remote payloads.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("network operation failed")]
    Network,
    #[error("operation timed out")]
    Timeout,
    #[error("session is closed")]
    Closed,
    #[error("authentication expired")]
    Unauthorized,
    #[error("permission denied")]
    Forbidden,
    #[error("rate limited; retry after {retry_after:?}")]
    RateLimited { retry_after: std::time::Duration },
    #[error("capacity limit exceeded")]
    Capacity,
    #[error("a call is already active or leaving")]
    Busy,
    #[error("the gateway is not ready")]
    NotReady,
    #[error("write outcome is unknown; do not automatically repeat it")]
    Ambiguous,

    #[error("invalid credential")]
    InvalidCredential,
    #[error("invalid protocol response")]
    Protocol,
}

/// Session-only secret. Debug output is redacted and memory is cleared on drop.
pub struct Secret(Zeroizing<String>);
impl Secret {
    pub fn new(value: String) -> Result<Self, Error> {
        let value = Zeroizing::new(value);
        if value.is_empty() || value.len() > 2048 || !value.bytes().all(|b| b.is_ascii_graphic()) {
            return Err(Error::InvalidCredential);
        }
        Ok(Self(value))
    }
    /// Expose only at the authenticated transport boundary.
    pub fn expose(&self) -> &str {
        &self.0
    }
}
impl fmt::Debug for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Secret([REDACTED])")
    }
}

pub const MAX_PARTICIPANTS: usize = 64;

/// Voice credentials assembled from matching Gateway session/server events.
#[derive(Debug)]
pub struct VoiceConnection {
    pub channel: Id,
    pub guild: Option<Id>,
    pub user: Id,
    pub peer: Option<Id>,
    pub session: Secret,
    pub token: Secret,
    pub endpoint: String,
    pub request: u64,
}

mod api;
pub use api::Api;
mod types;
pub use types::*;
mod gateway;
pub use gateway::{Session, SessionHandle};

#[doc(hidden)]
pub mod tls;
