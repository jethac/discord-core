//! Bounded REST requests. Writes are never automatically retried.
use crate::{Channel, Error, Guild, Id, Secret, User};
use futures_util::StreamExt;
use reqwest::{Client, Method};
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use std::{sync::Arc, time::Duration};
use tokio::{
    sync::{Mutex, Semaphore},
    time::Instant,
};

const MAX_BODY: usize = 8 * 1024 * 1024;

pub struct Api {
    client: Client,
    secret: Arc<Secret>,
    cooldown: Mutex<Instant>,
    permits: Semaphore,
    base: String,
}
impl Api {
    pub fn new(secret: Arc<Secret>) -> Result<Self, Error> {
        if secret.expose().starts_with("Bot ") || secret.expose().starts_with("Bearer ") {
            return Err(Error::InvalidCredential);
        }
        let client = Client::builder()
            .tls_backend_preconfigured((*crate::tls::config()).clone())
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .timeout(Duration::from_secs(20))
            .connect_timeout(Duration::from_secs(10))
            .user_agent(concat!("discord-core/", env!("CARGO_PKG_VERSION")))
            .build()
            .map_err(|_| Error::Network)?;
        Ok(Self {
            client,
            secret,
            cooldown: Mutex::new(Instant::now()),
            permits: Semaphore::new(4),
            base: "https://discord.com/api/v10".into(),
        })
    }
    pub async fn current_user(&self) -> Result<User, Error> {
        let user: User = self.get("/users/@me").await?;
        if user.bot {
            return Err(Error::InvalidCredential);
        }
        Ok(user)
    }
    pub async fn gateway_url(&self) -> Result<String, Error> {
        #[derive(serde::Deserialize)]
        struct Location {
            url: String,
        }
        let location: Location = self.get("/gateway").await?;
        crate::gateway::validate_url(&location.url)
    }
    pub async fn private_channels(&self) -> Result<Vec<Channel>, Error> {
        let channels: Vec<Channel> = self.get("/users/@me/channels").await?;
        if channels.len() > 4096 {
            return Err(Error::Capacity);
        }
        Ok(channels)
    }
    pub async fn guilds(&self) -> Result<Vec<Guild>, Error> {
        let mut all: Vec<Guild> = Vec::new();
        loop {
            let route = match all.last() {
                Some(last) => format!("/users/@me/guilds?limit=200&after={}", last.id),
                None => "/users/@me/guilds?limit=200".into(),
            };
            let page: Vec<Guild> = self.get(&route).await?;
            let done = page.len() < 200;
            if page.len() > 200 || all.len() + page.len() > 4096 {
                return Err(Error::Capacity);
            }
            if let (Some(last), Some(next)) = (all.last(), page.last())
                && next.id <= last.id
            {
                return Err(Error::Protocol);
            }
            all.extend(page);
            if done {
                return Ok(all);
            }
        }
    }
    pub async fn guild_channels(&self, guild: Id) -> Result<Vec<Channel>, Error> {
        let mut channels: Vec<Channel> = self.get(&format!("/guilds/{guild}/channels")).await?;
        if channels.len() > 4096 {
            return Err(Error::Capacity);
        }
        for channel in &mut channels {
            channel.guild_id = Some(guild);
        }
        Ok(channels)
    }
    pub async fn ring(&self, channel: Id, recipients: Option<&[Id]>) -> Result<(), Error> {
        if recipients.is_some_and(|r| r.len() > 64) {
            return Err(Error::Capacity);
        }
        self.request(
            Method::POST,
            &format!("/channels/{channel}/call/ring"),
            Some(json!({"recipients":recipients})),
        )
        .await
        .map(|_| ())
    }
    pub async fn stop_ringing(&self, channel: Id, recipients: Option<&[Id]>) -> Result<(), Error> {
        if recipients.is_some_and(|r| r.len() > 64) {
            return Err(Error::Capacity);
        }
        let body = recipients.map_or(json!({}), |r| json!({"recipients":r}));
        self.request(
            Method::POST,
            &format!("/channels/{channel}/call/stop-ringing"),
            Some(body),
        )
        .await
        .map(|_| ())
    }
    async fn get<T: DeserializeOwned>(&self, route: &str) -> Result<T, Error> {
        let bytes = self.request(Method::GET, route, None).await?;
        serde_json::from_slice(&bytes).map_err(|_| Error::Protocol)
    }
    async fn request(
        &self,
        method: Method,
        route: &str,
        body: Option<Value>,
    ) -> Result<Vec<u8>, Error> {
        let _permit = self.permits.acquire().await.map_err(|_| Error::Closed)?;
        let deadline = *self.cooldown.lock().await;
        if deadline > Instant::now() {
            return Err(Error::RateLimited {
                retry_after: deadline.saturating_duration_since(Instant::now()),
            });
        }
        let write = method != Method::GET;
        let mut auth = reqwest::header::HeaderValue::from_str(self.secret.expose())
            .map_err(|_| Error::InvalidCredential)?;
        auth.set_sensitive(true);
        let mut request = self
            .client
            .request(method, format!("{}{route}", self.base))
            .header(reqwest::header::AUTHORIZATION, auth);
        if let Some(body) = body {
            request = request.json(&body);
        }
        let response = request.send().await.map_err(|_| {
            if write {
                Error::Ambiguous
            } else {
                Error::Network
            }
        })?;
        let status = response.status();
        if response
            .content_length()
            .is_some_and(|n| n > MAX_BODY as u64)
        {
            return Err(Error::Capacity);
        }
        let remaining = response
            .headers()
            .get("x-ratelimit-remaining")
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned);
        let reset = response
            .headers()
            .get("x-ratelimit-reset-after")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<f64>().ok());
        let mut bytes = Vec::new();
        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|_| {
                if write {
                    Error::Ambiguous
                } else {
                    Error::Network
                }
            })?;
            if bytes.len() + chunk.len() > MAX_BODY {
                return Err(Error::Capacity);
            }
            bytes.extend_from_slice(&chunk);
        }
        if remaining.as_deref() == Some("0")
            && let Some(seconds) = reset.filter(|n| n.is_finite() && *n >= 0.0)
        {
            let mut deadline = self.cooldown.lock().await;
            *deadline =
                (*deadline).max(Instant::now() + Duration::from_secs_f64(seconds.min(3600.0)));
        }
        match status.as_u16() {
            200..=299 => Ok(bytes),
            401 => Err(Error::Unauthorized),
            403 => Err(Error::Forbidden),
            429 => {
                let data: Value = serde_json::from_slice(&bytes).map_err(|_| Error::Protocol)?;
                let seconds = data["retry_after"]
                    .as_f64()
                    .filter(|n| n.is_finite() && *n >= 0.0)
                    .ok_or(Error::Protocol)?;
                let retry_after = Duration::from_secs_f64(seconds.clamp(0.05, 3600.0));
                let mut deadline = self.cooldown.lock().await;
                *deadline = (*deadline).max(Instant::now() + retry_after);
                Err(Error::RateLimited { retry_after })
            }
            500..=599 if write => Err(Error::Ambiguous),
            _ => Err(Error::Protocol),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    async fn server(response: String) -> (String, tokio::task::JoinHandle<String>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut bytes = Vec::new();
            loop {
                let mut buf = [0u8; 1024];
                let n = stream.read(&mut buf).await.unwrap();
                assert!(n > 0);
                bytes.extend_from_slice(&buf[..n]);
                assert!(bytes.len() < 8192);
                if let Some(end) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
                    let headers = String::from_utf8_lossy(&bytes[..end]);
                    let len = headers
                        .lines()
                        .find_map(|line| {
                            line.to_ascii_lowercase()
                                .strip_prefix("content-length:")
                                .and_then(|n| n.trim().parse::<usize>().ok())
                        })
                        .unwrap_or(0);
                    if bytes.len() >= end + 4 + len {
                        break;
                    }
                }
            }
            stream.write_all(response.as_bytes()).await.unwrap();
            String::from_utf8(bytes).unwrap()
        });
        (base, task)
    }
    fn api(base: String) -> Api {
        let secret = Arc::new(Secret::new("synthetic-test-token".into()).unwrap());
        Api {
            base,
            ..Api::new(secret).unwrap()
        }
    }
    #[tokio::test]
    async fn ring_is_one_explicit_write_and_ambiguous_errors_are_not_retried() {
        let (base, task) = server(
            "HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                .into(),
        )
        .await;
        let error = api(base).ring(Id(3), Some(&[Id(4)])).await.unwrap_err();
        assert!(matches!(error, Error::Ambiguous));
        let request = task.await.unwrap();
        assert!(request.starts_with("POST /channels/3/call/ring HTTP/1.1"));
        assert!(request.ends_with("{\"recipients\":[\"4\"]}"));
    }
    #[tokio::test]
    async fn rate_limit_blocks_subsequent_requests_without_retrying() {
        let body = "{\"retry_after\":60}";
        let (base,task)=server(format!("HTTP/1.1 429 Too Many Requests\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len())).await;
        let api = api(base);
        assert!(matches!(
            api.current_user().await,
            Err(Error::RateLimited { .. })
        ));
        task.await.unwrap();
        // The listener is closed; a network request would fail differently.
        assert!(matches!(
            api.current_user().await,
            Err(Error::RateLimited { .. })
        ));
    }
    #[tokio::test]
    async fn rejected_redirects_and_oversized_bodies_do_not_escape_the_origin_or_budget() {
        let (base,task)=server("HTTP/1.1 302 Found\r\nLocation: https://example.invalid/\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".into()).await;
        assert!(matches!(
            api(base).current_user().await,
            Err(Error::Protocol)
        ));
        task.await.unwrap();
        let (base, task) = server(
            "HTTP/1.1 200 OK\r\nContent-Length: 999999999\r\nConnection: close\r\n\r\n".into(),
        )
        .await;
        assert!(matches!(
            api(base).current_user().await,
            Err(Error::Capacity)
        ));
        task.await.unwrap();
    }
}
