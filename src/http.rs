//! HTTP with a request rate cap and retries of 429 / 5xx / network errors.

use std::time::Duration;

use bytes::Bytes;
use reqwest::{Client, StatusCode};
use serde::de::DeserializeOwned;
use tokio::sync::Mutex;
use tokio::time::{Instant, sleep, sleep_until};

use crate::error::{Error, Result};

/// The API allows 20 requests per second per IP; stay well below.
const API_RATE: u32 = 8;
const DATA_RATE: u32 = 40;
const ATTEMPTS: u32 = 7;
const MAX_BACKOFF: Duration = Duration::from_secs(30);

/// Spaces requests evenly: at most `rate` starts per second.
struct Pacer {
    gap: Duration,
    next: Mutex<Instant>,
}

impl Pacer {
    fn new(rate: u32) -> Self {
        Pacer {
            gap: Duration::from_secs(1) / rate,
            next: Mutex::new(Instant::now()),
        }
    }

    async fn wait(&self) {
        let at = {
            let mut next = self.next.lock().await;
            let at = (*next).max(Instant::now());
            *next = at + self.gap;
            at
        };
        sleep_until(at).await;
    }
}

pub(crate) struct Http {
    client: Client,
    api: Pacer,
    data: Pacer,
}

enum Host {
    Api,
    Data,
}

impl Http {
    pub(crate) fn new() -> Result<Self> {
        let client = Client::builder()
            .user_agent(concat!("lucyd-backtest/", env!("CARGO_PKG_VERSION")))
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(120))
            .build()
            .map_err(|e| Error::Transport {
                url: String::new(),
                source: e,
            })?;
        Ok(Http {
            client,
            api: Pacer::new(API_RATE),
            data: Pacer::new(DATA_RATE),
        })
    }

    /// GET JSON from the API.
    pub(crate) async fn api_json<T: DeserializeOwned>(
        &self,
        url: &str,
        query: &[(&str, String)],
    ) -> Result<T> {
        let body = self
            .get(Host::Api, url, query)
            .await?
            .ok_or_else(|| Error::Http {
                status: 404,
                url: url.to_string(),
                body: String::new(),
            })?;
        serde_json::from_slice(&body).map_err(|e| crate::error::bad(url, e.to_string()))
    }

    /// GET a file from the data host; None on 404.
    pub(crate) async fn data_file(&self, url: &str) -> Result<Option<Bytes>> {
        self.get(Host::Data, url, &[]).await
    }

    async fn get(&self, host: Host, url: &str, query: &[(&str, String)]) -> Result<Option<Bytes>> {
        let pacer = match host {
            Host::Api => &self.api,
            Host::Data => &self.data,
        };
        let mut attempt = 0;
        loop {
            attempt += 1;
            pacer.wait().await;
            let res = self.client.get(url).query(query).send().await;
            let backoff = backoff(attempt);
            match res {
                Ok(r) if r.status().is_success() => match r.bytes().await {
                    Ok(b) => return Ok(Some(b)),
                    Err(_) if attempt < ATTEMPTS => sleep(backoff).await,
                    Err(e) => {
                        return Err(Error::Transport {
                            url: url.to_string(),
                            source: e,
                        });
                    }
                },
                Ok(r) if r.status() == StatusCode::NOT_FOUND => return Ok(None),
                Ok(r) if retryable(r.status()) && attempt < ATTEMPTS => {
                    let wait = retry_after(&r).unwrap_or(backoff).min(MAX_BACKOFF);
                    sleep(wait).await;
                }
                Ok(r) => {
                    let status = r.status().as_u16();
                    let body = r.text().await.unwrap_or_default();
                    return Err(Error::Http {
                        status,
                        url: url.to_string(),
                        body: body.chars().take(300).collect(),
                    });
                }
                Err(_) if attempt < ATTEMPTS => sleep(backoff).await,
                Err(e) => {
                    return Err(Error::Transport {
                        url: url.to_string(),
                        source: e,
                    });
                }
            }
        }
    }
}

fn retryable(s: StatusCode) -> bool {
    s == StatusCode::TOO_MANY_REQUESTS || s.is_server_error() || s == StatusCode::REQUEST_TIMEOUT
}

/// 0.5 s, 1 s, 2 s … capped at 30 s.
fn backoff(attempt: u32) -> Duration {
    (Duration::from_millis(500) * 2u32.saturating_pow(attempt.saturating_sub(1))).min(MAX_BACKOFF)
}

fn retry_after(r: &reqwest::Response) -> Option<Duration> {
    let secs: u64 = r
        .headers()
        .get("retry-after")?
        .to_str()
        .ok()?
        .trim()
        .parse()
        .ok()?;
    Some(Duration::from_secs(secs.max(1)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_grows_and_caps() {
        assert_eq!(backoff(1), Duration::from_millis(500));
        assert_eq!(backoff(3), Duration::from_secs(2));
        assert_eq!(backoff(20), MAX_BACKOFF);
    }

    #[tokio::test(start_paused = true)]
    async fn pacer_spaces_requests() {
        let p = Pacer::new(10);
        let t0 = Instant::now();
        for _ in 0..5 {
            p.wait().await;
        }
        assert!(Instant::now() - t0 >= Duration::from_millis(400));
    }
}
