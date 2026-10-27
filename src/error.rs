use thiserror::Error;

/// Everything that can go wrong while building a request or loading data.
#[derive(Debug, Error)]
pub enum Error {
    /// The request is incomplete or contradictory.
    #[error("invalid request: {0}")]
    InvalidRequest(String),
    /// The selected network has no public data yet.
    #[error(
        "{0} is not launched yet: no public data. Use testnet, or pass your own api_url and data_url"
    )]
    NetworkNotLaunched(&'static str),
    /// The API or the data host answered with an error that retries did not fix.
    #[error("HTTP {status} from {url}: {body}")]
    Http {
        /// HTTP status.
        status: u16,
        /// Requested URL.
        url: String,
        /// Response body (start of it).
        body: String,
    },
    /// Network failure after all retries.
    #[error("request to {url} failed: {source}")]
    Transport {
        /// Requested URL.
        url: String,
        #[source]
        /// The transport error.
        source: reqwest::Error,
    },
    /// A response or a file did not have the expected shape.
    #[error("bad data in {what}: {detail}")]
    BadData {
        /// Which file or response.
        what: String,
        /// What was wrong.
        detail: String,
    },
    #[error("cache: {0}")]
    /// Local file error.
    Io(#[from] std::io::Error),
    #[error("parquet: {0}")]
    /// Parquet decoding.
    Parquet(#[from] parquet::errors::ParquetError),
    #[error("arrow: {0}")]
    /// Arrow decoding.
    Arrow(#[from] arrow_schema::ArrowError),
    #[error("json: {0}")]
    /// JSON decoding.
    Json(#[from] serde_json::Error),
}

/// Result with the [`Error`](enum@Error) of this crate.
pub type Result<T, E = Error> = std::result::Result<T, E>;

pub(crate) fn bad(what: impl Into<String>, detail: impl Into<String>) -> Error {
    Error::BadData {
        what: what.into(),
        detail: detail.into(),
    }
}
