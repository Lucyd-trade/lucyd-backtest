//! Local file cache. Every write goes to a temporary file in the same folder and is renamed into
//! place, so an interrupted download never leaves a broken file behind.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::error::Result;
use crate::market::path_segment;
use crate::trades::Trade;

/// Default cache folder: `<system cache dir>/lucyd-backtest`.
pub fn default_cache_dir() -> PathBuf {
    dirs::cache_dir()
        .unwrap_or_else(std::env::temp_dir)
        .join("lucyd-backtest")
}

#[derive(Debug, Clone)]
pub struct Cache {
    root: PathBuf,
}

/// Cached trades of one market.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct TradesFile {
    /// The market is over and every trade is here.
    pub complete: bool,
    /// Unix seconds: every trade before this is here.
    pub fetched_to: i64,
    pub trades: Vec<Trade>,
}

impl Cache {
    /// Cache of one data source under `dir`: `profile` is the network name, or the data URL when
    /// URLs are overridden, so different sources never mix.
    pub fn new(dir: &Path, profile: &str) -> Self {
        Cache {
            root: dir.join(path_segment(profile)),
        }
    }

    pub(crate) fn book_path(&self, book_path: &str) -> PathBuf {
        self.root.join("books").join(book_path)
    }

    pub(crate) fn price_path(&self, feed: &str, day: &str) -> PathBuf {
        self.root
            .join("prices")
            .join("pyth")
            .join(feed)
            .join(format!("{day}.parquet"))
    }

    pub(crate) fn trades_path(&self, market: &str) -> PathBuf {
        self.root.join("trades").join(format!("{market}.json"))
    }

    /// File contents, None if absent.
    pub(crate) fn read(&self, path: &Path) -> Result<Option<Bytes>> {
        match std::fs::read(path) {
            Ok(b) => Ok(Some(Bytes::from(b))),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Marker: the source has no such file and never will.
    pub(crate) fn is_missing(&self, path: &Path) -> bool {
        missing_marker(path).exists()
    }

    pub(crate) fn mark_missing(&self, path: &Path) -> Result<()> {
        write_atomic(&missing_marker(path), b"")
    }

    pub(crate) fn write(&self, path: &Path, data: &[u8]) -> Result<()> {
        write_atomic(path, data)
    }

    pub(crate) fn read_trades(&self, market: &str) -> Result<Option<TradesFile>> {
        let Some(b) = self.read(&self.trades_path(market))? else {
            return Ok(None);
        };
        // A file from another version that does not parse is simply fetched again.
        Ok(serde_json::from_slice(&b).ok())
    }

    pub(crate) fn write_trades(&self, market: &str, f: &TradesFile) -> Result<()> {
        write_atomic(&self.trades_path(market), &serde_json::to_vec(f)?)
    }
}

fn missing_marker(path: &Path) -> PathBuf {
    let mut p = path.as_os_str().to_owned();
    p.push(".missing");
    PathBuf::from(p)
}

/// Writes `data` to `path` through a temporary file and a rename.
pub(crate) fn write_atomic(path: &Path, data: &[u8]) -> Result<()> {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let dir = path.parent().unwrap_or(Path::new("."));
    std::fs::create_dir_all(dir)?;
    let name = path
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default();
    let tmp = dir.join(format!(
        ".{name}.{}.{}.tmp",
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::Relaxed)
    ));
    let res = std::fs::write(&tmp, data).and_then(|_| std::fs::rename(&tmp, path));
    if res.is_err() {
        let _ = std::fs::remove_file(&tmp);
    }
    Ok(res?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn atomic_write_replaces_and_leaves_no_temp() {
        let dir = tempfile::tempdir().unwrap();
        let c = Cache::new(dir.path(), "testnet");
        let p = c.book_path("updown/5m/btc/2026-10-27/0xab.parquet");
        c.write(&p, b"one").unwrap();
        c.write(&p, b"two").unwrap();
        assert_eq!(c.read(&p).unwrap().unwrap().as_ref(), b"two");
        let names: Vec<_> = std::fs::read_dir(p.parent().unwrap())
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .collect();
        assert_eq!(names.len(), 1, "{names:?}");
        assert!(!c.is_missing(&p));
        c.mark_missing(&p).unwrap();
        assert!(c.is_missing(&p));
    }

    #[test]
    fn trades_roundtrip_and_bad_file() {
        let dir = tempfile::tempdir().unwrap();
        let c = Cache::new(dir.path(), "https://data.example");
        assert!(c.read_trades("0xab").unwrap().is_none());
        c.write_trades(
            "0xab",
            &TradesFile {
                complete: true,
                fetched_to: 5,
                trades: vec![],
            },
        )
        .unwrap();
        assert!(c.read_trades("0xab").unwrap().unwrap().complete);
        std::fs::write(c.trades_path("0xab"), b"{broken").unwrap();
        assert!(c.read_trades("0xab").unwrap().is_none());
    }
}
