//! Network profiles: base URLs of the markets API and of the data host (order book and price
//! history files).

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

/// Which Lucyd deployment to read.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Network {
    /// Ethereum Sepolia testnet, test USDC.
    #[default]
    Testnet,
    /// Not launched yet: needs your own URLs.
    Mainnet,
}

impl Network {
    pub fn name(self) -> &'static str {
        match self {
            Network::Testnet => "testnet",
            Network::Mainnet => "mainnet",
        }
    }

    /// Built-in URLs of the profile.
    pub fn default_endpoints(self) -> Result<Endpoints> {
        match self {
            Network::Testnet => Ok(Endpoints {
                api_url: "https://api.testnet.lucyd.trade".into(),
                data_url: "https://data.testnet.lucyd.trade".into(),
            }),
            Network::Mainnet => Err(Error::NetworkNotLaunched("mainnet")),
        }
    }
}

impl std::str::FromStr for Network {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        match s.to_ascii_lowercase().as_str() {
            "testnet" | "sepolia" => Ok(Network::Testnet),
            "mainnet" => Ok(Network::Mainnet),
            _ => Err(Error::InvalidRequest(format!(
                "unknown network {s:?}: testnet or mainnet"
            ))),
        }
    }
}

/// Base URLs, without a trailing slash.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Endpoints {
    /// Markets REST API (`/markets`, `/trades`).
    pub api_url: String,
    /// Static files: `books/…` and `prices/…` Parquet.
    pub data_url: String,
}

/// Network plus optional overrides of its URLs.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct NetworkConfig {
    pub network: Network,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data_url: Option<String>,
}

impl NetworkConfig {
    /// URLs to use: the overrides, else the profile. A network without a profile works when both
    /// URLs are given.
    pub fn endpoints(&self) -> Result<Endpoints> {
        let trim = |s: &String| s.trim_end_matches('/').to_string();
        match (&self.api_url, &self.data_url) {
            (Some(api), Some(data)) => Ok(Endpoints {
                api_url: trim(api),
                data_url: trim(data),
            }),
            (api, data) => {
                let base = self.network.default_endpoints()?;
                Ok(Endpoints {
                    api_url: api.as_ref().map(trim).unwrap_or(base.api_url),
                    data_url: data.as_ref().map(trim).unwrap_or(base.data_url),
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mainnet_needs_urls() {
        let cfg = NetworkConfig {
            network: Network::Mainnet,
            ..Default::default()
        };
        assert!(matches!(cfg.endpoints(), Err(Error::NetworkNotLaunched(_))));
        let cfg = NetworkConfig {
            network: Network::Mainnet,
            api_url: Some("https://api.example/".into()),
            data_url: Some("https://data.example".into()),
        };
        assert_eq!(cfg.endpoints().unwrap().api_url, "https://api.example");
    }

    #[test]
    fn testnet_override_one_url() {
        let cfg = NetworkConfig {
            api_url: Some("http://127.0.0.1:3000".into()),
            ..Default::default()
        };
        let e = cfg.endpoints().unwrap();
        assert_eq!(e.api_url, "http://127.0.0.1:3000");
        assert_eq!(e.data_url, "https://data.testnet.lucyd.trade");
    }
}
