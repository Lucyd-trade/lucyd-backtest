//! Data loader for backtests on [Lucyd](https://testnet.lucyd.trade) prediction markets.
//!
//! Build a [`Request`], hand it to a [`Loader`] and get a [`Dataset`]: the market catalog, the
//! order book history, CLOB fills, on-chain trades and Pyth prices of every selected market, one
//! time-ordered [`Event`] stream, and the full book at any moment.
//!
//! ```no_run
//! use lucyd_backtest::{DataKinds, Loader, Period, Request, parse_time};
//!
//! # async fn run() -> lucyd_backtest::Result<()> {
//! let req = Request::builder()
//!     .instrument("BTC")
//!     .periods([Period::M15, Period::H1])
//!     .range(parse_time("2026-10-20")?, parse_time("2026-10-21")?)
//!     .data(DataKinds::DEFAULT) // book, fills, prices
//!     .build()?;
//! let data = Loader::new()?.load(&req).await?;
//! for e in data.events() {
//!     println!("{} {} {:?}", e.ts_ms, e.kind.name(), e.market);
//! }
//! # Ok(()) }
//! ```

mod book;
mod cache;
mod error;
mod event;
pub mod export;
mod fills;
mod http;
mod loader;
mod market;
mod network;
mod pq;
mod prices;
mod request;
mod trades;

pub use chrono;

pub use book::{
    BookHistory, BookSide, BookUpdate, Level, OrderBook, OutcomeBook, TICK, price_ticks,
};
pub use cache::default_cache_dir;
pub use error::{Error, Result};
pub use event::{Event, EventKind};
pub use fills::{Execution, Fill, Settlement, sort_fills};
pub use loader::{Catalog, Dataset, Loader, MarketData};
pub use market::{MarketInfo, MarketState, PRICE_SCALE, outcome_id};
pub use network::{Endpoints, Network};
pub use prices::{FIRST_PRICE_DAY, PriceTick};
pub use request::{DataKinds, MarketType, Period, Request, RequestBuilder, parse_time};
pub use trades::{Side, TOKEN_SCALE, Trade};
