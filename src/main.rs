use std::path::PathBuf;
use std::process::ExitCode;

#[path = "../examples/strategies/ladder_mm.rs"]
mod ladder_mm;
#[path = "../examples/strategies/late_side.rs"]
mod late_side;
mod scaffold;

use clap::{Args, Parser, Subcommand};
use lucyd_backtest::engine::{ExecParams, QueueModel, Report, Strategy, write_journal};
use lucyd_backtest::export::{Format, export};
use lucyd_backtest::{
    DataKinds, Error, Loader, MarketInfo, Request, default_cache_dir, parse_time,
};

/// Lucyd backtests: select markets, download their order book, CLOB fills, on-chain trades and
/// prices, export the event stream, run a strategy, view its report.
#[derive(Parser)]
#[command(version, about)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
#[allow(clippy::large_enum_variant)] // parsed once
enum Cmd {
    /// Show the markets a request selects.
    List {
        #[command(flatten)]
        req: RequestArgs,
        /// Print JSON instead of a table.
        #[arg(long)]
        json: bool,
    },
    /// Download the selected data into the cache.
    Fetch {
        #[command(flatten)]
        req: RequestArgs,
    },
    /// Write the event stream to a Parquet or CSV file.
    Export {
        #[command(flatten)]
        req: RequestArgs,
        /// Output file; the format follows the extension (.parquet or .csv).
        #[arg(long, short)]
        out: PathBuf,
        /// csv or parquet, if the extension says neither.
        #[arg(long)]
        format: Option<String>,
    },
    /// Run an example strategy over the selected markets and print its report.
    Run {
        #[command(flatten)]
        req: RequestArgs,
        /// late-side or ladder-mm (examples/strategies).
        #[arg(long)]
        strategy: String,
        /// Strategy settings as JSON, e.g. '{"seconds_before": 60}'.
        #[arg(long)]
        config: Option<String>,
        #[command(flatten)]
        exec: ExecArgs,
        /// Write the full report as JSON.
        #[arg(long)]
        report: Option<PathBuf>,
        /// Also write the report as HTML next to the JSON (same name, .html).
        #[arg(long, requires = "report")]
        html: bool,
        /// Write the own-order journal (.csv or .parquet).
        #[arg(long)]
        journal: Option<PathBuf>,
    },
    /// Turn a JSON report into one self-contained HTML page (works offline).
    Html {
        /// Report written by `run --report` or `Report::write_json`.
        report: PathBuf,
        /// Output file; default: the report's name with .html.
        #[arg(long, short)]
        out: Option<PathBuf>,
    },
    /// Create a Cargo project with a strategy template that runs a backtest and writes reports.
    New {
        /// Folder to create; its name is the package name.
        path: PathBuf,
        /// Use a local checkout of lucyd-backtest instead of the git dependency.
        #[arg(long)]
        engine_path: Option<PathBuf>,
    },
}

#[derive(Args)]
struct ExecArgs {
    /// Execution parameters as a JSON file (engine `ExecParams`). Flags below override it.
    #[arg(long)]
    params: Option<PathBuf>,
    /// Starting USDC.
    #[arg(long)]
    initial_usdc: Option<f64>,
    /// Order latency, ms.
    #[arg(long)]
    latency_ms: Option<u64>,
    /// Cancel latency, ms.
    #[arg(long)]
    cancel_latency_ms: Option<u64>,
    /// Random extra latency up to this, ms.
    #[arg(long)]
    jitter_ms: Option<u64>,
    /// pessimistic or proportional.
    #[arg(long)]
    queue_model: Option<String>,
    /// Trading fee, basis points.
    #[arg(long)]
    fee_bps: Option<u32>,
    /// Chance that an own fill's settlement fails.
    #[arg(long)]
    revert_probability: Option<f64>,
    /// Last share of the range for validation, 0 = none.
    #[arg(long)]
    validation_fraction: Option<f64>,
    /// Seed of every random choice (jitter, reverts).
    #[arg(long)]
    seed: Option<u64>,
    /// Step of the returns behind Sharpe and Sortino, ms (default 3600000, one hour).
    #[arg(long)]
    risk_interval_ms: Option<u64>,
    /// Annualize Sharpe and Sortino (× √(intervals in 365 days)).
    #[arg(long)]
    annualize: bool,
}

impl ExecArgs {
    fn build(&self) -> lucyd_backtest::Result<ExecParams> {
        let mut p = match &self.params {
            Some(path) => serde_json::from_str(&std::fs::read_to_string(path)?)
                .map_err(|e| Error::InvalidRequest(format!("{}: {e}", path.display())))?,
            None => ExecParams::default(),
        };
        if let Some(v) = self.initial_usdc {
            p.initial_usdc = v;
        }
        if let Some(v) = self.latency_ms {
            p.submit_latency_ms = v;
        }
        if let Some(v) = self.cancel_latency_ms {
            p.cancel_latency_ms = v;
        }
        if let Some(v) = self.jitter_ms {
            p.latency_jitter_ms = v;
        }
        if let Some(v) = &self.queue_model {
            p.queue_model = match v.as_str() {
                "pessimistic" => QueueModel::Pessimistic,
                "proportional" => QueueModel::Proportional,
                _ => {
                    return Err(Error::InvalidRequest(format!(
                        "queue model {v:?}: pessimistic or proportional"
                    )));
                }
            };
        }
        if let Some(v) = self.fee_bps {
            p.fee_bps = v;
        }
        if let Some(v) = self.revert_probability {
            p.revert_probability = v;
        }
        if let Some(v) = self.validation_fraction {
            p.validation_fraction = v;
        }
        if let Some(v) = self.seed {
            p.seed = v;
        }
        if let Some(v) = self.risk_interval_ms {
            p.risk_interval_ms = v;
        }
        if self.annualize {
            p.annualize = true;
        }
        Ok(p)
    }
}

fn strategy(name: &str, config: Option<&str>) -> lucyd_backtest::Result<Box<dyn Strategy>> {
    fn cfg<T: serde::de::DeserializeOwned + Default>(c: Option<&str>) -> lucyd_backtest::Result<T> {
        c.map_or(Ok(T::default()), |c| {
            serde_json::from_str(c).map_err(|e| Error::InvalidRequest(format!("--config: {e}")))
        })
    }
    Ok(match name {
        "late-side" | "late_side" => Box::new(late_side::LateSide::new(cfg(config)?)),
        "ladder-mm" | "ladder_mm" => Box::new(ladder_mm::LadderMm::new(cfg(config)?)),
        _ => {
            return Err(Error::InvalidRequest(format!(
                "strategy {name:?}: late-side or ladder-mm"
            )));
        }
    })
}

fn print_report(r: &Report) {
    print!("{}", r.summary_text());
    warn(&r.warnings);
}

#[derive(Args)]
struct RequestArgs {
    /// Request as JSON (the library's Request format). Flags below override its fields.
    #[arg(long)]
    request: Option<PathBuf>,
    /// testnet or mainnet.
    #[arg(long)]
    network: Option<String>,
    /// Own markets API URL instead of the network's.
    #[arg(long)]
    api_url: Option<String>,
    /// Own data host URL instead of the network's.
    #[arg(long)]
    data_url: Option<String>,
    /// Market type: UpDown.
    #[arg(long)]
    market_type: Option<String>,
    /// Underlying asset, e.g. BTC.
    #[arg(long)]
    instrument: Option<String>,
    /// Series: 5m, 15m, 30m, 1h, 4h, 1d; comma-separated or repeated.
    #[arg(long, value_delimiter = ',')]
    period: Vec<String>,
    /// Market address instead of filters; comma-separated or repeated.
    #[arg(long, value_delimiter = ',')]
    market: Vec<String>,
    /// Range start, UTC: 2026-10-27, 2026-10-27T12:00:00Z or unix seconds.
    #[arg(long)]
    from: Option<String>,
    /// Range end, UTC, exclusive.
    #[arg(long)]
    to: Option<String>,
    /// book, fills, trades, prices in any combination, or all. Default: book,fills,prices
    /// (on-chain trades only when asked for).
    #[arg(long)]
    data: Option<String>,
    /// Only markets that have a CLOB fills file.
    #[arg(long)]
    fills_only: bool,
    /// Cache folder.
    #[arg(long, env = "LUCYD_BACKTEST_CACHE")]
    cache_dir: Option<PathBuf>,
}

impl RequestArgs {
    fn build(&self, default_data: DataKinds) -> lucyd_backtest::Result<Request> {
        let mut r = match &self.request {
            Some(p) => {
                let text = std::fs::read_to_string(p)?;
                serde_json::from_str::<Request>(&text).map_err(|e| {
                    lucyd_backtest::Error::InvalidRequest(format!("{}: {e}", p.display()))
                })?
            }
            None => Request {
                data: default_data,
                ..Request::default()
            },
        };
        if let Some(n) = &self.network {
            r.network = n.parse()?;
        }
        if self.api_url.is_some() {
            r.api_url = self.api_url.clone();
        }
        if self.data_url.is_some() {
            r.data_url = self.data_url.clone();
        }
        if let Some(t) = &self.market_type {
            r.market_type = t.parse()?;
        }
        if self.instrument.is_some() {
            r.instrument = self.instrument.clone();
        }
        if !self.period.is_empty() {
            r.periods = self
                .period
                .iter()
                .map(|p| p.parse())
                .collect::<Result<_, _>>()?;
        }
        if !self.market.is_empty() {
            r.markets = self.market.clone();
        }
        if let Some(f) = &self.from {
            r.from = Some(parse_time(f)?);
        }
        if let Some(t) = &self.to {
            r.to = Some(parse_time(t)?);
        }
        if let Some(d) = &self.data {
            r.data = d.parse()?;
        }
        if self.fills_only {
            r.fills_only = true;
        }
        r.validate()
    }

    fn loader(&self) -> lucyd_backtest::Result<Loader> {
        Loader::with_cache_dir(self.cache_dir.clone().unwrap_or_else(default_cache_dir))
    }
}

fn price(v: Option<f64>) -> String {
    v.map(|v| format!("{v:.2}")).unwrap_or_else(|| "-".into())
}

fn print_table(markets: &[MarketInfo]) {
    println!(
        "{:<66}  {:<6} {:<5} {:<17} {:<17} {:<8} {:>12} {:>12}  {:<6} fills",
        "market", "instr", "per", "start (UTC)", "end (UTC)", "state", "strike", "final", "winner"
    );
    for m in markets {
        let winner = if m.voided {
            "void".to_string()
        } else {
            m.winner
                .and_then(|w| m.outcomes.get(w as usize).cloned())
                .unwrap_or_else(|| "-".into())
        };
        let fills = match m.has_fills {
            Some(true) => "yes",
            Some(false) => "no",
            None => "-",
        };
        println!(
            "{:<66}  {:<6} {:<5} {:<17} {:<17} {:<8} {:>12} {:>12}  {:<6} {}",
            m.address,
            m.instrument,
            m.period.as_str(),
            m.start.format("%Y-%m-%d %H:%M"),
            m.end.format("%Y-%m-%d %H:%M"),
            format!("{:?}", m.state).to_lowercase(),
            price(m.strike),
            price(m.resolved_price),
            winner,
            fills
        );
    }
}

fn warn(warnings: &[String]) {
    for w in warnings {
        eprintln!("warning: {w}");
    }
}

async fn run(cli: Cli) -> lucyd_backtest::Result<()> {
    match cli.cmd {
        Cmd::List { req, json } => {
            let r = req.build(DataKinds::DEFAULT)?;
            let cat = req.loader()?.catalog(&r).await?;
            if json {
                println!("{}", serde_json::to_string_pretty(&cat)?);
            } else {
                print_table(&cat.markets);
                let mut total = format!("{} market(s)", cat.markets.len());
                if r.data.fills || r.fills_only {
                    let with = cat
                        .markets
                        .iter()
                        .filter(|m| m.has_fills == Some(true))
                        .count();
                    total.push_str(&format!(", {with} with fills"));
                }
                if r.fills_only {
                    total.push_str(&format!(
                        ", {} without fills left out",
                        cat.without_fills_excluded
                    ));
                }
                println!("{total}");
                warn(&cat.warnings);
            }
        }
        Cmd::Fetch { req } => {
            let r = req.build(DataKinds::DEFAULT)?;
            let loader = req.loader()?;
            let ds = loader.load(&r).await?;
            let books = ds.markets.iter().filter(|m| m.book.is_some()).count();
            let fills: usize = ds
                .markets
                .iter()
                .filter_map(|m| m.fills.as_ref())
                .map(Vec::len)
                .sum();
            let trades: usize = ds
                .markets
                .iter()
                .filter_map(|m| m.trades.as_ref())
                .map(Vec::len)
                .sum();
            let ticks: usize = ds.prices.values().map(Vec::len).sum();
            println!(
                "{} market(s), {books} book file(s), {fills} fill(s), {trades} trade(s), {ticks} price tick(s); cache {}",
                ds.markets.len(),
                loader.cache_dir().display()
            );
            warn(&ds.warnings);
        }
        Cmd::Export { req, out, format } => {
            let fmt = match format.as_deref().map(str::to_ascii_lowercase).as_deref() {
                Some("csv") => Format::Csv,
                Some("parquet") => Format::Parquet,
                Some(f) => {
                    return Err(lucyd_backtest::Error::InvalidRequest(format!(
                        "format {f:?}: csv or parquet"
                    )));
                }
                None => Format::from_path(&out).ok_or_else(|| {
                    lucyd_backtest::Error::InvalidRequest(
                        "output must end in .csv or .parquet, or pass --format".into(),
                    )
                })?,
            };
            let r = req.build(DataKinds::DEFAULT)?;
            let ds = req.loader()?.load(&r).await?;
            let events = ds.events();
            let rows = export(&events, &out, fmt)?;
            println!(
                "{} event(s), {rows} row(s) → {}",
                events.len(),
                out.display()
            );
            warn(&ds.warnings);
        }
        Cmd::Run {
            req,
            strategy: name,
            config,
            exec,
            report,
            html,
            journal,
        } => {
            let params = exec.build()?;
            let mut s = strategy(&name, config.as_deref())?;
            let r = req.build(DataKinds::DEFAULT)?;
            let ds = req.loader()?.load(&r).await?;
            let rep = lucyd_backtest::engine::run(&ds, &params, s.as_mut())?;
            print_report(&rep);
            if let Some(path) = report {
                rep.write_json(&path)?;
                println!("report -> {}", path.display());
                if html {
                    let h = path.with_extension("html");
                    rep.write_html(&h)?;
                    println!("html -> {}", h.display());
                }
            }
            if let Some(path) = journal {
                let fmt = Format::from_path(&path).ok_or_else(|| {
                    Error::InvalidRequest("journal must end in .csv or .parquet".into())
                })?;
                let n = write_journal(&rep.journal, &path, fmt)?;
                println!("{n} journal row(s) -> {}", path.display());
            }
        }
        Cmd::Html { report, out } => {
            let rep = Report::read_json(&report)?;
            let out = out.unwrap_or_else(|| report.with_extension("html"));
            rep.write_html(&out)?;
            println!("html -> {}", out.display());
        }
        Cmd::New { path, engine_path } => {
            let dir = scaffold::create(&path, engine_path.as_deref())?;
            println!(
                "created {0}
  cd {0}
  cargo run --release
then open reports/report.html",
                dir.display()
            );
        }
    }
    Ok(())
}

#[tokio::main]
async fn main() -> ExitCode {
    match run(Cli::parse()).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
    }
}
