//! Idle-connection survival probe: does a pooled connection outlive an idle gap under HTTP/2 pings?
//! Per URL, one request, then a request after each idle gap, each marked reused or new, with latency
//! next to a cold request on a fresh client. New connections are counted by a DNS resolver: hyper
//! resolves once per connection it dials and never for a pooled checkout (IP-literal hosts bypass it).
//!
//! Run: `h2_idle_probe <url>...` or `PROBE_URLS=<url>,<url> h2_idle_probe`; gaps from
//! `PROBE_GAPS_SECS` (default `60,300,600,1200,1800`). Output names hosts only, never full URLs.

use fast_wallet::{endpoint_host, redact_urls};
use futures_util::future::join_all;
use reqwest::dns::{Addrs, Name, Resolve, Resolving};
use reqwest::{Client, StatusCode, Version};
use serde_json::json;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

const DEFAULT_GAPS_SECS: &str = "60,300,600,1200,1800";

/// Counts resolutions, i.e. connections dialled by the client that owns it.
#[derive(Default)]
struct CountingResolver {
    dials: AtomicU64,
}

impl Resolve for CountingResolver {
    fn resolve(&self, name: Name) -> Resolving {
        self.dials.fetch_add(1, Ordering::SeqCst);
        let host = name.as_str().to_string();
        Box::pin(async move {
            let addrs: Vec<SocketAddr> =
                tokio::net::lookup_host((host.as_str(), 0)).await?.collect();
            let addrs: Addrs = Box::new(addrs.into_iter());
            Ok(addrs)
        })
    }
}

/// `RpcClient::new`'s reqwest settings (src/rpc.rs) with `pool_idle_timeout(None)`.
fn probe_client(resolver: Arc<CountingResolver>) -> Client {
    Client::builder()
        .pool_max_idle_per_host(10)
        .pool_idle_timeout(None)
        .timeout(Duration::from_secs(30))
        .tcp_nodelay(true)
        .tcp_keepalive(Duration::from_secs(15))
        .http2_keep_alive_interval(Duration::from_secs(10))
        .http2_keep_alive_timeout(Duration::from_secs(20))
        .http2_keep_alive_while_idle(true)
        .dns_resolver(resolver)
        .build()
        .expect("reqwest client builds")
}

/// One `eth_chainId` round trip on one client; `Err` only when no HTTP reply came back.
struct Sample {
    dials: u64,
    ms: f64,
    outcome: Result<Reply, String>,
}

/// Any HTTP reply proves the connection; `note` is empty for a JSON-RPC result.
struct Reply {
    version: Version,
    note: String,
}

async fn chain_id(client: &Client, url: &str) -> Result<Reply, String> {
    let body = json!({"jsonrpc": "2.0", "method": "eth_chainId", "params": [], "id": 1});
    let response = client
        .post(url)
        .json(&body)
        .send()
        .await
        .map_err(|e| redact_urls(&e.to_string()))?;
    let version = response.version();
    let status = response.status();
    let text = response
        .text()
        .await
        .map_err(|e| redact_urls(&e.to_string()))?;
    let note = rpc_note(status, &text);
    Ok(Reply { version, note })
}

/// Empty for a JSON-RPC result, else the status and error message on one line.
fn rpc_note(status: StatusCode, text: &str) -> String {
    let parsed: Option<serde_json::Value> = serde_json::from_str(text).ok();
    if status.is_success() && parsed.as_ref().is_some_and(|v| v.get("result").is_some()) {
        return String::new();
    }
    let message = parsed
        .as_ref()
        .and_then(|v| v["error"]["message"].as_str().map(String::from))
        .unwrap_or_else(|| text.chars().take(80).collect());
    let note = format!("HTTP {}: {message}", status.as_u16());
    redact_urls(&note).replace(['\n', '\r', '|'], " ")
}

async fn measure(client: &Client, resolver: &CountingResolver, url: &str) -> Sample {
    let before = resolver.dials.load(Ordering::SeqCst);
    let started = Instant::now();
    let outcome = chain_id(client, url).await;
    Sample {
        dials: resolver.dials.load(Ordering::SeqCst) - before,
        ms: started.elapsed().as_secs_f64() * 1000.0,
        outcome,
    }
}

/// A request on a client built just for it: the price of a cold connection right now.
async fn cold_control(url: &str) -> Sample {
    let resolver = Arc::new(CountingResolver::default());
    let client = probe_client(resolver.clone());
    measure(&client, &resolver, url).await
}

struct Row {
    host: String,
    gap: Duration,
    warm: Sample,
    cold: Option<Sample>,
}

impl Row {
    fn connection(&self) -> &'static str {
        match (&self.warm.outcome, self.warm.dials) {
            (Err(_), _) => "error",
            (Ok(_), 0) => "reused",
            (Ok(_), _) => "new",
        }
    }

    fn line(&self) -> String {
        let (proto, note) = match &self.warm.outcome {
            Ok(r) => (format!("{:?}", r.version), r.note.clone()),
            Err(e) => ("-".to_string(), e.replace(['\n', '\r', '|'], " ")),
        };
        let cold_ms = self
            .cold
            .as_ref()
            .map_or("-".to_string(), |c| match &c.outcome {
                Ok(_) => format!("{:.1}", c.ms),
                Err(_) => "error".to_string(),
            });
        format!(
            "| {} | {} | {} ({} dial) | {proto} | {:.1} | {cold_ms} | {note} |",
            self.host,
            gap_label(self.gap),
            self.connection(),
            self.warm.dials,
            self.warm.ms,
        )
    }
}

fn gap_label(gap: Duration) -> String {
    match gap.as_secs() {
        0 => "first".to_string(),
        s if s % 60 == 0 => format!("{} min", s / 60),
        s => format!("{s} s"),
    }
}

async fn probe(url: String, gaps: Vec<Duration>, started: Instant) -> Vec<Row> {
    let host = endpoint_host(&url).to_string();
    let resolver = Arc::new(CountingResolver::default());
    let client = probe_client(resolver.clone());
    let first = Row {
        host: host.clone(),
        gap: Duration::ZERO,
        warm: measure(&client, &resolver, &url).await,
        cold: None,
    };
    println!("t={:>5}s {}", started.elapsed().as_secs(), first.line());
    let mut rows = vec![first];
    for gap in gaps {
        tokio::time::sleep(gap).await;
        let warm = measure(&client, &resolver, &url).await;
        let cold = Some(cold_control(&url).await);
        let row = Row {
            host: host.clone(),
            gap,
            warm,
            cold,
        };
        println!("t={:>5}s {}", started.elapsed().as_secs(), row.line());
        rows.push(row);
    }
    rows
}

fn urls() -> Vec<String> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let raw = if args.is_empty() {
        std::env::var("PROBE_URLS").unwrap_or_default()
    } else {
        args.join(",")
    };
    raw.split([',', ' ', '\n'])
        .map(str::trim)
        .filter(|u| !u.is_empty())
        .map(String::from)
        .collect()
}

fn gaps() -> Vec<Duration> {
    let raw = std::env::var("PROBE_GAPS_SECS").unwrap_or_else(|_| DEFAULT_GAPS_SECS.to_string());
    raw.split(',')
        .map(|s| {
            s.trim()
                .parse::<u64>()
                .expect("PROBE_GAPS_SECS: comma-separated seconds")
        })
        .map(Duration::from_secs)
        .collect()
}

fn print_summary(rows: &[Row]) {
    println!("\n| endpoint | idle gap | connection | protocol | warm ms | cold ms (fresh client) | note |");
    println!("|---|---|---|---|---|---|---|");
    for row in rows {
        println!("{}", row.line());
    }
    let mut hosts: Vec<&str> = rows.iter().map(|r| r.host.as_str()).collect();
    hosts.dedup();
    for host in hosts {
        let of_host = || {
            rows.iter()
                .filter(move |r| r.host == host && r.gap > Duration::ZERO)
        };
        let survived = of_host()
            .filter(|r| r.connection() == "reused")
            .map(|r| r.gap)
            .max();
        let dropped = of_host()
            .filter(|r| r.connection() != "reused")
            .map(|r| r.gap)
            .min();
        println!(
            "{host}: longest idle gap survived = {}, shortest gap not survived = {}",
            survived.map_or("none".to_string(), gap_label),
            dropped.map_or("none".to_string(), gap_label),
        );
    }
}

#[tokio::main]
async fn main() {
    fast_wallet::tls::ensure_provider();
    let urls = urls();
    if urls.is_empty() {
        eprintln!("usage: h2_idle_probe <url>...  (or PROBE_URLS=<url>,<url>)");
        std::process::exit(2);
    }
    let gaps = gaps();
    let total: Duration = gaps.iter().sum();
    println!(
        "probing {} endpoint(s) in parallel; idle gaps {:?}; about {} min",
        urls.len(),
        gaps.iter().map(|g| gap_label(*g)).collect::<Vec<_>>(),
        total.as_secs() / 60 + 1
    );
    let started = Instant::now();
    let runs = urls
        .into_iter()
        .map(|url| probe(url, gaps.clone(), started));
    let rows: Vec<Row> = join_all(runs).await.into_iter().flatten().collect();
    print_summary(&rows);
}
