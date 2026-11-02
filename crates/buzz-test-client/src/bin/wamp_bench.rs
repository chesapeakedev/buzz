//! Paced kind:9 load generator for relay write-amplification benchmarking.
//!
//! Opens `conns` authenticated WebSocket connections (one shared identity)
//! and sends kind:9 text events to `channel_uuid` at a total target rate of `qps`
//! for `duration_secs`. Each connection is synchronous (send -> await OK),
//! paced by a per-connection tokio interval, so OK latency is measured
//! end-to-end. Emits a JSON summary on stdout and one raw latency sample
//! (milliseconds, f64) per line to `latency_out`.
//!
//! Usage: wamp-bench <channel_uuid> <qps> <duration_secs> <conns> <latency_out>
//! Env:   BUZZ_RELAY_URL (default ws://localhost:3000), BENCH_PRIVATE_KEY (hex)

use std::{
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use buzz_test_client::BuzzTestClient;
use nostr::{EventBuilder, Keys, Kind, Tag};
use tokio::{sync::Notify, time::MissedTickBehavior};

async fn create_channel(url: &str, keys: &Keys) -> anyhow::Result<String> {
    let channel_id = uuid::Uuid::new_v4().to_string();
    let event = EventBuilder::new(Kind::Custom(9007), "")
        .tags([
            Tag::parse(["h", &channel_id])?,
            Tag::parse(["name", &format!("embedded-bench-{channel_id}")])?,
            Tag::parse(["channel_type", "stream"])?,
            Tag::parse(["visibility", "open"])?,
        ])
        .sign_with_keys(keys)?;
    let http_url = url
        .replace("wss://", "https://")
        .replace("ws://", "http://");
    let response = reqwest::Client::new()
        .post(format!("{http_url}/events"))
        .header("X-Pubkey", keys.public_key().to_hex())
        .json(&event)
        .send()
        .await?;
    anyhow::ensure!(
        response.status().is_success(),
        "channel creation HTTP status: {}",
        response.status()
    );
    let body: serde_json::Value = response.json().await?;
    anyhow::ensure!(
        body["accepted"].as_bool() == Some(true),
        "channel creation rejected: {body}"
    );
    Ok(channel_id)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _ = rustls::crypto::CryptoProvider::install_default(
        rustls::crypto::aws_lc_rs::default_provider(),
    );
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 6 {
        eprintln!("Usage: wamp-bench <channel_uuid> <qps> <duration_secs> <conns> <latency_out>");
        std::process::exit(1);
    }
    let channel_arg = args[1].clone();
    let qps: f64 = args[2].parse()?;
    let duration_secs: u64 = args[3].parse()?;
    let conns: usize = args[4].parse()?;
    let latency_out = args[5].clone();

    let url = std::env::var("BUZZ_RELAY_URL").unwrap_or_else(|_| "ws://localhost:3000".into());
    let keys = match std::env::var("BENCH_PRIVATE_KEY") {
        Ok(hex) => Keys::parse(&hex)?,
        Err(_) => anyhow::bail!("BENCH_PRIVATE_KEY is required (channel member secret key)"),
    };

    let channel_id = if channel_arg == "auto" {
        let channel_id = create_channel(&url, &keys).await?;
        if let Ok(path) = std::env::var("BENCH_CHANNEL_FILE") {
            std::fs::write(path, &channel_id)?;
        }
        channel_id
    } else {
        channel_arg
    };

    let per_conn_interval = Duration::from_secs_f64(conns as f64 / qps);
    let connect_batch_size = std::env::var("BENCH_CONNECT_BATCH_SIZE")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|&value| value > 0)
        .unwrap_or(conns.max(1));
    let connect_batch_delay_ms = std::env::var("BENCH_CONNECT_BATCH_DELAY_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(100);
    let connected_count = Arc::new(AtomicUsize::new(0));
    let connection_failed = Arc::new(AtomicBool::new(false));
    let connection_notify = Arc::new(Notify::new());

    let mut tasks = Vec::new();
    for conn_idx in 0..conns {
        let url = url.clone();
        let keys = keys.clone();
        let channel_id = channel_id.clone();
        let connected_count = Arc::clone(&connected_count);
        let connection_failed = Arc::clone(&connection_failed);
        let connection_notify = Arc::clone(&connection_notify);
        tasks.push(tokio::spawn(async move {
            let batch = conn_idx / connect_batch_size;
            if batch > 0 {
                tokio::time::sleep(Duration::from_millis(
                    connect_batch_delay_ms.saturating_mul(batch as u64),
                ))
                .await;
            }
            let mut client = match BuzzTestClient::connect(&url, &keys).await {
                Ok(client) => client,
                Err(error) => {
                    connection_failed.store(true, Ordering::Release);
                    connection_notify.notify_waiters();
                    eprintln!("CONNECT_ERROR conn={conn_idx}: {error:#}");
                    return Ok::<_, anyhow::Error>((0, 0, 0, 1, Vec::new()));
                }
            };
            // Do not start the measurement window until every requested client
            // has authenticated. This separates connection-admission behavior
            // from the steady-state write workload.
            connected_count.fetch_add(1, Ordering::AcqRel);
            connection_notify.notify_waiters();
            while connected_count.load(Ordering::Acquire) < conns
                && !connection_failed.load(Ordering::Acquire)
            {
                connection_notify.notified().await;
            }
            if connection_failed.load(Ordering::Acquire) {
                client.disconnect().await?;
                return Ok::<_, anyhow::Error>((0, 0, 0, 0, Vec::new()));
            }
            // Phase the first write across one interval. Without this ramp,
            // every connection publishes on the same tick and the benchmark
            // measures an avoidable SQLite writer stampede instead of qps.
            let phase = Duration::from_secs_f64(conn_idx as f64 / qps);
            tokio::time::sleep(phase).await;
            let deadline = Instant::now() + Duration::from_secs(duration_secs);
            let mut interval = tokio::time::interval(per_conn_interval);
            interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
            let mut latencies: Vec<f64> = Vec::new();
            let mut sent: u64 = 0;
            let mut rejected: u64 = 0;
            let mut publish_errors: u64 = 0;
            let mut seq: u64 = 0;
            while Instant::now() < deadline {
                interval.tick().await;
                if Instant::now() >= deadline {
                    break;
                }
                seq += 1;
                let content = format!(
                    "wamp-bench c{conn_idx} m{seq} payload: the quick brown fox jumps over the lazy dog 0123456789"
                );
                let start = Instant::now();
                let ok = match client
                    .send_text_message(&keys, &channel_id, &content, 9)
                    .await
                {
                    Ok(ok) => ok,
                    Err(error) => {
                        publish_errors += 1;
                        eprintln!(
                            "PUBLISH_ERROR conn={conn_idx} seq={seq}: {error:#}"
                        );
                        break;
                    }
                };
                let elapsed_ms = start.elapsed().as_secs_f64() * 1e3;
                sent += 1;
                if ok.accepted {
                    latencies.push(elapsed_ms);
                } else {
                    rejected += 1;
                    eprintln!("REJECTED conn={conn_idx} seq={seq}: {}", ok.message);
                }
            }
            client.disconnect().await?;
            Ok::<_, anyhow::Error>((sent, rejected, publish_errors, 0, latencies))
        }));
    }

    let mut sent = 0u64;
    let mut rejected = 0u64;
    let mut publish_errors = 0u64;
    let mut connection_errors = 0u64;
    let mut latencies: Vec<f64> = Vec::new();
    for task in tasks {
        let (s, r, e, c, l) = task.await??;
        sent += s;
        rejected += r;
        publish_errors += e;
        connection_errors += c;
        latencies.extend(l);
    }
    latencies.sort_by(|a, b| a.partial_cmp(b).expect("finite latencies"));
    let pct = |p: f64| -> f64 {
        if latencies.is_empty() {
            return f64::NAN;
        }
        let idx = ((latencies.len() as f64 - 1.0) * p).round() as usize;
        latencies[idx]
    };
    let raw: String = latencies.iter().map(|l| format!("{l:.3}\n")).collect();
    std::fs::write(&latency_out, raw)?;
    println!(
        "{}",
        serde_json::json!({
            "sent": sent,
            "accepted": sent - rejected,
            "rejected": rejected,
            "publish_errors": publish_errors,
            "connection_errors": connection_errors,
            "qps_target": qps,
            "duration_secs": duration_secs,
            "conns": conns,
            "ok_latency_ms": {
                "p50": pct(0.50),
                "p95": pct(0.95),
                "p99": pct(0.99),
                "max": latencies.last().copied().unwrap_or(f64::NAN),
            },
        })
    );
    Ok(())
}
