//! Measure one-shot NIP-50 search latency against a running relay.
//!
//! Usage: `search-bench <channel_uuid> <query> <iterations> <output.json>`.
//! The caller supplies `BUZZ_RELAY_URL` and `BENCH_PRIVATE_KEY`.

use std::{time::Duration, time::Instant};

use anyhow::{Context, Result};
use buzz_test_client::BuzzTestClient;
use nostr::{Alphabet, Filter, Keys, Kind, SingleLetterTag};
use serde_json::json;

fn percentile(samples: &[f64], p: f64) -> Option<f64> {
    if samples.is_empty() {
        return None;
    }
    let index = ((samples.len() as f64 - 1.0) * p).round() as usize;
    samples.get(index).copied()
}

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 5 {
        anyhow::bail!("Usage: search-bench <channel_uuid> <query> <iterations> <output.json>");
    }
    let channel = &args[1];
    let query = &args[2];
    let iterations: usize = args[3].parse().context("iterations must be an integer")?;
    anyhow::ensure!(iterations > 0, "iterations must be positive");
    let output = &args[4];
    let relay_url =
        std::env::var("BUZZ_RELAY_URL").unwrap_or_else(|_| "ws://localhost:3000".to_owned());
    let key_hex =
        std::env::var("BENCH_PRIVATE_KEY").context("BENCH_PRIVATE_KEY is required (hex)")?;
    let keys = Keys::parse(&key_hex).context("BENCH_PRIVATE_KEY is invalid")?;
    let mut client = BuzzTestClient::connect(&relay_url, &keys).await?;
    let mut samples = Vec::with_capacity(iterations);

    for index in 0..iterations {
        let subscription = format!("search-bench-{index}");
        let filter = Filter::new()
            .kind(Kind::Custom(9))
            .search(query)
            .custom_tags(SingleLetterTag::lowercase(Alphabet::H), [channel.as_str()]);
        let start = Instant::now();
        client.subscribe(&subscription, vec![filter]).await?;
        let _events = client
            .collect_until_eose(&subscription, Duration::from_secs(10))
            .await?;
        samples.push(start.elapsed().as_secs_f64() * 1e3);
        client.close_subscription(&subscription).await?;
    }
    client.disconnect().await?;
    samples.sort_by(f64::total_cmp);

    let summary = json!({
        "iterations": iterations,
        "query": query,
        "p50_ms": percentile(&samples, 0.50),
        "p95_ms": percentile(&samples, 0.95),
        "p99_ms": percentile(&samples, 0.99),
        "max_ms": samples.last().copied(),
    });
    std::fs::write(output, serde_json::to_vec_pretty(&summary)?)?;
    println!("{}", serde_json::to_string(&summary)?);
    Ok(())
}
