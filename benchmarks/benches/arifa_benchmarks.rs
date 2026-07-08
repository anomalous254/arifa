//! Arifa benchmark suite
//!
//! Add to Cargo.toml:
//!
//! [dev-dependencies]
//! criterion = { version = "0.5", features = ["async_tokio"] }
//! tokio = { version = "1", features = ["full"] }
//! async-trait = "0.1"
//! serde_json = "1"
//!
//! [[bench]]
//! name = "arifa_benchmarks"
//! harness = false
//!
//! Requires a local Redis instance reachable at redis://127.0.0.1/
//! (e.g. `docker run -p 6379:6379 redis:7`).
//!
//! Run with:
//!   cargo bench --bench arifa_benchmarks
//!
//! Tune scale via env vars, e.g.:
//!   ARIFA_BENCH_SESSIONS=5000 cargo bench --bench arifa_benchmarks

use arifa::prelude::*;
use async_trait::async_trait;
use criterion::{criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use serde_json::json;
use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::runtime::Runtime;

const REDIS_URL: &str = "redis://127.0.0.1/";

// `WsSession::Error` requires `std::error::Error`. `anyhow::Error` is a
// dynamic wrapper around types that implement that trait, but it does not
// implement it itself, so it can't be used directly here. This mock error
// never actually gets constructed (our `send()` always returns `Ok`), but
// the trait still needs a concrete, real `std::error::Error` type.
#[derive(Debug)]
struct BenchError(String);

impl fmt::Display for BenchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for BenchError {}

// ---------------------------------------------------------------------
// Mock session: records receipt instead of doing real I/O, so we measure
// Arifa's routing/dispatch cost rather than network or println overhead.
// ---------------------------------------------------------------------
#[derive(Clone)]
struct BenchSession {
    counter: Arc<AtomicUsize>,
}

impl BenchSession {
    fn new() -> Self {
        Self {
            counter: Arc::new(AtomicUsize::new(0)),
        }
    }
}

#[async_trait]
impl WsSession for BenchSession {
    type Error = BenchError;

    async fn send(&self, _event: WsMessage) -> Result<(), Self::Error> {
        self.counter.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

fn sample_message() -> WsMessage {
    WsMessage {
        scope: MessageScope::Broadcast,
        kind: MessageKind::Feeds,
        node_id: None,
        payload: json!({ "message": "Hello world", "ts": 0u64 }),
    }
}

/// Distinguishes *why* `wait_for_routed` stopped waiting — these mean
/// very different things and should never be reported with the same
/// label. `Stalled` means something is actually stuck (no progress for
/// `plateau_window`). `TimedOut` means delivery was still advancing
/// normally right up until `max_wait` ran out — it's just slow relative
/// to the budget given, not broken.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WaitOutcome {
    Completed,
    Stalled,
    TimedOut,
}

/// Poll `messages_routed` until it reaches `expected`, or until it stops
/// changing for `plateau_window` (a straggler that will likely never
/// arrive), or until `max_wait` elapses as a hard backstop.
/// Returns (elapsed, final_routed, outcome).
async fn wait_for_routed(
    arifa: &Arifa,
    expected: u64,
    max_wait: Duration,
    plateau_window: Duration,
) -> (Duration, u64, WaitOutcome) {
    let start = Instant::now();
    let deadline = start + max_wait;
    let mut last_routed = arifa.metrics.snapshot().messages_routed;
    let mut last_change = Instant::now();

    loop {
        let routed = arifa.metrics.snapshot().messages_routed;
        if routed >= expected {
            return (start.elapsed(), routed, WaitOutcome::Completed);
        }
        if routed != last_routed {
            last_routed = routed;
            last_change = Instant::now();
        } else if last_change.elapsed() >= plateau_window {
            // No progress for a while — genuinely stuck, not just slow.
            return (start.elapsed(), routed, WaitOutcome::Stalled);
        }
        if Instant::now() >= deadline {
            // Ran out of budget. `last_change` tells us whether progress
            // was still happening right up to the deadline (TimedOut) —
            // if so this is a benchmark-budget problem, not an Arifa
            // problem. (If progress had actually stopped, the plateau
            // check above would have already returned Stalled first.)
            return (start.elapsed(), routed, WaitOutcome::TimedOut);
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
}

/// Conservative per-message-delivery budget used to size `max_wait` for
/// a given amount of expected work, so a large `sessions × messages`
/// benchmark isn't measured against the same fixed timeout as a small
/// one. Well below Arifa's observed ~250k+ msg/sec fan-out throughput,
/// so this only pads the budget rather than tightening it.
const ASSUMED_MIN_DELIVERY_RATE: f64 = 50_000.0; // messages/sec

fn budget_for_expected_routed(expected_routed: u64) -> Duration {
    let seconds = (expected_routed as f64 / ASSUMED_MIN_DELIVERY_RATE) * 1.5; // 50% safety margin
    Duration::from_secs_f64(seconds.max(15.0)) // never less than the old flat floor
}

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

// =======================================================================
// 1. Micro-benchmark: WsMessage serialization (pure CPU cost, no I/O)
// =======================================================================
fn bench_message_serialization(c: &mut Criterion) {
    let msg = sample_message();

    c.bench_function("serialize_wsmessage", |b| {
        b.iter(|| serde_json::to_vec(&msg).unwrap())
    });

    let bytes = serde_json::to_vec(&msg).unwrap();
    c.bench_function("deserialize_wsmessage", |b| {
        b.iter(|| {
            let _: WsMessage = serde_json::from_slice(&bytes).unwrap();
        })
    });
}

// =======================================================================
// 2. Subscribe / unsubscribe overhead (single node, real Redis)
// =======================================================================
fn bench_subscribe_unsubscribe(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let arifa = rt.block_on(async {
        Arifa::new(REDIS_URL, "bench-node-subscribe").await.unwrap()
    });

    let mut group = c.benchmark_group("subscribe_unsubscribe");
    group.bench_function("subscribe_then_unsubscribe", |b| {
        b.to_async(&rt).iter_batched(
            || {
                let session = BenchSession::new();
                let channels = vec!["Bench::Channel".to_string()];
                // Arifa wraps its shared state internally and is cheap to
                // clone (see the Actix example in the README); clone it
                // fresh per iteration rather than moving the original.
                let arifa = arifa.clone();
                (arifa, session, channels)
            },
            |(arifa, session, channels)| async move {
                let session_id = arifa.subscribe(channels.clone(), session, "bench-user").await;
                let _ = arifa.remove_online_user(&session_id).await;
                arifa.unsubscribe(&session_id, &channels);
            },
            BatchSize::SmallInput,
        )
    });
    group.finish();

    rt.block_on(async { arifa.shutdown() });
}

// =======================================================================
// 3. Single-node publish throughput at varying subscriber counts
//    Measures messages/sec and reports Arifa's own metrics for
//    routed vs dropped messages.
// =======================================================================
fn bench_publish_throughput(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let session_counts = [
        env_usize("ARIFA_BENCH_SESSIONS_SMALL", 100),
        env_usize("ARIFA_BENCH_SESSIONS_MEDIUM", 1_000),
        env_usize("ARIFA_BENCH_SESSIONS_LARGE", 10_000),
    ];
    let messages_per_run = env_usize("ARIFA_BENCH_MESSAGES", 1_000);

    let mut group = c.benchmark_group("publish_throughput");
    group.sample_size(10);

    for &n_sessions in &session_counts {
        group.throughput(Throughput::Elements(messages_per_run as u64));
        group.bench_function(format!("sessions_{n_sessions}"), |b| {
            b.iter_custom(|iters| {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    total += rt.block_on(async {
                        let arifa = Arifa::new(REDIS_URL, "bench-node-throughput")
                            .await
                            .unwrap();

                        let channel = "Bench::HotChannel".to_string();
                        let mut session_ids = Vec::with_capacity(n_sessions);
                        for i in 0..n_sessions {
                            let session = BenchSession::new();
                            let id = arifa.subscribe(
                                vec![channel.clone()],
                                session,
                                format!("user-{i}"),
                            )
                            .await;
                            session_ids.push(id);
                        }

                        let msg = sample_message();
                        let messages_published = messages_per_run as u64;
                        let expected_routed = n_sessions as u64 * messages_published;

                        let publish_start = Instant::now();
                        for _ in 0..messages_per_run {
                            arifa.publish(&channel, &msg).await.unwrap();
                        }
                        let publish_elapsed = publish_start.elapsed();

                        // publish() returning only means the Redis command
                        // was sent/acked; actual delivery to each subscriber
                        // happens in background forwarder tasks. Wait for
                        // delivery to finish, genuinely stall (no progress
                        // for 500ms), or exhaust a budget sized to the
                        // actual amount of work (sessions × messages) —
                        // a flat timeout unfairly flags large runs as
                        // "stalled" when they're just not done yet.
                        let (wait_elapsed, routed, outcome) = wait_for_routed(
                            &arifa,
                            expected_routed,
                            budget_for_expected_routed(expected_routed),
                            Duration::from_millis(500),
                        )
                        .await;
                        let total_elapsed = publish_start.elapsed();
                        let _ = wait_elapsed; // kept for clarity, superseded by total_elapsed

                        let metrics = arifa.metrics.snapshot();
                        let publish_rate = messages_published as f64 / publish_elapsed.as_secs_f64();
                        let delivery_rate = routed as f64 / total_elapsed.as_secs_f64();
                        let pct = 100.0 * routed as f64 / expected_routed as f64;
                        let status_label = match outcome {
                            WaitOutcome::Completed => "",
                            WaitOutcome::Stalled => {
                                "  [STALLED - no progress for 500ms, likely stuck]"
                            }
                            WaitOutcome::TimedOut => {
                                "  [BUDGET EXCEEDED - still progressing normally, \
                                 just needed more time than allotted]"
                            }
                        };

                        eprintln!(
                            "\n\
                             Sessions:            {}\n\
                             Messages Published:  {}\n\
                             Messages Routed:     {} / {} ({:.2}%){}\n\
                             Elapsed:              {:.3} s\n\
                             \n\
                             Publish Rate:        {:.0} messages/sec\n\
                             Delivery Rate:       {:.0} messages/sec\n\
                             Dropped Messages:    {}\n\
                             Redis Reconnects:    {}{}\n",
                            n_sessions,
                            messages_published,
                            routed,
                            expected_routed,
                            pct,
                            status_label,
                            total_elapsed.as_secs_f64(),
                            publish_rate,
                            delivery_rate,
                            metrics.messages_dropped,
                            metrics.redis_reconnects,
                            if metrics.redis_reconnects > 0 {
                                "  <-- Redis dropped the pub/sub connection mid-run; \
                                 messages published before/during the reconnect are lost, \
                                 not dropped by Arifa's own buffers."
                            } else {
                                ""
                            },
                        );
                        let elapsed = total_elapsed;

                        for id in &session_ids {
                            let _ = arifa.remove_online_user(id).await;
                        }
                        arifa.shutdown();
                        elapsed
                    });
                }
                total
            })
        });
    }
    group.finish();
}

// =======================================================================
// 4. Channel fan-out comparison: one hot channel with all sessions
//    vs. many narrow per-user channels (routing table scaling).
// =======================================================================
fn bench_channel_fanout(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let n_sessions = env_usize("ARIFA_BENCH_FANOUT_SESSIONS", 2_000);
    let msg = sample_message();

    let mut group = c.benchmark_group("channel_fanout");
    group.sample_size(10);

    group.bench_function("single_hot_channel", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                total += rt.block_on(async {
                    let arifa = Arifa::new(REDIS_URL, "bench-node-fanout-hot")
                        .await
                        .unwrap();
                    let channel = "Bench::Hot".to_string();
                    let mut ids = Vec::with_capacity(n_sessions);
                    for i in 0..n_sessions {
                        let id = arifa.subscribe(
                            vec![channel.clone()],
                            BenchSession::new(),
                            format!("user-{i}"),
                        )
                        .await;
                        ids.push(id);
                    }
                    let expected_routed = n_sessions as u64;
                    let start = Instant::now();
                    arifa.publish(&channel, &msg).await.unwrap();

                    // Wait for delivery to all subscribers, not just for
                    // publish() to return the Redis ack. Stop early if
                    // delivery genuinely stalls rather than always waiting
                    // out a fixed timeout.
                    let (_wait_elapsed, _routed, _completed) = wait_for_routed(
                        &arifa,
                        expected_routed,
                        Duration::from_secs(10),
                        Duration::from_millis(500),
                    )
                    .await;
                    let elapsed = start.elapsed();
                    for id in &ids {
                        let _ = arifa.remove_online_user(id).await;
                    }
                    arifa.shutdown();
                    elapsed
                });
            }
            total
        })
    });

    group.bench_function("many_narrow_channels", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                total += rt.block_on(async {
                    let arifa = Arifa::new(REDIS_URL, "bench-node-fanout-narrow")
                        .await
                        .unwrap();
                    let mut ids = Vec::with_capacity(n_sessions);
                    for i in 0..n_sessions {
                        let channel = format!("Bench::User::{i}");
                        let id = arifa.subscribe(
                            vec![channel],
                            BenchSession::new(),
                            format!("user-{i}"),
                        )
                        .await;
                        ids.push(id);
                    }
                    let start = Instant::now();
                    for i in 0..n_sessions {
                        let channel = format!("Bench::User::{i}");
                        arifa.publish(&channel, &msg).await.unwrap();
                    }
                    let elapsed = start.elapsed();
                    for id in &ids {
                        let _ = arifa.remove_online_user(id).await;
                    }
                    arifa.shutdown();
                    elapsed
                });
            }
            total
        })
    });

    group.finish();
}

// =======================================================================
// 5. Multi-node routing latency: publish on node A, receive on node B,
//    both backed by the same Redis instance. Exercises the real
//    Redis Pub/Sub round trip rather than in-process dispatch only.
// =======================================================================
fn bench_multi_node_routing(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    c.bench_function("multi_node_publish_to_receive", |b| {
        b.to_async(&rt).iter_custom(|iters| async move {
            let node_a = Arifa::new(REDIS_URL, "bench-node-a").await.unwrap();
            let node_b = Arifa::new(REDIS_URL, "bench-node-b").await.unwrap();

            let counter = Arc::new(AtomicUsize::new(0));
            let session = BenchSession {
                counter: counter.clone(),
            };
            let channel = "Bench::CrossNode".to_string();
            let session_id = node_b.subscribe(vec![channel.clone()], session, "cross-node-user").await;

            // Give the resubscription/routing table a moment to settle.
            tokio::time::sleep(Duration::from_millis(50)).await;

            let msg = sample_message();
            let start = Instant::now();
            for _ in 0..iters {
                node_a.publish(&channel, &msg).await.unwrap();
            }

            // Poll until node_b has received all messages, or time out.
            let deadline = Instant::now() + Duration::from_secs(5);
            while counter.load(Ordering::Relaxed) < iters as usize && Instant::now() < deadline {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            let elapsed = start.elapsed();

            let _ = node_b.remove_online_user(&session_id).await;
            node_a.shutdown();
            node_b.shutdown();

            elapsed
        })
    });
}

criterion_group!(
    benches,
    bench_message_serialization,
    bench_subscribe_unsubscribe,
    bench_publish_throughput,
    bench_channel_fanout,
    bench_multi_node_routing,
);
criterion_main!(benches);
