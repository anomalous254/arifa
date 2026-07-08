# Arifa Benchmarks

Criterion-based benchmark suite for [Arifa](../), covering message serialization, subscribe/unsubscribe overhead, publish throughput at varying subscriber counts, channel fan-out patterns, and cross-node routing latency.

This lives in its own crate (separate `Cargo.toml`, separate `Cargo.lock`) so the main `arifa` crate doesn't carry Criterion, Tokio's full feature set, or other dev-only dependencies as part of its published package.

## Prerequisites

A local Redis instance reachable at `redis://127.0.0.1/`:

```bash
docker run -p 6379:6379 redis:7
```

## Running

From this directory (`benchmarks/`):

```bash
cargo bench
```

Or from the repo root, without `cd`-ing in, using `--manifest-path`:

```bash
cargo bench --manifest-path benchmarks/Cargo.toml
```

Each benchmark prints a live summary to stderr as it runs (sessions, messages published/routed, publish rate, delivery rate, drops, reconnects), followed by Criterion's own statistical report (mean, confidence intervals, outliers) once each benchmark group finishes.

## What's measured

| Benchmark | What it tells you |
|---|---|
| `serialize_wsmessage` / `deserialize_wsmessage` | Pure CPU cost of `WsMessage` (de)serialization, no I/O involved |
| `subscribe_unsubscribe` | Round-trip cost of `subscribe()` → `remove_online_user()` → `unsubscribe()` against real Redis |
| `publish_throughput/sessions_{100,1000,10000}` | Publish rate vs. actual delivery rate at increasing subscriber counts on one channel, plus drop/reconnect counts |
| `channel_fanout/single_hot_channel` | Time to deliver one message to 2,000 subscribers on a single shared channel |
| `channel_fanout/many_narrow_channels` | Time to deliver to 2,000 subscribers spread across 2,000 separate channels (one publish per channel) — shown for comparison against the shared-channel case above |
| `multi_node_publish_to_receive` | End-to-end latency for a message published on one node to reach a subscriber on a different node, over real Redis Pub/Sub |

## Tuning scale

Session counts and message volume are configurable via environment variables, so you can push past the defaults or scale down for a quicker local run:

```bash
ARIFA_BENCH_SESSIONS_LARGE=20000 ARIFA_BENCH_MESSAGES=2000 cargo bench
```

| Variable | Default | Used by |
|---|---|---|
| `ARIFA_BENCH_SESSIONS_SMALL` | `100` | `publish_throughput` |
| `ARIFA_BENCH_SESSIONS_MEDIUM` | `1000` | `publish_throughput` |
| `ARIFA_BENCH_SESSIONS_LARGE` | `10000` | `publish_throughput` |
| `ARIFA_BENCH_MESSAGES` | `1000` | `publish_throughput` (messages published per sample) |
| `ARIFA_BENCH_FANOUT_SESSIONS` | `2000` | `channel_fanout` |

Note: raising `ARIFA_BENCH_SESSIONS_LARGE` or `ARIFA_BENCH_MESSAGES` significantly increases total benchmark time, since `publish_throughput` waits for full delivery confirmation (not just publish acknowledgment) before each sample completes.

## Interpreting the output

For each `publish_throughput` sample, the printed summary distinguishes two different things people often conflate:

- **Publish Rate** — how fast `publish()` calls return (i.e. how fast Redis acknowledges the command)
- **Delivery Rate** — how fast messages actually land in subscribers (tracked via Arifa's own `messages_routed` metric)

A run is only counted as complete once `Messages Routed` reaches 100% of the expected total (`sessions × messages`). If delivery genuinely stalls (no progress for 500ms), it's labeled `[STALLED]`. If it's still making steady progress but simply needs more time than the allotted budget, it's labeled `[BUDGET EXCEEDED]` instead — these are different situations and are reported differently on purpose.

## Regenerating the summary chart

The chart embedded in the root README (`arifa_benchmark_summary.png`) is generated from representative values pulled from a benchmark run — it isn't produced automatically by `cargo bench`. After a fresh run, update the numbers in the table above and regenerate the image if you're keeping the README in sync.
