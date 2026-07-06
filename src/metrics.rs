#![allow(dead_code)]
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Default)]
struct Inner {
    sessions_active: AtomicU64,
    messages_routed: AtomicU64,
    messages_dropped: AtomicU64,
    redis_reconnects: AtomicU64,
    heartbeat_evictions: AtomicU64,
}

/// Cheap, lock-free counters for the router's health. Clone freely —
/// it's an `Arc` under the hood, so every clone shares the same counters.
///
/// Wire `snapshot()` up to a `/metrics` or `/debug/arifa` endpoint, or
/// log it periodically, to actually see problems (queue buildup, drops,
/// reconnect churn) before they become outages.
#[derive(Clone, Default)]
pub struct Metrics(Arc<Inner>);

#[derive(Debug, Clone, Copy, serde::Serialize)]
pub struct MetricsSnapshot {
    pub sessions_active: u64,
    pub messages_routed: u64,
    pub messages_dropped: u64,
    pub redis_reconnects: u64,
    pub heartbeat_evictions: u64,
}

impl Metrics {
    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            sessions_active: self.0.sessions_active.load(Ordering::Relaxed),
            messages_routed: self.0.messages_routed.load(Ordering::Relaxed),
            messages_dropped: self.0.messages_dropped.load(Ordering::Relaxed),
            redis_reconnects: self.0.redis_reconnects.load(Ordering::Relaxed),
            heartbeat_evictions: self.0.heartbeat_evictions.load(Ordering::Relaxed),
        }
    }

    pub(crate) fn session_started(&self) {
        self.0.sessions_active.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn session_ended(&self) {
        self.0.sessions_active.fetch_sub(1, Ordering::Relaxed);
    }

    pub(crate) fn record_routed(&self, delivered: u64, dropped: u64) {
        self.0
            .messages_routed
            .fetch_add(delivered, Ordering::Relaxed);
        self.0
            .messages_dropped
            .fetch_add(dropped, Ordering::Relaxed);
    }

    pub(crate) fn record_reconnect(&self) {
        self.0.redis_reconnects.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn _record_heartbeat_eviction(&self) {
        self.0.heartbeat_evictions.fetch_add(1, Ordering::Relaxed);
    }
}
