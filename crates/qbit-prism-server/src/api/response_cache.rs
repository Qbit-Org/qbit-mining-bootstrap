use super::*;
use futures_util::{
    future::{BoxFuture, Shared},
    FutureExt,
};

type Computation<T> = Shared<BoxFuture<'static, ApiResult<T>>>;
/// One remembered answer and the one computation of its next value: a public
/// response here, and `network()`'s node answers (#858).
pub(super) struct CacheSlot<T = Payload> {
    state: Mutex<SlotState<T>>,
}
impl<T> Default for CacheSlot<T> {
    fn default() -> Self {
        Self {
            state: Mutex::new(SlotState {
                entry: None,
                inflight: None,
            }),
        }
    }
}
struct SlotState<T> {
    entry: Option<Entry<T>>,
    inflight: Option<Computation<T>>,
}
struct Entry<T> {
    payload: T,
    created: Instant,
}
/// The instant a slot's entry is dated from, and its lifetime counted from.
#[derive(Clone, Copy)]
enum Dated {
    /// When its flight ended: a public response, whose `Age` counts from
    /// when it was built.
    End,
    /// When its flight started, before it asked anything: `network()`'s node
    /// answers, so a height or tip served from the entry was read less than
    /// its lifetime earlier, however long the rest of its calls took (#858).
    /// A flight's own waiters get its answer as old as the flight was when
    /// they joined it.
    Start,
}
impl<T: Clone + Send + Sync + 'static> CacheSlot<T> {
    /// The slot's one computation: the one in flight, or else `compute`,
    /// started now. When it ends, a result `keep` accepts replaces the entry,
    /// dated as `dated` says, one it refuses removes it, and a failure leaves
    /// it as it was, so a failure is never remembered. A computation that
    /// panics fails with an internal error instead of leaving its waiters,
    /// and every later one, a poisoned flight. Flights never overlap: the next starts only after
    /// this one has cleared itself, so an older result cannot replace a newer
    /// one. A spawned task drives the flight to its end, so a waiter that
    /// gives up, its deadline passed or its client gone, never cancels it for
    /// the others; its own deadline still bounds that waiter.
    fn flight(
        self: &Arc<Self>,
        stored: &mut SlotState<T>,
        compute: impl FnOnce() -> BoxFuture<'static, ApiResult<T>>,
        keep: impl FnOnce(&T) -> bool + Send + 'static,
        dated: Dated,
    ) -> Computation<T> {
        if let Some(flight) = &stored.inflight {
            return flight.clone();
        }
        let owner = self.clone();
        let started = Instant::now();
        let running = std::panic::AssertUnwindSafe(compute()).catch_unwind();
        let flight = async move {
            let result = running.await.unwrap_or_else(|_| {
                tracing::error!("a shared public computation panicked");
                Err(ApiError::internal())
            });
            let mut slot = owner.state.lock().await;
            if let Ok(value) = &result {
                slot.entry = keep(value).then(|| Entry {
                    payload: value.clone(),
                    created: match dated {
                        Dated::End => Instant::now(),
                        Dated::Start => started,
                    },
                });
            }
            slot.inflight = None;
            result
        }
        .boxed()
        .shared();
        stored.inflight = Some(flight.clone());
        let background = flight.clone();
        tokio::spawn(async move {
            let _ = background.await;
        });
        flight
    }

    /// The entry while it is younger than `lifetime`, counted from when its
    /// flight started, else the result of the slot's flight, joined or
    /// started with `compute` and remembered when `keep` accepts it. The
    /// answer of a flight that took longer than `lifetime` has expired by the
    /// time it ends, so it answers only that flight's waiters.
    pub(super) async fn remembered(
        self: &Arc<Self>,
        lifetime: Duration,
        compute: impl FnOnce() -> BoxFuture<'static, ApiResult<T>>,
        keep: impl FnOnce(&T) -> bool + Send + 'static,
    ) -> ApiResult<T> {
        let flight = {
            let mut stored = self.state.lock().await;
            if let Some(entry) = &stored.entry {
                if entry.created.elapsed() < lifetime {
                    return Ok(entry.payload.clone());
                }
            }
            self.flight(&mut stored, compute, keep, Dated::Start)
        };
        flight.await
    }
}

pub(super) async fn public_response(
    state: &ApiState,
    path: &str,
    query: &Query,
    policy: &CachePolicy,
    view: Option<&public_service::ServiceView>,
) -> (ApiResult<Payload>, &'static str, u64) {
    let database = public_service::reads_database(path);
    if database {
        if let Some(error) = view.and_then(|v| v.replica_error.as_ref()) {
            return (Err(ApiError::upstream(error)), "BYPASS", 0);
        }
    }
    let degraded = database && view.is_some_and(|v| !v.database_ready);
    let budget = view.and_then(|_| public_service::staleness_budget(path));
    let compute = || public_compute(state.clone(), path.to_string(), query.clone());
    let unavailable = || Err(ApiError::upstream("public read database is unavailable"));
    if policy.ttl == 0 {
        return (
            if degraded {
                unavailable()
            } else {
                compute().await
            },
            "BYPASS",
            0,
        );
    }
    let key = cache_key(path, query);
    let slot = {
        let mut cache = state.cache.lock().await;
        if cache.len() >= state.config.cache_max_entries && !cache.contains_key(&key) {
            if let Some(victim) = cache
                .iter()
                .find(|(_, v)| Arc::strong_count(v) == 1)
                .map(|(k, _)| k.clone())
            {
                cache.remove(&victim);
            }
        }
        if cache.len() >= state.config.cache_max_entries && !cache.contains_key(&key) {
            None
        } else {
            Some(
                cache
                    .entry(key)
                    .or_insert_with(|| Arc::new(CacheSlot::default()))
                    .clone(),
            )
        }
    };
    let Some(slot) = slot else {
        return (
            if degraded {
                unavailable()
            } else {
                compute().await
            },
            "BYPASS",
            0,
        );
    };
    let mut stored = slot.state.lock().await;
    let swr = if degraded {
        0
    } else {
        budget.map_or(policy.stale, |budget| {
            policy.stale.min(budget.saturating_sub(policy.ttl))
        })
    };
    let mut stale = None;
    if let Some(entry) = &stored.entry {
        let age = entry.created.elapsed().as_secs();
        if budget.is_some_and(|budget| age > budget) {
            stored.entry = None;
            if degraded {
                return (
                    Err(ApiError::upstream(
                        "public read model exceeded its staleness budget",
                    )),
                    "BYPASS",
                    age,
                );
            }
        } else if age < policy.ttl {
            return (Ok(entry.payload.clone()), "HIT", age);
        } else if age <= policy.ttl.saturating_add(swr) && swr > 0 {
            stale = Some((entry.payload.clone(), age));
        }
    }
    if degraded {
        return (unavailable(), "BYPASS", 0);
    }
    // A disconnected HTTP waiter must not abandon the shared computation
    // while retaining its cache slot, and a STALE answer leaves its refresh
    // running: the flight's own task drives it. The request deadline still
    // bounds it.
    let max_bytes = state.config.cache_max_bytes;
    let flight = slot.flight(
        &mut stored,
        || compute().boxed(),
        move |payload| cacheable(payload, max_bytes),
        Dated::End,
    );
    drop(stored);
    if let Some((payload, age)) = stale {
        (Ok(payload), "STALE", age)
    } else {
        let result = flight.await;
        let bypass = result
            .as_ref()
            .is_ok_and(|payload| !cacheable(payload, max_bytes));
        (result, if bypass { "BYPASS" } else { "MISS" }, 0)
    }
}

/// Whether a response may be remembered: not a canonical fallback, and no
/// larger than `max_bytes`.
fn cacheable(payload: &Payload, max_bytes: usize) -> bool {
    payload.canonical_fallback.is_none() && payload.bytes.len() <= max_bytes
}

async fn public_compute(mut state: ApiState, path: String, query: Query) -> ApiResult<Payload> {
    state.pool = state.public_pool.clone();
    let read = async {
        if let Some(hash) = path.strip_prefix("/public/v1/artifacts/") {
            return super::read_models::artifact_document(
                &state,
                &clean_hash(hash, "artifact sha256")?,
            )
            .await;
        }
        public::dispatch(&state, &path, &query)
            .await
            .map(Payload::json)
    };
    let timeout = state.config.read_timeout;
    if timeout.is_zero() {
        return public_service::READ_DEADLINE.scope(None, read).await;
    }
    let deadline = tokio::time::Instant::now()
        .checked_add(timeout)
        .ok_or_else(ApiError::internal)?;
    public_service::READ_DEADLINE
        .scope(Some(deadline), async {
            tokio::time::timeout_at(deadline, read)
                .await
                .map_err(|_| ApiError::read_timeout())?
        })
        .await
}

#[cfg(test)]
mod tests {
    use super::*;
    fn state() -> ApiState {
        ApiState::new(
            sqlx::postgres::PgPoolOptions::new()
                .connect_lazy("postgres://invalid@127.0.0.1:1/invalid")
                .unwrap(),
            ApiConfig::default(),
            std::sync::Arc::new(crate::metrics::Metrics::default()),
        )
    }
    async fn seed(state: &ApiState, path: &str, age: u64) {
        state.cache.lock().await.insert(
            cache_key(path, &Query::default()),
            Arc::new(CacheSlot {
                state: Mutex::new(SlotState {
                    entry: Some(Entry {
                        payload: Payload::json(json!({"cached":true})),
                        created: Instant::now() - Duration::from_secs(age),
                    }),
                    inflight: None,
                }),
            }),
        );
    }
    #[tokio::test]
    async fn a_computation_that_panics_fails_and_leaves_nothing_in_flight() {
        let slot = Arc::new(CacheSlot::<u32>::default());
        let panicking = || async { panic!("computation panicked") }.boxed();
        let error = slot
            .remembered(Duration::from_secs(60), panicking, |_| true)
            .await
            .unwrap_err();
        assert_eq!(error.status, StatusCode::INTERNAL_SERVER_ERROR);
        assert!(slot.state.lock().await.inflight.is_none());
        let next = slot
            .remembered(
                Duration::from_secs(60),
                || async { Ok(7) }.boxed(),
                |_| true,
            )
            .await;
        assert_eq!(next.unwrap(), 7);
    }
    #[tokio::test]
    async fn stale_entry_returns_immediately_while_one_refresh_replaces_it() {
        let state = state();
        let path = "/public/v1/mining-configuration";
        seed(&state, path, 301).await;
        let policy = CachePolicy {
            ttl: 300,
            stale: 60,
            immutable: false,
            debug_headers: false,
        };
        let (result, cache, age) =
            public_response(&state, path, &Query::default(), &policy, None).await;
        assert_eq!(cache, "STALE");
        assert_eq!(age, 301);
        assert_eq!(
            serde_json::from_slice::<Value>(&result.unwrap().bytes).unwrap(),
            json!({"cached":true})
        );
        let slot = state
            .cache
            .lock()
            .await
            .get(&cache_key(path, &Query::default()))
            .unwrap()
            .clone();
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if slot.state.lock().await.inflight.is_none() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let (result, cache, _) =
            public_response(&state, path, &Query::default(), &policy, None).await;
        assert_eq!(cache, "HIT");
        assert_eq!(
            serde_json::from_slice::<Value>(&result.unwrap().bytes).unwrap()["schema"],
            "prism.dashboard.mining-configuration.v1"
        );
    }
    #[tokio::test]
    async fn healthy_origin_refreshes_an_entry_that_exceeded_its_budget() {
        let state = state();
        let (_, service) =
            public_service::router(state.clone(), public_service::ServiceConfig::default());
        let mut view = service.view();
        view.database_ready = true;
        let path = "/public/v1/mining-configuration";
        seed(&state, path, 901).await;
        let (result, cache, age) = public_response(
            &state,
            path,
            &Query::default(),
            &CachePolicy {
                ttl: 1000,
                stale: 0,
                immutable: false,
                debug_headers: false,
            },
            Some(&view),
        )
        .await;
        assert_eq!(cache, "MISS");
        assert_eq!(age, 0);
        assert_eq!(
            serde_json::from_slice::<Value>(&result.unwrap().bytes).unwrap()["schema"],
            "prism.dashboard.mining-configuration.v1"
        );
    }
    #[tokio::test]
    async fn database_outage_serves_only_fresh_bounded_entries_and_never_reads_origin() {
        let state = state();
        let (_, service) =
            public_service::router(state.clone(), public_service::ServiceConfig::default());
        let view = service.view();
        assert!(!view.database_ready);
        let path = "/public/v1/blocks";
        let policy = CachePolicy {
            ttl: 5,
            stale: 30,
            immutable: false,
            debug_headers: false,
        };
        seed(&state, path, 1).await;
        let (result, cache, age) =
            public_response(&state, path, &Query::default(), &policy, Some(&view)).await;
        assert!(result.is_ok());
        assert_eq!(cache, "HIT");
        let mut response = StatusCode::OK.into_response();
        public_service::decorate(&mut response, path, &policy, &view, age, cache);
        assert_eq!(response.headers()["x-prism-database-state"], "unavailable");
        assert!(response.headers().contains_key("warning"));
        seed(&state, path, 6).await;
        let (error, _, _) =
            public_response(&state, path, &Query::default(), &policy, Some(&view)).await;
        assert_eq!(error.unwrap_err().status, StatusCode::SERVICE_UNAVAILABLE);
        seed(&state, path, 16).await;
        let (error, _, age) = public_response(
            &state,
            path,
            &Query::default(),
            &CachePolicy { ttl: 100, ..policy },
            Some(&view),
        )
        .await;
        assert!(error.unwrap_err().message.contains("staleness budget"));
        assert_eq!(age, 16);
        assert_eq!(
            state.public_pool.size(),
            0,
            "outage responses must not ask PostgreSQL"
        );
    }
}
