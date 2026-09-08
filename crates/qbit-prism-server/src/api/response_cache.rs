use super::*;
use futures_util::{
    future::{BoxFuture, Shared},
    FutureExt,
};

type Computation = Shared<BoxFuture<'static, ApiResult<Payload>>>;
#[derive(Default)]
pub(super) struct CacheSlot {
    state: Mutex<SlotState>,
}
#[derive(Default)]
struct SlotState {
    entry: Option<Entry>,
    inflight: Option<Computation>,
}
struct Entry {
    payload: Payload,
    created: Instant,
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
    let flight = if let Some(flight) = &stored.inflight {
        flight.clone()
    } else {
        let owner = slot.clone();
        let state = state.clone();
        let path = path.to_string();
        let query = query.clone();
        let flight = async move {
            let result = public_compute(state.clone(), path, query).await;
            let mut slot = owner.state.lock().await;
            if let Ok(payload) = &result {
                if payload.canonical_fallback.is_none()
                    && payload.bytes.len() <= state.config.cache_max_bytes
                {
                    slot.entry = Some(Entry {
                        payload: payload.clone(),
                        created: Instant::now(),
                    });
                } else {
                    slot.entry = None;
                }
            }
            slot.inflight = None;
            result
        }
        .boxed()
        .shared();
        stored.inflight = Some(flight.clone());
        // A disconnected HTTP waiter must not abandon the shared computation
        // while retaining its cache slot. The request deadline still bounds it.
        let background = flight.clone();
        tokio::spawn(async move {
            let _ = background.await;
        });
        flight
    };
    drop(stored);
    if let Some((payload, age)) = stale {
        tokio::spawn(async move {
            let _ = flight.await;
        });
        (Ok(payload), "STALE", age)
    } else {
        let result = flight.await;
        let bypass = result.as_ref().is_ok_and(|payload| {
            payload.canonical_fallback.is_some()
                || payload.bytes.len() > state.config.cache_max_bytes
        });
        (result, if bypass { "BYPASS" } else { "MISS" }, 0)
    }
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
    async fn stale_entry_returns_immediately_while_one_refresh_replaces_it() {
        let state = state();
        let path = "/public/v1/mining-configuration";
        seed(&state, path, 301).await;
        let policy = CachePolicy {
            ttl: 300,
            stale: 60,
            immutable: false,
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
