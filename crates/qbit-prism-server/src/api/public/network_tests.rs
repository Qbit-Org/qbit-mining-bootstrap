//! `network()`'s memo (#858) against a counting fake node.
use super::*;
use axum::{extract::State, routing::post, Json};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Mutex as StdMutex,
};
use tokio::sync::watch;
use tokio_util::task::AbortOnDropHandle;

/// A node that counts each method it is asked, holds `getblockchaininfo`
/// while closed, and fails the next `fail_chain` of them, or every
/// `getnetworkinfo` while `fail_info` is set.
struct FakeNode {
    calls: StdMutex<BTreeMap<String, usize>>,
    open: watch::Sender<bool>,
    fail_chain: AtomicUsize,
    fail_info: AtomicUsize,
    tip: StdMutex<String>,
}
impl FakeNode {
    fn calls(&self, method: &str) -> usize {
        self.calls.lock().unwrap().get(method).copied().unwrap_or(0)
    }
    fn hold(&self) {
        self.open.send_replace(false);
    }
    fn release(&self) {
        self.open.send_replace(true);
    }
    fn set_tip(&self, tip: &str) {
        *self.tip.lock().unwrap() = tip.repeat(32);
    }
    async fn until(&self, method: &str, count: usize) {
        tokio::time::timeout(Duration::from_secs(10), async {
            while self.calls(method) < count {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("the node never saw {count} {method}"));
    }
}
async fn fake_node_reply(
    State(node): State<Arc<FakeNode>>,
    Json(input): Json<Value>,
) -> Json<Value> {
    let method = input["method"].as_str().unwrap_or("").to_owned();
    *node
        .calls
        .lock()
        .unwrap()
        .entry(method.clone())
        .or_default() += 1;
    let failed = |counter: &AtomicUsize| {
        counter
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
            .is_ok()
    };
    let result = match method.as_str() {
        "getblockchaininfo" => {
            let _ = node.open.subscribe().wait_for(|open| *open).await;
            if failed(&node.fail_chain) {
                return Json(json!({"id":input["id"],"result":null,"error":{"code":-1}}));
            }
            let tip = node.tip.lock().unwrap().clone();
            json!({"chain":"regtest","blocks":100,"bestblockhash":tip,"initialblockdownload":false})
        }
        "getblocktemplate" => json!({"bits":"207fffff"}),
        "getnetworkinfo" => {
            if failed(&node.fail_info) {
                return Json(json!({"id":input["id"],"result":null,"error":{"code":-1}}));
            }
            json!({"connections":2})
        }
        _ => Value::Null,
    };
    Json(json!({"id":input["id"],"result":result,"error":null}))
}

/// An API state whose node is at `rpc_url`, whose public cache lifetimes are
/// all `ttl` seconds (off at zero), and whose requests have `read_timeout`.
fn state_with(ttl: u64, read_timeout: Duration, rpc_url: String) -> ApiState {
    let lifetime = CacheLifetime {
        ttl,
        stale_while_revalidate: 0,
    };
    ApiState::new(
        sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://invalid@127.0.0.1:1/invalid")
            .unwrap(),
        ApiConfig {
            rpc_url,
            read_timeout,
            cache_enabled: ttl > 0,
            cache_lifetimes: CacheLifetimes {
                default: lifetime,
                aggregate: lifetime,
                ..CacheLifetimes::default()
            },
            ..ApiConfig::default()
        },
        Arc::new(crate::metrics::Metrics::default()),
    )
}

/// An API state whose node is a fresh [`FakeNode`], whose public cache
/// lifetimes are all `ttl` seconds (off at zero), and whose requests have
/// `read_timeout`.
async fn fake(
    ttl: u64,
    read_timeout: Duration,
) -> (ApiState, Arc<FakeNode>, AbortOnDropHandle<()>) {
    let node = Arc::new(FakeNode {
        calls: StdMutex::default(),
        open: watch::channel(true).0,
        fail_chain: AtomicUsize::new(0),
        fail_info: AtomicUsize::new(0),
        tip: StdMutex::new("ab".repeat(32)),
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let rpc_url = format!("http://{}/", listener.local_addr().unwrap());
    let app = axum::Router::new()
        .route("/", post(fake_node_reply))
        .with_state(node.clone());
    let server = AbortOnDropHandle::new(tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap()
    }));
    (state_with(ttl, read_timeout, rpc_url), node, server)
}

/// A node without sockets, for paused time: each set of calls counts itself
/// in `sets`, then answers at once, or, while `stalled` is set, holds `lost`
/// and fails after 10 s, as the node client's per-call timeout would.
#[derive(Clone, Default)]
struct StallingNode {
    sets: Arc<AtomicUsize>,
    stalled: Arc<std::sync::atomic::AtomicBool>,
    lost: Arc<()>,
}
impl StallingNode {
    fn calls(&self) -> impl FnOnce() -> BoxFuture<'static, ApiResult<NetworkAnswer>> {
        let node = self.clone();
        move || {
            async move {
                node.sets.fetch_add(1, Ordering::SeqCst);
                if node.stalled.load(Ordering::SeqCst) {
                    let _lost = node.lost.clone();
                    tokio::time::sleep(Duration::from_secs(10)).await;
                    return Err(ApiError::upstream("qbit RPC getblockchaininfo failed"));
                }
                Ok(NetworkAnswer {
                    network: expected("ab", 2),
                    degraded: false,
                })
            }
            .boxed()
        }
    }
    fn sets(&self) -> usize {
        self.sets.load(Ordering::SeqCst)
    }
}

fn expected(tip: &str, peers: u64) -> Value {
    json!({"name":"regtest","height":100,"tip_hash":tip.repeat(32),"bits":"207fffff","network_difficulty":"1000000","initial_block_download":false,"peers":peers})
}

#[test]
fn the_memo_lives_as_long_as_the_shortest_lifetime_of_its_routes() {
    let mut config = ApiConfig::default();
    assert_eq!(network_lifetime(&config), Duration::from_secs(5));
    config.cache_lifetimes.default.ttl = 20;
    config.cache_lifetimes.aggregate.ttl = 12;
    assert_eq!(network_lifetime(&config), Duration::from_secs(12));
    // Never past the leaderboard's 15 s staleness budget.
    config.cache_lifetimes.default.ttl = 60;
    config.cache_lifetimes.aggregate.ttl = 60;
    assert_eq!(network_lifetime(&config), Duration::from_secs(15));
    config.cache_lifetimes.aggregate.ttl = 0;
    assert_eq!(network_lifetime(&config), Duration::ZERO);
    config.cache_lifetimes = CacheLifetimes::default();
    config.cache_enabled = false;
    assert_eq!(network_lifetime(&config), Duration::ZERO);
}

#[tokio::test]
async fn concurrent_misses_wait_for_one_set_of_node_calls() {
    let (state, node, _server) = fake(60, Duration::from_secs(30)).await;
    node.hold();
    let waiters: Vec<_> = (0..24)
        .map(|_| {
            let state = state.clone();
            tokio::spawn(async move { network(&state).await })
        })
        .collect();
    node.until("getblockchaininfo", 1).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        waiters.iter().all(|waiter| !waiter.is_finished()),
        "every miss waits for the call in flight"
    );
    assert_eq!(node.calls("getblockchaininfo"), 1);
    node.release();
    for waiter in waiters {
        assert_eq!(waiter.await.unwrap().unwrap(), expected("ab", 2));
    }
    assert_eq!(network(&state).await.unwrap(), expected("ab", 2));
    for method in ["getblockchaininfo", "getblocktemplate", "getnetworkinfo"] {
        assert_eq!(node.calls(method), 1, "{method}");
    }
}

#[tokio::test]
async fn a_failed_call_is_not_remembered() {
    let (state, node, _server) = fake(60, Duration::from_secs(30)).await;
    node.fail_chain.store(1, Ordering::SeqCst);
    let error = network(&state).await.unwrap_err();
    assert_eq!(error.status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(error.message, "qbit RPC getblockchaininfo failed");
    assert_eq!(node.calls("getblocktemplate"), 0);
    assert_eq!(network(&state).await.unwrap(), expected("ab", 2));
    assert_eq!(
        node.calls("getblockchaininfo"),
        2,
        "the next call asks again"
    );
    assert_eq!(network(&state).await.unwrap(), expected("ab", 2));
    assert_eq!(node.calls("getblockchaininfo"), 2);
}

#[tokio::test]
async fn a_degraded_answer_is_not_remembered() {
    let (state, node, _server) = fake(60, Duration::from_secs(30)).await;
    node.fail_info.store(1, Ordering::SeqCst);
    assert_eq!(network(&state).await.unwrap(), expected("ab", 0));
    assert_eq!(network(&state).await.unwrap(), expected("ab", 2));
    assert_eq!(network(&state).await.unwrap(), expected("ab", 2));
    assert_eq!(node.calls("getnetworkinfo"), 2);
    assert_eq!(node.calls("getblocktemplate"), 2);
}

#[tokio::test]
async fn a_held_set_times_out_with_its_request_and_is_not_remembered() {
    let (state, node, _server) = fake(60, Duration::from_millis(200)).await;
    node.hold();
    let path = "/public/v1/leaderboard";
    let query = Query::parse("window=reward");
    let started = Instant::now();
    // ttl 0: no response cache. The request's own read deadline and the
    // held set's, from when the request started it, both end it at 200 ms.
    let (result, cache, _) = super::super::response_cache::public_response(
        &state,
        path,
        &query,
        &CachePolicy::default(),
        None,
    )
    .await;
    let waited = started.elapsed();
    assert_eq!(cache, "BYPASS");
    assert_eq!(result.unwrap_err().code, "read_timeout");
    assert!(
        waited >= Duration::from_millis(200) && waited < Duration::from_secs(5),
        "{waited:?}"
    );
    assert_eq!(node.calls("getblockchaininfo"), 1);
    assert_eq!(node.calls("getblocktemplate"), 0);
    // The held set has the same read timeout, from when it started, which
    // was before the waiter gave up: by 200 ms after that, it has failed and
    // been dropped, so the next miss asks the node again.
    tokio::time::sleep(Duration::from_millis(250)).await;
    node.release();
    assert_eq!(network(&state).await.unwrap(), expected("ab", 2));
    assert_eq!(node.calls("getblockchaininfo"), 2);
    assert_eq!(node.calls("getblocktemplate"), 1);
    assert_eq!(network(&state).await.unwrap(), expected("ab", 2));
    assert_eq!(node.calls("getblockchaininfo"), 2);
}

#[tokio::test]
async fn a_waiter_that_gives_up_leaves_the_shared_calls_running() {
    let (state, node, _server) = fake(60, Duration::from_secs(30)).await;
    node.hold();
    let gone = {
        let state = state.clone();
        tokio::spawn(async move { network(&state).await })
    };
    node.until("getblockchaininfo", 1).await;
    gone.abort();
    assert!(gone.await.unwrap_err().is_cancelled());
    // Nobody waits on it now, and it still finishes and is remembered.
    node.release();
    node.until("getnetworkinfo", 1).await;
    node.until("getblocktemplate", 1).await;
    assert_eq!(network(&state).await.unwrap(), expected("ab", 2));
    assert_eq!(node.calls("getblockchaininfo"), 1);
    assert_eq!(node.calls("getblocktemplate"), 1);
}

#[tokio::test]
async fn the_remembered_answer_expires_and_a_slow_call_is_never_overtaken() {
    let lifetime = Duration::from_secs(2);
    let (state, node, _server) = fake(2, Duration::from_secs(30)).await;
    assert_eq!(network(&state).await.unwrap(), expected("ab", 2));
    node.set_tip("cd");
    assert_eq!(
        network(&state).await.unwrap(),
        expected("ab", 2),
        "remembered within its lifetime"
    );
    assert_eq!(node.calls("getblockchaininfo"), 1);
    tokio::time::sleep(lifetime + Duration::from_millis(50)).await;
    // Expired: the next call asks again. Held past another lifetime, that
    // call is still the only one: a miss that arrives meanwhile waits for it
    // instead of starting a newer one it could later overwrite.
    node.hold();
    let slow = {
        let state = state.clone();
        tokio::spawn(async move { network(&state).await })
    };
    node.until("getblockchaininfo", 2).await;
    tokio::time::sleep(lifetime + Duration::from_millis(50)).await;
    let late = {
        let state = state.clone();
        tokio::spawn(async move { network(&state).await })
    };
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(node.calls("getblockchaininfo"), 2);
    node.set_tip("ef");
    node.release();
    assert_eq!(slow.await.unwrap().unwrap(), expected("ef", 2));
    assert_eq!(late.await.unwrap().unwrap(), expected("ef", 2));
    assert_eq!(node.calls("getblockchaininfo"), 2);
    assert_eq!(node.calls("getblocktemplate"), 2);
    // Its answer is dated from when its calls started, more than a lifetime
    // ago, so it answered only its own waiters and the next miss asks again.
    assert_eq!(network(&state).await.unwrap(), expected("ef", 2));
    assert_eq!(node.calls("getblockchaininfo"), 3);
    assert_eq!(network(&state).await.unwrap(), expected("ef", 2));
    assert_eq!(node.calls("getblockchaininfo"), 3);
}

#[tokio::test]
async fn a_zero_lifetime_asks_the_node_every_time() {
    let (state, node, _server) = fake(0, Duration::from_secs(30)).await;
    for _ in 0..3 {
        assert_eq!(network(&state).await.unwrap(), expected("ab", 2));
    }
    assert_eq!(node.calls("getblocktemplate"), 3);
}

#[tokio::test]
async fn the_lifetime_follows_the_configuration_a_state_serves_with() {
    let (mut state, node, _server) = fake(60, Duration::from_secs(30)).await;
    network(&state).await.unwrap();
    network(&state).await.unwrap();
    assert_eq!(node.calls("getblocktemplate"), 1);
    // A view of the same state with the public cache off, as tests and the
    // artifact admission harness make, asks the node on every call.
    state.config = Arc::new(ApiConfig {
        cache_enabled: false,
        ..(*state.config).clone()
    });
    network(&state).await.unwrap();
    network(&state).await.unwrap();
    assert_eq!(node.calls("getblocktemplate"), 3);
}

#[tokio::test(start_paused = true)]
async fn a_stalled_set_of_calls_fails_at_the_read_timeout_and_the_next_miss_asks_again() {
    let read_timeout = Duration::from_secs(4);
    let state = state_with(60, read_timeout, String::new());
    let node = StallingNode::default();
    node.stalled.store(true, Ordering::SeqCst);
    let started = tokio::time::Instant::now();
    let waiter = |state: &ApiState| {
        let (state, calls) = (state.clone(), node.calls());
        tokio::spawn(async move {
            let answer = remembered_network(&state, calls).await;
            (answer, started.elapsed())
        })
    };
    let first = waiter(&state);
    tokio::time::sleep(Duration::from_secs(1)).await;
    // The node recovers, but the set in flight stays stalled; a miss 2 s in
    // joins it rather than asking again.
    node.stalled.store(false, Ordering::SeqCst);
    tokio::time::sleep(Duration::from_secs(1)).await;
    let joined = waiter(&state);
    tokio::time::sleep(Duration::from_millis(1990)).await;
    assert!(!first.is_finished() && !joined.is_finished());
    assert_eq!(node.sets(), 1);
    // Both fail at the set's own deadline, 4 s after it started, not at the
    // node client's 10 s, and the stalled calls are dropped with it.
    for waiter in [first, joined] {
        let (answer, at) = waiter.await.unwrap();
        assert_eq!(answer.unwrap_err().code, "read_timeout");
        assert!(
            at >= read_timeout && at < read_timeout + Duration::from_millis(10),
            "{at:?}"
        );
    }
    assert_eq!(
        Arc::strong_count(&node.lost),
        1,
        "the stalled set is dropped"
    );
    // Not remembered: the next miss asks the recovered node, and its answer
    // is remembered.
    let (answer, at) = waiter(&state).await.unwrap();
    assert_eq!(answer.unwrap(), expected("ab", 2));
    assert!(at < read_timeout + Duration::from_millis(10), "{at:?}");
    assert_eq!(node.sets(), 2);
    let (answer, _) = waiter(&state).await.unwrap();
    assert_eq!(answer.unwrap(), expected("ab", 2));
    assert_eq!(node.sets(), 2);
}

#[tokio::test(start_paused = true)]
async fn without_a_read_timeout_a_set_has_no_deadline_of_its_own() {
    let state = state_with(60, Duration::ZERO, String::new());
    let node = StallingNode::default();
    node.stalled.store(true, Ordering::SeqCst);
    let started = tokio::time::Instant::now();
    let error = remembered_network(&state, node.calls()).await.unwrap_err();
    assert_eq!(error.message, "qbit RPC getblockchaininfo failed");
    assert!(started.elapsed() >= Duration::from_secs(10));
    node.stalled.store(false, Ordering::SeqCst);
    assert_eq!(
        remembered_network(&state, node.calls()).await.unwrap(),
        expected("ab", 2)
    );
    assert_eq!(node.sets(), 2);
}

#[tokio::test]
async fn an_answer_is_dated_from_when_its_calls_started() {
    // A 1 s lifetime and a first set of calls that takes longer: the chain
    // it read is past its lifetime by the time it answers, so it answers
    // only its own request, and the next miss asks again.
    let state = state_with(1, Duration::from_secs(30), String::new());
    let sets = Arc::new(AtomicUsize::new(0));
    let calls = || {
        let sets = sets.clone();
        move || {
            async move {
                if sets.fetch_add(1, Ordering::SeqCst) == 0 {
                    tokio::time::sleep(Duration::from_millis(1100)).await;
                }
                Ok(NetworkAnswer {
                    network: expected("ab", 2),
                    degraded: false,
                })
            }
            .boxed()
        }
    };
    for _ in 0..2 {
        assert_eq!(
            remembered_network(&state, calls()).await.unwrap(),
            expected("ab", 2)
        );
    }
    assert_eq!(sets.load(Ordering::SeqCst), 2);
}
