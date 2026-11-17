//! The query parameters each public route reads, against [`parameters`],
//! the ones its origin cache key is made of.
use super::*;
use axum::{body::to_bytes, http::Request};
use std::collections::BTreeSet;
use tower::ServiceExt;

/// Every public route `dispatch` answers, with concrete identifiers, and the
/// unknown endpoints under it.
fn routes() -> Vec<String> {
    let hash = "ab".repeat(32);
    let mut routes: Vec<String> = [
        "/public/v1/pool-summary",
        "/public/v1/mining-configuration",
        "/public/v1/blocks",
        "/public/v1/leaderboard",
        "/public/v1/hashrate-series",
        "/public/v1/block-markers",
        "/public/v1/fanouts/pending",
        "/public/v1/miners/bob",
        "/public/v1/miners/bob/earnings",
        "/public/v1/miners/bob/payouts",
        "/public/v1/miners/bob/workers",
        "/public/v1",
        "/public/v1/unknown",
        "/public/v1/miners/bob/unknown",
        "/public/v1/miners/bob/workers/unknown",
    ]
    .map(String::from)
    .into();
    routes.extend([
        format!("/public/v1/blocks/{hash}/settlement-artifacts"),
        format!("/public/v1/fanouts/{hash}"),
        format!("/public/v1/artifacts/{hash}"),
    ]);
    routes
}

/// Every parameter any route reads, each set to an accepted value other than
/// its default so a route takes its non-default branches, and one no route
/// reads.
const EVERY_PARAMETER: &str = "page=2&limit=3&chain_state=all&search=b&recipient_id=bob\
&window=reward&range=1w&bucket=5m&subject=miner:bob&view=both&hide_inactive=false&junk=1";

/// A state whose node refuses connections and whose ledger reads fail at
/// once, so a route's computation ends at its first read of either.
fn state() -> ApiState {
    let reads = sqlx::postgres::PgPoolOptions::new()
        .acquire_timeout(Duration::from_millis(10))
        .connect_lazy("postgres://invalid@127.0.0.1:1/invalid")
        .unwrap();
    let mut state = ApiState::new(
        reads.clone(),
        ApiConfig {
            rpc_url: "http://127.0.0.1:1/".into(),
            cache_debug_headers: true,
            ..ApiConfig::default()
        },
        Arc::new(crate::metrics::Metrics::default()),
    );
    state.public_pool = reads;
    state
}

/// The parameter names `dispatch` asks `query` for on `path`.
async fn reads(state: &ApiState, path: &str, query: &Query) -> BTreeSet<String> {
    QUERY_READS
        .scope(Default::default(), async {
            let _ = dispatch(state, path, query).await;
            QUERY_READS.with(std::cell::RefCell::take)
        })
        .await
}

/// The drift test: on every route, the handler asks for exactly the
/// parameters its cache key holds. One it asked for beyond them would be
/// one its answer depends on but its key does not; one it never asked for
/// would split its cache for nothing.
///
/// Each route runs as it is served, with only its own parameters, once with
/// none and once with every parameter set, and with the reward leaderboard's
/// window alone. Its computation ends at its first ledger or node read, which
/// every route reaches only after reading its parameters; a parameter read
/// after that would go unseen here, and, absent from the key, would also be
/// absent from what the route is given.
#[tokio::test]
async fn every_public_route_reads_exactly_the_parameters_its_cache_key_holds() {
    let state = state();
    for path in routes() {
        let declared: BTreeSet<String> = parameters(&path).iter().map(|p| p.to_string()).collect();
        let mut read = BTreeSet::new();
        for raw in ["", EVERY_PARAMETER, "window=reward"] {
            read.extend(reads(&state, &path, &Query::parse(raw).only(parameters(&path))).await);
        }
        assert_eq!(
            read, declared,
            "{path} reads {read:?}, but its cache key holds {declared:?}"
        );
    }
}

/// Two accepted values of each parameter, or one and its absence (`""`).
fn two_values(parameter: &str) -> [&'static str; 2] {
    match parameter {
        "page" => ["1", "2"],
        "limit" => ["15", "20"],
        "chain_state" => ["all", "reversed"],
        "search" | "recipient_id" => ["alice", "bob"],
        "window" => ["3h", "reward"],
        "range" => ["1w", "1m"],
        "bucket" => ["1h", "1d"],
        "subject" => ["pool", "miner:bob"],
        "view" => ["both", ""],
        "hide_inactive" => ["true", "false"],
        other => panic!("no test values for the parameter {other}"),
    }
}

/// For every parameter of every route, two requests that differ only in it
/// get two cache entries, and a third with the first one's value, repeated
/// with the second's after it, and a parameter no route reads, gets the first
/// one's: a route reads only a parameter's first value.
#[tokio::test]
async fn requests_that_differ_in_a_parameter_their_route_reads_get_different_entries() {
    let mut cases = 0;
    for path in routes() {
        for parameter in parameters(&path) {
            let state = state();
            let policy = CachePolicy::for_path(&path, &state.config);
            let [one, other] = two_values(parameter);
            let mut keys = Vec::new();
            for raw in [
                format!("{parameter}={one}"),
                format!("{parameter}={other}"),
                format!("{parameter}={one}&junk=1&{parameter}={other}"),
            ] {
                let _ = super::super::response_cache::public_response(
                    &state,
                    &path,
                    &Query::parse(&raw),
                    &policy,
                    None,
                )
                .await;
                keys.push(state.cache.lock().await.keys().cloned().collect::<Vec<_>>());
            }
            assert_eq!(keys[1].len(), 2, "{path} {parameter}: {keys:?}");
            assert_eq!(keys[2], keys[1], "{path} {parameter}: {keys:?}");
            cases += 1;
        }
    }
    assert_eq!(cases, 24, "every parameter of every route");
}

/// Requests that differ only in parameters their route does not read, one a
/// parameter another route reads, share its entry: the first misses and
/// every later one hits it, with the same bytes.
#[tokio::test]
async fn junk_parameters_share_the_route_entry_and_the_second_request_hits() {
    let state = state();
    let app = router(state.clone());
    let mut bodies = Vec::new();
    for (n, query) in [
        "x=0",
        "x=1",
        "x=2&utm_source=feed",
        "page=0",
        "_=1763337600",
    ]
    .into_iter()
    .enumerate()
    {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/public/v1/mining-configuration?{query}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK, "{query}");
        assert_eq!(
            response.headers()["x-prism-public-cache"],
            if n == 0 { "MISS" } else { "HIT" },
            "{query}"
        );
        bodies.push(to_bytes(response.into_body(), usize::MAX).await.unwrap());
    }
    assert!(bodies.windows(2).all(|pair| pair[0] == pair[1]));
    assert_eq!(state.cache.lock().await.len(), 1);
}

/// A parameter a route reads is still validated as before, whatever else
/// the request carries.
#[tokio::test]
async fn a_route_still_refuses_a_bad_value_of_a_parameter_it_reads() {
    let app = router(state());
    for (uri, message) in [
        ("/public/v1/blocks?page=0&junk=1", "page must be >= 1"),
        (
            "/public/v1/hashrate-series?junk=1&range=all&bucket=5m",
            "bucket 5m is not allowed for range all; allowed: 1d",
        ),
    ] {
        let response = app
            .clone()
            .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{uri}");
        let body: Value =
            serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        assert_eq!(body["error"]["message"], message, "{uri}");
    }
}
