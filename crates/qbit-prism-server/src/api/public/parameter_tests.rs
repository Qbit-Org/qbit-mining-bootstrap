//! The query parameters each public route reads, against [`parameters`],
//! the ones its origin cache key is made of, and against the ones the
//! OpenAPI document gives it.
use super::*;
use axum::{body::to_bytes, http::Request};
use std::collections::BTreeSet;
use tower::ServiceExt;

const OPENAPI: &str = include_str!("../../../../../docs/public-dashboard-api-v1.openapi.yaml");

/// Each path `docs/public-dashboard-api-v1.openapi.yaml` documents, under
/// `/public/v1`, with the query parameters it documents for it.
fn documented() -> BTreeMap<String, BTreeSet<String>> {
    let mut routes = BTreeMap::<String, BTreeSet<String>>::new();
    let mut route = None;
    let mut name = None;
    for line in OPENAPI.lines() {
        if line == "components:" {
            break;
        }
        if let Some(path) = line.strip_prefix("  /").and_then(|p| p.strip_suffix(':')) {
            let path = format!("/public/v1/{path}");
            routes.insert(path.clone(), BTreeSet::new());
            route = Some(path);
            continue;
        }
        let Some(route) = &route else {
            continue;
        };
        // The document names each inline parameter on the line before its
        // `in:`; anything else fails here rather than be read wrongly.
        let line = line.trim();
        let parameter =
            if let Some(component) = line.strip_prefix("- $ref: \"#/components/parameters/") {
                match component {
                    "Page\"" => "page",
                    "Limit\"" => "limit",
                    other => panic!("no test mapping for the parameter component {other}"),
                }
            } else if let Some(place) = line.strip_prefix("in: ") {
                let parameter = name
                    .take()
                    .expect("a parameter without a name before its `in:`");
                if place != "query" {
                    continue;
                }
                parameter
            } else {
                if let Some(parameter) = line.strip_prefix("- name: ") {
                    name = Some(parameter);
                }
                continue;
            };
        routes.get_mut(route).unwrap().insert(parameter.to_owned());
    }
    assert_eq!(routes.len(), 14, "the document's paths were not all found");
    routes
}

/// `path` with its documented identifiers filled in.
fn concrete(path: &str) -> String {
    path.replace("{recipient_id}", "bob")
        .replace("{block_hash}", &"ab".repeat(32))
        .replace("{fanout_txid}", &"cd".repeat(32))
        .replace("{sha256}", &"ef".repeat(32))
}

/// Every documented public route, with concrete identifiers, and unknown
/// endpoints under `/public/v1`.
fn routes() -> Vec<String> {
    let mut routes: Vec<String> = documented().keys().map(|path| concrete(path)).collect();
    routes.extend(
        [
            "/public/v1",
            "/public/v1/unknown",
            "/public/v1/miners/bob/unknown",
            "/public/v1/miners/bob/workers/unknown",
        ]
        .map(String::from),
    );
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
    without_database(ApiState::new(
        sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://invalid@127.0.0.1:1/invalid")
            .unwrap(),
        ApiConfig {
            rpc_url: "http://127.0.0.1:1/".into(),
            cache_debug_headers: true,
            ..ApiConfig::default()
        },
        Arc::new(crate::metrics::Metrics::default()),
    ))
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
/// parameters its cache key holds, and those are the ones the OpenAPI
/// document gives the route. One it asked for beyond them would be one its
/// answer depends on but its key does not; one it never asked for would
/// split its cache for nothing.
///
/// Each route runs as it is served, with only its own parameters, once with
/// none, once with every parameter set, and once with the reward
/// leaderboard's window alone. Its computation ends at its first ledger or
/// node read, which every route reaches only after reading its parameters. A
/// parameter read after that goes unseen here, but [`Query::get`]'s debug
/// assertion fails any test that runs the route with a database.
#[tokio::test]
async fn every_public_route_reads_exactly_the_parameters_its_cache_key_holds() {
    let state = state();
    let documented = documented();
    for path in routes() {
        let declared: BTreeSet<String> = parameters(&path).iter().map(|p| p.to_string()).collect();
        if let Some((_, given)) = documented.iter().find(|(doc, _)| concrete(doc) == path) {
            assert_eq!(
                &declared, given,
                "{path}: the OpenAPI document gives it {given:?}"
            );
        }
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

/// For every parameter of every route, with an answer remembered for each of
/// two values under the key its request gets, each request is served its
/// own value's answer: two requests that differ only in a parameter the route
/// reads never share an entry. A third, with the first one's value, then the
/// second's repeated after it, and a parameter no route reads, is served the
/// first one's: a route reads only a parameter's first value.
#[tokio::test]
async fn requests_that_differ_in_a_parameter_their_route_reads_get_different_entries() {
    let mut cases = 0;
    for path in routes() {
        for parameter in parameters(&path) {
            let state = state();
            let policy = CachePolicy::for_path(&path, &state.config);
            let [one, other] = two_values(parameter);
            for value in [one, other] {
                let key = cache_key(&path, &Query::parse(&format!("{parameter}={value}")));
                state.cache.lock().await.insert(
                    key,
                    super::super::response_cache::CacheSlot::holding(Payload::json(
                        json!({ "value": value }),
                    )),
                );
            }
            assert_eq!(state.cache.lock().await.len(), 2, "{path} {parameter}");
            for (raw, value) in [
                (format!("{parameter}={one}"), one),
                (format!("{parameter}={other}"), other),
                (format!("{parameter}={one}&junk=1&{parameter}={other}"), one),
            ] {
                let (result, cache, _) = super::super::response_cache::public_response(
                    &state,
                    &path,
                    &Query::parse(&raw),
                    &policy,
                    None,
                )
                .await;
                assert_eq!(cache, "HIT", "{path}?{raw}");
                assert_eq!(
                    serde_json::from_slice::<Value>(&result.unwrap().bytes).unwrap()["value"],
                    value,
                    "{path}?{raw}"
                );
            }
            cases += 1;
        }
    }
    assert_eq!(
        cases,
        documented().values().map(BTreeSet::len).sum::<usize>(),
        "every documented parameter of every route"
    );
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
