use super::*;
use axum::{extract::State, routing::post, Json, Router};
use std::sync::Mutex as StdMutex;

#[derive(Clone)]
struct FeeNode {
    estimate: Value,
    mempool: Value,
    methods: Arc<StdMutex<Vec<String>>>,
}

async fn node_reply(State(node): State<FeeNode>, Json(input): Json<Value>) -> Json<Value> {
    let method = input["method"].as_str().unwrap();
    node.methods.lock().unwrap().push(method.into());
    let result = match method {
        "estimatesmartfee" => node.estimate,
        "getmempoolinfo" => node.mempool,
        _ => panic!("unexpected fee RPC {method}"),
    };
    Json(json!({"id":input["id"],"result":result,"error":null}))
}

async fn resolve(
    configured: Option<FanoutFeeRatePolicy>,
    premium: u64,
    estimate: Value,
    mempool: Value,
) -> (Result<FanoutFeeRatePolicy>, Vec<String>) {
    let methods = Arc::new(StdMutex::new(Vec::new()));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let app = Router::new()
        .route("/", post(node_reply))
        .with_state(FeeNode {
            estimate,
            mempool,
            methods: methods.clone(),
        });
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let rpc = Rpc::new(
        format!("http://{address}/"),
        "test".into(),
        "test".into(),
        Duration::from_secs(2),
    )
    .unwrap();
    let result = validated_ctv_fee_policy(&rpc, configured, premium)
        .await
        .map(|validated| validated.policy);
    server.abort();
    let methods = methods.lock().unwrap().clone();
    (result, methods)
}

#[tokio::test]
async fn configured_ctv_fee_cannot_bypass_either_live_relay_floor() {
    for (relay, mempool) in [("0.00002", "0.00001"), ("0.00001", "0.00002")] {
        let (result, methods) = resolve(
            Some(FanoutFeeRatePolicy::new(1000, 12000)),
            12000,
            json!({"errors":["no fee estimate at genesis"]}),
            json!({"minrelaytxfee":relay,"mempoolminfee":mempool}),
        )
        .await;
        let error = result.unwrap_err().to_string();
        assert!(
            error.contains("below the connected node relay floor"),
            "{error}"
        );
        assert!(error.contains("required=2000"), "{error}");
        assert_eq!(methods, ["getmempoolinfo"]);
    }
}

#[tokio::test]
async fn valid_configured_fee_is_preserved_without_an_estimate() {
    let policy = FanoutFeeRatePolicy::new(2000, 12500);
    let (result, methods) = resolve(
        Some(policy),
        99000,
        json!({"errors":["no estimate"]}),
        serde_json::from_str(r#"{"minrelaytxfee":0.00001,"mempoolminfee":2e-5}"#).unwrap(),
    )
    .await;
    assert_eq!(result.unwrap(), policy);
    assert_eq!(methods, ["getmempoolinfo"]);
}

#[tokio::test]
async fn estimated_ctv_fee_obeys_the_same_floor_and_preserves_the_premium() {
    for (estimate, valid) in [("0.00001", false), ("0.00002", true)] {
        let (result, methods) = resolve(
            None,
            15000,
            json!({"feerate":estimate}),
            json!({"minrelaytxfee":"0.00001","mempoolminfee":"0.00002"}),
        )
        .await;
        if valid {
            assert_eq!(result.unwrap(), FanoutFeeRatePolicy::new(2000, 15000));
        } else {
            assert!(result.unwrap_err().to_string().contains("required=2000"));
        }
        assert_eq!(methods, ["estimatesmartfee", "getmempoolinfo"]);
    }
}

#[tokio::test]
async fn live_floor_rounding_keeps_arbitrary_precision_decimal_remainders() {
    for (floor, valid) in [("0.00001", true), ("0.000010000000000000000001", false)] {
        let mempool: Value =
            serde_json::from_str(&format!(r#"{{"minrelaytxfee":{floor}}}"#)).unwrap();
        let (result, _) = resolve(
            Some(FanoutFeeRatePolicy::new(1000, 12000)),
            12000,
            Value::Null,
            mempool,
        )
        .await;
        if valid {
            assert_eq!(result.unwrap().market_fee_rate_sats_per_1000_weight, 1000);
        } else {
            assert!(result.unwrap_err().to_string().contains("required=1001"));
        }
    }
}

#[tokio::test]
async fn a_discounting_premium_cannot_reduce_the_effective_fee_below_the_floor() {
    for configured in [None, Some(FanoutFeeRatePolicy::new(1000, 9999))] {
        let (result, _) = resolve(
            configured,
            9999,
            json!({"feerate":"0.00001"}),
            json!({"minrelaytxfee":"0.00001","mempoolminfee":"0.00001"}),
        )
        .await;
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("premium reduces the effective fee"));
    }
}

#[tokio::test]
async fn unavailable_or_invalid_live_floors_fail_closed() {
    for mempool in [
        Value::Null,
        json!([]),
        json!({}),
        json!({"minrelaytxfee":null,"mempoolminfee":null}),
        json!({"minrelaytxfee":"invalid","mempoolminfee":"0.00001"}),
        json!({"minrelaytxfee":"0"}),
        json!({"mempoolminfee":"-0.00001"}),
        json!({"mempoolminfee":"1e1000"}),
    ] {
        let (result, methods) = resolve(
            Some(FanoutFeeRatePolicy::new(1000, 12000)),
            12000,
            Value::Null,
            mempool.clone(),
        )
        .await;
        assert!(result.is_err(), "invalid floor was accepted: {mempool}");
        assert_eq!(methods, ["getmempoolinfo"]);
    }
    // Older nodes may expose only one of the two floor fields.
    let (result, _) = resolve(
        Some(FanoutFeeRatePolicy::new(1000, 12000)),
        12000,
        Value::Null,
        json!({"mempoolminfee":"1e-5"}),
    )
    .await;
    assert!(result.is_ok());
}
