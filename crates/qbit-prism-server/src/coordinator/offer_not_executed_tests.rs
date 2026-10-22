//! #526: which `submitblock` results prove that the node never ran the call.
//! Only a connection that was never established (#522) and a warmup answer
//! to `submitblock` itself do; every other result stays with
//! [`classify_offer`], where a failure is an unknown outcome.
use super::*;
use crate::rpc::{RpcNotSentError, RpcRelayRefused, RpcReplyError, RPC_IN_WARMUP};

/// The node's error object, as qbitd writes it into a JSON-RPC 1.0 reply.
fn reply(method: &str, error: Value) -> Result<Value> {
    Err(RpcReplyError {
        method: method.into(),
        error,
    }
    .into())
}

/// qbitd's reply to a call made while it is still starting, verbatim.
fn warmup(method: &str) -> Result<Value> {
    reply(
        method,
        json!({"code": RPC_IN_WARMUP, "message": "Loading banlist…"}),
    )
}

#[test]
fn a_warmup_answer_to_submitblock_was_not_executed() {
    let result = warmup("submitblock");
    let reason = offer_not_executed(&result).expect("a warmup answer is not executed");
    assert!(reason.contains("-28"), "{reason}");
    assert!(reason.contains("Loading banlist…"), "{reason}");
    assert!(reason.contains("warming up"), "{reason}");
}

#[test]
fn a_refused_connection_is_still_not_executed() {
    let result: Result<Value> = Err(RpcNotSentError {
        method: "submitblock".into(),
        cause: "connection refused".into(),
    }
    .into());
    let reason = offer_not_executed(&result).expect("a refused connection is not executed");
    assert!(reason.contains("connection refused"), "{reason}");
}

/// #291: a held frontend's client refuses `submitblock` before it is sent,
/// so the call provably never ran.
#[test]
fn a_relay_refused_under_the_kill_switch_was_not_executed() {
    let result: Result<Value> = Err(RpcRelayRefused {
        method: "submitblock".into(),
    }
    .into());
    let reason = offer_not_executed(&result).expect("a refused relay is not executed");
    assert!(reason.contains("PRISM_BLOCK_SUBMIT_ENABLED"), "{reason}");
}

/// Another call's warmup answer says nothing about this `submitblock`: the
/// classification is scoped to the one call it describes.
#[test]
fn a_warmup_answer_to_another_call_is_not_this_offers() {
    for method in [
        "getblockchaininfo",
        "getbestblockhash",
        "getblockheader",
        "getblock",
    ] {
        assert_eq!(offer_not_executed(&warmup(method)), None, "{method}");
    }
}

/// Every other answer to `submitblock` came from a node that ran it, or may
/// have: it stays an unknown outcome and is never offered again.
#[test]
fn every_other_submitblock_error_may_have_executed() {
    for error in [
        // RPC_MISC_ERROR: what an exception inside the call becomes.
        json!({"code": -1, "message": "block decode failed"}),
        json!({"code": -8, "message": "invalid parameter"}),
        json!({"code": -22, "message": "Block decode failed"}),
        json!({"code": -25, "message": "rejected"}),
        // RPC_CLIENT_NOT_CONNECTED: shutting down, raised inside a call.
        json!({"code": -9, "message": "Shutting down"}),
        json!({"code": -32603, "message": "internal error"}),
        // Not the integer code: never guessed at.
        json!({"code": "-28", "message": "Loading banlist…"}),
        json!({"code": -28.5, "message": "Loading banlist…"}),
        json!({"message": "Loading banlist…"}),
        json!("warming up"),
        Value::Null,
    ] {
        let result = reply("submitblock", error.clone());
        assert_eq!(offer_not_executed(&result), None, "{error}");
        assert_eq!(classify_offer(&result).0, OfferOutcome::Unknown, "{error}");
    }
}

#[test]
fn answers_and_other_failures_are_not_classified_here() {
    let transport: Result<Value> = Err(anyhow::anyhow!("qbit RPC submitblock transport failed"));
    let timeout: Result<Value> = Err(anyhow::Error::new(std::io::Error::from(
        std::io::ErrorKind::TimedOut,
    )));
    for result in [
        Ok(Value::Null),
        Ok(json!("duplicate")),
        Ok(json!({"unexpected": true})),
        transport,
        timeout,
    ] {
        assert_eq!(offer_not_executed(&result), None, "{result:?}");
    }
}
