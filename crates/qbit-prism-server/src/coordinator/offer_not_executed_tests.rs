//! #526: which `submitblock` results prove that the node never ran the call.
//! Only a connection that was never established (#522), a request hyper
//! handed back unwritten (#816), a warmup answer to `submitblock` itself and
//! qbitd's own pre-dispatch HTTP 503 refusal of it do; every other result
//! stays with [`classify_offer`], where a failure is an unknown outcome.
use super::*;
use crate::rpc::{
    NotDispatched, NotSentStage, RpcNotDispatchedError, RpcNotSentError, RpcRelayRefused,
    RpcReplyError, RPC_IN_WARMUP,
};

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
        stage: NotSentStage::Connect,
        cause: "connection refused".into(),
    }
    .into());
    let reason = offer_not_executed(&result).expect("a refused connection is not executed");
    assert!(reason.contains("connection refused"), "{reason}");
}

/// #816: a request hyper handed back unwritten, because the connection it was
/// given closed first, never reached the node either: the reservation goes
/// back to `pending` and the block is offered again.
#[test]
fn a_request_returned_unwritten_is_not_executed() {
    let result: Result<Value> = Err(RpcNotSentError {
        method: "submitblock".into(),
        stage: NotSentStage::Returned,
        cause: "connection closed".into(),
    }
    .into());
    let reason = offer_not_executed(&result).expect("a request returned unwritten is not executed");
    assert!(
        reason.contains("the connection closed before the request was written (connection closed)"),
        "{reason}"
    );
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

/// qbitd refused the `submitblock` with one of its two pre-dispatch 503
/// replies, a full work queue or a server shutting down: the node never ran
/// it, so the reservation goes back to `pending`. The same refusal of
/// another call says nothing about this one.
#[test]
fn a_submitblock_refused_before_dispatch_was_not_executed() {
    for (refusal, wording) in [
        (NotDispatched::WorkQueueFull, "Work queue depth exceeded"),
        (NotDispatched::ShuttingDown, "shutting down"),
    ] {
        let refused = |method: &str| -> Result<Value> {
            Err(RpcNotDispatchedError {
                method: method.into(),
                refusal,
            }
            .into())
        };
        let reason = offer_not_executed(&refused("submitblock"))
            .unwrap_or_else(|| panic!("{refusal:?} was taken for a call that may have run"));
        assert!(
            reason.contains("refused before dispatch (HTTP 503") && reason.contains(wording),
            "{reason}"
        );
        assert_eq!(offer_not_executed(&refused("sendrawtransaction")), None);
    }
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
    // Any other 503: a proxy's page, possibly after it forwarded the call.
    let unavailable: Result<Value> = Err(anyhow::anyhow!(
        "qbit RPC submitblock returned invalid JSON (HTTP 503 Service Unavailable)"
    ));
    let timeout: Result<Value> = Err(anyhow::Error::new(std::io::Error::from(
        std::io::ErrorKind::TimedOut,
    )));
    for result in [
        Ok(Value::Null),
        Ok(json!("duplicate")),
        Ok(json!({"unexpected": true})),
        transport,
        unavailable,
        timeout,
    ] {
        assert_eq!(offer_not_executed(&result), None, "{result:?}");
    }
}
