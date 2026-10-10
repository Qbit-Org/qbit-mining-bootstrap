//! What `qbit-prism-server healthcheck` concludes from one `/healthz` answer
//! (3.1, the D5 cutover decision).
//!
//! A single writer's healthcheck is 3.0's readiness rule, unchanged. A
//! dual-writer frontend's is liveness: its `ok` stays false for as long as
//! it catches up on its own log (D-8), and a converge that waits for the
//! container to be healthy must not time out on that. Routing reads
//! readiness from the readiness endpoint (`/readyz`), not from this.
use anyhow::{bail, ensure, Context, Result};
use reqwest::StatusCode;
use serde_json::Value;

/// The healthcheck subcommand's rule. A body that carries the `dual_writer`
/// object is a dual-writer frontend's, held to [`dual_writer_liveness`];
/// anything else is held to 3.0's readiness rule, with 3.0's messages.
pub fn container_health(status: StatusCode, body: &[u8]) -> Result<()> {
    let health = serde_json::from_slice::<Value>(body);
    if let Ok(health) = &health {
        if health.get("dual_writer").is_some_and(Value::is_object) {
            return dual_writer_liveness(health);
        }
    }
    readiness(status, health)
}

/// 3.0's rule, which `self-check` keeps in either mode: HTTP success, then
/// a JSON body whose `ok` is true.
pub fn readiness(status: StatusCode, health: serde_json::Result<Value>) -> Result<()> {
    ensure!(status.is_success(), "PRISM is unhealthy (HTTP {status})");
    let health = health.context("error decoding response body")?;
    ensure!(health["ok"] == true, "PRISM health is not ready");
    Ok(())
}

/// A dual-writer frontend is alive when it answers and its only reasons not
/// to be ready are the withdrawals it is expected to recover from by
/// itself: the own-log catch-up (D-8) and a dip inside the admission grace.
/// Everything else is a fault: a stale snapshot or stalled runtime (the
/// handler's `error`), stalled job delivery, a database that is not this
/// node's local writable primary or does not answer (`writer_path`, D-9),
/// and readiness lost for longer than the grace, which is also how a
/// cluster halted at runtime shows (its payout revision is no longer
/// served). A halted cluster refuses the frontend's start, so no answer at
/// all is the other way it shows.
pub fn dual_writer_liveness(health: &Value) -> Result<()> {
    if let Some(error) = health.get("error").filter(|error| !error.is_null()) {
        bail!("PRISM is unhealthy: {error}");
    }
    let status = health["status"].as_str().unwrap_or("unknown");
    ensure!(
        !matches!(status, "runtime-stalled" | "job-delivery-stalled"),
        "PRISM is unhealthy: {status}"
    );
    let writer = &health["dual_writer"]["writer_path"];
    ensure!(
        writer == "local",
        "PRISM is unhealthy: its database is not this node's local writable primary \
         (dual_writer.writer_path {writer})"
    );
    if health["ok"] == true
        || health["dual_writer"]["own_log_caught_up"] == false
        || health["admission"]["admitting"] == true
    {
        return Ok(());
    }
    bail!(
        "PRISM is unhealthy: not ready ({status}, admission {}), outside the own-log \
         catch-up and the admission grace",
        health["admission"]["state"]
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn dual(ok: bool, caught_up: bool, writer: Value, admission: &str) -> Value {
        json!({
            "ok": ok,
            "status": if ok { "ok" } else if !caught_up { "own-log-behind" } else { "unavailable" },
            "dual_writer": {
                "node_index": 1,
                "carry_owner": false,
                "own_log_caught_up": caught_up,
                "peer_sync": {"peer_reachable": false, "own_log_caught_up": caught_up, "per_table": {}},
                "writer_path": writer,
            },
            "admission": {
                "admitting": matches!(admission, "admitting" | "grace"),
                "state": admission,
                "reason": null,
                "grace_seconds": 10,
            },
        })
    }

    fn judge(status: StatusCode, health: &Value) -> Result<()> {
        container_health(status, health.to_string().as_bytes())
    }

    const UNAVAILABLE: StatusCode = StatusCode::SERVICE_UNAVAILABLE;

    #[test]
    fn a_dual_writer_frontend_is_alive_while_ready_catching_up_or_inside_the_grace() {
        for (case, status, health) in [
            (
                "ready",
                StatusCode::OK,
                dual(true, true, json!("local"), "admitting"),
            ),
            (
                "catching up",
                UNAVAILABLE,
                dual(false, false, json!("local"), "starting"),
            ),
            (
                "catching up again after a detected rollback",
                UNAVAILABLE,
                dual(false, false, json!("local"), "withdrawn"),
            ),
            (
                "a grace dip",
                UNAVAILABLE,
                dual(false, true, json!("local"), "grace"),
            ),
        ] {
            judge(status, &health).unwrap_or_else(|error| panic!("{case}: {error:#}"));
        }
    }

    #[test]
    fn a_dual_writer_frontend_with_a_fault_is_unhealthy_whatever_else_it_says() {
        for writer in [
            json!("remote"),
            json!("unidentified"),
            json!("read_only"),
            json!("unanswered"),
            Value::Null,
        ] {
            // Even while it catches up: the database is the fault.
            let error = judge(UNAVAILABLE, &dual(false, false, writer.clone(), "starting"))
                .unwrap_err()
                .to_string();
            assert!(error.contains("writer_path"), "{writer}: {error}");
        }
        let mut stale = dual(true, true, json!("local"), "admitting");
        stale["ok"] = json!(false);
        stale["error"] = json!("health snapshot is stale");
        let error = judge(UNAVAILABLE, &stale).unwrap_err().to_string();
        assert!(error.contains("stale"), "{error}");
        let mut stalled = dual(false, false, json!("local"), "grace");
        stalled["status"] = json!("runtime-stalled");
        stalled["error"] = json!("a critical runtime task is not making progress");
        assert!(judge(UNAVAILABLE, &stalled).is_err());
        let mut delivery = dual(false, true, json!("local"), "grace");
        delivery["status"] = json!("job-delivery-stalled");
        let error = judge(UNAVAILABLE, &delivery).unwrap_err().to_string();
        assert!(error.contains("job-delivery-stalled"), "{error}");
        // Not ready past the grace, or not yet ready once caught up.
        for state in ["withdrawn", "starting"] {
            let error = judge(UNAVAILABLE, &dual(false, true, json!("local"), state))
                .unwrap_err()
                .to_string();
            assert!(error.contains("not ready"), "{state}: {error}");
        }
    }

    #[test]
    fn a_single_writer_keeps_the_readiness_rule_and_its_messages() {
        let ready = json!({"ok": true, "status": "ok"});
        judge(StatusCode::OK, &ready).unwrap();
        // The HTTP status decides first, as in 3.0, whatever the body says;
        // a single writer's grace never makes it healthy.
        let not_ready = json!({"ok": false, "status": "unavailable",
            "admission": {"admitting": true, "state": "grace"}});
        assert_eq!(
            judge(UNAVAILABLE, &not_ready).unwrap_err().to_string(),
            "PRISM is unhealthy (HTTP 503 Service Unavailable)"
        );
        assert_eq!(
            judge(StatusCode::OK, &not_ready).unwrap_err().to_string(),
            "PRISM health is not ready"
        );
        assert!(container_health(StatusCode::OK, b"not JSON").is_err());
        assert_eq!(
            container_health(UNAVAILABLE, b"not JSON")
                .unwrap_err()
                .to_string(),
            "PRISM is unhealthy (HTTP 503 Service Unavailable)"
        );
        // A `dual_writer` that is not an object is no dual-writer body.
        let odd = json!({"ok": false, "dual_writer": null});
        assert!(judge(UNAVAILABLE, &odd).is_err());
    }

    #[test]
    fn self_check_keeps_requiring_readiness_in_dual_mode() {
        let catching_up = dual(false, false, json!("local"), "starting");
        assert!(readiness(UNAVAILABLE, Ok(catching_up)).is_err());
        readiness(
            StatusCode::OK,
            Ok(dual(true, true, json!("local"), "admitting")),
        )
        .unwrap();
    }
}
