//! The stored heartbeat contract shared by server writers and diagnostics.
use super::*;
use serde::{Deserializer, Serializer};
use serde_json::{json, Map};
use sqlx::Connection;
use std::time::Duration;

/// Lifecycle markers and health snapshots retain the JSON shapes used by
/// existing deployments. Readiness is a health property, not liveness.
#[derive(Clone, Debug, PartialEq)]
pub enum HeartbeatStatus {
    Starting,
    Stopped,
    Health(HeartbeatHealth),
}

#[derive(Clone, Debug, PartialEq, Serialize)]
enum HealthSchema {
    #[serde(rename = "qbit.prism.audit-health.v1")]
    V1,
}

impl<'de> Deserialize<'de> for HealthSchema {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        // A derived unit-enum decoder also accepts an externally tagged object.
        // Existing stored rows require this exact string to count as health.
        match String::deserialize(deserializer)?.as_str() {
            "qbit.prism.audit-health.v1" => Ok(Self::V1),
            _ => Err(serde::de::Error::custom(
                "unrecognized heartbeat health schema",
            )),
        }
    }
}

/// Only the schema and boolean readiness field identify a health heartbeat.
/// Extra fields remain opaque so older and newer stored payloads coexist.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct HeartbeatHealth {
    schema: HealthSchema,
    ready: bool,
    #[serde(flatten)]
    fields: Map<String, Value>,
}

impl<'de> Deserialize<'de> for HeartbeatHealth {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        // Decode extra values directly: Serde's derived flattened-field buffer
        // cannot retain arbitrary-precision JSON numbers from older payloads.
        let mut fields = Map::<String, Value>::deserialize(deserializer)?;
        let schema = HealthSchema::deserialize(fields.remove("schema").unwrap_or(Value::Null))
            .map_err(serde::de::Error::custom)?;
        let ready = bool::deserialize(fields.remove("ready").unwrap_or(Value::Null))
            .map_err(serde::de::Error::custom)?;
        Ok(Self {
            schema,
            ready,
            fields,
        })
    }
}

impl HeartbeatHealth {
    pub fn new(ready: bool, mut fields: Map<String, Value>) -> Self {
        // The typed fields are authoritative even if supplied in the payload.
        fields.remove("schema");
        fields.remove("ready");
        Self {
            schema: HealthSchema::V1,
            ready,
            fields,
        }
    }

    pub fn into_value(self) -> Value {
        serde_json::to_value(self).expect("heartbeat health contains only JSON values")
    }
}

impl Serialize for HeartbeatStatus {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Starting => json!({"state": "starting"}).serialize(serializer),
            Self::Stopped => json!({"state": "stopped"}).serialize(serializer),
            Self::Health(health) => health.serialize(serializer),
        }
    }
}

impl<'de> Deserialize<'de> for HeartbeatStatus {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = Value::deserialize(deserializer)?;
        // Preserve the original reader's health-before-lifecycle precedence.
        if let Ok(health) = HeartbeatHealth::deserialize(&value) {
            return Ok(Self::Health(health));
        }
        match value["state"].as_str() {
            Some("starting") => Ok(Self::Starting),
            Some("stopped") => Ok(Self::Stopped),
            _ => Err(serde::de::Error::custom("unrecognized heartbeat status")),
        }
    }
}

impl Ledger {
    pub async fn heartbeat(&self, status: HeartbeatStatus) -> Result<()> {
        let mut status = serde_json::to_value(status)?;
        // The stopped marker is proof about this process incarnation only.
        // Closing admission and checking pending/active guards happen under
        // one local mutex, before awaiting SQL; no new session can race it.
        // Check the persisted marker even in a legacy mixed health payload:
        // reservation reclamation also keys off this JSON field.
        if status["state"] == "stopped" {
            self.session_owner.stop()?;
        }
        status
            .as_object_mut()
            .expect("typed heartbeat is an object")
            .insert(
                "session_owner_token".into(),
                self.session_owner.token.clone().into(),
            );
        sqlx::query("INSERT INTO qbit_prism_instances(instance_id,status) VALUES($1,$2) ON CONFLICT(instance_id) DO UPDATE SET heartbeat_at=clock_timestamp(),status=EXCLUDED.status")
            .bind(&self.instance_id).bind(status).execute(&self.pool).await?;
        Ok(())
    }
}

// Sample with the configured database's clock. The reader supplies the same
// cadence-derived freshness budget used by health and metrics snapshots.
const LIVE_INSTANCES_QUERY: &str = r#"
WITH sample AS (SELECT clock_timestamp() AS observed_at)
SELECT observed_at::text, COALESCE((
    SELECT jsonb_agg(jsonb_build_object(
        'instance_id', instance_id, 'heartbeat_at', heartbeat_at,
        'age_seconds', extract(epoch FROM (observed_at - heartbeat_at)),
        'status', status
    ) ORDER BY instance_id) FROM qbit_prism_instances
), '[]'::jsonb) FROM sample
"#;

#[derive(Serialize)]
pub(crate) struct LiveInstancesReport {
    pub(crate) status: &'static str,
    observed_at: Option<String>,
    clock: &'static str,
    freshness_seconds: f64,
    count: Option<usize>,
    instance_ids: Option<Vec<Value>>,
    instances: Option<Vec<Value>>,
    stale_instances: Option<Vec<Value>>,
    inactive_instances: Option<Vec<Value>>,
    unknown_instances: Option<Vec<Value>>,
    single_instance: Option<bool>,
    ha_warning: Option<&'static str>,
}

pub(crate) fn unavailable_live_instances(
    status: &'static str,
    warning: &'static str,
    freshness: Duration,
) -> LiveInstancesReport {
    LiveInstancesReport {
        status,
        observed_at: None,
        clock: "PostgreSQL clock_timestamp() via PRISM_DATABASE_URL",
        freshness_seconds: freshness.as_secs_f64(),
        count: None,
        instance_ids: None,
        instances: None,
        stale_instances: None,
        inactive_instances: None,
        unknown_instances: None,
        single_instance: None,
        ha_warning: Some(warning),
    }
}

pub(crate) async fn live_instances(database_url: &str, freshness: Duration) -> LiveInstancesReport {
    // A separate read-only connection avoids Ledger::connect's heartbeat write.
    // Config already resolved/validated the DSN; do not read environment again.
    let result = tokio::time::timeout(Duration::from_secs(5), async {
        let mut connection = sqlx::PgConnection::connect(database_url).await?;
        sqlx::query("SET default_transaction_read_only = on")
            .execute(&mut connection)
            .await?;
        sqlx::query_as::<_, (String, Value)>(LIVE_INSTANCES_QUERY)
            .fetch_one(&mut connection)
            .await
    })
    .await;
    match result {
        Ok(Ok((observed_at, rows))) => summarize_live_instances(freshness, &observed_at, rows),
        // Do not print connection errors: they may contain a credentialed DSN.
        _ => unavailable_live_instances(
            "failed",
            "Heartbeat read failed or exceeded 5 seconds; HA is unknown",
            freshness,
        ),
    }
}

fn summarize_live_instances(
    freshness: Duration,
    observed_at: &str,
    rows: Value,
) -> LiveInstancesReport {
    let freshness_seconds = freshness.as_secs_f64();
    let mut live = Vec::new();
    let mut stale = Vec::new();
    let mut inactive = Vec::new();
    let mut unknown = Vec::new();
    for row in rows.as_array().into_iter().flatten() {
        match row["age_seconds"].as_f64() {
            Some(age) if age > freshness_seconds => stale.push(row.clone()),
            Some(age) if age >= 0.0 => {
                match serde_json::from_value::<HeartbeatStatus>(row["status"].clone()) {
                    Ok(HeartbeatStatus::Health(_)) => live.push(row.clone()),
                    Ok(HeartbeatStatus::Starting | HeartbeatStatus::Stopped) => {
                        inactive.push(row.clone());
                    }
                    Err(_) => unknown.push(row.clone()),
                }
            }
            _ => unknown.push(row.clone()),
        }
    }
    let status = if !unknown.is_empty() {
        "unknown"
    } else if !live.is_empty() {
        "observed"
    } else if !stale.is_empty() {
        "stale"
    } else if !inactive.is_empty() {
        "inactive"
    } else {
        "empty"
    };
    LiveInstancesReport {
        status,
        observed_at: Some(observed_at.to_owned()),
        clock: "PostgreSQL clock_timestamp() via PRISM_DATABASE_URL",
        freshness_seconds,
        count: unknown.is_empty().then_some(live.len()),
        instance_ids: Some(live.iter().map(|row| row["instance_id"].clone()).collect()),
        single_instance: (unknown.is_empty() && !live.is_empty()).then_some(live.len() == 1),
        ha_warning: if !unknown.is_empty() {
            Some("Unrecognized or future-dated heartbeats; HA is unknown")
        } else if live.len() < 2 {
            Some("Fewer than two live frontends observed; do not present this deployment as HA")
        } else {
            None
        },
        instances: Some(live),
        stale_instances: Some(stale),
        inactive_instances: Some(inactive),
        unknown_instances: Some(unknown),
    }
}

#[cfg(test)]
mod live_instance_tests {
    use super::*;

    const DEFAULT_FRESHNESS: Duration = Duration::from_secs(15);

    fn row(id: &str, age: f64, status: Value) -> Value {
        json!({"instance_id":id, "age_seconds":age, "status":status})
    }

    fn health() -> Value {
        json!({"schema":"qbit.prism.audit-health.v1", "ready":false})
    }

    #[test]
    fn typed_statuses_keep_existing_storage_shapes() -> Result<()> {
        for (status, stored) in [
            (HeartbeatStatus::Starting, json!({"state":"starting"})),
            (HeartbeatStatus::Stopped, json!({"state":"stopped"})),
        ] {
            assert_eq!(serde_json::to_value(&status)?, stored);
            let mut with_owner = stored;
            with_owner["session_owner_token"] = json!("older-process");
            with_owner["extra"] = json!({"ignored":true});
            assert_eq!(
                serde_json::from_value::<HeartbeatStatus>(with_owner)?,
                status
            );
        }
        let stored: Value = serde_json::from_str(
            r#"{"schema":"qbit.prism.audit-health.v1","ready":false,"ok":false,"session_owner_token":"older-process","stratum":{"clients":2},"future_counter":18446744073709551616}"#,
        )?;
        let decoded: HeartbeatStatus = serde_json::from_value(stored.clone())?;
        assert!(matches!(decoded, HeartbeatStatus::Health(_)));
        assert_eq!(serde_json::to_value(decoded)?, stored);
        let report = summarize_live_instances(
            DEFAULT_FRESHNESS,
            "db-time",
            json!([row("legacy", 0.0, stored)]),
        );
        assert_eq!(report.count, Some(1));
        Ok(())
    }

    #[test]
    fn typed_health_owns_schema_and_readiness() {
        let fields = Map::from_iter([
            ("schema".into(), json!("other-schema")),
            ("ready".into(), json!("not-a-boolean")),
            ("stratum".into(), json!({"clients":2})),
        ]);
        assert_eq!(
            HeartbeatHealth::new(false, fields).into_value(),
            json!({"schema":"qbit.prism.audit-health.v1","ready":false,"stratum":{"clients":2}})
        );
    }

    #[test]
    fn legacy_health_takes_precedence_over_lifecycle_markers() -> Result<()> {
        let mut stored = health();
        stored["state"] = json!("stopped");
        assert!(matches!(
            serde_json::from_value::<HeartbeatStatus>(stored.clone())?,
            HeartbeatStatus::Health(_)
        ));
        let report = summarize_live_instances(
            DEFAULT_FRESHNESS,
            "db-time",
            json!([row("legacy", 1.0, stored)]),
        );
        assert_eq!(report.count, Some(1));
        assert_eq!(report.inactive_instances.unwrap().len(), 0);
        // The original reader falls back to lifecycle if health is malformed.
        assert_eq!(
            serde_json::from_value::<HeartbeatStatus>(
                json!({"schema":"future-schema","ready":"invalid","state":"starting"})
            )?,
            HeartbeatStatus::Starting
        );
        Ok(())
    }

    #[test]
    fn malformed_or_unrecognized_live_rows_make_counts_unknown() {
        for stored in [
            Value::Null,
            json!([]),
            json!({}),
            json!({"state":"other"}),
            json!({"schema":"qbit.prism.audit-health.v1"}),
            json!({"schema":"qbit.prism.audit-health.v1","ready":null}),
            json!({"schema":"qbit.prism.audit-health.v1","ready":"false"}),
            json!({"schema":"qbit.prism.audit-health.v2","ready":true}),
            json!({"schema":{"qbit.prism.audit-health.v1":null},"ready":true}),
            json!({"schema":["qbit.prism.audit-health.v1"],"ready":true}),
            json!({"schema":1,"ready":true}),
        ] {
            let report = summarize_live_instances(
                DEFAULT_FRESHNESS,
                "db-time",
                json!([
                    row("live", 1.0, health()),
                    row("unknown", 1.0, stored.clone())
                ]),
            );
            assert_eq!(report.status, "unknown", "{stored}");
            assert_eq!(report.count, None);
            assert_eq!(report.single_instance, None);
            assert_eq!(report.instances.unwrap().len(), 1);
            assert_eq!(report.unknown_instances.unwrap().len(), 1);
            // Staleness takes precedence over payload classification.
            let report = summarize_live_instances(
                DEFAULT_FRESHNESS,
                "db-time",
                json!([row("old", 16.0, stored)]),
            );
            assert_eq!(report.status, "stale");
            assert_eq!(report.count, Some(0));
        }
    }

    #[test]
    fn empty_table_is_not_live() {
        let report = summarize_live_instances(DEFAULT_FRESHNESS, "db-time", json!([]));
        assert_eq!(report.status, "empty");
        assert_eq!(report.count, Some(0));
        assert_eq!(report.single_instance, None);
    }

    #[test]
    fn stale_rows_do_not_count() {
        let report = summarize_live_instances(
            DEFAULT_FRESHNESS,
            "db-time",
            json!([row("old", 16.0, health())]),
        );
        assert_eq!(report.status, "stale");
        assert_eq!(report.count, Some(0));
        assert_eq!(report.single_instance, None);
    }

    #[test]
    fn startup_rows_are_inactive_not_live() {
        let report = summarize_live_instances(
            DEFAULT_FRESHNESS,
            "db-time",
            json!([
                row("starting", 0.0, json!({"state":"starting"})),
                row("stopped", 0.0, json!({"state":"stopped"}))
            ]),
        );
        assert_eq!(report.status, "inactive");
        assert_eq!(report.count, Some(0));
        assert_eq!(report.inactive_instances.unwrap().len(), 2);
        assert_eq!(report.single_instance, None);
    }

    #[test]
    fn inclusive_freshness_boundary_and_unready_servers_are_live() {
        let report = summarize_live_instances(
            DEFAULT_FRESHNESS,
            "db-time",
            json!([row("a", 15.0, health()), row("b", 0.0, health())]),
        );
        assert_eq!(report.status, "observed");
        assert_eq!(report.count, Some(2));
        assert_eq!(report.instance_ids, Some(vec![json!("a"), json!("b")]));
        assert_eq!(report.single_instance, Some(false));
    }

    #[test]
    fn single_live_instance_warns() {
        let report = summarize_live_instances(
            DEFAULT_FRESHNESS,
            "db-time",
            json!([row("a", 1.0, health())]),
        );
        assert_eq!(report.single_instance, Some(true));
        assert!(report.ha_warning.is_some());
    }

    #[test]
    fn slow_heartbeat_cadence_preserves_ha_until_the_freshness_boundary() {
        let freshness = crate::api::health_stale_after(Duration::from_secs(20));
        let report = summarize_live_instances(
            freshness,
            "db-time",
            json!([
                row("between-publications", 19.0, health()),
                row("boundary", 60.0, health()),
                row("expired", 60.001, health())
            ]),
        );
        assert_eq!(report.status, "observed");
        assert_eq!(report.freshness_seconds, 60.0);
        assert_eq!(report.count, Some(2));
        assert_eq!(report.single_instance, Some(false));
        assert_eq!(report.ha_warning, None);
        assert_eq!(report.stale_instances.unwrap()[0]["instance_id"], "expired");
    }

    #[test]
    fn future_dated_row_is_unknown() {
        let report = summarize_live_instances(
            DEFAULT_FRESHNESS,
            "db-time",
            json!([row("a", -1.0, health())]),
        );
        assert_eq!(report.status, "unknown");
        assert_eq!(report.count, None);
        assert_eq!(report.single_instance, None);
    }

    #[tokio::test]
    async fn failed_heartbeat_connection_is_not_zero_or_healthy() {
        let report = live_instances(
            "postgresql://127.0.0.1:0/unavailable",
            Duration::from_secs(60),
        )
        .await;
        assert_eq!(report.status, "failed");
        assert_eq!(report.count, None);
        assert_eq!(report.observed_at, None);
        assert_eq!(report.freshness_seconds, 60.0);
    }

    #[tokio::test]
    async fn heartbeat_sql_observes_empty_stale_and_missing_table() -> Result<()> {
        let Some(url) = qbit_prism_test_gate::database_url(qbit_prism_test_gate::site!())? else {
            return Ok(());
        };
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect(&url)
            .await?;
        let mut connection = pool.acquire().await?;
        // Connection-local table: never modify deployment heartbeat rows.
        sqlx::raw_sql("SET search_path = pg_temp; CREATE TEMP TABLE qbit_prism_instances (instance_id text PRIMARY KEY, heartbeat_at timestamptz NOT NULL DEFAULT clock_timestamp(), status jsonb)")
            .execute(&mut *connection).await?;
        let (at, rows) = sqlx::query_as::<_, (String, Value)>(LIVE_INSTANCES_QUERY)
            .fetch_one(&mut *connection)
            .await?;
        let report = summarize_live_instances(DEFAULT_FRESHNESS, &at, rows);
        assert_eq!(report.status, "empty");
        assert_eq!(report.count, Some(0));
        sqlx::query("INSERT INTO qbit_prism_instances VALUES ('old', clock_timestamp() - interval '1 minute', $1)")
            .bind(json!({"schema":"qbit.prism.audit-health.v1", "ready":true}))
            .execute(&mut *connection).await?;
        let (at, rows) = sqlx::query_as::<_, (String, Value)>(LIVE_INSTANCES_QUERY)
            .fetch_one(&mut *connection)
            .await?;
        let report = summarize_live_instances(DEFAULT_FRESHNESS, &at, rows);
        assert_eq!(report.status, "stale");
        assert_eq!(report.count, Some(0));
        assert_eq!(report.stale_instances.unwrap()[0]["instance_id"], "old");

        // Exercise the actual writer against the same connection-local table.
        // No schema migration or deployment heartbeat is touched by this probe.
        drop(connection);
        let ledger = Ledger::offline_for_tests(pool.clone(), "typed-writer".into());
        for status in [
            HeartbeatStatus::Starting,
            HeartbeatStatus::Health(HeartbeatHealth::new(
                false,
                Map::from_iter([("session_owner_token".into(), json!("forged"))]),
            )),
            HeartbeatStatus::Stopped,
        ] {
            ledger.heartbeat(status.clone()).await?;
            let stored: Value = sqlx::query_scalar(
                "SELECT status FROM qbit_prism_instances WHERE instance_id='typed-writer'",
            )
            .fetch_one(&pool)
            .await?;
            assert_eq!(stored["session_owner_token"], ledger.session_owner.token);
            let mut expected = serde_json::to_value(&status)?;
            expected["session_owner_token"] = json!(ledger.session_owner.token);
            assert_eq!(stored, expected);
            let (at, rows) = sqlx::query_as::<_, (String, Value)>(LIVE_INSTANCES_QUERY)
                .fetch_one(&pool)
                .await?;
            let report = summarize_live_instances(DEFAULT_FRESHNESS, &at, rows);
            assert_eq!(
                report.count,
                Some(usize::from(matches!(status, HeartbeatStatus::Health(_))))
            );
            assert_eq!(report.stale_instances.unwrap().len(), 1);
        }
        let mut connection = pool.acquire().await?;
        sqlx::query("DROP TABLE qbit_prism_instances")
            .execute(&mut *connection)
            .await?;
        assert!(sqlx::query_as::<_, (String, Value)>(LIVE_INSTANCES_QUERY)
            .fetch_one(&mut *connection)
            .await
            .is_err());
        drop(connection);
        pool.close().await;
        Ok(())
    }
}
