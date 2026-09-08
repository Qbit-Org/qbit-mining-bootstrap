use super::{read_models::*, *};
use chrono::DateTime;
use num_bigint::BigUint;
use num_traits::Zero;

/// The node returns decimal H/s. Preserve its decimal digits and apply the
/// dashboard's 28-significant-digit, ties-to-even formatting in TH/s.
pub(super) fn network_hashrate(value: &Value) -> Option<String> {
    let text = match value {
        Value::Number(number) => number.to_string(),
        Value::String(text) => text.clone(),
        _ => return None,
    };
    if text.len() > 4096 {
        return None;
    }
    let decimal = text.parse::<serde_json::Number>().ok()?.to_string();
    let (mantissa, exponent) = decimal.split_once(['e', 'E']).unwrap_or((&decimal, "0"));
    let exponent = exponent.parse::<i32>().ok()?;
    if !(-4096..=4096).contains(&exponent) {
        return None;
    }
    let (whole, fraction) = mantissa
        .trim_start_matches('-')
        .split_once('.')
        .unwrap_or((mantissa.trim_start_matches('-'), ""));
    let numerator = format!("{whole}{fraction}").parse::<BigUint>().ok()?;
    if decimal.starts_with('-') && !numerator.is_zero() {
        return None;
    }
    let power = exponent - fraction.len() as i32 - 12;
    let ten = BigUint::from(10u8);
    Some(if power >= 0 {
        ratio(numerator * ten.pow(power as u32), BigUint::from(1u8))
    } else {
        ratio(numerator, ten.pow((-power) as u32))
    })
}

fn chart_parameters(range: &str, bucket: &str) -> (i64, Option<i64>) {
    let seconds = match bucket {
        "5m" => 300,
        "1h" => 3600,
        _ => 86400,
    };
    let range_seconds = match range {
        "1w" => Some(7 * 86400),
        "1m" => Some(30 * 86400),
        "6m" => Some(180 * 86400),
        _ => None,
    };
    (seconds, range_seconds)
}

pub(super) async fn hashrate_series(
    state: &ApiState,
    subject: Option<&str>,
    range: &str,
    bucket: &str,
    dual: bool,
) -> ApiResult<Value> {
    let (seconds, range_seconds) = chart_parameters(range, bucket);
    let smoothing = std::env::var("PRISM_PUBLIC_HASHRATE_SMOOTHING_SECONDS")
        .ok()
        .and_then(|v| v.trim().parse::<i64>().ok())
        .unwrap_or(1800)
        .clamp(0, 86400);
    let context = if smoothing / seconds >= 2 {
        (smoothing / seconds) * seconds
    } else {
        0
    };
    let epoch = Utc::now().timestamp();
    // The original v1 coarse series anchors on the DB clock and retains its
    // leading partial bucket. V2 and smoothed views share one application anchor.
    let anchor = (dual || context > 0).then_some(epoch);
    let rollups_present: bool = sqlx::query_scalar("SELECT to_regclass('qbit_hashrate_rollup_progress') IS NOT NULL AND to_regclass('qbit_hashrate_rollup_pool') IS NOT NULL AND to_regclass('qbit_hashrate_rollup_miner') IS NOT NULL")
        .fetch_one(&state.pool).await?;
    let sql = if rollups_present {
        include_str!("queries/dashboard_hashrate_rollups.sql")
    } else {
        include_str!("queries/dashboard_hashrate_series.sql")
    };
    let value: Value = sqlx::query_scalar(sql)
        .bind(seconds)
        .bind(range_seconds.map(|n| n + context))
        .bind(anchor.map(|n| n as f64))
        .bind(subject)
        .fetch_one(&state.pool)
        .await?;
    Ok(series_payload(
        value,
        subject,
        range,
        bucket,
        seconds,
        range_seconds,
        context,
        epoch,
        dual,
    ))
}

#[allow(clippy::too_many_arguments)]
fn series_payload(
    value: Value,
    subject: Option<&str>,
    range: &str,
    bucket: &str,
    seconds: i64,
    range_seconds: Option<i64>,
    context: i64,
    epoch: i64,
    dual: bool,
) -> Value {
    let mut points = Vec::new();
    let mut history = std::collections::VecDeque::<(i64, BigUint)>::new();
    let mut total = BigUint::zero();
    let mut rows = value.as_array().cloned().unwrap_or_default();
    rows.sort_by(|a, b| a["timestamp"].as_str().cmp(&b["timestamp"].as_str()));
    for mut row in rows {
        let Some(at) = row["timestamp"]
            .as_str()
            .and_then(|v| DateTime::parse_from_rfc3339(v).ok())
            .map(|v| v.timestamp())
        else {
            continue;
        };
        let difficulty = big(&row["accepted_share_difficulty"]);
        total += &difficulty;
        history.push_back((at, difficulty));
        while history.front().is_some_and(|(old, _)| *old <= at - context) {
            let (_, d) = history.pop_front().unwrap();
            total -= d;
        }
        let raw = hashrate(&row["accepted_share_difficulty"], seconds as u64);
        let smoothed = if context > 0 {
            hashrate(&json!(total.to_string()), context as u64)
        } else {
            raw.clone()
        };
        if (dual || context > 0)
            && range_seconds.is_some_and(|n| {
                at < (epoch - n).div_euclid(seconds) * seconds
                    + if (epoch - n).rem_euclid(seconds) == 0 {
                        0
                    } else {
                        seconds
                    }
            })
        {
            continue;
        }
        if dual {
            points.push(json!({"timestamp":row["timestamp"],"raw_hashrate_ths":raw,"smoothed_hashrate_ths":smoothed,"accepted_share_count":row["accepted_share_count"],"accepted_share_difficulty":row["accepted_share_difficulty"],"complete":epoch>=at+seconds}));
        } else {
            row["hashrate_ths"] = smoothed;
            points.push(row);
        }
    }
    let mut payload = json!({"schema":if dual {"prism.dashboard.hashrate-series.v2"} else {"prism.dashboard.hashrate-series.v1"},"generated_at":DateTime::from_timestamp(epoch,0).unwrap().to_rfc3339_opts(SecondsFormat::Secs,true),"subject":{"type":if subject.is_some(){"miner"}else{"pool"},"id":subject},"range":range,"bucket":bucket,"unit":"ths","points":points});
    if dual {
        payload["bucket_seconds"] = json!(seconds);
        payload["rate_basis"] = json!("accepted_share_difficulty");
        payload["smoothing"] = json!({"method":if context>0 {"trailing"} else {"none"},"window_seconds":if context>0 {context} else {seconds}});
    }
    payload
}

pub(super) async fn block_markers(state: &ApiState, range: &str, bucket: &str) -> ApiResult<Value> {
    let (seconds, range_seconds) = chart_parameters(range, bucket);
    let epoch = Utc::now().timestamp();
    let lower = range_seconds.map(|span| {
        let start = epoch - span;
        start.div_euclid(seconds) * seconds
            + if start.rem_euclid(seconds) == 0 {
                0
            } else {
                seconds
            }
    });
    let mut payload: Value =
        sqlx::query_scalar(include_str!("queries/dashboard_block_markers.sql"))
            .bind(seconds)
            .bind(lower.map(|v| v as f64))
            .fetch_one(&state.pool)
            .await?;
    payload["schema"] = json!("prism.dashboard.block-markers.v1");
    payload["generated_at"] = json!(DateTime::from_timestamp(epoch, 0)
        .unwrap()
        .to_rfc3339_opts(SecondsFormat::Secs, true));
    payload["range"] = json!(range);
    payload["bucket"] = json!(bucket);
    payload["bucket_seconds"] = json!(seconds);
    Ok(payload)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn network_rate_is_exact_nullable_and_nonnegative() {
        assert_eq!(
            network_hashrate(&json!("1234567890123.125")),
            Some("1.234567890123125".into())
        );
        assert_eq!(network_hashrate(&json!("1e12")), Some("1".into()));
        assert_eq!(network_hashrate(&json!(0)), Some("0".into()));
        for value in [
            Value::Null,
            json!(true),
            json!(-1),
            json!("nan"),
            json!("1e10000"),
        ] {
            assert_eq!(network_hashrate(&value), None);
        }
    }
    #[test]
    fn dual_rates_keep_raw_credit_and_gap_aware_trailing_rates() {
        let at = |epoch| {
            DateTime::from_timestamp(epoch, 0)
                .unwrap()
                .to_rfc3339_opts(SecondsFormat::Secs, true)
        };
        let rows = json!([{"timestamp":at(600),"accepted_share_count":2,"accepted_share_difficulty":"2000000"},{"timestamp":at(1500),"accepted_share_count":1,"accepted_share_difficulty":"1000000"}]);
        let output = series_payload(rows, None, "all", "5m", 300, None, 1800, 1600, true);
        assert_eq!(
            output["smoothing"],
            json!({"method":"trailing","window_seconds":1800})
        );
        assert_eq!(
            output["points"][1]["raw_hashrate_ths"],
            hashrate(&json!("1000000"), 300)
        );
        assert_eq!(
            output["points"][1]["smoothed_hashrate_ths"],
            hashrate(&json!("3000000"), 1800)
        );
        assert_eq!(output["points"][0]["complete"], true);
        assert_eq!(output["points"][1]["complete"], false);
    }
    #[test]
    fn only_v2_or_smoothed_series_trim_a_partial_leading_bucket() {
        let rows = json!([{"timestamp":"1970-01-01T00:00:00Z","accepted_share_count":1,"accepted_share_difficulty":"1000000"}]);
        let v1 = series_payload(
            rows.clone(),
            None,
            "1m",
            "1h",
            3600,
            Some(3600),
            0,
            3700,
            false,
        );
        let v2 = series_payload(rows, None, "1m", "1h", 3600, Some(3600), 0, 3700, true);
        assert_eq!(v1["points"].as_array().unwrap().len(), 1);
        assert_eq!(v2["points"], json!([]));
    }
}
