use super::*;
use chrono::DateTime;
use num_bigint::BigUint;
use num_traits::Zero;

pub(super) fn big(v: &Value) -> BigUint {
    v.as_str()
        .map(str::to_string)
        .unwrap_or_else(|| v.to_string())
        .parse()
        .unwrap_or_default()
}
pub(super) fn big_json(n: BigUint) -> Value {
    serde_json::from_str(&n.to_string()).expect("integer")
}
pub(super) fn number(v: &Value) -> Value {
    if v.is_string() {
        serde_json::from_str(v.as_str().unwrap()).unwrap_or(Value::Null)
    } else {
        v.clone()
    }
}
pub(super) fn numeric_fields(value: &mut Value, fields: &[&str]) {
    if let Some(rows) = value.as_array_mut() {
        for row in rows {
            for field in fields {
                if row.get(*field).is_some() {
                    row[*field] = number(&row[*field]);
                }
            }
        }
    }
}
fn round_even(n: BigUint, d: BigUint) -> BigUint {
    let q = &n / &d;
    let rem = &n % &d;
    let twice = rem * 2u8;
    if twice > d || twice == d && (&q % 2u8) != BigUint::zero() {
        q + 1u8
    } else {
        q
    }
}
pub(super) fn ratio(n: BigUint, d: BigUint) -> String {
    if n.is_zero() || d.is_zero() {
        return "0".into();
    }
    let ten = BigUint::from(10u8);
    let mut exponent = n.to_string().len() as i32 - d.to_string().len() as i32;
    let below = if exponent >= 0 {
        n < (&d * ten.pow(exponent as u32))
    } else {
        (&n * ten.pow((-exponent) as u32)) < d
    };
    if below {
        exponent -= 1;
    }
    let places = 27 - exponent;
    let value = if places >= 0 {
        round_even(n * ten.pow(places as u32), d)
    } else {
        round_even(n, d * ten.pow((-places) as u32))
    };
    if places <= 0 {
        return format!("{}{}", value, "0".repeat((-places) as usize));
    }
    let text = format!("{:0>width$}", value, width = places as usize + 1);
    let split = text.len() - places as usize;
    let fraction = text[split..].trim_end_matches('0');
    if fraction.is_empty() {
        text[..split].into()
    } else {
        format!("{}.{fraction}", &text[..split])
    }
}
pub(super) fn normalized_decimal(value: &Value) -> Value {
    match value.as_str() {
        Some(v) if v.contains('.') => json!(v.trim_end_matches('0').trim_end_matches('.')),
        _ => value.clone(),
    }
}
/// Qbit uses a 207fffff pow limit and millionths of its own difficulty unit.
pub(super) fn hashes_per_scaled_unit() -> (BigUint, BigUint) {
    (
        BigUint::from(1u8) << 256usize,
        (BigUint::from(0x7fffffu64) << 232usize) * 1_000_000u64,
    )
}
pub(super) fn hashrate(d: &Value, seconds: u64) -> Value {
    let (n, den) = hashes_per_scaled_unit();
    json!(ratio(
        big(d) * n,
        den * seconds.max(1) * 1_000_000_000_000u64
    ))
}
pub(super) fn eta(hashrate: &Value, difficulty: &Value) -> Value {
    let Some(rate) = hashrate.as_str() else {
        return Value::Null;
    };
    let (whole, fraction) = rate.split_once('.').unwrap_or((rate, ""));
    let Ok(rate_n) = format!("{whole}{fraction}").parse::<BigUint>() else {
        return Value::Null;
    };
    if rate_n.is_zero() {
        return Value::Null;
    }
    let rate_d = BigUint::from(10u8).pow(fraction.len() as u32);
    let (hash_n, hash_d) = hashes_per_scaled_unit();
    big_json(round_even(
        big(difficulty) * hash_n * rate_d,
        hash_d * rate_n * 1_000_000_000_000u64,
    ))
}
pub(super) fn pagination(page: i64, limit: i64, total: i64) -> Value {
    json!({"page":page,"limit":limit,"total_count":total,"total_pages":if total==0 {0}else{(total-1)/limit+1}})
}
pub(super) fn page_payload(mut value: Value, page: i64, limit: i64) -> Value {
    value["pagination"] = pagination(page, limit, value["total_count"].as_i64().unwrap_or(0));
    value.as_object_mut().unwrap().remove("total_count");
    value
}
pub(super) fn timestamp(v: &Value) -> Value {
    let Some(s) = v.as_str() else {
        return Value::Null;
    };
    DateTime::parse_from_rfc3339(s)
        .or_else(|_| DateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f%#z"))
        .map(|d| json!(d.to_rfc3339_opts(SecondsFormat::Secs, true)))
        .unwrap_or(Value::Null)
}
pub(super) fn link(prefix: &Option<String>, id: &Value) -> Value {
    match (prefix, id.as_str().filter(|s| !s.is_empty())) {
        (Some(p), Some(id)) => json!(format!("{}/{id}", p.trim_end_matches('/'))),
        _ => Value::Null,
    }
}
pub(super) async fn owed(state: &ApiState) -> ApiResult<Value> {
    Ok(sqlx::query_scalar("SELECT COALESCE(jsonb_agg(jsonb_build_object('recipient_id',miner_id,'order_key',payout_order_key,'p2mr_program_hex',encode(p2mr_program,'hex'),'balance_sats',owed_balance_sats) ORDER BY payout_order_key,miner_id),'[]'::jsonb) FROM qbit_current_owed_balances()").fetch_one(&state.pool).await?)
}
pub(super) async fn audit_payouts(state: &ApiState, hash: &str) -> ApiResult<Value> {
    let mut rows: Value = sqlx::query_scalar(include_str!("queries/audit_block_payouts.sql"))
        .bind(hash)
        .fetch_one(&state.pool)
        .await?;
    if rows.as_array().is_none_or(Vec::is_empty) {
        return Err(ApiError::missing("unknown PRISM block"));
    }
    numeric_fields(&mut rows, &["carry_forward_balance_sats"]);
    Ok(
        json!({"schema":"qbit.prism.audit-block-payouts.v1","ledger_backend":"postgres-native","block_hash":hash,"rows":rows}),
    )
}
pub(super) async fn bundle(state: &ApiState, id: &str, commitment: bool) -> ApiResult<Value> {
    let sql = if commitment {
        include_str!("queries/audit_bundle_by_commitment.sql")
    } else {
        include_str!("queries/audit_bundle.sql")
    };
    let mut value: Value = sqlx::query_scalar(sql)
        .bind(id)
        .fetch_one(&state.pool)
        .await?;
    if value.is_null() {
        return Err(ApiError::missing(if commitment {
            "unknown PRISM audit commitment"
        } else {
            "unknown PRISM block"
        }));
    }
    crate::ledger::materialize_audit_row(&state.pool, &mut value)
        .await
        .map_err(|error| {
            tracing::warn!(%error,"audit snapshot reconstruction failed");
            ApiError::internal()
        })?;
    if !value["share_snapshot_sha256"].is_null() {
        let body = value["audit_bundle"].clone();
        let expected = value["audit_bundle_sha256"]
            .as_str()
            .ok_or_else(ApiError::internal)?
            .to_string();
        tokio::task::spawn_blocking(move || -> ApiResult<()> {
            let bundle: qbit_prism::AuditBundle =
                serde_json::from_value(body).map_err(|_| ApiError::internal())?;
            let canonical = qbit_prism::canonical_audit_bundle_bytes(&bundle)
                .map_err(|_| ApiError::internal())?;
            if hex::encode(Sha256::digest(&canonical)) != expected {
                return Err(ApiError::internal());
            }
            Ok(())
        })
        .await
        .map_err(|_| ApiError::internal())??;
    }
    if value["audit_bundle"].is_null() {
        let uri = value["body_uri"]
            .as_str()
            .ok_or_else(ApiError::internal)?
            .to_string();
        let expected = value["audit_bundle_sha256"]
            .as_str()
            .ok_or_else(ApiError::internal)?
            .to_string();
        value["audit_bundle"] = tokio::task::spawn_blocking(move || -> ApiResult<Value> {
            let path = std::path::Path::new(uri.strip_prefix("file://").unwrap_or(&uri));
            let bytes = std::fs::read(path)
                .map_err(|_| ApiError::missing("audit bundle body is not retrievable"))?;
            let body: Value = serde_json::from_slice(&bytes)
                .map_err(|_| ApiError::missing("audit bundle body is not valid JSON"))?;
            if matches!(
                body["schema"].as_str(),
                Some(qbit_prism::AUDIT_BODY_REF_SCHEMA | qbit_prism::AUDIT_BUNDLE_V2_SCHEMA)
            ) {
                let parsed = qbit_prism::parse_audit_bundle_value(body, path.parent())
                    .map_err(|_| ApiError::missing("audit bundle body is not retrievable"))?;
                let canonical = qbit_prism::canonical_audit_bundle_bytes(&parsed)
                    .map_err(|_| ApiError::internal())?;
                if hex::encode(Sha256::digest(&canonical)) != expected {
                    return Err(ApiError::missing("audit bundle body hash mismatch"));
                }
                serde_json::to_value(parsed).map_err(|_| ApiError::internal())
            } else {
                if hex::encode(Sha256::digest(&bytes)) != expected {
                    return Err(ApiError::missing("audit bundle body hash mismatch"));
                }
                Ok(body)
            }
        })
        .await
        .map_err(|_| ApiError::internal())??;
    }
    value.as_object_mut().unwrap().remove("body_uri");
    value
        .as_object_mut()
        .unwrap()
        .remove("share_snapshot_sha256");
    Ok(value)
}
pub(super) async fn manifest_set(state: &ApiState, hash: &str) -> ApiResult<Value> {
    let v: Value = sqlx::query_scalar("SELECT qbit_audit_block_fanouts($1)")
        .bind(hash)
        .fetch_one(&state.pool)
        .await?;
    if v.is_null() {
        Err(ApiError::missing("unknown CTV fanout block"))
    } else {
        Ok(v)
    }
}
pub(super) async fn fanout(state: &ApiState, hash: &str) -> ApiResult<Value> {
    let v: Value = sqlx::query_scalar("SELECT qbit_fanout_status($1)")
        .bind(hash)
        .fetch_one(&state.pool)
        .await?;
    if v.is_null() {
        Err(ApiError::missing("unknown CTV fanout"))
    } else {
        Ok(v)
    }
}
pub(super) async fn pending_fanouts(state: &ApiState, page: i64, limit: i64) -> ApiResult<Value> {
    let value:Value=sqlx::query_scalar("WITH eligible AS (SELECT a.fanout_txid,a.chunk_index,b.block_height FROM qbit_ctv_fanout_artifacts a JOIN qbit_pool_blocks b USING(block_hash) WHERE a.settlement_status NOT IN ('confirmed','reorged','failed') AND (a.next_broadcast_attempt_at IS NULL OR a.next_broadcast_attempt_at <= clock_timestamp())), page AS (SELECT * FROM eligible ORDER BY block_height,chunk_index,fanout_txid LIMIT $1 OFFSET $2) SELECT jsonb_build_object('total_count',(SELECT count(*) FROM eligible),'rows',COALESCE((SELECT jsonb_agg(qbit_fanout_status(fanout_txid) ORDER BY block_height,chunk_index,fanout_txid) FROM page),'[]'::jsonb))").bind(limit).bind((page-1)*limit).fetch_one(&state.pool).await?;
    Ok(page_payload(value, page, limit))
}
pub(super) async fn artifact(state: &ApiState, hash: &str) -> ApiResult<Value> {
    let v:Option<Value>=sqlx::query_scalar("SELECT artifact FROM (SELECT manifest_set AS artifact FROM qbit_ctv_fanout_sets WHERE manifest_set_sha256=$1 UNION ALL SELECT manifest FROM qbit_ctv_fanout_artifacts WHERE manifest_sha256=$1) t LIMIT 1").bind(hash).fetch_optional(&state.pool).await?;
    if let Some(value) = v {
        return Ok(value);
    }
    let hash: Option<String> = sqlx::query_scalar(
        "SELECT block_hash FROM qbit_pool_audit_bundles WHERE audit_bundle_sha256=$1 LIMIT 1",
    )
    .bind(hash)
    .fetch_optional(&state.pool)
    .await?;
    if let Some(hash) = hash {
        return Ok(bundle(state, &hash, false)
            .await
            .map_err(|_| ApiError::internal())?["audit_bundle"]
            .take());
    }
    Err(ApiError::missing("unknown public PRISM artifact"))
}
pub(super) async fn blocks(state: &ApiState, page: i64, limit: i64) -> ApiResult<Value> {
    let mut value: Value = sqlx::query_scalar(include_str!("queries/dashboard_blocks.sql"))
        .bind(limit)
        .bind((page - 1) * limit)
        .fetch_one(&state.pool)
        .await?;
    for row in value["rows"].as_array_mut().unwrap() {
        row["explorer_url"] = link(&state.config.explorer_block_url, &row["hash"]);
    }
    Ok(page_payload(value, page, limit))
}
pub(super) async fn workers(
    state: &ApiState,
    id: &str,
    page: i64,
    limit: i64,
    search: Option<&str>,
    hide: bool,
) -> ApiResult<Value> {
    let mut value: Value =
        sqlx::query_scalar(include_str!("queries/dashboard_miner_worker_rows.sql"))
            .bind(id)
            .bind(search)
            .bind(hide)
            .bind(limit)
            .bind((page - 1) * limit)
            .fetch_one(&state.pool)
            .await?;
    for row in value["rows"].as_array_mut().unwrap() {
        row["hashrate_ths_60s"] = hashrate(&row["m1_difficulty"], 60);
        row["hashrate_ths_3h"] = hashrate(&row["h3_difficulty"], 10800);
        row.as_object_mut().unwrap().remove("m1_difficulty");
        row.as_object_mut().unwrap().remove("h3_difficulty");
    }
    Ok(page_payload(value, page, limit))
}
pub(super) async fn payouts(
    state: &ApiState,
    id: &str,
    page: i64,
    limit: i64,
    earnings: bool,
) -> ApiResult<Value> {
    let sql = if earnings {
        include_str!("queries/dashboard_miner_earning_rows.sql")
    } else {
        include_str!("queries/dashboard_miner_payout_rows.sql")
    };
    let mut value: Value = sqlx::query_scalar(sql)
        .bind(id)
        .bind(limit)
        .bind((page - 1) * limit)
        .fetch_one(&state.pool)
        .await?;
    for row in value["rows"].as_array_mut().unwrap() {
        *row = if earnings {
            let gross = big(&row["gross_amount_sats"]);
            let fee = big(&row["settlement_fee_sats"]);
            let net = if gross >= fee {
                &gross - &fee
            } else {
                BigUint::zero()
            };
            json!({"block_height":row["block_height"],"block_hash":row["block_hash"],"found_at":timestamp(&row["found_at"]),"reward_share_percent":normalized_decimal(&row["reward_share_percent"]),"gross_earning_bits":big_json(gross),"settlement_fee_bits":big_json(fee),"net_earning_bits":big_json(net),"maturity_state":row["maturity_state"],"settlement_artifacts_url":format!("/public/v1/blocks/{}/settlement-artifacts",row["block_hash"].as_str().unwrap_or("")),"explorer_url":link(&state.config.explorer_block_url,&row["block_hash"])})
        } else {
            let onchain = row["action"] == "onchain";
            let fanout = onchain && row["fanout_txid"].as_str().is_some_and(|s| !s.is_empty());
            let txid = if fanout {
                row["fanout_txid"].clone()
            } else if onchain {
                row["coinbase_txid"].clone()
            } else {
                Value::Null
            };
            json!({"block_height":row["block_height"],"block_hash":row["block_hash"],"created_at":timestamp(&row["created_at"]),"transaction_id":txid,"transaction_kind":if fanout{"ctv_fanout"}else if !txid.is_null(){"coinbase"}else{"carry_forward"},"onchain_amount_bits":if fanout{row["fanout_amount_sats"].clone()}else{row["onchain_amount_sats"].clone()},"carry_forward_balance_bits":number(&row["carry_forward_balance_sats"]),"action":row["action"],"maturity_state":row["maturity_state"],"explorer_url":link(&state.config.explorer_tx_url,&txid)})
        };
    }
    Ok(page_payload(value, page, limit))
}
pub(super) async fn reward_leaderboard(
    state: &ApiState,
    difficulty: &str,
    page: i64,
    limit: i64,
    search: Option<&str>,
    recipient: Option<&str>,
) -> ApiResult<Value> {
    let value: Value = sqlx::query_scalar(include_str!("queries/dashboard_reward_leaderboard.sql"))
        .bind(difficulty)
        .bind(search)
        .bind(recipient)
        .bind(limit)
        .bind((page - 1) * limit)
        .fetch_one(&state.pool)
        .await?;
    let span = value["observed_span_seconds"].as_u64();
    let counted = big(&value["counted_window_weight"]);
    let requested = difficulty.parse::<BigUint>().unwrap_or_default() * 8u8;
    let pool_hashrate = span
        .filter(|n| *n > 0)
        .map(|n| hashrate(&value["counted_window_weight"], n))
        .unwrap_or(Value::Null);
    let mut rows = value["rows"].clone();
    for row in rows.as_array_mut().unwrap() {
        row["share_percent"] = normalized_decimal(&row["share_percent"]);
        row["hashrate_ths"] = span
            .filter(|n| *n > 0)
            .map(|n| hashrate(&row["counted_share_difficulty"], n))
            .unwrap_or(Value::Null);
    }
    Ok(
        json!({"schema":"prism.dashboard.leaderboard.v2","generated_at":value["ended_at"],"window":{"id":"reward","started_at":value["oldest_share_accepted_at"],"ended_at":value["ended_at"],"observed_span_seconds":value["observed_span_seconds"],"network_difficulty":difficulty,"window_multiplier":8,"requested_window_weight":requested.to_string(),"counted_window_weight":counted.to_string(),"included_share_count":value["included_share_count"],"is_complete":!requested.is_zero()&&counted>=requested},"totals":{"pool_hashrate_ths":pool_hashrate,"pool_counted_share_difficulty":counted.to_string(),"participant_count":value["participant_count"],"expected_time_to_block_seconds":eta(&pool_hashrate,&json!(difficulty))},"pagination":pagination(page,limit,value["total_count"].as_i64().unwrap_or(0)),"rows":rows}),
    )
}
pub(super) async fn leaderboard(
    state: &ApiState,
    page: i64,
    limit: i64,
    search: Option<&str>,
) -> ApiResult<Value> {
    let value: Value = sqlx::query_scalar(include_str!("queries/dashboard_leaderboard.sql"))
        .bind(search)
        .bind(limit)
        .bind((page - 1) * limit)
        .fetch_one(&state.pool)
        .await?;
    let mut rows = value["rows"].clone();
    for row in rows.as_array_mut().unwrap() {
        row["hashrate_ths_3h"] = hashrate(&row["accepted_share_difficulty"], 10800);
        row["share_percent"] = normalized_decimal(&row["share_percent"]);
        row["hash_percent"] = row["share_percent"].clone();
        row.as_object_mut()
            .unwrap()
            .remove("accepted_share_difficulty");
    }
    Ok(
        json!({"schema":"prism.dashboard.leaderboard.v1","generated_at":now(),"window":{"id":"3h","started_at":value["started_at"],"ended_at":value["ended_at"]},"totals":{"pool_hashrate_ths":hashrate(&value["total_difficulty"],10800),"pool_accepted_share_difficulty":value["total_difficulty"],"participant_count":value["participant_count"]},"pagination":pagination(page,limit,value["participant_count"].as_i64().unwrap_or(0)),"rows":rows}),
    )
}
pub(super) async fn hashrate_series(
    state: &ApiState,
    subject: Option<&str>,
    range: &str,
    bucket: &str,
) -> ApiResult<Value> {
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
    let value: Value = sqlx::query_scalar(include_str!("queries/dashboard_hashrate_series.sql"))
        .bind(seconds)
        .bind(range_seconds.map(|n: i64| n + context))
        .bind(epoch as f64)
        .bind(subject)
        .fetch_one(&state.pool)
        .await?;
    let mut points = Vec::new();
    let mut history = std::collections::VecDeque::<(i64, BigUint)>::new();
    let mut total = BigUint::zero();
    for mut row in value.as_array().cloned().unwrap_or_default() {
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
        row["hashrate_ths"] = if context > 0 {
            hashrate(&json!(total.to_string()), context as u64)
        } else {
            hashrate(&row["accepted_share_difficulty"], seconds as u64)
        };
        if range_seconds.is_none_or(|n| at >= ((epoch - n + seconds - 1) / seconds) * seconds) {
            points.push(row);
        }
    }
    Ok(
        json!({"schema":"prism.dashboard.hashrate-series.v1","generated_at":now(),"subject":{"type":if subject.is_some(){"miner"}else{"pool"},"id":subject},"range":range,"bucket":bucket,"unit":"ths","points":points}),
    )
}

pub(super) async fn latest_evidence(state: &ApiState) -> ApiResult<Value> {
    let hash:Option<String>=sqlx::query_scalar("SELECT a.block_hash FROM qbit_pool_audit_bundles a JOIN qbit_pool_blocks b USING(block_hash) WHERE b.chain_state='confirmed' ORDER BY b.block_height DESC,a.created_at DESC LIMIT 1").fetch_optional(&state.pool).await?;
    let hash = hash.ok_or_else(|| ApiError::missing("no PRISM evidence has been produced"))?;
    let body = bundle(state, &hash, false).await?;
    let counts:Value=sqlx::query_scalar("SELECT jsonb_build_object('accepted_share_count',count(*),'distinct_miner_count',count(DISTINCT miner_id)) FROM qbit_share_ledger WHERE accepted").fetch_one(&state.pool).await?;
    let payout_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM qbit_pool_payout_entries WHERE block_hash=$1")
            .bind(&hash)
            .fetch_one(&state.pool)
            .await?;
    Ok(
        json!({"schema":"qbit.prism.live-stratum-evidence.v1","block_hash":hash,"block_height":body["block_height"],"coinbase_tx_hex":body["coinbase_tx_hex"],"audit_bundle_path":format!("/audit/blocks/{hash}/bundle"),"audit_report":{"audit_bundle_sha256":body["audit_bundle_sha256"],"payout_manifest_sha256":body["payout_manifest_sha256"]},"ledger_backend":"postgres-native","persistence":{"backend":"postgres-native","payout_entry_count":payout_count},"confirmation":{"block_hash":hash,"chain_state":"confirmed"},"ctv_persistence":null,"accepted_share_count":counts["accepted_share_count"],"distinct_miner_count":counts["distinct_miner_count"],"job_share_count":body["audit_bundle"]["shares"].as_array().map_or(0,Vec::len)}),
    )
}
