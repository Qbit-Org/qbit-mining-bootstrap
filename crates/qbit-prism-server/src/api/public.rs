use super::read_models::*;
use super::*;
use num_bigint::BigUint;
use num_traits::Zero;

pub(super) async fn dispatch(state: &ApiState, path: &str, q: &Query) -> ApiResult<Value> {
    match path {
        "/public/v1/pool-summary" => return pool_summary(state).await,
        "/public/v1/mining-configuration" => return Ok(mining_configuration(&state.config)),
        "/public/v1/blocks" => {
            let (p, l) = q.page()?;
            return Ok(wrap("blocks", blocks(state, p, l).await?));
        }
        "/public/v1/leaderboard" => {
            let (p, l) = q.page()?;
            let search = q.search()?;
            let id = q.get("recipient_id");
            match q.get("window").unwrap_or("3h") {
                "3h" => {
                    if id.is_some() {
                        return Err(ApiError::bad("recipient_id requires window=reward"));
                    }
                    return leaderboard(state, p, l, search).await;
                }
                "reward" => {
                    if search.is_some() && id.is_some() {
                        return Err(ApiError::bad(
                            "search and recipient_id are mutually exclusive",
                        ));
                    }
                    let id = id.map(recipient).transpose()?;
                    let (network, _) = network(state).await?;
                    return reward_leaderboard(
                        state,
                        network["network_difficulty"].as_str().unwrap(),
                        p,
                        l,
                        search,
                        id.as_deref(),
                    )
                    .await;
                }
                _ => return Err(ApiError::bad("window must be one of 3h, reward")),
            }
        }
        "/public/v1/hashrate-series" => {
            let range = q.get("range").unwrap_or("1m");
            if !matches!(range, "1w" | "1m" | "6m" | "all") {
                return Err(ApiError::bad("range must be one of 1w, 1m, 6m, all"));
            }
            let bucket = match q.get("bucket").unwrap_or("auto") {
                "auto" => {
                    if matches!(range, "1w" | "1m") {
                        "1h"
                    } else {
                        "1d"
                    }
                }
                b @ ("5m" | "1h" | "1d") => b,
                _ => return Err(ApiError::bad("bucket must be one of auto, 5m, 1h, 1d")),
            };
            let subject = match q.get("subject").unwrap_or("pool") {
                "pool" => None,
                s => Some(
                    s.strip_prefix("miner:")
                        .filter(|v| !v.is_empty())
                        .ok_or_else(|| {
                            ApiError::bad("subject must be pool or miner:{recipient_id}")
                        })?,
                ),
            };
            return hashrate_series(state, subject, range, bucket).await;
        }
        "/public/v1/fanouts/pending" => {
            let (p, l) = q.page()?;
            let mut value = pending_fanouts(state, p, l).await?;
            for row in value["rows"].as_array_mut().unwrap() {
                *row = fanout_row(state, row)?;
            }
            return Ok(wrap("pending-fanouts", value));
        }
        _ => {}
    }
    if let Some(suffix) = path.strip_prefix("/public/v1/miners/") {
        let mut parts = suffix.split('/');
        let id = recipient(parts.next().unwrap_or(""))?;
        let action = parts.next();
        if parts.next().is_some() {
            return Err(ApiError::missing("unknown public dashboard endpoint"));
        }
        match action {
            None => return miner(state, &id).await,
            Some("earnings" | "payouts") => {
                let (p, l) = q.page()?;
                let kind = action.unwrap();
                let mut value = wrap(
                    &format!("miner-{kind}"),
                    payouts(state, &id, p, l, kind == "earnings").await?,
                );
                value["recipient_id"] = json!(id);
                return Ok(value);
            }
            Some("workers") => {
                let (p, l) = q.page()?;
                let hide = matches!(
                    q.get("hide_inactive")
                        .unwrap_or("true")
                        .to_ascii_lowercase()
                        .as_str(),
                    "1" | "true" | "yes" | "on"
                );
                let mut value = workers(state, &id, p, l, q.search()?, hide).await?;
                value.as_object_mut().unwrap().remove("active_count");
                value["recipient_id"] = json!(id);
                return Ok(wrap("miner-workers", value));
            }
            _ => return Err(ApiError::missing("unknown public dashboard endpoint")),
        }
    }
    if let Some(hash) = path
        .strip_prefix("/public/v1/blocks/")
        .and_then(|p| p.strip_suffix("/settlement-artifacts"))
    {
        return settlement_artifacts(state, &clean_hash(hash, "hash")?).await;
    }
    if let Some(hash) = path.strip_prefix("/public/v1/fanouts/") {
        let hash = clean_hash(hash, "fanout txid")?;
        return Ok(wrap(
            "fanout",
            json!({"fanout":fanout_row(state,&fanout(state,&hash).await?)?}),
        ));
    }
    if let Some(hash) = path.strip_prefix("/public/v1/artifacts/") {
        return artifact(state, &clean_hash(hash, "artifact sha256")?).await;
    }
    Err(ApiError::missing("unknown public dashboard endpoint"))
}
fn wrap(kind: &str, mut value: Value) -> Value {
    value["schema"] = json!(format!("prism.dashboard.{kind}.v1"));
    value["generated_at"] = json!(now());
    value
}

pub(super) fn scaled_difficulty(bits: &str) -> Option<BigUint> {
    if bits.len() != 8 {
        return None;
    }
    let compact = u32::from_str_radix(bits, 16).ok()?;
    let exponent = (compact >> 24) as usize;
    let mantissa = compact & 0x007fffff;
    if compact & 0x00800000 != 0 || mantissa == 0 || exponent > 34 {
        return None;
    }
    let target = if exponent <= 3 {
        BigUint::from(mantissa >> (8 * (3 - exponent)))
    } else {
        BigUint::from(mantissa) << (8 * (exponent - 3))
    };
    if target.is_zero() || target.bits() > 256 {
        return None;
    }
    Some(
        (((BigUint::from(0x7fffffu64) << 232usize) * 1_000_000u64) / target)
            .max(BigUint::from(1u8)),
    )
}
async fn network(state: &ApiState) -> ApiResult<(Value, Value)> {
    let chain = state.rpc("getblockchaininfo", json!([])).await?;
    if !chain.is_object() {
        return Err(ApiError::upstream(
            "qbit RPC getblockchaininfo returned an invalid payload",
        ));
    }
    let name = chain["chain"].as_str().unwrap_or("qbit");
    let rules = if name.to_ascii_lowercase().contains("signet") {
        json!(["segwit", "signet"])
    } else {
        json!(["segwit"])
    };
    let (template, info) = tokio::join!(
        state.rpc("getblocktemplate", json!([{"rules":rules}])),
        state.rpc("getnetworkinfo", json!([]))
    );
    let template = template.unwrap_or_else(|_| json!({}));
    let info = info.unwrap_or_else(|_| json!({}));
    let (bits, difficulty) = [template.get("bits"), chain.get("bits")]
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .find_map(|b| scaled_difficulty(b).map(|d| (b.to_ascii_lowercase(), d)))
        .ok_or_else(|| ApiError::upstream("qbit RPC did not provide valid compact bits"))?;
    let counter = |v: &Value| {
        v.as_u64()
            .or_else(|| v.as_str().and_then(|v| v.parse::<u64>().ok()))
    };
    let height = counter(&chain["blocks"])
        .or_else(|| counter(&chain["headers"]))
        .unwrap_or(0);
    Ok((
        json!({"name":name,"height":height,"tip_hash":chain["bestblockhash"].as_str().unwrap_or(&"0".repeat(64)).to_ascii_lowercase(),"bits":bits,"network_difficulty":difficulty.to_string(),"initial_block_download":chain["initialblockdownload"].as_bool().unwrap_or(false),"peers":counter(&info["connections"]).unwrap_or(0)}),
        template,
    ))
}
async fn pool_summary(state: &ApiState) -> ApiResult<Value> {
    let (network, _) = network(state).await?;
    let value: Value = sqlx::query_scalar(include_str!("queries/dashboard_pool_snapshot.sql"))
        .bind(network["network_difficulty"].as_str().unwrap())
        .fetch_one(&state.pool)
        .await?;
    let h3 = hashrate(&value["h3_difficulty"], 10800);
    Ok(wrap(
        "pool-summary",
        json!({"network":network,"pool":{"name":state.config.pool_name,"hashrate_ths":{"h1":hashrate(&value["h1_difficulty"],3600),"h3":h3,"h24":hashrate(&value["h24_difficulty"],86400)},"participants_3h":value["participants_3h"],"blocks_found_total":value["blocks_found_total"],"prism_blocks_total":value["prism_blocks_total"],"total_mined_bits":value["total_mined_bits"],"expected_time_to_block_seconds":eta(&h3,&network["network_difficulty"]),"latest_block":value["latest_block"],"reward_window":{"window_multiplier":8,"requested_window_weight":(big(&network["network_difficulty"])*8u8).to_string(),"oldest_share_accepted_at":value["oldest_share_accepted_at"],"newest_share_accepted_at":value["newest_share_accepted_at"],"included_share_count":value["included_share_count"]}}}),
    ))
}
fn mining_configuration(config: &ApiConfig) -> Value {
    let host = if config.stratum_host.contains(':') && !config.stratum_host.starts_with('[') {
        format!("[{}]", config.stratum_host)
    } else {
        config.stratum_host.clone()
    };
    let primary = std::env::var("PRISM_PUBLIC_STRATUM_URL")
        .ok()
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| format!("stratum+tcp://{host}:{}", config.stratum_port));
    let endpoint = |label: &str, uri: String, fallback: u16| {
        let port = url::Url::parse(&uri)
            .ok()
            .and_then(|u| u.port())
            .unwrap_or(fallback);
        json!({"label":label,"url":uri,"protocol":"stratum_v1","default_port":port})
    };
    let mut endpoints = vec![endpoint("Primary", primary.clone(), config.stratum_port)];
    if let Some(port) = std::env::var("PRISM_STRATUM_HIGHDIFF_PORT")
        .ok()
        .and_then(|v| v.parse::<u16>().ok())
        .filter(|v| *v > 0)
    {
        let highdiff = std::env::var("PRISM_PUBLIC_STRATUM_HIGHDIFF_URL")
            .ok()
            .filter(|v| !v.is_empty())
            .or_else(|| {
                let mut uri = url::Url::parse(&primary).ok()?;
                uri.set_port(Some(port)).ok()?;
                Some(uri.to_string().trim_end_matches('/').to_string())
            })
            .unwrap_or_else(|| format!("stratum+tcp://{host}:{port}"));
        endpoints.push(endpoint("High-diff", highdiff, port));
    }
    wrap(
        "mining-configuration",
        json!({"active_configuration_id":"default","configurations":[{"id":"default","label":env("PRISM_PUBLIC_CONFIGURATION_LABEL","PRISM default"),"description":env("PRISM_PUBLIC_CONFIGURATION_DESCRIPTION","Default PRISM Stratum endpoint using the pool's current block template and payout policy."),"pool_fee_bps":config.pool_fee_bps,"block_template_policy":env("PRISM_PUBLIC_BLOCK_TEMPLATE_POLICY","pool-selected qbit block template with PRISM payout settlement"),"stratum_endpoints":endpoints}]}),
    )
}
async fn miner(state: &ApiState, id: &str) -> ApiResult<Value> {
    let (network, _) = network(state).await?;
    let summary: Value =
        sqlx::query_scalar(include_str!("queries/dashboard_miner_share_summary.sql"))
            .bind(id)
            .fetch_one(&state.pool)
            .await?;
    let reward = reward_leaderboard(
        state,
        network["network_difficulty"].as_str().unwrap(),
        1,
        1,
        None,
        Some(id),
    )
    .await?;
    let share = reward["rows"]
        .as_array()
        .and_then(|rows| rows.first())
        .map(|r| r["share_percent"].clone())
        .unwrap_or(Value::Null);
    let balance:Value=sqlx::query_scalar("SELECT jsonb_build_object('owed',COALESCE((SELECT sum(owed_balance_sats) FROM qbit_current_owed_balances() WHERE miner_id=$1),0),'lifetime',COALESCE((SELECT sum(c.gross_amount_sats) FROM qbit_payout_carry_forward c JOIN qbit_pool_blocks b USING(block_hash) WHERE c.miner_id=$1 AND c.maturity_state<>'reversed' AND b.chain_state='confirmed' AND b.maturity_state<>'reversed'),0),'pending',COALESCE((SELECT sum(GREATEST(c.onchain_amount_sats-c.settlement_fee_sats,0)) FROM qbit_payout_carry_forward c JOIN qbit_pool_blocks b USING(block_hash) WHERE c.miner_id=$1 AND c.action='onchain' AND c.maturity_state='immature' AND b.chain_state='confirmed' AND b.maturity_state='immature'),0))").bind(id).fetch_one(&state.pool).await?;
    let workers = workers(state, id, 1, 5, None, false).await?;
    let payouts = payouts(state, id, 1, 5, false).await?;
    let expected=sqlx::query_scalar::<_,Value>("SELECT COALESCE(to_jsonb(COALESCE(a.found_block_coinbase_value_sats,(a.audit_bundle#>>'{found_block,coinbase_value_sats}')::bigint)),'null'::jsonb) FROM qbit_pool_blocks b LEFT JOIN qbit_pool_audit_bundles a USING(block_hash) WHERE b.chain_state='confirmed' ORDER BY b.block_height DESC,b.found_at DESC LIMIT 1").fetch_optional(&state.pool).await?.filter(|v|big(v)>BigUint::zero()).unwrap_or(Value::Null);
    let estimated = if !share.is_null() && !expected.is_null() {
        let part = reward["rows"]
            .as_array()
            .and_then(|r| r.first())
            .map(|r| big(&r["counted_share_difficulty"]))
            .unwrap_or_default();
        let total = big(&reward["totals"]["pool_counted_share_difficulty"]);
        if total.is_zero() {
            Value::Null
        } else {
            big_json(
                big(&expected) * part * (10000u16 - state.config.pool_fee_bps) / (total * 10000u32),
            )
        }
    } else {
        Value::Null
    };
    Ok(wrap(
        "miner",
        json!({"recipient_id":id,"display_name":null,"owed_balance_bits":balance["owed"],"lifetime_earnings_bits":balance["lifetime"],"pending_maturity_bits":balance["pending"],"unpaid_earnings_bits":balance["owed"],"minimum_payout_bits":state.config.minimum_payout_bits,"hashrate_ths":{"m1":hashrate(&summary["m1_difficulty"],60),"m5":hashrate(&summary["m5_difficulty"],300),"m10":hashrate(&summary["m10_difficulty"],600),"h3":hashrate(&summary["h3_difficulty"],10800),"h24":hashrate(&summary["h24_difficulty"],86400)},"shares":{"accepted_3h":summary["accepted_3h"],"accepted_difficulty_3h":summary["h3_difficulty"],"last_share_at":summary["last_share_at"]},"estimated_next_block":{"share_percent":share,"estimated_reward_bits":estimated},"estimated_time_to_minimum_payout_seconds":if big(&balance["owed"])>=BigUint::from(state.config.minimum_payout_bits){json!(0)}else{Value::Null},"reward_window_percent":share,"workers_currently_hashing":workers["active_count"],"workers":workers["rows"],"recent_payouts":payouts["rows"]}),
    ))
}
pub(super) fn fanout_row(state: &ApiState, row: &Value) -> ApiResult<Value> {
    let txid = clean_hash(row["fanout_txid"].as_str().unwrap_or(""), "fanout txid")
        .map_err(|_| ApiError::internal())?;
    let status = row["settlement_status"]
        .as_str()
        .or_else(|| row["status"].as_str())
        .unwrap_or("awaiting_maturity");
    let fee = row["fanout_fee_sats"].as_u64().unwrap_or_else(|| {
        row["covenant_output_value_sats"]
            .as_u64()
            .unwrap_or(0)
            .saturating_sub(row["fanout_output_sum_sats"].as_u64().unwrap_or(0))
    });
    let height = row["block_height"].as_u64().unwrap_or(0);
    let manifest = row["manifest_sha256"].as_str().unwrap_or("");
    Ok(
        json!({"fanout_txid":txid,"block_hash":row["block_hash"],"block_height":height,"status":status,"broadcastable_at_height":row["broadcastable_at_height"].as_u64().or_else(||if height>0{Some(height+1000)}else{None}),"manifest_set_sha256":row["manifest_set_sha256"],"manifest_sha256":manifest,"manifest_url":if manifest.is_empty(){Value::Null}else{json!(format!("/public/v1/artifacts/{manifest}"))},"audit_bundle_sha256":row["audit_bundle_sha256"],"parent_coinbase_txid":row["parent_coinbase_txid"],"parent_coinbase_vout":row["parent_coinbase_vout"],"anchor_vout":row["anchor_vout"],"covenant_output_value_bits":row["covenant_output_value_sats"],"fanout_output_sum_bits":row["fanout_output_sum_sats"],"fanout_fee_bits":fee,"fanout_tx_hex":row["fanout_tx_hex"],"fanout_tx_sha256":txid,"cpfp_anchor_spendable":status=="broadcastable"&&!row["anchor_vout"].is_null()&&fee==0,"last_broadcast_attempt_at":timestamp(&row["last_broadcast_attempt_at"]),"last_broadcast_error":row["last_broadcast_error"],"explorer_url":link(&state.config.explorer_tx_url,&json!(txid))}),
    )
}
async fn settlement_artifacts(state: &ApiState, hash: &str) -> ApiResult<Value> {
    let maybe: Value = sqlx::query_scalar("SELECT qbit_audit_block_fanouts($1)")
        .bind(hash)
        .fetch_one(&state.pool)
        .await?;
    let payload = if maybe.is_null() {
        let body = bundle(state, hash, false).await.map_err(|e| {
            if e.status == StatusCode::NOT_FOUND {
                ApiError::missing("unknown PRISM settlement artifact block")
            } else {
                e
            }
        })?;
        if body["audit_bundle"]["settlement_mode_decision"]["mode"] != "direct_coinbase" {
            return Err(ApiError::missing("unknown PRISM settlement artifact block"));
        }
        json!({"block_hash":hash,"block_height":body["block_height"],"settlement_mode":"direct_coinbase","audit_bundle_sha256":body["audit_bundle_sha256"],"payout_manifest_sha256":body["payout_manifest_sha256"],"artifacts":[]})
    } else {
        maybe
    };
    let mut links = Vec::new();
    let audit_sha = payload["audit_bundle_sha256"].as_str();
    if let Some(sha) = audit_sha {
        // Inline artifacts need no body read. External legacy bodies are validated before linking.
        let metadata:Option<bool>=sqlx::query_scalar("SELECT audit_bundle IS NOT NULL FROM qbit_pool_audit_bundles WHERE audit_bundle_sha256=$1 LIMIT 1").bind(sha).fetch_optional(&state.pool).await?;
        if metadata == Some(true) || artifact(state, sha).await.is_ok() {
            links.push(artifact_link("audit_bundle", sha, None));
        }
    }
    if let Some(sha) = payload["manifest_set_sha256"].as_str() {
        links.push(artifact_link(
            "ctv_manifest_set",
            sha,
            payload["manifest_set_json"].as_str().map(str::len),
        ));
    }
    let mut fanouts = Vec::new();
    for row in payload["artifacts"].as_array().into_iter().flatten() {
        let mut row = row.clone();
        for key in [
            "block_hash",
            "block_height",
            "audit_bundle_sha256",
            "manifest_set_sha256",
        ] {
            row[key] = payload[key].clone();
        }
        fanouts.push(fanout_row(state, &row)?);
    }
    Ok(wrap(
        "settlement-artifacts",
        json!({"block_hash":hash,"block_height":payload["block_height"],"settlement_mode":payload["settlement_mode"],"audit_bundle_sha256":payload["audit_bundle_sha256"],"payout_manifest_sha256":payload["payout_manifest_sha256"],"artifact_links":links,"fanouts":fanouts}),
    ))
}
fn artifact_link(kind: &str, sha: &str, bytes: Option<usize>) -> Value {
    json!({"kind":kind,"sha256":sha,"url":format!("/public/v1/artifacts/{sha}"),"content_type":"application/json","byte_length":bytes})
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn qbit_scaled_units_are_exact() {
        assert_eq!(
            scaled_difficulty("207fffff").unwrap().to_string(),
            "1000000"
        );
        assert!(scaled_difficulty("00000000").is_none());
        assert!(scaled_difficulty("20800001").is_none());
        assert!(scaled_difficulty("2300ffff").is_none());
    }
    #[test]
    fn small_hashrates_remain_decimal_strings() {
        let h = hashrate(&json!(1000000), 1);
        assert!(h.as_str().unwrap().starts_with("0.000000000002"));
        assert!(h.as_str().unwrap().parse::<f64>().unwrap() > 0.0);
    }
}
