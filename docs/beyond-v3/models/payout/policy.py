"""Python port of PRISM 3's payout policy (v3.0.0-rc.6, 81c592c3).

Ported from crates/qbit-prism/src/lib.rs:
  apply_payout_policy            :1405-1640
  pool_fee_manifest              :2941-2968
  carry_forward_by_payout_program:3168-3190
  aggregate_entitlements_by_payout_program :3192-3222
  add_swept_dust_to_pool_fee     :3224-3241
  eligible_onchain_accounts      :3243-3251
  select_onchain_accounts        :3265-3315
  allocate_weighted_amounts      :3317-3394
and crates/qbit-prism/src/settlement.rs:
  apply_proportional_fanout_fee  :325-420, allocate_proportional_fee :501-562

`check_vectors()` replays every apply_payout_policy and
apply_proportional_fanout_fee case in crates/qbit-prism/fixtures/vectors/*.json
and compares the full output. Run: python3 -I policy.py <prism-export-root>
"""
from __future__ import annotations

import json
import sys


class PolicyError(Exception):
    pass


def program_key(hexstr):
    return hexstr.lower()


def _canon(e):
    return (e["order_key"], e["recipient_id"], e["p2mr_program_hex"])


def allocate_weighted_amounts(total, entitlements):
    ordered = sorted(entitlements, key=_canon)
    total_weight = sum(e["weight"] for e in ordered)
    if total_weight == 0:
        raise PolicyError("payout policy arithmetic overflowed")
    prov = []
    for e in ordered:
        product = total * e["weight"]
        prov.append([e, product // total_weight, product % total_weight])
    allocated = sum(p[1] for p in prov)
    remainder = total - allocated
    order = sorted(range(len(prov)),
                   key=lambda i: (-prov[i][2], prov[i][0]["order_key"],
                                  prov[i][0]["recipient_id"], prov[i][0]["p2mr_program_hex"]))
    for i in order[:remainder]:
        prov[i][1] += 1
    return [(p[0], p[1]) for p in prov]


def aggregate_entitlements_by_payout_program(entitlements):
    by = {}
    for e in entitlements:
        k = program_key(e["p2mr_program_hex"])
        cur = by.get(k)
        if cur is None:
            cur = {"recipient_id": e["recipient_id"], "order_key": e["order_key"],
                   "p2mr_program_hex": e["p2mr_program_hex"], "weight": 0}
            by[k] = cur
        if (e["order_key"], e["recipient_id"]) < (cur["order_key"], cur["recipient_id"]):
            cur["recipient_id"] = e["recipient_id"]
            cur["order_key"] = e["order_key"]
        cur["weight"] += e["weight"]
    return [by[k] for k in sorted(by)]


def carry_forward_by_payout_program(balances):
    by = {}
    for b in balances:
        k = program_key(b["p2mr_program_hex"])
        cur = by.get(k)
        if cur is None:
            cur = {"recipient_id": b["recipient_id"], "order_key": b["order_key"],
                   "p2mr_program_hex": b["p2mr_program_hex"], "balance_sats": 0}
            by[k] = cur
        if (b["order_key"], b["recipient_id"]) < (cur["order_key"], cur["recipient_id"]):
            cur["recipient_id"] = b["recipient_id"]
            cur["order_key"] = b["order_key"]
        cur["balance_sats"] += b["balance_sats"]
    return {k: by[k] for k in sorted(by)}


def min_output_sats(policy):
    if policy.get("min_output_sats") is not None:
        if policy["min_output_sats"] == 0:
            raise PolicyError("minimum output floor must be positive")
        return policy["min_output_sats"]
    return policy["p2mr_spend_input_bytes"] * policy["target_feerate_sats_per_byte"] * policy["safety_multiplier"]


def floor_formula(policy):
    if policy.get("min_output_sats") is not None:
        return f"configured fixed floor: {policy['min_output_sats']} sats"
    return (f"{policy['p2mr_spend_input_bytes']} bytes/input * {policy['target_feerate_sats_per_byte']}"
            f" sat/byte * {policy['safety_multiplier']}x safety")


def pool_fee_manifest(coinbase, policy):
    pf = policy.get("pool_fee_policy")
    if pf is None:
        return None
    if pf["fee_bps"] > 10_000:
        raise PolicyError(f"pool fee bps {pf['fee_bps']} exceeds 10000")
    fee = coinbase * pf["fee_bps"] // 10_000
    return {"fee_bps": pf["fee_bps"], "earned_pool_fee_sats": fee, "swept_dust_liability_sats": 0,
            "amount_sats": fee, "recipient_id": pf["recipient_id"], "order_key": pf["order_key"],
            "p2mr_program_hex": pf["p2mr_program_hex"]}


def add_swept(pool_fee, swept):
    if swept == 0:
        return
    if pool_fee is None:
        raise PolicyError("payout policy arithmetic overflowed")
    pool_fee["swept_dust_liability_sats"] += swept
    pool_fee["amount_sats"] = pool_fee["earned_pool_fee_sats"] + pool_fee["swept_dust_liability_sats"]


def eligible(accounts, floor):
    return [i for i, a in enumerate(accounts) if a["candidate_balance_sats"] >= floor]


def select_onchain_accounts(accounts, coinbase, floor):
    selected = eligible(accounts, floor)
    while True:
        if not selected:
            raise PolicyError("no on-chain recipients")
        s = sum(accounts[i]["candidate_balance_sats"] for i in selected)
        if s < coinbase:
            raise PolicyError(
                f"on-chain payouts would exceed selected candidate balances: coinbase value {coinbase} > selected candidate balance {s}")
        weights = [{"recipient_id": accounts[i]["recipient_id"], "order_key": accounts[i]["order_key"],
                    "p2mr_program_hex": accounts[i]["p2mr_program_hex"],
                    "weight": accounts[i]["candidate_balance_sats"]} for i in selected]
        alloc = allocate_weighted_amounts(coinbase, weights)
        under = {_canon(e) for e, amt in alloc if amt < floor}
        if not under:
            return selected
        selected = [i for i in selected if _canon(accounts[i]) not in under]


def apply_payout_policy(reward_manifest, prior_balances, policy):
    floor = min_output_sats(policy)
    coinbase = reward_manifest["coinbase_value_sats"]
    pool_fee = pool_fee_manifest(coinbase, policy)
    cop = policy.get("coinbase_output_policy", "canonical")
    if cop == "pool-fee-first" and pool_fee is None:
        raise PolicyError("pool-fee-first coinbase output policy requires a configured pool fee policy")
    fee_amount = pool_fee["amount_sats"] if pool_fee else 0
    miner_reward = coinbase - fee_amount
    if miner_reward < floor:
        raise PolicyError(f"coinbase value {miner_reward} is below minimum output floor {floor}")
    sett = aggregate_entitlements_by_payout_program(reward_manifest["entitlements"])
    gross_amounts = allocate_weighted_amounts(miner_reward, sett)
    prior_by_program = carry_forward_by_payout_program(prior_balances)
    seen = set()
    seeds = []
    for e, gross in gross_amounts:
        k = program_key(e["p2mr_program_hex"])
        seen.add(k)
        pb = prior_by_program.get(k)
        seeds.append((e["recipient_id"], e["order_key"], e["p2mr_program_hex"], gross,
                      pb["balance_sats"] if pb else 0))
    for b in prior_by_program.values():
        k = program_key(b["p2mr_program_hex"])
        if k in seen:
            continue
        seeds.append((b["recipient_id"], b["order_key"], b["p2mr_program_hex"], 0, b["balance_sats"]))
        seen.add(k)
    accounts = []
    for rid, ok, prog, gross, prior in seeds:
        cand = prior + gross
        accounts.append({"account_type": "miner", "recipient_id": rid, "order_key": ok,
                         "p2mr_program_hex": prog, "gross_amount_sats": gross,
                         "prior_balance_sats": prior, "candidate_balance_sats": cand,
                         "onchain_amount_sats": 0, "settlement_fee_sats": 0,
                         "carry_forward_balance_sats": cand, "action": "accrued"})
    onchain = []
    elig = eligible(accounts, floor)
    if not elig:
        if pool_fee is None:
            raise PolicyError("no on-chain recipients")
        add_swept(pool_fee, miner_reward)
    else:
        esum = sum(accounts[i]["candidate_balance_sats"] for i in elig)
        if esum < miner_reward:
            if pool_fee is None:
                raise PolicyError(
                    f"on-chain payouts would exceed selected candidate balances: coinbase value {miner_reward} > selected candidate balance {esum}")
            add_swept(pool_fee, miner_reward - esum)
            for i in elig:
                a = accounts[i]
                onchain.append(({"recipient_id": a["recipient_id"], "order_key": a["order_key"],
                                 "p2mr_program_hex": a["p2mr_program_hex"]}, a["candidate_balance_sats"]))
        else:
            emitted = select_onchain_accounts(accounts, miner_reward, floor)
            weights = [{"recipient_id": accounts[i]["recipient_id"], "order_key": accounts[i]["order_key"],
                        "p2mr_program_hex": accounts[i]["p2mr_program_hex"],
                        "weight": accounts[i]["candidate_balance_sats"]} for i in emitted]
            onchain = allocate_weighted_amounts(miner_reward, weights)
    for e, amt in onchain:
        if amt < floor:
            raise PolicyError(f"payout below floor for {e['recipient_id']}")
        acct = next(a for a in accounts if _canon(a) == _canon(e))
        if amt > acct["candidate_balance_sats"]:
            raise PolicyError("on-chain payouts would exceed selected candidate balances")
        acct["onchain_amount_sats"] = amt
        acct["carry_forward_balance_sats"] = acct["candidate_balance_sats"] - amt
        acct["action"] = "onchain"
    if pool_fee is not None:
        if program_key(pool_fee["p2mr_program_hex"]) in seen:
            raise PolicyError("duplicate pool fee account")
        accounts.append({"account_type": "pool_fee", "recipient_id": pool_fee["recipient_id"],
                         "order_key": pool_fee["order_key"], "p2mr_program_hex": pool_fee["p2mr_program_hex"],
                         "gross_amount_sats": pool_fee["amount_sats"], "prior_balance_sats": 0,
                         "candidate_balance_sats": pool_fee["amount_sats"],
                         "onchain_amount_sats": pool_fee["amount_sats"], "settlement_fee_sats": 0,
                         "carry_forward_balance_sats": 0, "action": "onchain"})
    ents = [{"recipient_id": a["recipient_id"], "order_key": a["order_key"],
             "p2mr_program_hex": a["p2mr_program_hex"], "weight": a["onchain_amount_sats"]}
            for a in accounts if a["onchain_amount_sats"] > 0]
    if sum(e["weight"] for e in ents) != coinbase:
        raise PolicyError("payout policy arithmetic overflowed")
    accounts.sort(key=_canon)
    out = {"schema": "qbit.prism.payout-policy.v1", "block_height": reward_manifest["block_height"],
           "coinbase_value_sats": coinbase, "min_output_sats": floor, "floor_formula": floor_formula(policy)}
    if cop != "canonical":
        out["coinbase_output_policy"] = cop
    if pool_fee is not None:
        out["pool_fee"] = pool_fee
    out["accounts"] = accounts
    out["onchain_entitlements"] = ents
    return out


def apply_proportional_fanout_fee(recipients, fee, floor):
    """settlement.rs:325-420 (returns (payable, carried, applied_fee))."""
    if floor == 0:
        raise PolicyError("cannot select a settlement mode: min_output_sats must be positive")
    if not recipients:
        raise PolicyError("cannot select a settlement mode: no fanout recipients to charge")
    cands = sorted([dict(r) for r in recipients], key=_canon)
    carried = []
    while True:
        if not cands:
            return [], carried, 0
        s = sum(r["amount_sats"] for r in cands)
        if fee >= s:
            carried.extend({"recipient_id": r["recipient_id"], "order_key": r["order_key"],
                            "p2mr_program_hex": r["p2mr_program_hex"], "gross_amount_sats": r["amount_sats"],
                            "fee_sats": 0, "net_amount_sats": r["amount_sats"]} for r in cands)
            return [], carried, 0
        alloc = []
        for r in cands:
            prod = fee * r["amount_sats"]
            alloc.append([{"recipient_id": r["recipient_id"], "order_key": r["order_key"],
                           "p2mr_program_hex": r["p2mr_program_hex"], "gross_amount_sats": r["amount_sats"],
                           "fee_sats": prod // s, "net_amount_sats": r["amount_sats"] - prod // s}, prod % s])
        rem = fee - sum(a[0]["fee_sats"] for a in alloc)
        alloc.sort(key=lambda a: (-a[1], _canon(a[0])))
        for a in alloc[:rem]:
            a[0]["fee_sats"] += 1
            a[0]["net_amount_sats"] = a[0]["gross_amount_sats"] - a[0]["fee_sats"]
        payable = [a[0] for a in alloc if a[0]["net_amount_sats"] >= floor]
        below = [a[0] for a in alloc if a[0]["net_amount_sats"] < floor]
        if not below:
            return sorted(payable, key=_canon), carried, fee
        bk = {_canon(b) for b in below}
        carried.extend({"recipient_id": b["recipient_id"], "order_key": b["order_key"],
                        "p2mr_program_hex": b["p2mr_program_hex"], "gross_amount_sats": b["gross_amount_sats"],
                        "fee_sats": 0, "net_amount_sats": b["gross_amount_sats"]} for b in below)
        cands = [r for r in cands if _canon(r) not in bk]


# ---------------------------------------------------------------------------
# Model-level helper: accounts are short names; order_key = recipient = name.
# ---------------------------------------------------------------------------
_PROG = {}


def _prog(name):
    if name not in _PROG:
        _PROG[name] = (name.encode().hex() + "00" * 32)[:64]
    return _PROG[name]


def simple_policy(coinbase, fee_bps, floor, weights, prior):
    """weights: {acct: int window weight>0}; prior: {acct: int}. Returns
    (per-account dict acct -> (gross, prior, onchain, carry), pool_fee_amount, swept)."""
    policy = {"p2mr_spend_input_bytes": 1, "target_feerate_sats_per_byte": 1, "safety_multiplier": 1,
              "min_output_sats": floor,
              "pool_fee_policy": {"fee_bps": fee_bps, "recipient_id": "~fee", "order_key": "~fee",
                                  "p2mr_program_hex": "f" * 64}}
    rm = {"block_height": 1, "coinbase_value_sats": coinbase,
          "entitlements": [{"recipient_id": a, "order_key": a, "p2mr_program_hex": _prog(a), "weight": w}
                           for a, w in sorted(weights.items()) if w > 0]}
    pb = [{"recipient_id": a, "order_key": a, "p2mr_program_hex": _prog(a), "balance_sats": v}
          for a, v in sorted(prior.items()) if v != 0]
    out = apply_payout_policy(rm, pb, policy)
    res = {}
    for a in out["accounts"]:
        if a["account_type"] != "miner":
            continue
        res[a["recipient_id"]] = (a["gross_amount_sats"], a["prior_balance_sats"],
                                  a["onchain_amount_sats"], a["carry_forward_balance_sats"])
    pf = out["pool_fee"]
    return res, pf["amount_sats"], pf["swept_dust_liability_sats"]


def check_vectors(root):
    import glob
    n_ok = n_fail = 0
    for f in sorted(glob.glob(root + "/crates/qbit-prism/fixtures/vectors/*.json")):
        d = json.load(open(f))
        for case in d["cases"]:
            ep = case["entry_point"]
            if ep not in ("apply_payout_policy", "apply_proportional_fanout_fee"):
                continue
            exp = case["expected"]
            inp = case["input"]
            try:
                if ep == "apply_payout_policy":
                    got = {"ok": apply_payout_policy(inp["reward_manifest"], inp["prior_balances"], inp["policy"])}
                else:
                    payable, carried, applied = apply_proportional_fanout_fee(
                        inp["recipients"], inp["fee_sats"], inp["min_output_sats"])
                    got = {"ok": {"payable": payable, "carried": carried, "applied": applied}}
            except PolicyError as e:
                got = {"error": str(e)}
            if "error" in exp:
                good = "error" in got and got["error"] == exp["error"]
            elif ep == "apply_payout_policy":
                good = "ok" in got and _norm(got["ok"]) == _norm(exp["ok"])
            else:
                eo = exp["ok"]
                g = got.get("ok")
                good = g is not None and _fee_match(g, eo)
            if good:
                n_ok += 1
            else:
                n_fail += 1
                print("MISMATCH", f.split("/")[-1], case["name"])
                print(" expected:", json.dumps(exp)[:600])
                print(" got     :", json.dumps(got)[:600])
    print(f"vectors: {n_ok} match, {n_fail} mismatch")
    if n_ok + n_fail == 0:
        print("no fixture vectors found: pass the root of a v3.0.0-rc.6 checkout")
        return False
    return n_fail == 0


def _norm(o):
    # drop defaulted/omitted fields the Rust serializer skips
    o = json.loads(json.dumps(o))
    for a in o.get("accounts", []):
        if a.get("account_type") == "miner":
            a.pop("account_type")
        if a.get("settlement_fee_sats") == 0:
            a.pop("settlement_fee_sats")
    return o


def _fee_match(g, eo):
    keys = {k for k in eo}
    payable = eo.get("payable_recipients", eo.get("payable"))
    carried = eo.get("carry_forward_recipients", eo.get("carried"))
    applied = eo.get("applied_fee_sats", eo.get("applied"))
    ok = True
    if payable is not None:
        ok &= [_canon(x) + (x["fee_sats"], x["net_amount_sats"]) for x in payable] == \
              [_canon(x) + (x["fee_sats"], x["net_amount_sats"]) for x in g["payable"]]
    if carried is not None:
        ok &= sorted(_canon(x) for x in carried) == sorted(_canon(x) for x in g["carried"])
    if applied is not None:
        ok &= applied == g["applied"]
    return ok


if __name__ == "__main__":
    sys.exit(0 if check_vectors(sys.argv[1]) else 1)
