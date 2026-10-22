"""Carry-free economics with production-like magnitudes.

Coinbase 1.92e10 bits (about 192 QBIT), pool fee 100 bps, floor 14,720 bits
(PRISM day-one default), 300 miners with a power-law hashrate split so that
some miners' per-block gross is below the floor. One chain of N pool blocks,
same windows in every run; a carry-free episode of K blocks in the middle.

Compares, per run: what miners were paid on chain, what is owed at the end
(truth = sum of as-issued deltas), what a node using the carry-vector
formulation would believe is owed, the IOU float held by the pool-fee output
(sum of swept dust), and how long positive carries wait.

Run: python3 -I econ.py
"""
from __future__ import annotations

import json
import os
import random
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from policy import simple_policy  # noqa: E402

COINBASE = 19_200_000_000
FEE_BPS = 100
FLOOR = 14_720


def miners(rng, n=300):
    # power-law shares; the smallest ~1e-7 of the pool -> gross ~ 1.9e3 bits < floor
    shares = {}
    for i in range(n):
        x = rng.random()
        shares[f"m{i:03d}"] = 10 ** (-7 + 6.5 * x ** 2)
    return shares


def windows(rng, shares, nblocks):
    out = []
    for _ in range(nblocks):
        w = {}
        for k, s in shares.items():
            # TIDES window luck: lognormal noise; a miner may be absent from a window
            v = s * rng.lognormvariate(0, 0.6)
            if rng.random() < 0.05:
                v = 0
            w[k] = max(0, int(v * 1e12))
        out.append(w)
    return out


def run(ws, init, cf_blocks, formulation):
    truth = dict(init)
    believed = dict(init)       # what the builder uses as prior (sum: == truth; cv: carry vector)
    paid = {k: 0 for k in init}
    fee_total = 0
    swept_total = 0
    float_track = []
    for i, w in enumerate(ws):
        mode = "cf" if i in cf_blocks else "cp"
        prior = believed if mode == "cp" else {k: min(v, 0) for k, v in believed.items()}
        res, fee, swept = simple_policy(COINBASE, FEE_BPS, FLOOR, w, prior)
        fee_total += fee
        swept_total += swept
        for k, (g, _p, o, c) in res.items():
            truth[k] = truth.get(k, 0) + g - o
            paid[k] = paid.get(k, 0) + o
        if formulation == "sum":
            believed = dict(truth)
        else:
            nb = {k: 0 for k in truth}
            for k, (g, _p, o, c) in res.items():
                nb[k] = c
            believed = nb
        float_track.append(sum(v for v in truth.values() if v > 0))
    over = sum(max(0, -v) for v in truth.values())
    lost = sum(max(0, truth[k] - believed.get(k, 0)) for k in truth)
    return {"miners_paid": sum(paid.values()), "fee_output_total": fee_total, "swept_dust_total": swept_total,
            "owed_positive_end": sum(v for v in truth.values() if v > 0), "debt_end": over,
            "believed_owed_end": sum(v for v in believed.values() if v > 0),
            "owed_forgotten_by_builder": lost, "float_track": float_track}


def main():
    rng = random.Random(7)
    sh = miners(rng)
    N = 200
    ws = windows(rng, sh, N)
    init = {k: 0 for k in sh}
    # warm-up so that carries exist
    base = run(ws, init, set(), "sum")
    res = {"blocks": N, "miners": len(sh)}
    for K in (0, 1, 10, 50):
        cfb = set(range(100, 100 + K))
        s = run(ws, init, cfb, "sum")
        c = run(ws, init, cfb, "cv")
        res[f"cf_episode_{K}"] = {
            "sum": {k: v for k, v in s.items() if k != "float_track"},
            "cv": {k: v for k, v in c.items() if k != "float_track"},
            "float_before_episode": s["float_track"][99],
            "float_after_episode_sum": s["float_track"][99 + K] if K else s["float_track"][99],
            "float_end_sum": s["float_track"][-1],
            "float_end_baseline": base["float_track"][-1],
        }
    # per-block sweep in carry-free vs carry-paying mode, as a fraction of the miner reward
    miner_reward = COINBASE - COINBASE * FEE_BPS // 10_000
    cfrun = run(ws, init, set(range(N)), "sum")
    res["avg_swept_per_block_all_carry_free_fraction"] = cfrun["swept_dust_total"] / N / miner_reward
    res["avg_swept_per_block_all_carry_paying_fraction"] = base["swept_dust_total"] / N / miner_reward
    print(json.dumps(res, indent=1))
    od = os.path.join(os.path.dirname(os.path.abspath(__file__)), "out")
    os.makedirs(od, exist_ok=True)
    with open(os.path.join(od, "econ.json"), "w") as f:
        json.dump(res, f, indent=1)


if __name__ == "__main__":
    main()
