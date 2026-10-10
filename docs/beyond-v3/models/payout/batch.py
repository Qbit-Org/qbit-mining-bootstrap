"""Run the simulator over a scenario matrix in parallel.

Run: python3 -I batch.py [seeds-per-config] [workers] [fixed]
Writes out/sim_results.json (out/sim_results_fixed.json with `fixed`, which
applies the data fixes) and prints one line per configuration.
"""
from __future__ import annotations

import json
import os
import sys
from collections import Counter
from multiprocessing import Pool

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import sim  # noqa: E402

FAST = ["--p-slow-ack", "0.02"]
MATRIX = [
    # name, args
    ("F1 none", ["--shape", "F1", "--scenario", "none"]),
    ("F1 A dies (disk kept)", ["--shape", "F1", "--scenario", "A-dies"]),
    ("F1 A dies (disk kept) + auto salvage", ["--shape", "F1", "--scenario", "A-dies", "--salvage"]),
    ("F1 A dies, disk destroyed", ["--shape", "F1", "--scenario", "A-dies-disk-lost"]),
    ("F1 B dies", ["--shape", "F1", "--scenario", "B-dies"]),
    ("F1 VLAN+tailnet partition", ["--shape", "F1", "--scenario", "partition"]),
    ("F1 partition, A goes alone after 60 s", ["--shape", "F1", "--scenario", "partition", "--alone-on-partition"]),
    ("F1 lagging standby, then A dies", ["--shape", "F1", "--scenario", "lag-then-A-dies"]),
    ("F1 lagging standby, then A dies + auto salvage", ["--shape", "F1", "--scenario", "lag-then-A-dies", "--salvage"]),
    ("F1 lagging standby, then A dies + FIX cf prior 0, reconstructible",
     ["--shape", "F1", "--scenario", "lag-then-A-dies", "--cf-prior", "zero", "--cf-recon"]),
    ("F1 lagging standby, then A dies, disk destroyed", ["--shape", "F1", "--scenario", "lag-then-A-dies-disk-lost"]),
    ("F1 partition + UNFENCED promotion, no salvage", ["--shape", "F1", "--scenario", "partition-unfenced", "--unfenced"]),
    ("F1 partition + UNFENCED promotion + alone", ["--shape", "F1", "--scenario", "partition-unfenced", "--unfenced", "--alone-on-partition"]),
    ("F1 partition + UNFENCED + alone + auto salvage", ["--shape", "F1", "--scenario", "partition-unfenced", "--unfenced", "--alone-on-partition", "--salvage"]),
    ("F1 DOUBLE: B dies then A dies (disk lost)", ["--shape", "F1", "--scenario", "B-dies-then-A-dies"]),
    ("F1 DOUBLE: partition(alone) then A dies", ["--shape", "F1", "--scenario", "partition-then-A-dies", "--alone-on-partition"]),
    ("F1 partition, A alone at 60 s, B fences A at 90 s", ["--shape", "F1", "--scenario", "partition-fence-A", "--alone-on-partition"]),
    ("F1 partition, B fences A at 30 s (before A goes alone at 60 s)", ["--shape", "F1", "--scenario", "partition-fence-A", "--alone-on-partition", "--fence-after", "30"]),
    ("F1 partition, A alone, B fences A + auto salvage", ["--shape", "F1", "--scenario", "partition-fence-A", "--alone-on-partition", "--salvage"]),
    ("F1 A dies + miners self-broadcast dead node's job", ["--shape", "F1", "--scenario", "A-dies", "--p-selfbroadcast", "0.05"]),
    ("F1 spoofed markers 1%/block, relaxed", ["--shape", "F1", "--scenario", "none", "--p-spoof", "0.01"]),
    ("F1 spoofed markers 1%/block, strict", ["--shape", "F1", "--scenario", "none", "--p-spoof", "0.01", "--rchain", "strict"]),
    ("F1 carry-vector formulation, lag then A dies", ["--shape", "F1", "--scenario", "lag-then-A-dies", "--formulation", "cv"]),
    ("F1 carry-vector formulation, partition alone", ["--shape", "F1", "--scenario", "partition", "--alone-on-partition", "--formulation", "cv"]),
    ("F2 none", ["--shape", "F2", "--scenario", "none"]),
    ("F2 A dies (disk kept)", ["--shape", "F2", "--scenario", "A-dies"]),
    ("F2 A dies, disk destroyed", ["--shape", "F2", "--scenario", "A-dies-disk-lost"]),
    ("F2 partition (T_cf 600)", ["--shape", "F2", "--scenario", "partition"]),
    ("F2 partition, T_cf 60", ["--shape", "F2", "--scenario", "partition", "--tcf", "60"]),
    ("F2 lagging peer, then A dies (disk kept)", ["--shape", "F2", "--scenario", "lag-then-A-dies"]),
    ("F2 lagging peer, then A dies, disk destroyed", ["--shape", "F2", "--scenario", "lag-then-A-dies-disk-lost"]),
    ("F2 lagging peer, A dies, disk destroyed + FIX", ["--shape", "F2", "--scenario", "lag-then-A-dies-disk-lost", "--cf-prior", "zero", "--cf-recon"]),
    ("F2 lagging peer, then A dies + FIX", ["--shape", "F2", "--scenario", "lag-then-A-dies", "--cf-prior", "zero", "--cf-recon"]),
    ("F2 DOUBLE: partition then A dies", ["--shape", "F2", "--scenario", "partition-then-A-dies", "--tcf", "60"]),
    ("F2 DOUBLE: partition then A dies, disk destroyed", ["--shape", "F2", "--scenario", "partition-then-A-dies-disk-lost", "--tcf", "60"]),
    ("F2 carry-vector, partition T_cf 60", ["--shape", "F2", "--scenario", "partition", "--tcf", "60", "--formulation", "cv"]),
]


FIXED = False
FIX_FLAGS = ["--cf-prior", "zero", "--cf-recon", "--salvage"]


def one(job):
    name, args, seed = job
    if FIXED:
        args = [x for x in args if x != "--alone-on-partition"] + FIX_FLAGS
    a = sim.parse(args + FAST + ["--seed", str(seed)])
    return name, sim.Sim(a).run()


def _init(fixed):
    global FIXED
    FIXED = fixed


def main():
    global FIXED
    seeds = int(sys.argv[1]) if len(sys.argv) > 1 else 200
    workers = int(sys.argv[2]) if len(sys.argv) > 2 else 12
    FIXED = len(sys.argv) > 3 and sys.argv[3] == "fixed"
    jobs = [(n, a, s) for n, a in MATRIX for s in range(1, seeds + 1)]
    agg = {n: Counter() for n, _ in MATRIX}
    ex = {n: [] for n, _ in MATRIX}
    cf = {n: [] for n, _ in MATRIX}
    with Pool(workers, initializer=_init, initargs=(FIXED,)) as p:
        for name, r in p.imap_unordered(one, jobs, chunksize=8):
            c = agg[name]
            c["runs"] += 1
            st = r["stats"]
            c["pool_blocks"] += st.get("pool_blocks", 0)
            c["pool_blocks_cf"] += st.get("pool_blocks_cf", 0)
            c["fallback_blocks"] += st.get("pool_blocks_on_fallback", 0)
            c["nondurable_at_find"] += st.get("pool_blocks_nondurable_at_find", 0)
            c["overpay_runs"] += 1 if st.get("max_overpay", 0) > 0 else 0
            c["splitbrain_runs"] += 1 if r["splitbrain"] else 0
            c["views_disagree_runs"] += 0 if r["views_agree"] else 1
            c["stuck_cf_at_end_runs"] += 1 if any(r["stuck_carry_free_at_end"].values()) else 0
            c["view_under_truth_runs"] += 1 if r["view_under_truth"] else 0
            c["view_over_truth_runs"] += 1 if r["view_over_truth"] else 0
            c["view_under_truth_total"] += r["view_under_truth"]
            c["latent_double_pay_runs"] += 1 if r["latent_double_pay"] else 0
            c["latent_double_pay_total"] += r["latent_double_pay"]
            if r["final_lost_blocks"]:
                c["runs_with_final_loss"] += 1
            for lb in r["final_lost_blocks"]:
                c["lost_blocks"] += 1
                c["lost_" + lb["mode"] + ("_fallback" if lb["fallback"] else "") + "_in_" + lb["where"]] += 1
                c["lost_ctv_value"] += lb["ctv"]
                c["lost_accrual"] += lb["accrual"]
            cf[name].append(max(v for v in r["cf_fraction"].values() if v is not None) if any(
                v is not None for v in r["cf_fraction"].values()) else 0)
            if (r["final_lost_blocks"] or st.get("max_overpay", 0) or not r["views_agree"]) and len(ex[name]) < 1:
                ex[name].append({k: r[k] for k in ("seed", "final_lost_blocks", "trace", "stuck_carry_free_at_end",
                                                   "view_under_truth", "view_over_truth", "cf_fraction")})
    out = []
    for n, _ in MATRIX:
        c = agg[n]
        fr = sorted(cf[n])
        c_cf = {"cf_frac_median": fr[len(fr) // 2] if fr else None, "cf_frac_max": fr[-1] if fr else None}
        out.append({"name": n, "aggregate": dict(c), **c_cf, "example": ex[n][:1]})
        print(f"{n:72s} runs={c['runs']} blocks={c['pool_blocks']} cf={c['pool_blocks_cf']} fb={c['fallback_blocks']} "
              f"overpay_runs={c['overpay_runs']} loss_runs={c['runs_with_final_loss']} lost_blocks={c['lost_blocks']} "
              f"lost_ctv={c['lost_ctv_value']} lost_accrual={c['lost_accrual']} stuck={c['stuck_cf_at_end_runs']} "
              f"disagree={c['views_disagree_runs']} under={c['view_under_truth_runs']} over={c['view_over_truth_runs']} "
              f"split={c['splitbrain_runs']} latent={c['latent_double_pay_runs']}/{c['latent_double_pay_total']} cf_med={c_cf['cf_frac_median']} cf_max={c_cf['cf_frac_max']}", flush=True)
    od = os.path.join(os.path.dirname(os.path.abspath(__file__)), "out")
    os.makedirs(od, exist_ok=True)
    with open(os.path.join(od, "sim_results_fixed.json" if FIXED else "sim_results.json"), "w") as f:
        json.dump({"seeds_per_config": seeds, "results": out}, f, indent=1, default=str)


if __name__ == "__main__":
    main()
