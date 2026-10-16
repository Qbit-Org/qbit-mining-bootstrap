"""Exhaustive search for double payouts on one final active chain.

Why the final chain suffices: a job's prior depends only on its parent's
ancestry and on what its builder knew; a block's delta depends only on its job.
So every interleaving of writers, promotions, partitions, replication lag,
reorgs, sibling races and rejoins that ends with a given active chain is
covered by enumerating, for each pool block on that chain, an ARBITRARY subset
of earlier pool blocks its builder knew (a superset of what two nodes can
realise). Reorgs only matter to variants that read state outside the ancestry
(the DB-view and checkpoint variants), modelled with extra choices.

Run: python3 -I exhaustive.py [config-name-prefix ...]   (no argument: every configuration)
"""
from __future__ import annotations

import itertools
import json
import os
import sys
import time
from collections import Counter

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from core import (Cfg, PBlock, decide, final_truth, run_policy)  # noqa: E402

WINDOWS = [{"a": 70, "b": 25, "c": 5}, {"a": 10, "b": 45, "c": 45}]
INITS = [{"a": 0, "b": 0, "c": 0}, {"a": 1500, "b": 0, "c": 600}, {"a": 2500, "b": -700, "c": 900}]


def subsets(xs):
    for r in range(len(xs) + 1):
        for c in itertools.combinations(xs, r):
            yield frozenset(c)


class Explorer:
    def __init__(self, name, cfg, init, maxlen, opts):
        self.name, self.cfg, self.init, self.maxlen, self.opts = name, cfg, init, maxlen, opts
        self.stats = Counter()
        self.cex = []
        self.loss = Counter()
        self.max_loss = 0
        self.max_overcredit = 0

    # -- variant choices per pool block --------------------------------------
    def variants(self, chain, K, h):
        recs = (True, False) if self.opts.get("false_negative") else (True,)
        flips = (False, True) if self.opts.get("mode_flip") else (False,)
        legs = (False, True) if self.opts.get("legacy") else (False,)
        if self.opts.get("db_view"):
            kconfs = list(subsets(sorted(K)))
        else:
            kconfs = [None]
        cks = [None]
        M = self.opts.get("checkpoint")
        if M is not None:
            ck_h = (h - 1) - M
            if ck_h >= 1:
                below = [b.idx for b in chain if b.kind == "P" and b.height <= ck_h]
                cks = []
                for kck in subsets(below):
                    vec = dict(self.init)
                    for b in chain:
                        if b.kind == "P" and b.idx in kck:
                            for k, d in b.manifest.delta.items():
                                vec[k] = vec.get(k, 0) + d
                    cks.append((ck_h, vec, kck))
        for rec in recs:
            for fl in flips:
                for leg in legs:
                    for kc in kconfs:
                        for ck in cks:
                            yield rec, fl, kc, leg, ck

    def run(self):
        t = time.time()
        self.dfs([], 0)
        self.stats["seconds"] = round(time.time() - t, 1)
        return self

    def dfs(self, chain, height):
        if len(chain) >= self.maxlen:
            return
        h = height + 1
        idx = len(chain)
        pool = [b.idx for b in chain if b.kind == "P"]
        if self.opts.get("foreign"):
            self.visit(chain + [PBlock(idx, "F", height=h)], h)
        if self.opts.get("spoof"):
            for pm in ("cp", "cf"):
                self.visit(chain + [PBlock(idx, "S", recognized=True, perceived_mode=pm, height=h)], h)
        for K in subsets(pool):
            for wi, w in enumerate(WINDOWS):
                for rec, fl, kc, leg, ck in self.variants(chain, K, h):
                    ck2 = None if ck is None else (ck[0], ck[1])
                    res = decide(chain, K, self.cfg, self.init, ck=ck2, known_conf=kc, legacy=leg)
                    if res is None:
                        self.stats["halted_builds"] += 1
                        continue
                    mode, view = res
                    m = run_policy(w, view, mode, self.cfg)
                    if m is None:
                        self.stats["policy_build_errors"] += 1
                        continue
                    pm = mode if not fl else ("cf" if mode == "cp" else "cp")
                    b = PBlock(idx, "P", manifest=m, recognized=rec, perceived_mode=pm, height=h)
                    b.meta = {"K": sorted(K), "w": wi, "Kconf": None if kc is None else sorted(kc),
                              "legacy": leg, "flip": fl, "recognized": rec,
                              "ck": None if ck is None else {"h": ck[0], "knew": sorted(ck[2])}}
                    self.visit(chain + [b], h)

    def visit(self, chain, h):
        self.stats["chains"] += 1
        b = chain[-1]
        if b.kind == "P":
            self.stats["blocks_" + b.manifest.mode] += 1
            before = final_truth(chain[:-1], self.init)
            after = dict(before)
            for k, d in b.manifest.delta.items():
                after[k] = after.get(k, 0) + d
            bad = {k: max(0, -after.get(k, 0)) - max(0, -before.get(k, 0)) for k in after}
            bad = {k: v for k, v in bad.items() if v > 0}
            if bad:
                self.stats["overpay_chains"] += 1
                self.record(chain, bad)
                return  # keep counterexamples minimal: do not extend
            self.formulation_gap(chain)
        self.dfs(chain, h)

    def formulation_gap(self, chain):
        """Full-knowledge node: sum view (= truth) vs carry vector of latest pool block."""
        truth = final_truth(chain, self.init)
        last = None
        for b in chain:
            if b.kind == "P":
                last = b
        cv = dict(last.manifest.carry)
        keys = set(truth) | set(cv)
        under = sum(max(0, truth.get(k, 0) - cv.get(k, 0)) for k in keys)
        over = sum(max(0, cv.get(k, 0) - truth.get(k, 0)) for k in keys)
        if under or over:
            has_cf = any(x.kind == "P" and x.manifest.mode == "cf" for x in chain)
            has_leg = any(x.kind == "P" and x.meta.get("legacy") for x in chain)
            has_skip = any(x.kind == "P" and x.manifest.mode == "cp" and x.meta.get("K") is not None
                           and any(y.kind == "P" and y.idx < x.idx and y.idx not in x.meta["K"]
                                   for y in chain) for x in chain)
            self.loss[("cv<truth" if under else "") + ("cv>truth" if over else ""),
                      "cf-in-ancestry" if has_cf else "no-cf",
                      "legacy" if has_leg else "", "skip" if has_skip else ""] += 1
            self.max_loss = max(self.max_loss, under)
            self.max_overcredit = max(self.max_overcredit, over)
        else:
            self.loss["equal"] += 1

    def record(self, chain, bad):
        if len(self.cex) >= 3 and len(chain) >= max(len(c["trace"]) for c in self.cex):
            return
        trace = []
        for b in chain:
            if b.kind != "P":
                trace.append({"blk": b.idx, "h": b.height, "kind": b.kind, "marker": b.perceived_mode})
                continue
            m = b.manifest
            trace.append({"blk": b.idx, "h": b.height, "kind": "P", "mode": m.mode,
                          "perceived": b.perceived_mode, **b.meta,
                          "prior": m.prior, "gross": m.gross, "onchain": m.onchain, "delta": m.delta})
        self.cex.append({"overpay": bad, "init": self.init, "trace": trace})
        self.cex.sort(key=lambda c: len(c["trace"]))
        del self.cex[3:]

    def summary(self):
        cex = self.cex[:1]
        return {"name": self.name, "init": self.init, "stats": dict(self.stats),
                "formulation_gap": {" ".join(x for x in k if x) if isinstance(k, tuple) else k: v
                                    for k, v in self.loss.items()},
                "max_cv_under_truth": self.max_loss, "max_cv_over_truth": self.max_overcredit,
                "shortest_counterexample": cex[0] if cex else None}


CONFIGS = [
    # name, cfg, options, maxlen
    ("P1 proposal: sum + R-Chain(relaxed) + carry-free(clamp)", Cfg("sum", "relaxed"), {}, 6),
    ("P2 sum + R-Chain(strict)", Cfg("sum", "strict"), {}, 6),
    ("P3 sum + relaxed + carry-free prior:=0", Cfg("sum", "relaxed", cf_prior="zero"), {}, 6),
    ("P4 sum + relaxed, halt instead of carry-free", Cfg("sum", "relaxed", on_missing="halt"), {}, 6),
    ("C1 carry-vector + R-Chain(relaxed)", Cfg("cv", "relaxed"), {}, 6),
    ("C2 carry-vector + latest-only (alt R4) + relaxed", Cfg("cv", "latest"), {}, 6),
    ("C3 carry-vector, cf manifests record view+delta", Cfg("cv", "relaxed", cv_cf_record="view"), {}, 6),
    ("X0 sanity: no R-Chain at all", Cfg("sum", "none"), {}, 4),
    ("X1 sum + latest-only check", Cfg("sum", "latest"), {}, 5),
    ("X2 false-negative marker (block not recognised)", Cfg("sum", "relaxed"), {"false_negative": True}, 5),
    ("X3 mode byte misread (cp<->cf), relaxed", Cfg("sum", "relaxed"), {"mode_flip": True}, 5),
    ("X4 mode byte misread (cp<->cf), strict", Cfg("sum", "strict"), {"mode_flip": True}, 5),
    ("X5 naive checkpoint M=1 (lookback stops at checkpoint)", Cfg("sum", "relaxed"),
     {"checkpoint": 1, "foreign": True}, 5),
    ("X6 R1 over DB 'confirmed' rows (known but unconfirmed)", Cfg("sum", "relaxed"), {"db_view": True}, 4),
    ("X7 mixed version: one builder without R1/R-Chain", Cfg("sum", "relaxed"), {"legacy": True}, 4),
    ("X8 carry-vector + mixed version", Cfg("cv", "relaxed"), {"legacy": True}, 4),
    ("S1 spoofed markers (cp/cf) on foreign blocks, relaxed", Cfg("sum", "relaxed"), {"spoof": True}, 5),
]


def main():
    only = sys.argv[1:] or None
    results = []
    for name, cfg, opts, maxlen in CONFIGS:
        if only and not any(name.startswith(o) for o in only):
            continue
        for init in INITS:
            ex = Explorer(name, cfg, init, maxlen, opts).run()
            s = ex.summary()
            results.append(s)
            st = s["stats"]
            print(f"{name} | init={init} | chains={st.get('chains',0)} cp={st.get('blocks_cp',0)} "
                  f"cf={st.get('blocks_cf',0)} overpay_chains={st.get('overpay_chains',0)} "
                  f"halts={st.get('halted_builds',0)} errs={st.get('policy_build_errors',0)} "
                  f"cv_under={s['max_cv_under_truth']} cv_over={s['max_cv_over_truth']} t={st['seconds']}s",
                  flush=True)
    out = os.path.join(os.path.dirname(os.path.abspath(__file__)), "out")
    os.makedirs(out, exist_ok=True)
    tag = "_".join(only) if only else "all"
    with open(os.path.join(out, f"exhaustive_{tag}.json"), "w") as f:
        json.dump(results, f, indent=1, default=str)


if __name__ == "__main__":
    main()
