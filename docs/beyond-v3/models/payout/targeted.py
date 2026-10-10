"""Targeted deterministic checks.

T1 convergence with checkpoints: nodes A and B hold the same manifests at the
   end, but A formed its checkpoint before learning carry-free block U.
T2 debt recovered twice: sum vs carry-vector bookkeeping.
Run: python3 -I targeted.py
"""
import json, os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from core import Cfg, PBlock, decide, final_truth, run_policy, view_for

W0 = {"a": 70, "b": 25, "c": 5}
W1 = {"a": 10, "b": 45, "c": 45}
out = {}

# ---- T1 -------------------------------------------------------------------
cfg = Cfg("sum", "relaxed")
init = {"a": 1500, "b": 0, "c": 600}
chain = []
def add(kind, known, w, h, mode_override=None):
    mode, view = decide(chain, known, cfg, init)
    if mode_override:
        mode = mode_override
    m = run_policy(w, view, mode, cfg)
    b = PBlock(len(chain), kind, manifest=m, recognized=True, perceived_mode=mode, height=h)
    chain.append(b)
    return b
x = add("P", set(), W0, 1)                       # blk0 carry-paying, built by B
u = add("P", set(), W1, 2, mode_override="cf")   # blk1 carry-free (builder lacked blk0)
chain.append(PBlock(2, "F", height=3)); chain.append(PBlock(3, "F", height=4))
# A forms a checkpoint at h2 knowing only blk0 (blk1 is carry-free and unknown: relaxation lets it skip)
ckA = (2, final_truth([chain[0]], init))
# B forms it at h2 knowing both
ckB = (2, final_truth(chain[:2], init))
# later both hold both manifests
vA, mA, _ = view_for(chain, {0, 1}, cfg, init, ck=ckA)
vB, mB, _ = view_for(chain, {0, 1}, cfg, init, ck=ckB)
out["T1_checkpoint_convergence"] = {"A_view": vA, "B_view": vB, "truth": final_truth(chain, init),
                                     "agree": vA == vB}

# ---- T2 -------------------------------------------------------------------
init2 = {"a": 3000, "b": -1200, "c": 0}
for form in ("sum", "cv"):
    cfg2 = Cfg(form, "relaxed")
    ch = []
    def add2(known, w, h, mode=None):
        md, view = decide(ch, known, cfg2, init2)
        md = mode or md
        m = run_policy(w, view, md, cfg2)
        ch.append(PBlock(len(ch), "P", manifest=m, recognized=True, perceived_mode=md, height=h))
    add2(set(), W1, 1)            # blk0 cp: recovers b's debt
    add2(set(), W1, 2, mode="cf") # blk1 cf, builder lacked blk0: recovers b's debt again
    full_view, missing, _ = view_for(ch, {0, 1}, cfg2, init2)
    out[f"T2_debt_twice_{form}"] = {"truth": final_truth(ch, init2), "full_knowledge_view": full_view,
                                    "b_paid": [b.manifest.onchain.get('b', 0) for b in ch],
                                    "b_gross": [b.manifest.gross.get('b', 0) for b in ch]}
print(json.dumps(out, indent=1))
