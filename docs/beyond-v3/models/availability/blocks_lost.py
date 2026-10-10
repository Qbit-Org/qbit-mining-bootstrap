"""Expected pool blocks lost per year, per HA option (red team D's estimate, not a forecast).

Every input below is an assumption; see docs/beyond-v3/ha-recommendation.md, section 2.
Option labels:
  M  manual today        -- today's manual failover
  ii async+SO+halt       -- step 1: automatic standby-only fenced failover, money path untouched
  F1 3.x rules+SO        -- steps 1 + 2 (chain-anchored payout rules), fenced promotion only
  F1 + unfenced policy   -- steps 1 + 2, promoting without a fence when the PhoenixNAP API is down
  F2 active-active       -- step 3 (PRISM 4.0): two always-on nodes
  i  sync+WF             -- synchronous replication with writer-fences-first fencing

Run: python3 -I blocks_lost.py
"""
S100 = 86400/100      # seconds per expected pool block at ~100 pool blocks/day
def per_event(S, mid=True, case='mid'):
    # per-event block losses; values (low, mid, high)
    T_man = {'low':600,'mid':1800,'high':7200}[case]
    T_fo  = {'low':15,'mid':25,'high':60}[case]        # (ii)/F1 miner-visible gap, SO fencing, improved LB checks
    T_det = {'low':8,'mid':15,'high':30}[case]         # F2: LB detection + reconnect for the dead node's miners
    P_api = {'low':0.001,'mid':0.01,'high':0.05}[case] # P(fence API unusable when A dies)
    p619  = {'low':0.00001,'mid':0.001,'high':0.0035}[case]  # P(block on gap work) per promotion, healthy standby
    T_stall = {'low':7,'mid':15,'high':70}[case]      # (i) sync: write stall when B dies (fence-before-degrade)
    D_S = 15                                          # (i) WF: standby's fence delay
    o = {}
    o['M  manual today']        = (T_man/S, 0.0)
    o['ii async+SO+halt']       = (T_fo/S + P_api*T_man/S + p619, 0.0)
    o['F1 3.x rules+SO']        = (T_fo/S + P_api*T_man/S, 0.0)
    o['F1 + unfenced policy']   = (T_fo/S + P_api*(T_fo+60)/S, 0.0)
    o['F2 active-active']       = (0.5*T_det/S, 0.5*T_det/S)
    o['i  sync+WF']             = ((T_fo+D_S)/S + P_api*T_man/S, T_stall/S)
    return o
print("Per-event expected blocks lost (A death, B death), at 100 pool blocks/day")
for case in ('low','mid','high'):
    print(f"  case={case}")
    for k,(a,b) in per_event(S100, case=case).items():
        print(f"    {k:24s} A {a:8.4f}  B {b:8.4f}")
print()
print("Expected blocks lost per year, lambda per node per year (same for A and B), mid per-event case")
lams = (0.1, 1, 4, 12)
print("  option                   " + "".join(f"lam={l:<6}" for l in lams))
for k,(a,b) in per_event(S100, case='mid').items():
    print(f"  {k:24s} " + "".join(f"{l*(a+b):<10.3f}" for l in lams))
print()
print("Same, high per-event case (slow fence/API, slow detection, 2 h manual MTTR)")
for k,(a,b) in per_event(S100, case='high').items():
    print(f"  {k:24s} " + "".join(f"{l*(a+b):<10.3f}" for l in lams))
print()
# Miner-retention sensitivity: firmware abandons the pool after X s of failed reconnects and returns after 300 s.
X = 20; T_back = 300
def retention(S, T_fo=25, T_det=15):
    ii = (T_back/S if T_fo > X else T_fo/S)
    f2 = 0.5*(T_back/S if T_det > X else T_det/S)
    return ii, f2
for (tfo, tdet) in ((25,15),(25,8),(60,30)):
    ii, f2 = retention(S100, tfo, tdet)
    print(f"retention X={X}s back={T_back}s T_fo={tfo} T_det={tdet}: (ii) per A death {ii:.3f}; F2 per node death {f2:.3f}; per year at lam=4: (ii) {4*ii:.2f}, F2 {8*f2:.2f}")
print()
print("Pool blocks per year at 100/day:", 365*100)
