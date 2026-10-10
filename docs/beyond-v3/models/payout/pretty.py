"""Print the shortest counterexample of each exhaustive configuration."""
import glob, json, os, sys
here = os.path.dirname(os.path.abspath(__file__))
seen = {}
for f in sorted(glob.glob(os.path.join(here, "out", "exhaustive_*.json"))):
    for r in json.load(open(f)):
        c = r.get("shortest_counterexample")
        if not c:
            continue
        k = r["name"]
        if k in seen and len(seen[k]["trace"]) <= len(c["trace"]):
            continue
        seen[k] = c
def nz(d):
    return {k: v for k, v in d.items() if v}
for name, c in sorted(seen.items()):
    print("==", name, "| init", nz(c["init"]), "| overpay", c["overpay"])
    for b in c["trace"]:
        if b["kind"] != "P":
            print(f"   h{b['h']} {b['kind']} marker={b.get('marker')}")
            continue
        extra = []
        if b.get("legacy"): extra.append("LEGACY builder (no R1/R-Chain)")
        if not b.get("recognized", True): extra.append("marker NOT recognised")
        if b.get("flip"): extra.append(f"marker read as {b['perceived']}")
        if b.get("Kconf") is not None: extra.append(f"view sums only {b['Kconf']}")
        if b.get("ck"): extra.append(f"checkpoint h{b['ck']['h']} formed knowing {b['ck']['knew']}")
        print(f"   h{b['h']} blk{b['blk']} {b['mode']} builder knew {b['K']} window W{b['w']} prior={nz(b['prior'])} "
              f"paid={nz(b['onchain'])} delta={nz(b['delta'])} {'; '.join(extra)}")
