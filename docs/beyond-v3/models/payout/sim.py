"""Timed two-node simulation of the chain-anchored payout core.

One tick = 1 s. Two nodes A and B; a qbit chain with foreign blocks, pool
blocks, forks and reorgs; per-job replication with lag; faults (death with or
without disk loss, return, VLAN+tailnet partition, lagging standby); R1 (sum or
carry-vector), R-Publish (durable on the peer before publication, carry-free
fallback at a tip change, "alone" mode), adopt-from-chain (a node that holds a
job's inputs knows any block found on it), R-Chain (carry-free when an unknown
carry-paying pool block is in the ancestry; unknown carry-free blocks ignored).

Two deployment shapes:
  F1  single PostgreSQL writer + async standby; only the writer builds work;
      promotion after the writer is silent (fenced unless --unfenced); a
      returning node rejoins by fresh base backup (its unique rows discarded,
      optionally salvaged into the writer: --salvage).
  F2  two writable nodes, each builds work from its own store, owner-only rows
      replicated both ways; T_cf carry-free period while the peer is unreachable.

Checks every tick on the active chain:
  * overpay (debt increase) - must be 0;
  * data: every pool block on the active chain has its job inputs on a live
    node (or is reconstructible); else it is "lost" (counted with the number
    of faults so far);
At the end (all nodes up, link healed, salvage per variant): convergence of
the two nodes' derived balances, and carry-vector vs sum vs truth.

Run: python3 -I sim.py --help
"""
from __future__ import annotations

import argparse
import json
import os
import random
import sys
from collections import Counter, defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from policy import PolicyError, simple_policy  # noqa: E402

COINBASE = 10_000
FEE_BPS = 200
FLOOR = 1_000
DIRECT_FLOOR = 3_000          # below this an on-chain amount goes to a CTV fanout (model units)
ACCTS = ["m1", "m2", "m3", "m4", "m5", "m6"]


class Block:
    __slots__ = ("bid", "parent", "height", "job", "marker", "spoof")

    def __init__(self, bid, parent, height, job=None, marker=None, spoof=False):
        self.bid, self.parent, self.height, self.job, self.marker, self.spoof = bid, parent, height, job, marker, spoof


class Job:
    __slots__ = ("jid", "node", "parent", "mode", "prior", "gross", "onchain", "carry", "delta",
                 "fallback", "durable", "ctv_value", "swept", "published", "recon")

    def __init__(self, **kw):
        for k, v in kw.items():
            setattr(self, k, v)


class Node:
    def __init__(self, name):
        self.name = name
        self.alive = True
        self.disk = set()          # job ids whose inputs this node holds durably
        self.disk_ok = True        # False after a destroyed disk
        self.role = None           # F1: "primary" | "standby" | None
        self.current = {}          # parent block id -> published job id (current work per parent)
        self.cf_ticks = 0
        self.cp_ticks = 0
        self.issue_ticks = 0
        self.peer_unreachable_since = None
        self.alone = False


class Sim:
    def __init__(self, a):
        self.a = a
        self.rng = random.Random(a.seed)
        self.blocks = {0: Block(0, None, 0)}
        self.tip = 0
        self.jobs = {}
        self.nodes = {"A": Node("A"), "B": Node("B")}
        self.link_up = True
        self.queue = []            # (deliver_tick, src, dst, jid)
        self.faults = []           # (tick, description)
        self.t = 0
        self.init = {k: 0 for k in ACCTS}
        self.st = Counter()
        self.events = []
        self.lost_blocks = {}      # bid -> (tick first seen lost, faults at that time)
        self.salvage_dir = defaultdict(set)   # node -> job ids discarded at rejoin
        self.lag_until = -1
        self.windows_seed = self.rng.random()
        if a.shape == "F1":
            self.nodes["A"].role = "primary"
            self.nodes["B"].role = "standby"
        self.splitbrain = False
        self.promote_at = None
        self.log = []

    def L(self, msg):
        self.log.append((self.t, msg))

    # ------------------------------------------------------------------ chain
    def ancestry(self, bid):
        out = []
        while bid is not None and bid != 0:
            out.append(self.blocks[bid])
            bid = self.blocks[bid].parent
        out.reverse()
        return out

    def new_block(self, parent, job=None, marker=None, spoof=False):
        bid = len(self.blocks)
        self.blocks[bid] = Block(bid, parent, self.blocks[parent].height + 1, job, marker, spoof)
        return bid

    def chain_events(self):
        a = self.a
        r = self.rng.random()
        if r < a.p_foreign:
            spoof = self.rng.random() < a.p_spoof
            marker = ("cp" if self.rng.random() < 0.5 else "cf") if spoof else None
            nb = self.new_block(self.tip, marker=marker, spoof=spoof)
            self.set_tip(nb)
        elif r < a.p_foreign + a.p_fork:
            # competing block on the tip's parent; becomes active with prob 1/2 later
            par = self.blocks[self.tip].parent
            if par is not None:
                nb = self.new_block(par)
                if self.rng.random() < 0.5:
                    # it wins: next block extends it -> reorg of depth 1 (or deeper below)
                    nb2 = self.new_block(nb)
                    self.set_tip(nb2)
                    self.st["reorgs"] += 1
        if self.rng.random() < a.p_deep_reorg:
            # deep reorg: a side branch from k blocks below the tip overtakes
            k = self.rng.randint(2, 6)
            base = self.tip
            for _ in range(k):
                if self.blocks[base].parent is None:
                    break
                base = self.blocks[base].parent
            cur = base
            for _ in range(k + 1):
                cur = self.new_block(cur)
            self.set_tip(cur)
            self.st["deep_reorgs"] += 1

    def set_tip(self, bid):
        self.tip = bid

    # ------------------------------------------------------------ knowledge
    def knows(self, node, job):
        if job.jid in node.disk:
            return True
        if job.fallback and self.a.cf_recon and job.recon:
            return True        # fix: carry-free fallback work is reconstructible from durable inputs
        return False

    def view(self, node, parent):
        """R1 + R-Chain from `node`'s store, for work on `parent`."""
        a = self.a
        v = dict(self.init)
        missing = False
        latest_cv = None
        horizon = self.blocks[parent].height - a.lookback if a.lookback else -1
        for b in self.ancestry(parent):
            if b.job is None and not b.spoof:
                continue
            if b.spoof:
                if (b.marker == "cp" or a.rchain == "strict") and b.height > horizon:
                    missing = True
                continue
            job = self.jobs[b.job]
            if self.knows(node, job):
                for k, d in job.delta.items():
                    v[k] = v.get(k, 0) + d
                latest_cv = job.carry
            else:
                if job.mode == "cf" and a.rchain == "relaxed":
                    continue
                if b.height <= horizon:
                    continue    # bounded lookback: blocks behind the checkpoint are not checked
                missing = True
        if a.formulation == "cv" and latest_cv is not None:
            v = dict(latest_cv)
        return v, missing

    # ---------------------------------------------------------------- jobs
    def windows(self):
        # a random window over the accounts: some large, some sub-floor
        w = {}
        for k in ACCTS:
            x = self.rng.random()
            w[k] = int(1 + 100 * x ** 3)
        return w

    def build(self, node, parent, force_cf=False, fallback=False):
        view, missing = self.view(node, parent)
        mode = "cf" if (missing or force_cf) else "cp"
        if mode == "cf" and self.a.on_missing == "halt" and missing:
            return None
        if mode == "cf":
            prior = {k: (min(v, 0) if self.a.cf_prior == "clamp" else 0) for k, v in view.items()}
        else:
            prior = view
        try:
            res, fee, swept = simple_policy(COINBASE, FEE_BPS, FLOOR, self.windows(), prior)
        except PolicyError:
            self.st["policy_build_errors"] += 1
            return None
        gross = {k: r[0] for k, r in res.items()}
        onchain = {k: r[2] for k, r in res.items()}
        carry = {k: r[3] for k, r in res.items()}
        delta = {k: gross[k] - onchain[k] for k in res}
        ctv = sum(x for x in onchain.values() if 0 < x < DIRECT_FLOOR)
        jid = len(self.jobs)
        job = Job(jid=jid, node=node.name, parent=parent, mode=mode, prior=prior, gross=gross,
                  onchain=onchain, carry=carry, delta=delta, fallback=fallback, durable=False,
                  ctv_value=ctv, swept=swept, published=None,
                  recon=(self.a.cf_prior == "zero"))
        self.jobs[jid] = job
        node.disk.add(jid)
        return job

    def peer(self, node):
        return self.nodes["B" if node.name == "A" else "A"]

    def can_reach(self, x, y):
        return x.alive and y.alive and self.link_up

    def lag(self):
        if self.t < self.lag_until:
            return self.rng.randint(self.a.lag_slow_min, self.a.lag_slow_max)
        if self.rng.random() < self.a.p_slow_ack:
            return self.rng.randint(1, 3)
        return 0

    def replicate(self, src, jid, lag):
        dst = self.peer(src)
        self.queue.append((self.t + lag, src.name, dst.name, jid))

    def deliver(self):
        keep = []
        for (when, s, d, jid) in self.queue:
            src, dst = self.nodes[s], self.nodes[d]
            if not src.alive:
                # unsent WAL / log tail is gone with the source process; it stays on the
                # source's disk and is re-sent if the source returns (F2) or discarded (F1 rejoin)
                continue
            if self.a.shape == "F1" and not (src.role == "primary" and dst.role == "standby"):
                # diverged timelines (split brain) or a non-streaming pair: nothing flows
                if src.role == "primary" and dst.role == "primary":
                    continue
                keep.append((when, s, d, jid))
                continue
            if when <= self.t and self.can_reach(src, dst):
                if jid not in dst.disk and any(b.job == jid for b in self.blocks.values()):
                    self.L(f"j{jid} replicated {s}->{d}")
                dst.disk.add(jid)
                self.jobs[jid].durable = True
            else:
                keep.append((max(when, self.t), s, d, jid))
        self.queue = keep

    def issuers(self):
        if self.a.shape == "F1":
            return [n for n in self.nodes.values() if n.alive and n.role == "primary"]
        return [n for n in self.nodes.values() if n.alive]

    def publish(self, node, tip_changed):
        """R-Publish for `node` on the current tip."""
        a = self.a
        peer = self.peer(node)
        reachable = self.can_reach(node, peer)
        if a.shape == "F1":
            alone = (not peer.alive) or (not reachable and a.alone_on_partition
                                         and node.peer_unreachable_since is not None
                                         and self.t - node.peer_unreachable_since >= a.alone_after)
        else:
            alone = (not reachable) and node.peer_unreachable_since is not None \
                and self.t - node.peer_unreachable_since >= a.tcf
        node.alone = alone
        cur = node.current.get(self.tip)
        need_new = tip_changed or cur is None or (self.t % a.reanchor == 0)
        if not need_new:
            return
        if not a.rpublish:
            job = self.build(node, self.tip)
            if job:
                job.published = self.t
                node.current[self.tip] = job.jid
                self.replicate(node, job.jid, self.lag())
            return
        if alone:
            job = self.build(node, self.tip)
            if job:
                job.published = self.t
                node.current[self.tip] = job.jid
                self.replicate(node, job.jid, 0)
            self.st["alone_publications"] += 1
            return
        if not reachable and a.shape == "F2":
            # peer unreachable, before T_cf: carry-free, not durable
            job = self.build(node, self.tip, force_cf=True, fallback=True)
            if job:
                job.published = self.t
                node.current[self.tip] = job.jid
                self.replicate(node, job.jid, 0)
            return
        # normal path: prepare, wait for the peer's flush within the bound
        lg = self.lag() if reachable else 10 ** 9
        job = self.build(node, self.tip)
        if job is None:
            return
        self.replicate(node, job.jid, lg if reachable else 0)
        if lg == 0:
            job.published = self.t
            job.durable = True
            peer.disk.add(job.jid)
            node.current[self.tip] = job.jid
            return
        # unconfirmed within the bound
        if tip_changed or cur is None:
            fb = self.build(node, self.tip, force_cf=True, fallback=True)
            self.st["fallback_publications"] += 1
            if fb:
                self.L(f"{node.name} tip {self.tip}: standby ack not within bound (lag {lg}); publishes carry-free fallback job j{fb.jid} (not durable)")
                fb.published = self.t
                node.current[self.tip] = fb.jid
                self.replicate(node, fb.jid, lg if reachable else 0)
            # the carry-paying job is published when its ack arrives (next ticks)
            self.pending_cp.append((self.t + lg, node.name, self.tip, job.jid))
        else:
            self.pending_cp.append((self.t + lg, node.name, self.tip, job.jid))

    def promote_pending(self):
        keep = []
        for (when, n, parent, jid) in self.pending_cp:
            node = self.nodes[n]
            if not node.alive or self.tip != parent:
                continue
            if when <= self.t and jid in self.peer(node).disk:
                self.jobs[jid].published = self.t
                node.current[parent] = jid
            elif when > self.t or self.can_reach(node, self.peer(node)):
                keep.append((when, n, parent, jid))
        self.pending_cp = keep

    # --------------------------------------------------------------- finds
    def finds(self):
        a = self.a
        for node in self.issuers():
            jid = node.current.get(self.tip)
            if jid is None:
                continue
            share = 1.0 / max(1, len(self.issuers())) if a.shape == "F2" else 1.0
            if self.rng.random() < a.p_pool * share:
                job = self.jobs[jid]
                nb = self.new_block(self.tip, job=jid, marker=job.mode)
                self.set_tip(nb)
                self.L(f"block b{nb} found on {node.name}'s job j{jid} mode={job.mode} fallback={job.fallback} durable_on_peer={jid in self.peer(node).disk} ctv={job.ctv_value}")
                self.st["pool_blocks"] += 1
                self.st["pool_blocks_" + job.mode] += 1
                if job.fallback:
                    self.st["pool_blocks_on_fallback"] += 1
                if not job.durable:
                    self.st["pool_blocks_nondurable_at_find"] += 1
                return True
        # a dead node's miners finishing its last job and self-broadcasting
        if a.p_selfbroadcast:
            for node in self.nodes.values():
                if not node.alive and node.current.get(self.tip) is not None \
                        and self.rng.random() < a.p_selfbroadcast:
                    job = self.jobs[node.current[self.tip]]
                    nb = self.new_block(self.tip, job=job.jid, marker=job.mode)
                    self.set_tip(nb)
                    self.st["selfbroadcast_blocks"] += 1
                    return True
        return False

    # --------------------------------------------------------------- faults
    def apply_faults(self):
        a = self.a
        for (when, what, arg) in self.schedule:
            if when != self.t:
                continue
            self.faults.append((self.t, what, arg))
            self.L(f"FAULT {what} {arg if arg is not None else ''}")
            if what == "die":
                n = self.nodes[arg]
                n.alive = False
                if a.shape == "F1" and n.role == "primary":
                    self.promote_at = self.t + a.detect
                n.role = None if a.shape == "F1" else n.role
            elif what == "destroy":
                n = self.nodes[arg]
                n.disk = set()
                n.disk_ok = False
            elif what == "return":
                self.node_return(self.nodes[arg])
            elif what == "partition":
                self.link_up = False
            elif what == "heal":
                self.link_up = True
            elif what == "lag":
                self.lag_until = self.t + arg
            elif what == "unfenced_promote":
                self.promote_at = self.t + a.detect

    def node_return(self, n):
        a = self.a
        n.alive = True
        n.current = {}
        if a.shape == "F1":
            prim = [x for x in self.nodes.values() if x is not n and x.alive and x.role == "primary"]
            if prim:
                p = prim[0]
                unique = n.disk - p.disk
                self.salvage_dir[n.name] |= unique
                if unique:
                    self.L(f"{n.name} rejoins by base backup from {p.name}; {len(unique)} job records only it held go to the salvage dir" + (" and are imported" if self.a.salvage else ""))
                if a.salvage:
                    p.disk |= unique      # automatic salvage import of the loser's rows
                n.disk = set(p.disk)      # fresh base backup
                n.role = "standby"
            else:
                n.role = "primary"
        else:
            # F2: catch up both ways
            peer = self.peer(n)
            if peer.alive:
                for jid in list(n.disk - peer.disk):
                    self.replicate(n, jid, 0)
                for jid in list(peer.disk - n.disk):
                    self.replicate(peer, jid, 0)

    def maybe_promote(self):
        a = self.a
        if a.shape != "F1" or self.promote_at is None or self.t < self.promote_at:
            return
        self.promote_at = None
        stb = [x for x in self.nodes.values() if x.alive and x.role == "standby"]
        if not stb:
            return
        s = stb[0]
        other = self.peer(s)
        if other.alive and other.role == "primary":
            if not a.unfenced or self.can_reach(s, other):
                return
            self.splitbrain = True
            self.st["splitbrain"] += 1
        s.role = "primary"
        self.L(f"{s.name} promoted to primary" + (" (split brain)" if self.splitbrain else ""))
        # undelivered replication from the dead primary is lost
        self.queue = [q for q in self.queue if q[1] != other.name or other.alive]

    # --------------------------------------------------------------- checks
    def truth_and_overpay(self):
        truth = dict(self.init)
        over = 0
        for b in self.ancestry(self.tip):
            if b.job is None:
                continue
            j = self.jobs[b.job]
            for k, d in j.delta.items():
                before = truth.get(k, 0)
                after = before + d
                inc = max(0, -after) - max(0, -before)
                if inc > 0:
                    over += inc
                truth[k] = after
        return truth, over

    def check_data(self):
        live = [n for n in self.nodes.values() if n.alive]
        dead_disks = [n for n in self.nodes.values() if not n.alive and n.disk_ok]
        for b in self.ancestry(self.tip):
            if b.job is None:
                continue
            job = self.jobs[b.job]
            if any(self.knows(n, job) for n in live):
                if b.bid in self.lost_blocks:
                    del self.lost_blocks[b.bid]
                continue
            where = "dead-disk" if any(job.jid in n.disk for n in dead_disks) else (
                "salvage-dir" if any(job.jid in s for s in self.salvage_dir.values()) else "nowhere")
            if b.bid not in self.lost_blocks:
                self.lost_blocks[b.bid] = {"tick": self.t, "faults": len(self.faults), "where": where, "job": job.jid, "block": b.bid,
                                           "mode": job.mode, "fallback": job.fallback,
                                           "alone": job.node, "ctv": job.ctv_value,
                                           "accrual": sum(max(0, d) for d in job.delta.values())}
            else:
                self.lost_blocks[b.bid]["where"] = where

    # ------------------------------------------------------------------ run
    def run(self):
        a = self.a
        self.pending_cp = []
        self.schedule = self.make_schedule()
        prev_tip = None
        max_over = 0
        for self.t in range(a.ticks):
            self.apply_faults()
            self.maybe_promote()
            for n in self.nodes.values():
                peer = self.peer(n)
                if n.alive and not self.can_reach(n, peer):
                    if n.peer_unreachable_since is None:
                        n.peer_unreachable_since = self.t
                else:
                    n.peer_unreachable_since = None
            self.deliver()
            self.promote_pending()
            self.chain_events()
            tip_changed = self.tip != prev_tip
            for n in self.issuers():
                self.publish(n, tip_changed)
            prev_tip = self.tip
            if self.finds():
                for n in self.issuers():
                    self.publish(n, True)
                prev_tip = self.tip
            for n in self.issuers():
                jid = n.current.get(self.tip)
                if jid is not None:
                    n.issue_ticks += 1
                    if self.jobs[jid].mode == "cf":
                        n.cf_ticks += 1
                    else:
                        n.cp_ticks += 1
            _, over = self.truth_and_overpay()
            max_over = max(max_over, over)
            if self.t % 5 == 0:
                self.check_data()
        self.check_data()
        self.st["max_overpay"] = max_over
        return self.finish()

    def make_schedule(self):
        a = self.a
        T = a.ticks
        s = []
        sc = a.scenario
        r = self.rng
        t1 = r.randint(T // 5, T // 2)
        if sc == "A-dies":
            s += [(t1, "die", "A"), (t1 + a.down, "return", "A")]
        elif sc == "A-dies-disk-lost":
            s += [(t1, "die", "A"), (t1, "destroy", "A"), (t1 + a.down, "return", "A")]
        elif sc == "B-dies":
            s += [(t1, "die", "B"), (t1 + a.down, "return", "B")]
        elif sc == "partition":
            s += [(t1, "partition", None), (t1 + a.down, "heal", None)]
        elif sc == "lag-then-A-dies-disk-lost":
            td = t1 + r.randint(1, a.down)
            s += [(t1, "lag", a.down), (td, "die", "A"), (td, "destroy", "A"), (t1 + 2 * a.down, "return", "A")]
        elif sc == "lag-then-A-dies":
            s += [(t1, "lag", a.down), (t1 + r.randint(1, a.down), "die", "A"),
                  (t1 + 2 * a.down, "return", "A")]
        elif sc == "partition-unfenced":
            s += [(t1, "partition", None), (t1 + 5, "unfenced_promote", None),
                  (t1 + a.down, "heal", None), (t1 + a.down + 1, "die", "A"), (t1 + a.down + 2, "return", "A")]
        elif sc == "B-dies-then-A-dies":     # declared double fault
            s += [(t1, "die", "B"), (t1 + a.down // 2, "die", "A"), (t1 + a.down // 2, "destroy", "A"),
                  (t1 + a.down, "return", "B"), (t1 + a.down + 5, "return", "A")]
        elif sc == "partition-then-A-dies":  # declared double fault
            s += [(t1, "partition", None), (t1 + a.down // 2, "die", "A"),
                  (t1 + a.down, "heal", None), (t1 + a.down + 5, "return", "A")]
        elif sc == "partition-then-A-dies-disk-lost":  # declared double fault
            s += [(t1, "partition", None), (t1 + a.down // 2, "die", "A"), (t1 + a.down // 2, "destroy", "A"),
                  (t1 + a.down, "heal", None), (t1 + a.down + 5, "return", "A")]
        elif sc == "partition-fence-A":
            # one network fault: A and B cannot reach each other on any path; B still reaches the
            # PhoenixNAP API, sees A silent on every path and powers it off (disk kept), then promotes
            s += [(t1, "partition", None), (t1 + a.fence_after, "die", "A"),
                  (t1 + a.down, "heal", None), (t1 + a.down + 5, "return", "A")]
        elif sc == "chronic-lag":
            s += [(1, "lag", T)]
        elif sc == "none":
            pass
        else:
            raise SystemExit("unknown scenario " + sc)
        return s

    def trace_for(self, lost):
        if not lost:
            return []
        jids = {x["job"] for x in lost}
        out = []
        for t, m in self.log:
            if m.startswith("FAULT") or "promoted" in m or "rejoins" in m or any(f"j{j} " in m + " " or f"j{j})" in m for j in jids):
                out.append(f"t={t}: {m}")
        return out[:40]

    def finish(self):
        a = self.a
        # heal everything for the convergence check
        self.link_up = True
        for n in self.nodes.values():
            if not n.alive:
                self.node_return(n)
        for _ in range(3):
            self.queue = [(0, s, d, j) for (_, s, d, j) in self.queue]
            self.deliver()
        if a.shape == "F2":
            A, B = self.nodes["A"], self.nodes["B"]
            A.disk |= B.disk
            B.disk |= A.disk
        during = {bid: dict(v) for bid, v in self.lost_blocks.items()}
        self.lost_blocks = {}
        self.check_data()
        final_lost = list(self.lost_blocks.values())
        self.lost_blocks = during
        truth, over = self.truth_and_overpay()
        views = {n: self.view(self.nodes[n], self.tip)[0] for n in self.nodes}
        missing = {n: self.view(self.nodes[n], self.tip)[1] for n in self.nodes}
        agree = views["A"] == views["B"]
        # latent double payment: a node that is carry-paying while its books owe more than the chain-truth
        latent = 0
        for n in self.nodes:
            if not missing[n]:
                latent = max(latent, sum(max(0, views[n].get(k, 0) - truth.get(k, 0)) for k in set(truth) | set(views[n])))
        keys = set(truth) | set(views["A"])
        under = sum(max(0, truth.get(k, 0) - views["A"].get(k, 0)) for k in keys)
        overc = sum(max(0, views["A"].get(k, 0) - truth.get(k, 0)) for k in keys)
        lost = [v for v in self.lost_blocks.values()]
        res = {
            "seed": a.seed, "shape": a.shape, "scenario": a.scenario, "formulation": a.formulation,
            "stats": dict(self.st),
            "faults": len(self.faults),
            "final_overpay_total": over,
            "views_agree": agree,
            "stuck_carry_free_at_end": {n: missing[n] for n in missing},
            "view_under_truth": under, "view_over_truth": overc, "latent_double_pay": latent,
            "lost_blocks": lost,
            "final_lost_blocks": final_lost,
            "cf_fraction": {n: (round(x.cf_ticks / x.issue_ticks, 3) if x.issue_ticks else None)
                            for n, x in self.nodes.items()},
            "splitbrain": self.splitbrain,
            "trace": self.trace_for(final_lost),
        }
        return res


def parse(argv=None):
    p = argparse.ArgumentParser()
    p.add_argument("--shape", default="F1", choices=["F1", "F2"])
    p.add_argument("--scenario", default="A-dies")
    p.add_argument("--formulation", default="sum", choices=["sum", "cv"])
    p.add_argument("--rchain", default="relaxed", choices=["relaxed", "strict"])
    p.add_argument("--on-missing", dest="on_missing", default="carryfree", choices=["carryfree", "halt"])
    p.add_argument("--cf-prior", dest="cf_prior", default="clamp", choices=["clamp", "zero"])
    p.add_argument("--cf-recon", dest="cf_recon", action="store_true",
                   help="fix: carry-free fallback work reconstructible from durable inputs")
    p.add_argument("--no-rpublish", dest="rpublish", action="store_false")
    p.add_argument("--alone-on-partition", dest="alone_on_partition", action="store_true")
    p.add_argument("--alone-after", dest="alone_after", type=int, default=60)
    p.add_argument("--tcf", type=int, default=600)
    p.add_argument("--lookback", type=int, default=0, help="R-Chain checks only the last N blocks (0 = all)")
    p.add_argument("--unfenced", action="store_true")
    p.add_argument("--salvage", action="store_true")
    p.add_argument("--ticks", type=int, default=4000)
    p.add_argument("--down", type=int, default=300)
    p.add_argument("--detect", type=int, default=30)
    p.add_argument("--fence-after", dest="fence_after", type=int, default=90)
    p.add_argument("--reanchor", type=int, default=60)
    p.add_argument("--p-foreign", dest="p_foreign", type=float, default=1 / 40)
    p.add_argument("--p-pool", dest="p_pool", type=float, default=1 / 40)
    p.add_argument("--p-fork", dest="p_fork", type=float, default=1 / 400)
    p.add_argument("--p-deep-reorg", dest="p_deep_reorg", type=float, default=1 / 4000)
    p.add_argument("--p-spoof", dest="p_spoof", type=float, default=0.0)
    p.add_argument("--p-slow-ack", dest="p_slow_ack", type=float, default=0.02)
    p.add_argument("--lag-slow-min", dest="lag_slow_min", type=int, default=2)
    p.add_argument("--lag-slow-max", dest="lag_slow_max", type=int, default=30)
    p.add_argument("--p-selfbroadcast", dest="p_selfbroadcast", type=float, default=0.0)
    p.add_argument("--seed", type=int, default=1)
    p.add_argument("--seeds", type=int, default=1)
    p.add_argument("--json", action="store_true")
    return p.parse_args(argv)


def main():
    a = parse()
    agg = Counter()
    examples = []
    base_seed = a.seed
    for s in range(base_seed, base_seed + a.seeds):
        a.seed = s
        r = Sim(a).run()
        agg["runs"] += 1
        agg["pool_blocks"] += r["stats"].get("pool_blocks", 0)
        agg["fallback_blocks"] += r["stats"].get("pool_blocks_on_fallback", 0)
        agg["overpay_runs"] += 1 if r["stats"].get("max_overpay", 0) > 0 else 0
        agg["disagree_runs"] += 0 if r["views_agree"] else 1
        agg["stuck_runs"] += 1 if any(r["stuck_carry_free_at_end"].values()) else 0
        agg["under_truth_runs"] += 1 if r["view_under_truth"] else 0
        agg["over_truth_runs"] += 1 if r["view_over_truth"] else 0
        agg["splitbrain_runs"] += 1 if r["splitbrain"] else 0
        agg["latent_double_pay_runs"] += 1 if r["latent_double_pay"] else 0
        agg["latent_double_pay_total"] += r["latent_double_pay"]
        for lb in r["lost_blocks"]:
            agg["during_unavailable_" + lb["where"] + "_" + lb["mode"] + ("_fallback" if lb["fallback"] else "")] += 1
        for lb in r["final_lost_blocks"]:
            key = lb["where"] + "_" + lb["mode"] + ("_fallback" if lb["fallback"] else "")
            agg["FINAL_lost_" + key] += 1
            agg["FINAL_lost_ctv_value"] += lb["ctv"]
            agg["FINAL_lost_accrual"] += lb["accrual"]
        if r["final_lost_blocks"]:
            agg["runs_with_final_loss"] += 1
        if r["final_lost_blocks"] or r["stats"].get("max_overpay", 0) or not r["views_agree"]:
            if len(examples) < 3:
                examples.append(r)
    out = {"args": {k: v for k, v in vars(a).items() if k not in ("seed",)}, "base_seed": base_seed,
           "aggregate": dict(agg), "examples": examples}
    if a.json:
        print(json.dumps(out, default=str))
    else:
        print(json.dumps(out["aggregate"]))


if __name__ == "__main__":
    main()
