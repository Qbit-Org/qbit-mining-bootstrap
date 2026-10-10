"""Core of the red-team model: jobs, R1 / R-Chain variants, truth and invariants.

Economy (abstract units; the policy is the faithful port in policy.py):
  coinbase 10_000, pool fee 200 bps (miner reward 9_800), floor 1_000.
A job = (parent, window weights, prior vector, mode); its manifest comes from
apply_payout_policy. A pool block's as-issued delta per account is
gross - onchain (PRISM 3: sql/001_share_ledger.sql:1333-1367, the balance is
SUM(gross - onchain) over active blocks).

Truth on a chain = init + sum of deltas of every pool block on it (whatever
any node knows). Overpay at a block = increase of max(0, -truth) for some
account (PRISM's own debt definition, ledger/divergence.rs:1-21).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from policy import PolicyError, simple_policy

COINBASE = 10_000
FEE_BPS = 200
FLOOR = 1_000
ACCOUNTS = ("a", "b", "c")


@dataclass(frozen=True)
class Cfg:
    formulation: str = "sum"      # "sum" | "cv" (carry vector of latest known pool block)
    rchain: str = "relaxed"       # "relaxed" (unknown carry-free blocks ignored) | "strict" | "none"
                                  # | "latest" (only the latest marked pool block must be known)
    on_missing: str = "carryfree" # "carryfree" | "halt"
    cf_prior: str = "clamp"       # "clamp": min(prior,0) (proposal) | "zero": prior := 0
    cv_cf_record: str = "clamped" # cv only: carry vector a carry-free manifest records:
                                  # "clamped" (what apply_payout_policy emits) | "view" (view + delta)


@dataclass
class Manifest:
    mode: str                 # "cp" carry-paying | "cf" carry-free
    prior: dict
    gross: dict
    onchain: dict
    carry: dict               # as the manifest records it (policy output)
    delta: dict               # gross - onchain
    pool_fee: int
    swept: int


def run_policy(weights, prior, mode, cfg: Cfg) -> Optional[Manifest]:
    if mode == "cf":
        if cfg.cf_prior == "clamp":
            used = {k: min(v, 0) for k, v in prior.items()}
        else:
            used = {k: 0 for k in prior}
    else:
        used = dict(prior)
    try:
        res, fee, swept = simple_policy(COINBASE, FEE_BPS, FLOOR, weights, used)
    except PolicyError:
        return None
    gross = {k: v[0] for k, v in res.items()}
    onchain = {k: v[2] for k, v in res.items()}
    carry = {k: v[3] for k, v in res.items()}
    delta = {k: gross[k] - onchain[k] for k in res}
    if mode == "cf" and cfg.cv_cf_record == "view":
        carry = {k: prior.get(k, 0) + delta.get(k, 0) for k in set(prior) | set(delta)}
    return Manifest(mode, used, gross, onchain, carry, delta, fee, swept)


@dataclass
class PBlock:
    """A block on the (final) chain as the model sees it."""
    idx: int
    kind: str                 # "P" pool, "F" foreign, "S" foreign carrying a copied/spoofed marker
    manifest: Optional[Manifest] = None
    recognized: bool = True   # does a node's marker check classify it as a pool block?
    perceived_mode: Optional[str] = None  # the mode a node reads from the marker
    height: int = 0


def view_for(ancestry, known, cfg: Cfg, init, ck=None, known_conf=None):
    """Prior a builder derives for work on the tip of `ancestry`.

    known: set of block idx whose manifest the builder holds (verified).
    ck: optional checkpoint (height, vector): blocks at height <= ck height are
        summarised by vector and NOT checked by R-Chain.
    known_conf: optional subset of `known` whose deltas the view sums (models an
        R1 implemented over the DB 'confirmed' set instead of the ancestry).
    Returns (view, missing_cp, n_unknown_cf_skipped).
    """
    start_h = -1
    base = dict(init)
    if ck is not None:
        start_h, base = ck[0], dict(ck[1])
    missing = False
    skipped = 0
    view = dict(base)
    latest_cv = None
    for b in ancestry:
        if b.height <= start_h:
            continue
        is_marked = (b.kind in ("P", "S")) and b.recognized
        if b.kind == "P" and b.idx in known:
            src = known_conf if known_conf is not None else known
            if b.idx in src:
                for k, d in b.manifest.delta.items():
                    view[k] = view.get(k, 0) + d
                latest_cv = b.manifest.carry
            continue
        if is_marked:
            # authentic-looking pool block whose manifest the builder lacks
            if cfg.rchain == "none":
                continue
            if b.perceived_mode == "cf" and cfg.rchain == "relaxed":
                skipped += 1
                continue
            missing = True
    if cfg.rchain == "latest":
        # alt-chain-anchored R4: only L(P), the latest marked pool block, must be
        # known (walking past unknown carry-free-marked ones).
        missing = False
        for b in reversed(ancestry):
            if b.height <= start_h:
                break
            if b.kind == "P" and b.idx in known:
                break
            if (b.kind in ("P", "S")) and b.recognized:
                if b.perceived_mode == "cf":
                    continue
                missing = True
                break
    if cfg.formulation == "cv" and latest_cv is not None:
        view = dict(latest_cv)
    return view, missing, skipped


def decide(ancestry, known, cfg: Cfg, init, ck=None, known_conf=None, legacy=False):
    """Mode and prior for a new job, or None (halt)."""
    view, missing, _ = view_for(ancestry, known, cfg, init, ck, known_conf)
    if legacy:
        return "cp", view   # mixed version: no R-Chain gate, DB view
    if missing:
        if cfg.on_missing == "halt":
            return None
        return "cf", view
    return "cp", view


def truth_walk(chain, init):
    """Yield (block, before, after) along the chain for pool blocks."""
    truth = dict(init)
    for b in chain:
        if b.kind != "P":
            continue
        before = dict(truth)
        for k, d in b.manifest.delta.items():
            truth[k] = truth.get(k, 0) + d
        yield b, before, dict(truth)


def overpays(chain, init):
    """List of (block idx, account, debt increase) along the chain."""
    out = []
    for b, before, after in truth_walk(chain, init):
        for k in set(before) | set(after):
            inc = max(0, -after.get(k, 0)) - max(0, -before.get(k, 0))
            if inc > 0:
                out.append((b.idx, k, inc))
    return out


def cumulative_violations(chain, init):
    """User's invariant for accounts with init >= 0: paid <= init + gross."""
    paid = {}
    gross = {}
    for b in chain:
        if b.kind != "P":
            continue
        for k in b.manifest.onchain:
            paid[k] = paid.get(k, 0) + b.manifest.onchain[k]
            gross[k] = gross.get(k, 0) + b.manifest.gross.get(k, 0)
    bad = []
    for k in paid:
        if init.get(k, 0) >= 0 and paid[k] > init.get(k, 0) + gross.get(k, 0):
            bad.append((k, paid[k] - init.get(k, 0) - gross.get(k, 0)))
    return bad


def final_truth(chain, init):
    t = dict(init)
    for b in chain:
        if b.kind == "P":
            for k, d in b.manifest.delta.items():
                t[k] = t.get(k, 0) + d
    return t
