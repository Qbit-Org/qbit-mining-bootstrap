"""Pure PRISM submit classification boundaries.

This is the narrow decision layer extracted in PR #77, kept independent of the
coordinator so transition authority cannot accidentally acquire delivery,
ledger, or block-submission side effects.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


PRISM_CREDIT_POLICY_STALE_GRACE = "stale-grace"
PRISM_CREDIT_POLICY_FANOUT_TRANSITION = "fanout-transition"
PRISM_PRIOR_TIP_CREDIT_POLICIES = frozenset(
    {
        PRISM_CREDIT_POLICY_STALE_GRACE,
        PRISM_CREDIT_POLICY_FANOUT_TRANSITION,
    }
)


@dataclass(frozen=True)
class SubmitContextInput:
    """Side-effect-free inputs for selecting one immutable job context."""

    active_context: Any | None
    retained_context: Any | None
    transition_context: Any | None
    current_tip: str
    stale_grace_eligible: bool


@dataclass(frozen=True)
class SubmitContextDecision:
    context: Any
    source: str
    credit_policy: str | None


@dataclass(frozen=True)
class SubmitWorkDecision:
    block_worthy: bool
    credit_share_on_accept: bool
    route: str


def classify_submit_context(value: SubmitContextInput) -> SubmitContextDecision | None:
    """Select normal, transition, or stale-grace authority.

    Missing contexts and policy-specific rejection reasons remain coordinator
    concerns because only the coordinator owns connection-local lease state.
    """

    context = value.active_context or value.retained_context
    source = "active" if value.active_context is not None else "retained"
    if context is not None and str(context.template["previousblockhash"]) == value.current_tip:
        return SubmitContextDecision(context=context, source=source, credit_policy=None)
    if value.transition_context is not None:
        return SubmitContextDecision(
            context=value.transition_context,
            source="fanout-transition",
            credit_policy=PRISM_CREDIT_POLICY_FANOUT_TRANSITION,
        )
    if context is not None and value.stale_grace_eligible:
        return SubmitContextDecision(
            context=context,
            source=source,
            credit_policy=PRISM_CREDIT_POLICY_STALE_GRACE,
        )
    return None


def classify_submit_work(
    submission: Any,
    *,
    credit_policy: str | None,
) -> SubmitWorkDecision:
    """Route one validated proof without permitting prior-tip candidates."""

    block_worthy = bool(submission.block_pass) and (
        credit_policy not in PRISM_PRIOR_TIP_CREDIT_POLICIES
    )
    if not block_worthy:
        route = "share"
    elif submission.share_pass:
        route = "async_block"
    else:
        route = "synchronous_block"
    return SubmitWorkDecision(
        block_worthy=block_worthy,
        credit_share_on_accept=route == "synchronous_block",
        route=route,
    )
