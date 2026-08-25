"""Pure vardiff math for the AuxPoW Stratum bridge."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal


@dataclass(frozen=True)
class VardiffConfig:
    enabled: bool
    target_share_interval_seconds: Decimal
    min_difficulty: Decimal
    max_difficulty: Decimal
    retarget_interval_seconds: Decimal
    max_step_factor: Decimal
    startup_difficulty: Decimal
    max_step_down_factor: Decimal = Decimal("2")
    ewma_alpha: Decimal = Decimal("0.4")
    retarget_tolerance: Decimal = Decimal("0.25")
    # Fast-arrival initial convergence. A session whose real accepted-share
    # rate sits far above its lane start difficulty -- a rental router that
    # filters submissions at its own, much higher difficulty, so the pool
    # only ever sees the survivors -- would otherwise need several ordinary
    # max_step_factor retargets to reach the difficulty it is really working
    # at. Because credit is target-based, every retarget interval spent
    # climbing is credit the miner never receives. These bound ONE early,
    # high-confidence move per connection: initial_min_accepted_shares of
    # evidence over at least initial_min_elapsed_seconds, showing an observed
    # difficulty at least initial_min_step_factor above the current one,
    # buys a single step capped at initial_max_step_factor instead of
    # max_step_factor. Set initial_convergence_enabled=False to restore the
    # pre-#132 behavior exactly.
    initial_convergence_enabled: bool = True
    initial_max_step_factor: Decimal = Decimal("64")
    initial_min_accepted_shares: int = 8
    initial_min_step_factor: Decimal = Decimal("4")
    initial_min_elapsed_seconds: Decimal = Decimal("1")
    # Observation-only difficulty at which a session counts as having reached
    # rental-router scale; matches the high-diff listener's default floor.
    # Metrics only -- it never gates or clamps a retarget.
    high_diff_arrival_threshold: Decimal = Decimal("500000")

    def __post_init__(self) -> None:
        positive_fields = {
            "target_share_interval_seconds": self.target_share_interval_seconds,
            "min_difficulty": self.min_difficulty,
            "max_difficulty": self.max_difficulty,
            "retarget_interval_seconds": self.retarget_interval_seconds,
            "max_step_factor": self.max_step_factor,
            "startup_difficulty": self.startup_difficulty,
            "max_step_down_factor": self.max_step_down_factor,
            "ewma_alpha": self.ewma_alpha,
        }
        for name, value in positive_fields.items():
            if not value.is_finite() or value <= 0:
                raise ValueError(f"{name} must be a positive finite decimal")
        for name, value in {
            "initial_max_step_factor": self.initial_max_step_factor,
            "initial_min_step_factor": self.initial_min_step_factor,
            "initial_min_elapsed_seconds": self.initial_min_elapsed_seconds,
            "high_diff_arrival_threshold": self.high_diff_arrival_threshold,
        }.items():
            if not value.is_finite() or value <= 0:
                raise ValueError(f"{name} must be a positive finite decimal")
        if self.min_difficulty > self.max_difficulty:
            raise ValueError("min_difficulty must be <= max_difficulty")
        if self.max_step_factor < 1:
            raise ValueError("max_step_factor must be >= 1")
        if self.max_step_down_factor < 1:
            raise ValueError("max_step_down_factor must be >= 1")
        if self.ewma_alpha > 1:
            raise ValueError("ewma_alpha must be <= 1")
        if not self.retarget_tolerance.is_finite() or self.retarget_tolerance < 0:
            raise ValueError("retarget_tolerance must be a non-negative finite decimal")
        # The initial cap is a relaxation of the ordinary bound, never a
        # tightening of it: a configuration that inverts them would silently
        # make the "fast" path slower than the one it replaces.
        if self.initial_max_step_factor < self.max_step_factor:
            raise ValueError("initial_max_step_factor must be >= max_step_factor")
        if self.initial_min_step_factor < 1:
            raise ValueError("initial_min_step_factor must be >= 1")
        if self.initial_min_accepted_shares < 1:
            raise ValueError("initial_min_accepted_shares must be >= 1")


def clamp(value: Decimal, minimum: Decimal, maximum: Decimal) -> Decimal:
    return min(max(value, minimum), maximum)


def observed_difficulty(
    *,
    accepted_difficulty: Decimal,
    elapsed_seconds: Decimal,
    target_share_interval_seconds: Decimal,
) -> Decimal | None:
    if not accepted_difficulty.is_finite() or accepted_difficulty < 0:
        raise ValueError("accepted_difficulty must be a non-negative finite decimal")
    if not elapsed_seconds.is_finite() or elapsed_seconds <= 0:
        raise ValueError("elapsed_seconds must be a positive finite decimal")
    if not target_share_interval_seconds.is_finite() or target_share_interval_seconds <= 0:
        raise ValueError("target_share_interval_seconds must be a positive finite decimal")
    if accepted_difficulty == 0:
        return None
    return accepted_difficulty * target_share_interval_seconds / elapsed_seconds


def smooth_difficulty_estimate(
    *,
    observed: Decimal,
    previous: Decimal | None,
    config: VardiffConfig,
) -> Decimal:
    if not observed.is_finite() or observed <= 0:
        raise ValueError("observed must be a positive finite decimal")
    if previous is None:
        return clamp(observed, config.min_difficulty, config.max_difficulty)
    if not previous.is_finite() or previous <= 0:
        raise ValueError("previous must be a positive finite decimal")
    alpha = config.ewma_alpha
    smoothed = (observed * alpha) + (previous * (Decimal(1) - alpha))
    return clamp(smoothed, config.min_difficulty, config.max_difficulty)


def should_retarget(current_difficulty: Decimal, next_difficulty: Decimal, tolerance: Decimal) -> bool:
    if not current_difficulty.is_finite() or current_difficulty <= 0:
        raise ValueError("current_difficulty must be a positive finite decimal")
    if not next_difficulty.is_finite() or next_difficulty <= 0:
        raise ValueError("next_difficulty must be a positive finite decimal")
    if not tolerance.is_finite() or tolerance < 0:
        raise ValueError("tolerance must be a non-negative finite decimal")
    if next_difficulty == current_difficulty:
        return False
    ratio = abs(next_difficulty - current_difficulty) / current_difficulty
    return ratio >= tolerance


def initial_convergence_ready(
    *,
    current_difficulty: Decimal,
    accepted_shares: int,
    accepted_difficulty: Decimal,
    elapsed_seconds: Decimal,
    config: VardiffConfig,
) -> bool:
    """Whether this window is decisive enough for one early initial retarget.

    Three independent guards, all of which must hold:

    * at least ``initial_min_accepted_shares`` of evidence when the caller
      evaluates this one-shot gate for the first time the share-count and
      elapsed-time floors are both met, so a single lucky share cannot move
      the session and repeatedly testing the same window cannot compound the
      false-positive probability;
    * ``initial_min_elapsed_seconds`` of window, so a burst delivered in a
      near-zero interval cannot divide a real work total by an
      almost-zero elapsed time and explode the observed rate;
    * an observed difficulty at least ``initial_min_step_factor`` above the
      current one, so only evidence far outside the Poisson noise of a
      small sample qualifies. At the 8-share default a miner exactly on
      target has a ~35% relative spread on its observed rate; requiring 4x
      keeps an on-cadence miner from being pushed up merely because this
      path exists, and makes the early move upward-only.

    The comparison uses this window's raw observed difficulty rather than
    the caller's smoothed estimate: the question here is what THIS window
    proves, and the smoothing that follows is free to temper the answer.
    """
    if not config.enabled or not config.initial_convergence_enabled:
        return False
    if accepted_shares < config.initial_min_accepted_shares:
        return False
    if not current_difficulty.is_finite() or current_difficulty <= 0:
        return False
    if not elapsed_seconds.is_finite() or elapsed_seconds < config.initial_min_elapsed_seconds:
        return False
    observed = observed_difficulty(
        accepted_difficulty=accepted_difficulty,
        elapsed_seconds=elapsed_seconds,
        target_share_interval_seconds=config.target_share_interval_seconds,
    )
    if observed is None:
        return False
    return observed >= current_difficulty * config.initial_min_step_factor


def calculate_next_difficulty(
    *,
    current_difficulty: Decimal,
    accepted_shares: int,
    elapsed_seconds: Decimal,
    config: VardiffConfig,
    accepted_difficulty: Decimal | None = None,
    difficulty_estimate: Decimal | None = None,
    initial_convergence: bool = False,
) -> Decimal:
    """Return the next difficulty for an observed share window.

    Difficulty moves toward a share-weighted observed difficulty estimate,
    then clamps to the configured per-retarget step and absolute bounds. A
    zero-share window steps down by the configured down factor.

    ``initial_convergence`` raises only the UPWARD step bound, to
    ``initial_max_step_factor``, for a session still in its one qualifying
    fast-arrival phase (see :func:`initial_convergence_ready`). The
    step-down bound, the EWMA estimate, and the absolute lane min/max are
    untouched, so a larger initial step can still never leave the lane.
    """

    if not current_difficulty.is_finite() or current_difficulty <= 0:
        raise ValueError("current_difficulty must be a positive finite decimal")
    if accepted_shares < 0:
        raise ValueError("accepted_shares must be >= 0")
    if not elapsed_seconds.is_finite() or elapsed_seconds <= 0:
        raise ValueError("elapsed_seconds must be a positive finite decimal")

    current_difficulty = clamp(current_difficulty, config.min_difficulty, config.max_difficulty)
    if difficulty_estimate is not None:
        if not difficulty_estimate.is_finite() or difficulty_estimate <= 0:
            raise ValueError("difficulty_estimate must be a positive finite decimal")
        target_difficulty = difficulty_estimate
    elif accepted_shares == 0:
        target_difficulty = current_difficulty / config.max_step_down_factor
    else:
        if accepted_difficulty is None:
            accepted_difficulty = current_difficulty * accepted_shares
        target_difficulty = observed_difficulty(
            accepted_difficulty=accepted_difficulty,
            elapsed_seconds=elapsed_seconds,
            target_share_interval_seconds=config.target_share_interval_seconds,
        )
        if target_difficulty is None:
            target_difficulty = current_difficulty / config.max_step_down_factor

    share_ratio = target_difficulty / current_difficulty
    min_step = Decimal(1) / config.max_step_down_factor
    max_step = (
        config.initial_max_step_factor
        if initial_convergence and config.initial_convergence_enabled
        else config.max_step_factor
    )
    step = clamp(share_ratio, min_step, max_step)
    return clamp(current_difficulty * step, config.min_difficulty, config.max_difficulty)
