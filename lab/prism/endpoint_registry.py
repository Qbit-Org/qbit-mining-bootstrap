"""Declarative classification of every endpoint the PRISM HTTP surface serves.

The coordinator process answers three unrelated kinds of request on one
listener (``lab/prism/audit_http.py``): a public read surface anyone on the
internet may poll, an operator surface describing this process instance, and a
set of payout-operations reads that contend with the writer lease the
block-landing path holds. Issue #145 extracts the first of those into its own
process; this module is the boundary made explicit so the split can be reviewed
one route at a time before anything moves.

Nothing here changes request handling. ``AuditHttpFacade`` still owns dispatch,
and this registry is asserted against it in
``tests/test_prism_endpoint_registry.py`` so a new route cannot be added without
being classified.

The boundary is not invented here. ``PRISM.md`` already states it: the
coordinator "exposes a private audit/ops listener and a dashboard-safe public
API from the same process", lists ``/audit/*``, ``/healthz``, ``/metrics`` and
``/owed-balances`` as private/internal, and concludes that "operators can expose
only ``/public/v1`` through a reverse proxy or dashboard frontend".
``docs/public-dashboard-api/README.md`` repeats it as a contract -- "the public
API must not expose ``/audit/*``, ``/metrics``, ``/healthz``, operator controls
..." -- and ``tests/test_public_dashboard_api_contract.py`` enforces it by
asserting the OpenAPI document contains no ``/audit/`` path.

So published audit evidence reaches the public through the content-addressed
``/public/v1/artifacts/{sha256}`` route and the artifact links that reference
it, not through ``/audit/*``. The evidence content is public; the ``/audit/*``
routes are internal tooling over the same data, with raw shapes
(``fanout_tx_hex``, ``manifest_set_json``, full broadcast-attempt histories),
raw ``sats`` units, no pagination, and in one case a caller-controlled unbounded
window.

The three axes a classification answers:

``Audience``
    ``PUBLIC_READ`` is the documented, versioned dashboard contract.
    ``OPERATOR`` describes a process instance or the private audit/ops tooling.
    ``PAYOUT_AFFECTING`` reports live payout obligations -- the state the
    settlement machinery itself acts on.

``LedgerAccess``
    How the response reaches durable state. ``WRITER_LOCK`` reads take
    ``share_ledger._operation_gate(self._lock, "writer lock")`` and therefore
    serialize against the lease-holding writer -- public volume on one of these
    is share-ingest backpressure. ``READ_SLOT`` reads take the bounded read
    semaphore (``PRISM_POSTGRES_READ_CONCURRENCY``) and are replica-eligible.
    ``ARTIFACT_FILE`` reads come from the published audit artifacts, which are
    already the durable public record. ``PROCESS_STATE`` reads in-memory
    coordinator state and cannot leave the process at all.

``Disposition``
    Where the route lands after issue #145.

``max_staleness_seconds``
    How old an answer an extracted route may serve before it must refuse.

Staleness is a contract, not a nicety: a silently stale dashboard is worse than
an honest one. ``lab/prism/observability.py`` set the precedent for this
process -- the cached ``/metrics`` endpoint returns 503 rather than serve an
unbounded-stale snapshot, using ``stale_after = max(3 * refresh_seconds,
MINIMUM_HEALTH_STALE_SECONDS)`` with a 15-second floor, and always publishes
the observed age. The extracted public tier follows the same discipline with
budgets derived from the caches that actually sit under each route rather than
hand-picked per endpoint:

    budget = max(3 * (cache_ttl_seconds + underlying_cache_seconds),
                 MINIMUM_PUBLIC_STALE_SECONDS)

``cache_ttl_seconds`` is the route's own shared-response TTL from
``public_api.public_cache_policy`` at its documented default: 5s for the plain
row reads, 30s for the pool-wide aggregates (``/public/v1/pool-summary``,
``/public/v1/hashrate-series``, ``/public/v1/miners/{recipient_id}/workers``),
and 300s for ``/public/v1/mining-configuration``.

``underlying_cache_seconds`` is any second cache stacked beneath that one. Only
one exists today: the ledger's pool reward-window aggregate
(``share_ledger._pool_reward_window_aggregate``,
``PRISM_PUBLIC_REWARD_WINDOW_CACHE_SECONDS``, default 30s), reached from
``dashboard_miner_reward_window`` and therefore counted only for the miner page
``/public/v1/miners/{recipient_id}``. Two caches in series compound, so the
budget must cover their sum, not the larger of the two.

The factor of 3 and the 15-second floor are the precedent's, deliberately: a
budget of exactly one TTL would refuse a response that is merely due for
refresh, and a sub-15s budget on a 5s TTL would make ordinary scheduling jitter
look like an outage.

``/public/v1/artifacts/{sha256}`` is the one route with no budget at all, and
says so through ``immutable_content`` rather than by omission. Its content is
immutable and content-addressed: a body that hashes to the requested sha256 is
correct no matter how old it is, so refusing it for age would be refusing a
correct answer. The only observable staleness is the 404 -> 200 transition for
an artifact published moments ago, which no budget would fix.

Replica lag is the third staleness source named in
``docs/prism-public-operator-endpoint-split.md``. No replica exists yet, so it
contributes 0 here; when one lands it becomes another
``underlying_cache_seconds`` term rather than a new mechanism.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class Audience(Enum):
    PUBLIC_READ = "public_read"
    OPERATOR = "operator"
    PAYOUT_AFFECTING = "payout_affecting"


class LedgerAccess(Enum):
    WRITER_LOCK = "writer_lock"
    READ_SLOT = "read_slot"
    ARTIFACT_FILE = "artifact_file"
    PROCESS_STATE = "process_state"
    QBIT_RPC = "qbit_rpc"
    NONE = "none"


class Disposition(Enum):
    EXTRACT = "extract"
    RETAIN = "retain"


# The staleness floor, matching observability.MINIMUM_HEALTH_STALE_SECONDS.
MINIMUM_PUBLIC_STALE_SECONDS = 15

# Documented defaults of the caches a public response can sit behind. These are
# the defaults the budgets below are derived from; an operator who raises a TTL
# past its budget is told so by tests/test_prism_endpoint_registry.py.
PUBLIC_CACHE_TTL_DEFAULT_SECONDS = 5
PUBLIC_AGGREGATE_CACHE_TTL_DEFAULT_SECONDS = 30
PUBLIC_CONFIG_CACHE_TTL_DEFAULT_SECONDS = 300
POOL_REWARD_WINDOW_CACHE_DEFAULT_SECONDS = 30


def staleness_budget_seconds(
    *,
    cache_ttl_seconds: int,
    underlying_cache_seconds: int = 0,
) -> int:
    """Derive one route's staleness budget from the caches beneath it.

    See the module docstring: three refresh intervals of every cache in the
    series, floored at MINIMUM_PUBLIC_STALE_SECONDS.
    """

    if cache_ttl_seconds < 0 or underlying_cache_seconds < 0:
        raise ValueError("cache seconds must be nonnegative")
    return max(
        3 * (cache_ttl_seconds + underlying_cache_seconds),
        MINIMUM_PUBLIC_STALE_SECONDS,
    )


@dataclass(frozen=True)
class Endpoint:
    """One classified route.

    ``paths`` lists every request path that reaches the same handler, so an
    alias cannot be silently dropped by the extraction.
    """

    paths: tuple[str, ...]
    audience: Audience
    disposition: Disposition
    access: tuple[LedgerAccess, ...]
    rationale: str
    # Ledger methods reached, as evidence for the access classification.
    ledger_methods: tuple[str, ...] = ()
    # Set when a route's audience and its current implementation disagree --
    # the work the extraction must do before the route can move.
    extraction_blocker: str | None = None
    # How old a response this route may serve before it must refuse with 503.
    # Derived through staleness_budget_seconds(), never hand-picked. None means
    # no budget, which is only legitimate together with immutable_content.
    max_staleness_seconds: float | None = None
    # Content-addressed, immutable bodies: correct at any age, so this route
    # never refuses for staleness. Stated rather than left implicit.
    immutable_content: bool = False

    def __post_init__(self) -> None:
        if not self.paths:
            raise ValueError("endpoint must list at least one path")
        for path in self.paths:
            if not path.startswith("/"):
                raise ValueError(f"endpoint path must be absolute: {path!r}")
        if not self.access:
            raise ValueError(f"endpoint {self.paths[0]} must declare ledger access")
        if not self.rationale:
            raise ValueError(f"endpoint {self.paths[0]} must carry a rationale")
        if (self.audience is Audience.PUBLIC_READ) != (
            self.disposition is Disposition.EXTRACT
        ):
            raise ValueError(
                f"endpoint {self.paths[0]}: exactly the public read surface is "
                "extracted, so audience and disposition must agree"
            )
        if (
            LedgerAccess.WRITER_LOCK in self.access
            and self.disposition is Disposition.EXTRACT
            and self.extraction_blocker is None
        ):
            raise ValueError(
                f"endpoint {self.paths[0]} takes the writer lock and cannot be "
                "extracted without recording the blocker that must be removed"
            )
        if self.immutable_content and self.disposition is not Disposition.EXTRACT:
            raise ValueError(
                f"endpoint {self.paths[0]}: immutable_content describes the "
                "extracted public contract only"
            )
        if self.immutable_content and self.max_staleness_seconds is not None:
            raise ValueError(
                f"endpoint {self.paths[0]}: immutable content is correct at any "
                "age, so it must not also carry a staleness budget"
            )
        if self.disposition is Disposition.EXTRACT:
            if self.max_staleness_seconds is None and not self.immutable_content:
                raise ValueError(
                    f"endpoint {self.paths[0]}: every extracted route must state "
                    "the staleness it tolerates, or declare immutable_content"
                )
            if (
                self.max_staleness_seconds is not None
                and self.max_staleness_seconds < MINIMUM_PUBLIC_STALE_SECONDS
            ):
                raise ValueError(
                    f"endpoint {self.paths[0]}: staleness budget must not fall "
                    f"below {MINIMUM_PUBLIC_STALE_SECONDS}s"
                )
        elif self.max_staleness_seconds is not None:
            raise ValueError(
                f"endpoint {self.paths[0]}: the staleness contract covers the "
                "extracted public surface; retained routes answer for "
                "themselves"
            )

    @property
    def primary_path(self) -> str:
        return self.paths[0]


# --- Operator surface -------------------------------------------------------
#
# Process state, plus the private audit/ops listener. PRISM.md lists these as
# private/internal and instructs operators not to expose them to the internet.

_OPERATOR: tuple[Endpoint, ...] = (
    Endpoint(
        paths=("/healthz",),
        audience=Audience.OPERATOR,
        disposition=Disposition.RETAIN,
        access=(LedgerAccess.PROCESS_STATE,),
        rationale=(
            "Liveness of this coordinator instance: job-build progress, tip "
            "refresh, writer-lease state. Answered from an in-memory snapshot "
            "refreshed by a background thread, and overlaid with live progress "
            "state on every request so a cached ok=true cannot mask a failed "
            "refresh. A different process cannot answer it. Wired to the "
            "compose healthcheck."
        ),
    ),
    Endpoint(
        paths=("/metrics",),
        audience=Audience.OPERATOR,
        disposition=Disposition.RETAIN,
        access=(LedgerAccess.PROCESS_STATE,),
        rationale=(
            "Prometheus exposition of this process's counters and gauges. "
            "Serves only the complete cached snapshot and returns 503 once the "
            "snapshot exceeds max(3 * refresh, 15s) -- the staleness precedent "
            "the extracted service follows."
        ),
    ),
    Endpoint(
        paths=("/audit/latest",),
        audience=Audience.OPERATOR,
        disposition=Disposition.RETAIN,
        access=(LedgerAccess.ARTIFACT_FILE,),
        rationale=(
            "The most recently published evidence envelope, read from the "
            "audit artifact directory. Public consumers reach the same "
            "evidence content-addressed via /public/v1/artifacts/{sha256}; "
            "this route is the operator's view of the live envelope and is "
            "listed as private/internal in PRISM.md."
        ),
    ),
    Endpoint(
        paths=("/audit/share-window",),
        audience=Audience.OPERATOR,
        disposition=Disposition.RETAIN,
        access=(LedgerAccess.WRITER_LOCK,),
        ledger_methods=("audit_share_window",),
        rationale=(
            "Raw per-share rows for a caller-supplied anchor. The window is "
            "caller-controlled and unbounded, and the read is fenced behind "
            "the writer lock, so this is an operator reconciliation tool -- "
            "the public equivalent is the aggregated reward leaderboard."
        ),
    ),
    Endpoint(
        paths=("/audit/blocks/{block_hash}/payouts", "/audit/block/{block_hash}"),
        audience=Audience.OPERATOR,
        disposition=Disposition.RETAIN,
        access=(LedgerAccess.WRITER_LOCK,),
        ledger_methods=("audit_block_payouts",),
        rationale=(
            "Per-recipient payout rows for a settled block in raw sats, with "
            "maturity_state and chain_state. The public per-miner slice is "
            "/public/v1/miners/{recipient_id}/payouts. The short "
            "/audit/block/{block_hash} form is a legacy alias for the same "
            "handler."
        ),
    ),
    Endpoint(
        paths=("/audit/blocks/{block_hash}/bundle",),
        audience=Audience.OPERATOR,
        disposition=Disposition.RETAIN,
        access=(LedgerAccess.WRITER_LOCK, LedgerAccess.ARTIFACT_FILE),
        ledger_methods=("audit_bundle",),
        rationale=(
            "The full inline audit bundle plus coinbase_tx_hex for one block. "
            "The bundle body itself is public, content-addressed at "
            "/public/v1/artifacts/{sha256} and linked from the public "
            "settlement-artifacts route; this is the internal by-block "
            "lookup."
        ),
    ),
    Endpoint(
        paths=("/audit/commitments/{commitment_leaf_hex}/bundle",),
        audience=Audience.OPERATOR,
        disposition=Disposition.RETAIN,
        access=(LedgerAccess.WRITER_LOCK, LedgerAccess.ARTIFACT_FILE),
        ledger_methods=("audit_bundle_by_commitment",),
        rationale=(
            "The same bundle addressed by audit commitment leaf. No public "
            "commitment-leaf lookup exists; a verifier holding a leaf resolves "
            "the bundle sha256 from the block's public settlement artifacts "
            "and fetches it content-addressed."
        ),
    ),
    Endpoint(
        paths=("/audit/blocks/{block_hash}/ctv-fanout-manifest-set",),
        audience=Audience.OPERATOR,
        disposition=Disposition.RETAIN,
        access=(LedgerAccess.READ_SLOT,),
        ledger_methods=("audit_ctv_fanout_manifest_set",),
        rationale=(
            "The full CTV recovery payload, including parent_coinbase_tx_hex "
            "and manifest_set_json -- broadcast material, not a dashboard "
            "read. /public/v1/blocks/{block_hash}/settlement-artifacts is the "
            "public reshaping of the same ledger read."
        ),
    ),
    Endpoint(
        paths=("/audit/blocks/{block_hash}/ctv-fanouts",),
        audience=Audience.OPERATOR,
        disposition=Disposition.RETAIN,
        access=(LedgerAccess.READ_SLOT,),
        ledger_methods=("audit_ctv_fanouts", "audit_ctv_fanout_manifest_set"),
        rationale=(
            "Raw artifact dicts with fanout_tx_hex and sats fields, delegating "
            "to the manifest-set read above."
        ),
    ),
    Endpoint(
        paths=("/audit/fanouts/pending",),
        audience=Audience.OPERATOR,
        disposition=Disposition.RETAIN,
        access=(LedgerAccess.READ_SLOT,),
        ledger_methods=("pending_ctv_fanout_statuses",),
        rationale=(
            "The broadcaster's work queue, filtered to fanouts due now, with "
            "full broadcast-attempt histories. /public/v1/fanouts/pending is "
            "the paginated public view built from a different read model."
        ),
    ),
    Endpoint(
        paths=("/audit/fanouts/{fanout_txid}/status",),
        audience=Audience.OPERATOR,
        disposition=Disposition.RETAIN,
        access=(LedgerAccess.READ_SLOT,),
        ledger_methods=("ctv_fanout_status",),
        rationale=(
            "Raw broadcast status including submit_result and attempt detail. "
            "/public/v1/fanouts/{fanout_txid} wraps the same ledger read in "
            "the public schema."
        ),
    ),
)


# --- Payout-affecting surface ----------------------------------------------
#
# Live payout obligations: the state the settlement machinery acts on. Out of
# scope for extraction by the terms of issue #145.

_PAYOUT_AFFECTING: tuple[Endpoint, ...] = (
    Endpoint(
        paths=("/audit/carry-forward-integrity", "/audit/ledger-integrity"),
        audience=Audience.PAYOUT_AFFECTING,
        disposition=Disposition.RETAIN,
        access=(LedgerAccess.WRITER_LOCK,),
        ledger_methods=("carry_forward_integrity_report",),
        rationale=(
            "The drift count and mismatch list over active carry-forward rows, "
            "plus the audit_head_sha256 operators are told to mirror after "
            "payout-affecting blocks. It reports whether the payout ledger "
            "disagrees with itself. It runs two statements under one held "
            "writer gate, so it is also the worst route on the surface to "
            "expose to uncontrolled volume."
        ),
    ),
    Endpoint(
        paths=("/owed", "/owed-balances"),
        audience=Audience.PAYOUT_AFFECTING,
        disposition=Disposition.RETAIN,
        access=(LedgerAccess.WRITER_LOCK,),
        ledger_methods=("current_owed_balances",),
        rationale=(
            "Every recipient's outstanding owed balance -- the obligations the "
            "next settlement will pay. O(recipients) under the writer lock. A "
            "miner's own balance is available per-recipient from the public "
            "tier; there is no public all-recipients dump."
        ),
    ),
    Endpoint(
        paths=("/miners/{recipient_id}/status", "/payouts/{recipient_id}/status"),
        audience=Audience.PAYOUT_AFFECTING,
        disposition=Disposition.RETAIN,
        access=(LedgerAccess.WRITER_LOCK,),
        ledger_methods=("current_owed_balances", "recipient_payout_history"),
        rationale=(
            "The pre-dashboard payout-operations view: owed balances plus "
            "payout history in raw sats. Both aliases reach one handler. "
            "Superseded for public consumption by "
            "/public/v1/miners/{recipient_id}, which reports bits."
        ),
    ),
)


# --- Public read surface ----------------------------------------------------
#
# The documented, versioned dashboard contract. Exactly this moves.

_PUBLIC_READ: tuple[Endpoint, ...] = (
    Endpoint(
        paths=("/public/v1/pool-summary",),
        audience=Audience.PUBLIC_READ,
        disposition=Disposition.EXTRACT,
        access=(LedgerAccess.READ_SLOT, LedgerAccess.QBIT_RPC),
        ledger_methods=("dashboard_pool_snapshot",),
        rationale=(
            "Pool-wide headline figures. Needs a qbit RPC client for the "
            "network summary, which the extracted service gets its own copy "
            "of -- it is a node read, not coordinator state."
        ),
        # Pool-wide aggregate: the 30s aggregate TTL, nothing beneath it.
        max_staleness_seconds=staleness_budget_seconds(
            cache_ttl_seconds=PUBLIC_AGGREGATE_CACHE_TTL_DEFAULT_SECONDS,
        ),
    ),
    Endpoint(
        paths=("/public/v1/blocks",),
        audience=Audience.PUBLIC_READ,
        disposition=Disposition.EXTRACT,
        access=(LedgerAccess.READ_SLOT,),
        ledger_methods=("dashboard_blocks",),
        rationale="Paginated found-block history.",
        max_staleness_seconds=staleness_budget_seconds(
            cache_ttl_seconds=PUBLIC_CACHE_TTL_DEFAULT_SECONDS,
        ),
    ),
    Endpoint(
        paths=("/public/v1/leaderboard",),
        audience=Audience.PUBLIC_READ,
        disposition=Disposition.EXTRACT,
        access=(LedgerAccess.READ_SLOT, LedgerAccess.QBIT_RPC),
        ledger_methods=("dashboard_leaderboard", "dashboard_reward_leaderboard"),
        rationale=(
            "Both windows. window=3h is a plain read-slot aggregate; "
            "window=reward additionally needs the network difficulty from RPC "
            "to size the reward window."
        ),
        # Both windows read the ledger directly. dashboard_reward_leaderboard
        # runs its own qbit_prism_window scan rather than going through the
        # cached pool aggregate, so nothing stacks under the response TTL.
        max_staleness_seconds=staleness_budget_seconds(
            cache_ttl_seconds=PUBLIC_CACHE_TTL_DEFAULT_SECONDS,
        ),
    ),
    Endpoint(
        paths=("/public/v1/hashrate-series",),
        audience=Audience.PUBLIC_READ,
        disposition=Disposition.EXTRACT,
        access=(LedgerAccess.READ_SLOT,),
        ledger_methods=("dashboard_hashrate_series",),
        rationale=(
            "Bucketed hashrate history for the pool or one miner. The widest "
            "ledger scan on the public surface and the strongest single "
            "argument for moving this traffic off the primary."
        ),
        max_staleness_seconds=staleness_budget_seconds(
            cache_ttl_seconds=PUBLIC_AGGREGATE_CACHE_TTL_DEFAULT_SECONDS,
        ),
    ),
    Endpoint(
        paths=("/public/v1/mining-configuration",),
        audience=Audience.PUBLIC_READ,
        disposition=Disposition.EXTRACT,
        access=(LedgerAccess.NONE,),
        rationale=(
            "Stratum endpoints and pool fee. Touches no ledger at all -- it "
            "is assembled from environment configuration."
        ),
        extraction_blocker=(
            "Falls back to getattr(coordinator, 'port') and "
            "getattr(coordinator, 'bind') when the public URL env vars are "
            "unset. The extracted service has no coordinator, so "
            "PRISM_PUBLIC_STRATUM_URL / PRISM_STRATUM_PORT must be supplied "
            "explicitly rather than inferred."
        ),
        # Environment-derived and near-static, so it carries the longest
        # response TTL on the surface and, from it, the longest budget.
        max_staleness_seconds=staleness_budget_seconds(
            cache_ttl_seconds=PUBLIC_CONFIG_CACHE_TTL_DEFAULT_SECONDS,
        ),
    ),
    Endpoint(
        paths=("/public/v1/miners/{recipient_id}",),
        audience=Audience.PUBLIC_READ,
        disposition=Disposition.EXTRACT,
        access=(
            LedgerAccess.WRITER_LOCK,
            LedgerAccess.READ_SLOT,
            LedgerAccess.QBIT_RPC,
        ),
        ledger_methods=(
            "current_owed_balances",
            "dashboard_miner_share_summary",
            "dashboard_miner_reward_window",
            "dashboard_miner_payout_rows",
            "dashboard_miner_worker_rows",
            "dashboard_miner_lifetime_earnings_bits",
            "dashboard_miner_pending_maturity_bits",
            "dashboard_blocks",
        ),
        rationale=(
            "A miner's own dashboard page. The most-polled route on the "
            "surface and the clearest public read."
        ),
        extraction_blocker=(
            "owed_balance_for_recipient() calls current_owed_balances(), a "
            "writer-lock read that returns every recipient's balance and then "
            "filters in Python. So the single most-polled public route takes "
            "the writer lock today. The extracted service needs a "
            "per-recipient owed-balance read that does not fence."
        ),
        # The one route with a second cache beneath the response cache: its
        # reward-window slice comes from the shared pool aggregate, so the two
        # TTLs compound.
        max_staleness_seconds=staleness_budget_seconds(
            cache_ttl_seconds=PUBLIC_CACHE_TTL_DEFAULT_SECONDS,
            underlying_cache_seconds=POOL_REWARD_WINDOW_CACHE_DEFAULT_SECONDS,
        ),
    ),
    Endpoint(
        paths=("/public/v1/miners/{recipient_id}/earnings",),
        audience=Audience.PUBLIC_READ,
        disposition=Disposition.EXTRACT,
        access=(LedgerAccess.READ_SLOT,),
        ledger_methods=("dashboard_miner_earning_rows",),
        rationale="Per-block earnings history for one miner.",
        max_staleness_seconds=staleness_budget_seconds(
            cache_ttl_seconds=PUBLIC_CACHE_TTL_DEFAULT_SECONDS,
        ),
    ),
    Endpoint(
        paths=("/public/v1/miners/{recipient_id}/payouts",),
        audience=Audience.PUBLIC_READ,
        disposition=Disposition.EXTRACT,
        access=(LedgerAccess.READ_SLOT,),
        ledger_methods=("dashboard_miner_payout_rows",),
        rationale="Per-payout history for one miner.",
        max_staleness_seconds=staleness_budget_seconds(
            cache_ttl_seconds=PUBLIC_CACHE_TTL_DEFAULT_SECONDS,
        ),
    ),
    Endpoint(
        paths=("/public/v1/miners/{recipient_id}/workers",),
        audience=Audience.PUBLIC_READ,
        disposition=Disposition.EXTRACT,
        access=(LedgerAccess.READ_SLOT,),
        ledger_methods=("dashboard_miner_worker_rows",),
        rationale="Worker list for one miner.",
        # Classified as a pool-wide aggregate by public_cache_policy: it
        # aggregates a miner's whole recent share history.
        max_staleness_seconds=staleness_budget_seconds(
            cache_ttl_seconds=PUBLIC_AGGREGATE_CACHE_TTL_DEFAULT_SECONDS,
        ),
    ),
    Endpoint(
        paths=("/public/v1/blocks/{block_hash}/settlement-artifacts",),
        audience=Audience.PUBLIC_READ,
        disposition=Disposition.EXTRACT,
        access=(
            LedgerAccess.WRITER_LOCK,
            LedgerAccess.READ_SLOT,
            LedgerAccess.ARTIFACT_FILE,
        ),
        ledger_methods=(
            "audit_ctv_fanout_manifest_set",
            "audit_bundle",
            "ctv_fanout_status",
            "dashboard_public_artifact_exists",
        ),
        rationale=(
            "The public settlement view of a block, linking to the artifacts "
            "that prove it. This is how published evidence reaches the public "
            "without exposing /audit/*."
        ),
        extraction_blocker=(
            "The CTV path is read-slot, but a direct-coinbase block falls "
            "through to direct_coinbase_settlement_payload(), which calls "
            "audit_bundle() under the writer lock. The extracted service must "
            "take the direct-coinbase branch from the artifact file."
        ),
        max_staleness_seconds=staleness_budget_seconds(
            cache_ttl_seconds=PUBLIC_CACHE_TTL_DEFAULT_SECONDS,
        ),
    ),
    Endpoint(
        paths=("/public/v1/fanouts/pending",),
        audience=Audience.PUBLIC_READ,
        disposition=Disposition.EXTRACT,
        access=(LedgerAccess.READ_SLOT,),
        ledger_methods=("dashboard_pending_fanout_rows",),
        rationale="Public paginated view of fanouts awaiting broadcast.",
        max_staleness_seconds=staleness_budget_seconds(
            cache_ttl_seconds=PUBLIC_CACHE_TTL_DEFAULT_SECONDS,
        ),
    ),
    Endpoint(
        paths=("/public/v1/fanouts/{fanout_txid}",),
        audience=Audience.PUBLIC_READ,
        disposition=Disposition.EXTRACT,
        access=(LedgerAccess.READ_SLOT,),
        ledger_methods=("ctv_fanout_status",),
        rationale="Public view of one fanout's broadcast state.",
        max_staleness_seconds=staleness_budget_seconds(
            cache_ttl_seconds=PUBLIC_CACHE_TTL_DEFAULT_SECONDS,
        ),
    ),
    Endpoint(
        paths=("/public/v1/artifacts/{sha256}",),
        audience=Audience.PUBLIC_READ,
        disposition=Disposition.EXTRACT,
        access=(LedgerAccess.READ_SLOT, LedgerAccess.ARTIFACT_FILE),
        ledger_methods=("dashboard_public_artifact",),
        rationale=(
            "Content-addressed fetch of a published artifact -- the sanctioned "
            "public path to audit evidence, and the reason the public API can "
            "keep /audit/* internal. Already resolves the body from body_uri "
            "on disk when the inline column is NULL, verified against the "
            "requested sha256. Its PRISM_PUBLIC_ARTIFACT_CACHE_* knobs move "
            "with the extracted service. Content-addressed and immutable: a "
            "body that hashes to the requested sha256 is correct at any age, "
            "so this route carries no staleness budget and must never refuse "
            "for staleness."
        ),
        immutable_content=True,
    ),
)


ENDPOINTS: tuple[Endpoint, ...] = _OPERATOR + _PAYOUT_AFFECTING + _PUBLIC_READ


def endpoints_by_audience(audience: Audience) -> tuple[Endpoint, ...]:
    return tuple(item for item in ENDPOINTS if item.audience is audience)


def endpoints_by_disposition(disposition: Disposition) -> tuple[Endpoint, ...]:
    return tuple(item for item in ENDPOINTS if item.disposition is disposition)


def extracted_paths() -> tuple[str, ...]:
    """Every request path the extracted public service must answer."""

    return tuple(
        path
        for item in endpoints_by_disposition(Disposition.EXTRACT)
        for path in item.paths
    )


def retained_paths() -> tuple[str, ...]:
    """Every request path the coordinator keeps."""

    return tuple(
        path
        for item in endpoints_by_disposition(Disposition.RETAIN)
        for path in item.paths
    )


def extraction_blockers() -> tuple[tuple[str, str], ...]:
    """(path, blocker) for each route whose implementation must change first."""

    return tuple(
        (item.primary_path, item.extraction_blocker)
        for item in ENDPOINTS
        if item.extraction_blocker is not None
    )


def _template_matches(template: str, path: str) -> bool:
    """True when a concrete request path fills one registry path template."""

    template_parts = template.split("/")
    path_parts = path.split("/")
    if len(template_parts) != len(path_parts):
        return False
    for expected, actual in zip(template_parts, path_parts):
        if expected.startswith("{") and expected.endswith("}"):
            if not actual:
                return False
            continue
        if expected != actual:
            return False
    return True


def endpoint_for_request_path(path: str) -> Endpoint | None:
    """Classify one concrete request path, or None when it is not a route.

    Literal paths win over templates so /public/v1/fanouts/pending resolves to
    the pending list rather than to /public/v1/fanouts/{fanout_txid}, which is
    the same precedence dispatch() applies.
    """

    for endpoint in ENDPOINTS:
        if path in endpoint.paths:
            return endpoint
    for endpoint in ENDPOINTS:
        for template in endpoint.paths:
            if "{" in template and _template_matches(template, path):
                return endpoint
    return None


def writer_lock_paths() -> tuple[str, ...]:
    """Paths that currently serialize against the lease-holding writer."""

    return tuple(
        path
        for item in ENDPOINTS
        if LedgerAccess.WRITER_LOCK in item.access
        for path in item.paths
    )


__all__ = [
    "Audience",
    "Disposition",
    "ENDPOINTS",
    "Endpoint",
    "LedgerAccess",
    "MINIMUM_PUBLIC_STALE_SECONDS",
    "POOL_REWARD_WINDOW_CACHE_DEFAULT_SECONDS",
    "PUBLIC_AGGREGATE_CACHE_TTL_DEFAULT_SECONDS",
    "PUBLIC_CACHE_TTL_DEFAULT_SECONDS",
    "PUBLIC_CONFIG_CACHE_TTL_DEFAULT_SECONDS",
    "endpoint_for_request_path",
    "endpoints_by_audience",
    "endpoints_by_disposition",
    "extracted_paths",
    "extraction_blockers",
    "retained_paths",
    "staleness_budget_seconds",
    "writer_lock_paths",
]
