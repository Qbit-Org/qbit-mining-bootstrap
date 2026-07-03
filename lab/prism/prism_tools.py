"""Resolution of qbit-prism CLI tool invocations.

The coordinator and ledger shell out to Rust helpers (audit bundle builder,
verifier, canonicalizer). Invoking them through ``cargo run`` re-checks the
build graph on every call, which is measurable overhead on a hot path and
much worse under load. When ``PRISM_TOOL_BIN_DIR`` points at a directory with
prebuilt binaries (the container image sets it to the release target dir),
run the binary directly; otherwise fall back to ``cargo run`` so developer
flows keep working without a prebuilt target dir.
"""

from __future__ import annotations

import os
import shlex
from pathlib import Path


def prism_tool_command(bin_name: str) -> list[str]:
    """Return the argv prefix for a qbit-prism CLI tool.

    Tool arguments should be appended to the returned list; the ``cargo run``
    fallback already ends with ``--`` so positional arguments are never
    swallowed by cargo itself.
    """
    bin_dir = os.environ.get("PRISM_TOOL_BIN_DIR")
    if bin_dir:
        candidate = Path(bin_dir) / bin_name
        if candidate.is_file() and os.access(candidate, os.X_OK):
            return [str(candidate)]
    cargo = shlex.split(os.environ.get("PRISM_CARGO", os.environ.get("CARGO", "cargo")))
    return cargo + ["run", "--quiet", "-p", "qbit-prism", "--bin", bin_name, "--"]
