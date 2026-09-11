"""Address-space caps for isolated PRISM helper processes.

Stdlib only. ``statement_spool._EXEC_LIMITED`` runs as ``python -c`` without a
guaranteed import path, so it carries an inline copy of the same policy; a
parity test keeps the two in step.
"""

from __future__ import annotations

import sys


def apply_helper_memory_limit(limit_bytes: int) -> None:
    """Cap this process's address space (``RLIMIT_AS``) at ``limit_bytes``.

    Strict everywhere except macOS: a refused cap propagates and the helper
    dies. macOS rejects any address-space limit below the process's current
    virtual size (a fresh interpreter already reserves hundreds of GiB) and
    offers no working alternative, so there a refusal writes one line to
    stderr and the helper continues uncapped.
    """
    import resource

    try:
        resource.setrlimit(resource.RLIMIT_AS, (limit_bytes, limit_bytes))
    except (ValueError, OSError) as exc:
        if sys.platform != "darwin":
            raise
        sys.stderr.write(
            f"prism helper: RLIMIT_AS {limit_bytes} refused on darwin ({exc}); continuing uncapped\n"
        )


__all__ = ["apply_helper_memory_limit"]
