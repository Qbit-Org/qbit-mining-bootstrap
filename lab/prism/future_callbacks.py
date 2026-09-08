"""Completion callbacks whose captured work is released after invocation."""

from __future__ import annotations

from concurrent.futures import Future
from typing import Callable, TypeVar


_Result = TypeVar("_Result")


def add_releasing_done_callback(
    future: Future[_Result], callback: Callable[[Future[_Result]], object]
) -> None:
    """Keep a callback alive until completion, then release its captures.

    Future retains registered callbacks after invoking them. A callback that
    captures a request holding that future therefore retains the request and
    result until cyclic GC, even when all external owners release them.
    Leave only this empty wrapper registered once the callback runs, including
    when it raises. Future still owns callback ordering and error handling.
    """
    pending: Callable[[Future[_Result]], object] | None = callback

    def finish(completed: Future[_Result]) -> None:
        nonlocal pending
        completing, pending = pending, None
        if completing is not None:
            completing(completed)

    future.add_done_callback(finish)
