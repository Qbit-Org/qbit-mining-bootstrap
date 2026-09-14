"""Failures crossing executor boundaries without retaining execution frames."""

from __future__ import annotations

import copy
import sys
import traceback
from concurrent.futures import Future
from typing import Any, Callable


def _copy_error(error: BaseException, seen: frozenset[int] = frozenset()) -> BaseException:
    if isinstance(error, BaseExceptionGroup):
        private = error.derive([_copy_error(child, seen | {id(error)}) for child in error.exceptions])
    else:
        try:
            private = copy.copy(error)
        except TypeError:
            # Custom exceptions may have keyword-only constructor arguments;
            # their already-initialized state can be copied without rerunning
            # that constructor. Built-in special layouts use copy above.
            private = BaseException.__new__(type(error), *error.args)
    private.__dict__.update(error.__dict__)
    private.__traceback__ = None
    private.__cause__ = None
    private.__context__ = None
    private.__suppress_context__ = error.__suppress_context__
    notes = list(getattr(error, "__notes__", ()))
    if error.__traceback__ is not None:
        # Diagnostics contain locations, never frame locals or frame objects.
        notes.append("Executor origin:\n" + "".join(
            traceback.format_tb(error.__traceback__, limit=32)
        )[-16384:])
    if notes:
        private.__notes__ = notes
    seen = seen | {id(error)}
    for name in ("__cause__", "__context__"):
        chained = getattr(error, name)
        if chained is not None and id(chained) not in seen:
            setattr(private, name, _copy_error(chained, seen))
    return private


class DetachedFailure:
    """An unraised prototype; every observer raises a private copy."""

    def __init__(self, error: BaseException) -> None:
        self._error = _copy_error(error)

    def raise_error(self) -> Any:
        raise _copy_error(self._error)


def _release_invocation_frames(error: BaseException, boundary: Any) -> None:
    pending = [error]
    seen = set()
    while pending:
        current = pending.pop()
        if id(current) in seen:
            continue
        seen.add(id(current))
        pending.extend(chained for chained in (current.__cause__, current.__context__)
                       if chained is not None)
        if isinstance(current, BaseExceptionGroup):
            pending.extend(current.exceptions)
        node = current.__traceback__
        while node is not None:
            parent = node.tb_frame.f_back
            while parent is not None and parent is not boundary:
                parent = parent.f_back
            if parent is boundary:
                node.tb_frame.clear()
            node = node.tb_next


def capture_failure(function: Callable[..., Any], *args: Any) -> Any:
    try:
        return function(*args)
    except BaseException as error:
        failure = DetachedFailure(error)
        # The exception has unwound out of every frame below this boundary.
        # Release those finished producers before publishing the detached
        # outcome. A producer may itself keep its exception in a local,
        # forming a cycle even though the Future never receives that error.
        # The running capture frame and any foreign cause frames are excluded;
        # no traceback links on the original/shared exception are mutated.
        boundary = sys._getframe()
        _release_invocation_frames(error, boundary)
        del boundary
        return failure


def detached_future_result(future: Future[Any], timeout: float | None = None) -> Any:
    # exception() raises only for actual wait expiry or Future cancellation.
    # A worker-raised TimeoutError is returned, so it cannot race a done()
    # check and be mistaken for expiry of this bounded join.
    error = future.exception(timeout=timeout)
    if error is not None:
        DetachedFailure(error).raise_error()
    result = future.result()
    if isinstance(result, DetachedFailure):
        result.raise_error()
    return result
