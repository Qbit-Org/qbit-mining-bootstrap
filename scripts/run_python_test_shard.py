#!/usr/bin/env python3
"""Run one deterministic shard of the standard Python unittest discovery suite.

Discovery sorts module paths; distribute its top-level suites round-robin so
module/class fixtures stay together and new test modules join CI automatically.
Python citations in the deleted-test map must have a successful execution in
their assigned shard; unittest's successful exit for skipped fixtures is not
parity evidence.
"""

from __future__ import annotations

import argparse
import inspect
import os
from pathlib import Path
import sys
import unittest

if __package__:
    from .check_deleted_test_map import MAP, references_in
else:
    from check_deleted_test_map import MAP, references_in


ROOT = Path(__file__).resolve().parents[1]


def select_shard(
    suite: unittest.TestSuite, shard_index: int, shard_count: int
) -> unittest.TestSuite:
    if shard_count < 1 or not 0 <= shard_index < shard_count:
        raise ValueError("require shard-count > 0 and 0 <= shard-index < shard-count")
    modules = [module for module in suite if module.countTestCases()]
    selected = unittest.TestSuite(modules[shard_index::shard_count])
    if selected.countTestCases() == 0:
        raise ValueError("shard contains no tests; reduce shard-count")
    return selected


def cited_test_ids(suite: unittest.TestSuite, root: Path) -> dict[str, set[str]]:
    """Capture cited cases before unittest runs fixtures and consumes the suite."""
    citations = {}
    for line in (root / MAP).read_text(encoding="utf-8").splitlines():
        for path, extension, name in references_in(line)[0]:
            if extension == "py":
                citations[(root.joinpath(path).resolve(), name)] = f"{path}::{name}"
    expected: dict[str, set[str]] = {}
    pending = [suite]
    while pending:
        test = pending.pop()
        if isinstance(test, unittest.TestSuite):
            pending.extend(test)
            continue
        name = test._testMethodName
        method = inspect.unwrap(getattr(test, name))
        if not inspect.isroutine(method):
            continue
        source = inspect.getsourcefile(method)
        citation = citations.get((Path(source).resolve(), name)) if source else None
        if citation is not None:
            expected.setdefault(citation, set()).add(test.id())
    return expected


class ParityTestResult(unittest.TextTestResult):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.successful_ids: set[str] = set()

    def addSuccess(self, test):
        super().addSuccess(test)
        self.successful_ids.add(test.id())


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--shard-index", type=int, required=True)
    parser.add_argument("--shard-count", type=int, required=True)
    args = parser.parse_args(argv)

    # Match `python -m unittest discover -s tests -p 'test_*.py'` even when
    # invoked by absolute script path from another working directory.
    os.chdir(ROOT)
    sys.path.insert(0, str(ROOT))
    suite = unittest.TestLoader().discover("tests", pattern="test_*.py")
    try:
        selected = select_shard(suite, args.shard_index, args.shard_count)
        expected = cited_test_ids(selected, ROOT)
    except ValueError as error:
        parser.error(str(error))
    except (OSError, UnicodeDecodeError) as error:
        parser.error(f"cannot read {MAP}: {error}")
    print(
        f"Python test shard {args.shard_index}/{args.shard_count}: "
        f"{selected.countTestCases()} of {suite.countTestCases()} tests",
        flush=True,
    )
    result = unittest.TextTestRunner(verbosity=2, resultclass=ParityTestResult).run(selected)
    missing = sorted(citation for citation, ids in expected.items() if not ids & result.successful_ids)
    for citation in missing:
        print(f"Python parity citation `{citation}`: no successful execution in this shard", file=sys.stderr)
    return 0 if result.wasSuccessful() and not missing else 1


if __name__ == "__main__":
    raise SystemExit(main())
