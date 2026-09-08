#!/usr/bin/env python3
"""Run one deterministic shard of the standard Python unittest discovery suite.

Discovery sorts module paths; distribute its top-level suites round-robin so
module/class fixtures stay together and new test modules join CI automatically.
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import sys
import unittest


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
    except ValueError as error:
        parser.error(str(error))
    print(
        f"Python test shard {args.shard_index}/{args.shard_count}: "
        f"{selected.countTestCases()} of {suite.countTestCases()} tests",
        flush=True,
    )
    result = unittest.TextTestRunner(verbosity=2).run(selected)
    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    raise SystemExit(main())
