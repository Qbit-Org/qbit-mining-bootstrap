#!/usr/bin/env python3
"""Generate a valid qbit AuxPoW payload from createauxblock JSON.

This helper intentionally reuses qbit's functional-test AuxPoW implementation
instead of maintaining a second serializer here.
"""

from __future__ import annotations

import argparse
import inspect
import json
import os
import sys
import time
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--template-file", help="Path to createauxblock JSON")
    parser.add_argument("--template-json", help="Raw createauxblock JSON string")
    parser.add_argument("--qbit-src", help="Path to a separate qbit source tree")
    parser.add_argument("--parent-time", type=int, help="Unix time for the synthetic parent header")
    parser.add_argument("--nonce", type=int, default=0, help="Nonce used for the AuxPoW slot LCG")
    parser.add_argument(
        "--output",
        choices=("hex", "json"),
        default="hex",
        help="Print raw auxpow hex or a small JSON object with hash and auxpow_hex",
    )
    return parser.parse_args()


def resolve_qbit_src(explicit: str | None) -> Path:
    candidate = explicit or os.environ.get("QBIT_SRC_DIR")
    if not candidate:
        raise SystemExit(
            "Set --qbit-src or QBIT_SRC_DIR to a separate qbit source checkout "
            "so this script can import test_framework.auxpow."
        )

    qbit_src = Path(candidate).expanduser()
    helper = qbit_src / "test/functional/test_framework/auxpow.py"
    if helper.exists():
        return qbit_src.resolve()

    raise SystemExit(f"--qbit-src / QBIT_SRC_DIR does not contain {helper.relative_to(qbit_src)}: {qbit_src}")


def load_template(args: argparse.Namespace) -> dict[str, object]:
    if args.template_file:
        return json.loads(Path(args.template_file).read_text())
    if args.template_json:
        return json.loads(args.template_json)
    return json.load(sys.stdin)


def template_commitment_order(template: dict[str, object]) -> str | None:
    raw_order = template.get("commitmentorder")
    if raw_order is None:
        return None
    order = str(raw_order).lower()
    if order in {"display", "internal"}:
        return order
    raise SystemExit(f"createauxblock returned unsupported commitmentorder={raw_order!r}")


def require_commitment_order_helper(helper: object, qbit_src: Path) -> None:
    parameters = inspect.signature(helper).parameters.values()
    if any(parameter.name == "commitment_order" or parameter.kind == inspect.Parameter.VAR_KEYWORD for parameter in parameters):
        return
    helper_path = qbit_src / "test/functional/test_framework/auxpow.py"
    raise SystemExit(
        f"{helper_path} does not support createauxblock.commitmentorder. "
        "Use a qbit checkout with the activation-aware AuxPoW functional-test helper."
    )


def main() -> int:
    args = parse_args()
    template = load_template(args)
    qbit_src = resolve_qbit_src(args.qbit_src)
    commitment_order = template_commitment_order(template)

    sys.path.insert(0, str(qbit_src / "test/functional"))
    from test_framework.auxpow import make_valid_auxpow_from_template

    parent_time = args.parent_time if args.parent_time is not None else int(time.time())
    auxpow_kwargs = {"parent_time": parent_time, "nonce": args.nonce}
    if commitment_order is not None:
        require_commitment_order_helper(make_valid_auxpow_from_template, qbit_src)
        auxpow_kwargs["commitment_order"] = commitment_order
    auxpow = make_valid_auxpow_from_template(template, **auxpow_kwargs)
    auxpow_hex = auxpow.to_hex()

    if args.output == "json":
        print(
            json.dumps(
                {
                    "hash": template["hash"],
                    "chainid": template["chainid"],
                    "auxpow_hex": auxpow_hex,
                }
            )
        )
    else:
        print(auxpow_hex)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
