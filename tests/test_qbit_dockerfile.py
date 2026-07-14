#!/usr/bin/env python3

from __future__ import annotations

import re
import unittest
from pathlib import Path


DOCKERFILE = Path(__file__).parents[1] / "docker" / "qbit" / "Dockerfile"


class QbitDockerfileTests(unittest.TestCase):
    def test_qbit_runtime_installs_qbit_named_components(self) -> None:
        dockerfile = DOCKERFILE.read_text(encoding="utf-8")

        self.assertRegex(dockerfile, r"cmake --build build --target bitcoind bitcoin-cli")
        self.assertRegex(dockerfile, r"--component qbitd\b")
        self.assertRegex(dockerfile, r"--component qbit-cli\b")
        self.assertNotRegex(dockerfile, r"--component bitcoind\b")
        self.assertNotRegex(dockerfile, r"--component bitcoin-cli\b")
        self.assertRegex(dockerfile, r"install -m 0755 -D build/bin/qbitd /opt/qbit/bin/qbitd")
        self.assertRegex(dockerfile, r"install -m 0755 -D build/bin/qbit-cli /opt/qbit/bin/qbit-cli")
        self.assertRegex(dockerfile, r"install -m 0755 -D build/bin/bitcoind /opt/qbit/bin/qbitd")
        self.assertRegex(dockerfile, r"install -m 0755 -D build/bin/bitcoin-cli /opt/qbit/bin/qbit-cli")

    def test_qbit_runtime_copies_qbit_named_binaries(self) -> None:
        dockerfile = DOCKERFILE.read_text(encoding="utf-8")
        copy_sources = re.findall(r"COPY --from=build-from-source ([^ ]+) ", dockerfile)

        self.assertIn("/opt/qbit/bin/qbitd", copy_sources)
        self.assertIn("/opt/qbit/bin/qbit-cli", copy_sources)

    def test_qbit_runtime_keeps_tini_and_uses_exec_entrypoint(self) -> None:
        dockerfile = DOCKERFILE.read_text(encoding="utf-8")

        self.assertIn(
            "COPY --chmod=0755 docker/qbit/qbit-entrypoint.sh /usr/local/bin/qbit-entrypoint.sh",
            dockerfile,
        )
        self.assertIn(
            'ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/qbit-entrypoint.sh"]',
            dockerfile,
        )


if __name__ == "__main__":
    unittest.main()
