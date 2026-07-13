#!/usr/bin/env python3

from __future__ import annotations

import re
import unittest
from pathlib import Path


ROOT = Path(__file__).parents[1]
DOCKERFILE = ROOT / "docker" / "bitcoin" / "Dockerfile"
COMPOSE_FILE = ROOT / "compose.yaml"
UPSTREAM_ENV = ROOT / "config" / "upstream.env"


def parse_env(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if line and not line.startswith("#") and "=" in line:
            key, value = line.split("=", 1)
            values[key] = value
    return values


class BitcoinDockerfileTests(unittest.TestCase):
    def test_release_archive_is_verified_before_extraction(self) -> None:
        dockerfile = DOCKERFILE.read_text(encoding="utf-8")

        self.assertIn("ARG BITCOIN_RELEASE_SHA256_AMD64", dockerfile)
        self.assertIn("ARG BITCOIN_RELEASE_SHA256_ARM64", dockerfile)
        self.assertRegex(
            dockerfile,
            r'amd64\) release_arch="x86_64-linux-gnu"; release_sha256="\$\{BITCOIN_RELEASE_SHA256_AMD64\}"',
        )
        self.assertRegex(
            dockerfile,
            r'arm64\) release_arch="aarch64-linux-gnu"; release_sha256="\$\{BITCOIN_RELEASE_SHA256_ARM64\}"',
        )
        verify_at = dockerfile.index("sha256sum -c -")
        extract_at = dockerfile.index("tar -xzf /tmp/bitcoin.tar.gz")
        bitcoind_assert_at = dockerfile.index("test -x /usr/local/bin/bitcoind")
        cli_assert_at = dockerfile.index("test -x /usr/local/bin/bitcoin-cli")
        self.assertLess(verify_at, extract_at)
        self.assertGreater(bitcoind_assert_at, extract_at)
        self.assertGreater(cli_assert_at, bitcoind_assert_at)

    def test_release_digest_is_mandatory_and_not_bypassed_by_custom_url(self) -> None:
        dockerfile = DOCKERFILE.read_text(encoding="utf-8")

        digest_validation_at = dockerfile.index("64-character lowercase Bitcoin Core SHA256")
        custom_url_at = dockerfile.index('if [ -n "${BITCOIN_RELEASE_URL}" ]')
        checksum_at = dockerfile.index("sha256sum -c -")
        self.assertLess(digest_validation_at, custom_url_at)
        self.assertGreater(checksum_at, custom_url_at)

    def test_checked_in_release_has_architecture_specific_sha256_digests(self) -> None:
        env = parse_env(UPSTREAM_ENV)

        self.assertEqual(env["BITCOIN_RELEASE_VERSION"], "30.2")
        self.assertEqual(
            env["BITCOIN_RELEASE_SHA256_AMD64"],
            "6aa7bb4feb699c4c6262dd23e4004191f6df7f373b5d5978b5bcdd4bb72f75d8",
        )
        self.assertEqual(
            env["BITCOIN_RELEASE_SHA256_ARM64"],
            "73e76c14edc79808a0511c744d102ffbb494807ee90cbcba176568243254b532",
        )
        for key in ("BITCOIN_RELEASE_SHA256_AMD64", "BITCOIN_RELEASE_SHA256_ARM64"):
            self.assertRegex(env[key], re.compile(r"^[0-9a-f]{64}$"))

    def test_compose_passes_both_release_digests_to_the_image_build(self) -> None:
        compose = COMPOSE_FILE.read_text(encoding="utf-8")

        self.assertIn(
            "BITCOIN_RELEASE_SHA256_AMD64: ${BITCOIN_RELEASE_SHA256_AMD64}",
            compose,
        )
        self.assertIn(
            "BITCOIN_RELEASE_SHA256_ARM64: ${BITCOIN_RELEASE_SHA256_ARM64}",
            compose,
        )


if __name__ == "__main__":
    unittest.main()
