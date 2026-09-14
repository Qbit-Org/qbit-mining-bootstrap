"""Regression cases for retired operator configuration guidance."""
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

from scripts.check_prism_settings import check


class PrismSettingsGuardTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        source = self.root / 'crates/qbit-prism-server/src'
        (source / 'config').mkdir(parents=True)
        (source / 'config/native-settings.txt').write_text('PRISM_ACTIVE\n')
        (source / 'config/retired-settings.txt').write_text('PRISM_RETIRED\n')
        (source / 'config.rs').write_text('let setting = optional("PRISM_ACTIVE");')
        (self.root / 'scripts').mkdir()
        (self.root / 'scripts/check-env.sh').write_text('echo ready\n')
        (self.root / 'docs').mkdir()

    def test_live_retired_guidance_is_rejected_with_location(self):
        (self.root / 'docs/operator.md').write_text('Set `PRISM_RETIRED=1` to tune work.\n')
        errors = check(self.root)
        self.assertEqual(len(errors), 1)
        self.assertIn('docs/operator.md:1:', errors[0])
        self.assertIn('PRISM_RETIRED', errors[0])

    def test_historical_marker_is_local_and_cannot_exempt_shell(self):
        (self.root / 'docs/operator.md').write_text(
            'Removed `PRISM_RETIRED`. <!-- retired-setting: PRISM_RETIRED -->\n')
        self.assertEqual(check(self.root), [])
        (self.root / 'scripts/check-env.sh').write_text(
            'echo "$PRISM_RETIRED" # <!-- retired-setting: PRISM_RETIRED -->\n')
        self.assertTrue(any('scripts/check-env.sh:1:' in error for error in check(self.root)))

    def test_inventory_tracks_added_and_removed_runtime_readers(self):
        source = self.root / 'crates/qbit-prism-server/src/config.rs'
        source.write_text('let setting = optional("PRISM_NEW");')
        errors = check(self.root)
        self.assertTrue(any('missing from inventory: PRISM_NEW' in error for error in errors))
        self.assertTrue(any('without a native reader: PRISM_ACTIVE' in error for error in errors))

    def test_patch_deletions_are_historical_but_additions_are_checked(self):
        patch = self.root / 'docs/migration.patch'
        patch.write_text('-PRISM_RETIRED=1\n')
        self.assertEqual(check(self.root), [])
        patch.write_text('+PRISM_RETIRED=1\n')
        self.assertTrue(any('PRISM_RETIRED' in error for error in check(self.root)))

    def test_operator_readmes_env_example_and_doc_tree_are_checked(self):
        guidance = [
            'doc/mining/tuning.md',
            'PRISM.md',
            'README.md',
            'crates/qbit-prism-server/README.md',
            '.env.example',
        ]
        for name in guidance:
            path = self.root / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text('intro\nPRISM_RETIRED=1\n')
        errors = check(self.root)
        self.assertEqual(sorted(errors), sorted(
            f'{name}:2: retired setting presented as live: PRISM_RETIRED' for name in guidance))
        for name in guidance:
            (self.root / name).write_text(
                'intro\nPRISM_RETIRED was removed. <!-- retired-setting: PRISM_RETIRED -->\n')
        # Only Markdown can carry a historical marker; an environment template is live config.
        self.assertEqual(check(self.root), [
            '.env.example:2: retired setting presented as live: PRISM_RETIRED'])

    def test_marker_exempts_only_the_named_setting(self):
        (self.root / 'crates/qbit-prism-server/src/config/retired-settings.txt').write_text(
            'PRISM_RETIRED\nPRISM_OTHER_RETIRED\n')
        (self.root / 'docs/operator.md').write_text(
            'Set PRISM_RETIRED and PRISM_OTHER_RETIRED. <!-- retired-setting: PRISM_RETIRED -->\n')
        self.assertEqual(check(self.root), [
            'docs/operator.md:1: retired setting presented as live: PRISM_OTHER_RETIRED'])

    @unittest.skipUnless(shutil.which('git'), 'git is required to evaluate ignore rules')
    def test_gitignored_generated_guidance_is_not_scanned(self):
        subprocess.run(['git', 'init', '-q', str(self.root)], check=True)
        (self.root / '.gitignore').write_text('doc/generated/\n')
        generated = self.root / 'doc/generated/report.md'
        generated.parent.mkdir(parents=True)
        generated.write_text('PRISM_RETIRED=1\n')
        (self.root / 'doc/operator.md').write_text('PRISM_RETIRED=1\n')
        self.assertEqual(check(self.root), [
            'doc/operator.md:1: retired setting presented as live: PRISM_RETIRED'])


if __name__ == '__main__':
    unittest.main()
