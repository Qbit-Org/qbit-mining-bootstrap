"""Exercise helper backpressure and shutdown with real child processes."""

from __future__ import annotations

import subprocess
import sys
import threading
import time
import unittest
from unittest import mock

from lab.prism import candidate_store as store
from lab.prism.candidate_codec import prepare_candidate_intent


class CandidateHelperLifecycleTests(unittest.TestCase):
    def helper(self, **kwargs):
        return store.LegacyCandidateHelper(
            store.LegacyTransport(database_url="unused", psql_command=()), **kwargs
        )

    def test_stalled_input_obeys_deadline_and_reaps_child(self):
        body = prepare_candidate_intent({
            "block_hash_hex": "aa" * 32, "shares_json": [], "pad": "x" * (2 * 1024 * 1024),
        }).body
        real_popen = subprocess.Popen
        children = []

        def sleeping_child(_args, **kwargs):
            process = real_popen([sys.executable, "-c", "import time; time.sleep(60)"], **kwargs)
            children.append(process)
            return process

        started = time.monotonic()
        with mock.patch.object(store.subprocess, "Popen", sleeping_child):
            with self.assertRaisesRegex(store.CandidateStorageError, "deadline"):
                self.helper(timeout_seconds=0.2).compare("aa" * 32, body)
        self.assertLess(time.monotonic() - started, 3.0)
        self.assertEqual(len(children), 1)
        self.assertIsNotNone(children[0].poll())
        self.assertTrue(all(pipe.closed for pipe in (children[0].stdin, children[0].stdout, children[0].stderr)))
        self.assertFalse(any(t.name.startswith("prism-legacy-helper-") for t in threading.enumerate()))

    def test_cancellation_wrapper_waits_for_same_admission_slot(self):
        cancelled = threading.Event()
        helper = self.helper(timeout_seconds=1.0)
        wrapper = helper.with_cancellation(cancelled.is_set)
        helper._lock.acquire()
        cancelled.set()
        try:
            with mock.patch.object(store.subprocess, "Popen") as spawn:
                with self.assertRaisesRegex(store.CandidateStorageError, "cancelled"):
                    wrapper.convert("aa" * 32, "unused.body", "unused.idx")
                spawn.assert_not_called()
        finally:
            helper._lock.release()

    def test_child_diagnostic_backpressure_does_not_deadlock(self):
        real_popen = subprocess.Popen

        def noisy_child(_args, **kwargs):
            return real_popen([
                sys.executable, "-c",
                "import sys; sys.stdin.readline(); "
                "sys.stderr.write('x' * 1000000); sys.stderr.flush(); "
                "sys.stdout.write('{\"equal\":true}\\n'); sys.stdout.flush(); sys.stdin.read()",
            ], **kwargs)

        body = prepare_candidate_intent({"block_hash_hex": "aa" * 32, "shares_json": []}).body
        with mock.patch.object(store.subprocess, "Popen", noisy_child):
            self.assertTrue(self.helper(timeout_seconds=3.0).compare("aa" * 32, body))


if __name__ == "__main__":
    unittest.main()
