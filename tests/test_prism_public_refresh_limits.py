"""Global background refresh admission across cache keys and instances."""

import threading
import time
import unittest
from unittest.mock import patch

from lab.prism import public_api


class PublicRefreshLimitsTests(unittest.TestCase):
    def test_saturated_refreshes_serve_stale_without_registering_waiters(self):
        slots = threading.BoundedSemaphore(1)
        entered, release = threading.Event(), threading.Event()
        first, second = public_api.PublicResponseCache(), public_api.PublicResponseCache()
        key_a, key_b = ("/first", ()), ("/second", ())
        for cache, key in ((first, key_a), (second, key_b)):
            cache.get_or_compute(key=key, ttl_seconds=5, compute=lambda: (200, {"old": True}))
            cache._entries[key].stored_at = time.monotonic() - 6
            cache._entries[key].expires_at = time.monotonic() - 1

        def blocked():
            entered.set()
            self.assertTrue(release.wait(5))
            return 200, {"new": True}

        def stale(cache, key, compute):
            return cache.get_or_compute(
                key=key, ttl_seconds=5, stale_while_revalidate_seconds=30, compute=compute,
            )

        with patch.object(public_api, "_PUBLIC_REFRESH_SLOTS", slots):
            try:
                self.assertEqual(stale(first, key_a, blocked)[2], "STALE")
                self.assertTrue(entered.wait(2))
                with patch.object(threading.Thread, "start") as start:
                    for _ in range(20):
                        self.assertEqual(stale(second, key_b, lambda: self.fail("unadmitted refresh"))[2], "STALE")
                    start.assert_not_called()
                self.assertNotIn(key_b, second._inflight)
            finally:
                release.set()
                self.assertTrue(slots.acquire(timeout=5))
                slots.release()
            # Once capacity returns, the previously deferred key can refresh.
            done = threading.Event()

            def recovered():
                done.set()
                return 200, {"recovered": True}

            self.assertEqual(stale(second, key_b, recovered)[2], "STALE")
            self.assertTrue(done.wait(2))
            self.assertTrue(slots.acquire(timeout=5))
            slots.release()

    def test_start_failure_and_compute_failure_both_return_capacity(self):
        slots = threading.BoundedSemaphore(1)
        cache = public_api.PublicResponseCache()
        key = ("/failure", ())
        cache.get_or_compute(key=key, ttl_seconds=5, compute=lambda: (200, {}))
        cache._entries[key].expires_at = time.monotonic() - 1

        def fail():
            raise RuntimeError("refresh failed")

        with patch.object(public_api, "_PUBLIC_REFRESH_SLOTS", slots):
            with patch.object(threading.Thread, "start", side_effect=RuntimeError("start failed")):
                cache.get_or_compute(key=key, ttl_seconds=5, stale_while_revalidate_seconds=30, compute=fail)
            self.assertTrue(slots.acquire(timeout=2))
            slots.release()
            self.assertNotIn(key, cache._inflight)
            cache.get_or_compute(key=key, ttl_seconds=5, stale_while_revalidate_seconds=30, compute=fail)
            self.assertTrue(slots.acquire(timeout=2))
            slots.release()
            self.assertNotIn(key, cache._inflight)
