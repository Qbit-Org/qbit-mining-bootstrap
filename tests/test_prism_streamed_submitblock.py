"""Real HTTP transport for a replayed block whose hex stays on disk."""

import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import threading
import tracemalloc
import unittest

from lab.prism.rpc import JsonRpc


class StreamedSubmitblockTests(unittest.TestCase):
    def test_large_block_uses_content_length_and_bounded_stream(self):
        class Block:
            byte_length = 2 + 256 * 65536

            def iter_byte_chunks(self):
                yield b'"'
                for _ in range(256):
                    yield b"ab" * 32768
                yield b'"'

            def __str__(self):
                raise AssertionError("block was materialized")

        observed = []

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                length = int(self.headers["Content-Length"])
                digest = hashlib.sha256()
                remaining = length
                while remaining:
                    chunk = self.rfile.read(min(65536, remaining))
                    if not chunk:
                        break
                    digest.update(chunk)
                    remaining -= len(chunk)
                observed.append((length, remaining, digest.hexdigest(), self.headers.get("Transfer-Encoding")))
                payload = b'{"result":null,"error":null,"id":"submitblock"}'
                self.send_response(200)
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                try:
                    self.wfile.write(payload)
                except OSError:
                    pass  # A deliberately interrupted upload closed its socket.

            def log_message(self, *args):
                pass

        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever)
        thread.start()
        rpc = JsonRpc(host="127.0.0.1", port=server.server_port, user="u", password="p")
        try:
            tracemalloc.start()
            try:
                self.assertIsNone(rpc.call("submitblock", [Block()]))
                _, peak = tracemalloc.get_traced_memory()
            finally:
                tracemalloc.stop()
            expected = json.dumps({"jsonrpc": "1.0", "id": "submitblock", "method": "submitblock",
                                   "params": ["ab" * (256 * 32768)]}, separators=(",", ":")).encode()
            self.assertEqual(observed, [(len(expected), 0, hashlib.sha256(expected).hexdigest(), None)])
            self.assertLess(peak, 2 * 1024 * 1024)

            class FailedBlock(Block):
                def iter_byte_chunks(self):
                    yield b'"ab'
                    raise ValueError("source closed")

            with self.assertRaisesRegex(ValueError, "source closed"):
                rpc.call("submitblock", [FailedBlock()])
            self.assertIsNone(rpc._connections.conn)
        finally:
            rpc._drop_connection()
            server.shutdown()
            server.server_close()
            thread.join()


if __name__ == "__main__":
    unittest.main()
