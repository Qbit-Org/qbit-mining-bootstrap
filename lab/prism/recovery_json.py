"""Cooperative stdlib JSON for the standalone recovery process only.

Python's fallback codecs yield the interpreter between records. They trade
throughput for heartbeat responsiveness; they are not a byte/memory bound.
Never install these process-global settings in an existing coordinator.
"""

from __future__ import annotations

import json
import json.decoder
import json.encoder
import json.scanner
from contextlib import contextmanager
from typing import Iterator


@contextmanager
def cooperative_json() -> Iterator[None]:
    previous = (
        json.scanner.make_scanner,
        json.decoder.scanstring,
        json.encoder.c_make_encoder,
        json._default_decoder,
    )
    try:
        json.scanner.make_scanner = json.scanner.py_make_scanner
        json.decoder.scanstring = json.decoder.py_scanstring
        json.encoder.c_make_encoder = None
        json._default_decoder = json.JSONDecoder()
        yield
    finally:
        (
            json.scanner.make_scanner,
            json.decoder.scanstring,
            json.encoder.c_make_encoder,
            json._default_decoder,
        ) = previous
