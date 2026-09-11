#!/usr/bin/env python3
"""Crash once after durable candidate publication and before its first node offer.

Only the disposable regtest harness launches this entry point. The successor
uses the normal coordinator entry point and discovers the candidate from SQL.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import psycopg

from lab.prism.block_candidates import BlockCandidateService
from lab.prism.prism_coordinator import main


def crash_before_offer(self, candidate):
    block_hash = candidate.submission.block_hash_hex.lower()
    with psycopg.connect(os.environ["PRISM_DATABASE_URL"]) as connection:
        row = connection.execute("""
            SELECT o.candidate_sha256, o.share_id, s.writer_epoch,
                   COALESCE((to_jsonb(o)->>'storage_version')::integer, 1),
                   to_jsonb(o)->>'body_id'
            FROM qbit_block_candidate_outbox o
            JOIN qbit_share_ledger s ON s.share_id = o.share_id
            WHERE o.block_hash = %s AND o.state = 'pending' AND s.accepted
        """, (block_hash,)).fetchone()
        if row is None:
            raise RuntimeError("crash fixture reached node offer before atomic share/outbox durability")
        requested_version = os.environ.get("PRISM_CANDIDATE_STORAGE_VERSION")
        if requested_version is not None and row[3] != int(requested_version):
            raise RuntimeError("crash fixture persisted a different candidate storage version")
        if row[3] == 2:
            body = connection.execute("""
                SELECT b.state, b.candidate_sha256, b.chunk_count, b.byte_count,
                       count(c.ordinal), COALESCE(sum(octet_length(c.chunk)), 0),
                       min(c.ordinal), max(c.ordinal),
                       bool_and(sha256(c.chunk) = decode(c.chunk_sha256, 'hex'))
                FROM qbit_block_candidate_body b
                LEFT JOIN qbit_block_candidate_body_chunk c USING (body_id)
                WHERE b.body_id = %s
                GROUP BY b.body_id
            """, (row[4],)).fetchone()
            if body is None or not (
                body[0] == "sealed" and body[1] == row[0]
                and body[2] == body[4] and body[3] == body[5]
                and body[6] == 0 and body[7] == body[2] - 1 and body[8] is True
            ):
                raise RuntimeError("crash fixture found an incomplete published candidate body")
    marker = Path(sys.argv[1])
    with marker.open("x", encoding="utf-8") as handle:
        json.dump({
            "block_hash": block_hash, "candidate_sha256": row[0],
            "share_id": row[1], "writer_epoch": row[2],
            "storage_version": row[3], "body_id": row[4],
        }, handle)
        handle.flush()
        os.fsync(handle.fileno())
    print(f"regtest intentional exit after durable candidate {block_hash}", flush=True)
    os._exit(75)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("requires the disposable crash-marker path")
    BlockCandidateService._submit_block_candidate_to_node = crash_before_offer
    raise SystemExit(main())
