-- Four-byte extranonce1 remains unchanged. STOP/DRAIN ALL pre-009 frontends
-- before applying this migration: their unchecked nextval calls cannot coexist
-- with CYCLE. Two >=009 frontends may migrate/start concurrently under the
-- migration runner's transaction lock. Existing job rows are not rewritten.
--
-- Reservations have no time-based expiry: an idle miner can retain its current
-- in-memory job after its persisted job expires. The session guard deletes only
-- its own token on disconnect/cancellation. Allocation can reclaim a reservation
-- from an explicitly stopped instance, but never from heartbeat staleness or an
-- absent owner; crash/failed-cleanup reservations conservatively remain until
-- the owner is proven stopped. Unexpired jobs independently prevent reuse.
-- Owner tokens distinguish process incarnations even if instance_id is reused.
-- Reporting stopped closes allocation and requires zero pending/live guards.
CREATE TABLE qbit_prism_session_reservations (
    extranonce1 bigint PRIMARY KEY CHECK (extranonce1 BETWEEN 1 AND 4294967295),
    instance_id text NOT NULL,
    owner_token text NOT NULL,
    reservation_token text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

-- StoredJob keeps extranonce1 at the top level. Prepared records without that
-- field are harmless; lower() also protects pre-009 rows using uppercase hex.
CREATE INDEX qbit_prism_jobs_extranonce1_expiry_idx
    ON qbit_prism_jobs (lower(payload->>'extranonce1'), expires_at);

ALTER SEQUENCE qbit_prism_session_sequence CYCLE;
