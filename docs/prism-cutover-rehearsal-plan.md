# PRISM 3.x.x cutover qualification: infrastructure and execution plan for #291

Written 2026-09-24 against `Qbit-Org/qbit-mining-bootstrap` branch `3.x.x` at
`7621f7aa` and `SwapLabsInc/qbit-tools` `main` at `f99caf92`. Where a fact
comes from an open pull request it is cited by that PR's head: qbit-tools
#1107 at `1ed15370`, bootstrap #473 at `6fa88da8`. Anchors in this repository
are `path:line` at `7621f7aa`; anchors in qbit-tools are `path` (line numbers
quoted in prose where they matter) at `main`.

Convention: every bullet or row is tagged **[V]** (verified, with the anchor)
or **[E]** (estimate, with the assumption it rests on). Nothing untagged is a
claim. No secret values, addresses or local paths appear in this file.

## Executive summary

The rehearsal needs **eight hosts, three of them new** (a database primary
host, a dedicated standby host, and a load-generator host), plus HAProxy on
`hashrouter-testnet4` and a PostgreSQL exporter on `jarvis-testnet4`; a fourth
new small VM for the load balancer is recommended but not required. The
existing testnet4 hosts are reused as follows: `meadow-testnet4` is frontend-1
with its own node and is the deploy-path proof for qbit-tools #1107,
`fermion-testnet4` is frontend-2 with its own node, `hashrouter-testnet4` is the
operator Stratum TCP endpoint, `jarvis-testnet4` carries the D3 alerts, and
`qbit-deploy-testnet4` runs every play. The working conclusion that existing
hosts suffice for the migration rehearsal is **not** upheld: the restore drill
role refuses any `qbit_mining_pool` host and the production-sized copy needs
roughly 400 GB of free disk, so the migration rehearsal also lands on the new
database host. The three exercises that need load or failure injection run in
three separate data domains that cannot be merged (a mainnet-signed production
copy, the live testnet4 ledger, and the harness's own synthetic ledger), and the
plan sequences them so that one PostgreSQL pair serves all three. Two numbers
decide whether D1's 500 shares/s row is reachable on this hardware and both are
measured on day 1 before any harness run: the WAL flush cost from
`pg_test_fsync` and the round-trip time between the load host and the primary.
**Earliest realistic start is Monday 2026-10-05**, assuming the VMs, Vault and
R2 read-only access exist by 2026-10-01 and qbit-tools #1107 plus a small
follow-up are merged by 2026-10-02; the go/no-go record then lands on
2026-10-16, which is #260's target date for all P1s closed. Everything a human
must do first is listed in section 7 in a form that can be pasted into #477.

## 1. Host list

### 1.1 What each existing host is today

| Host | Role today | Evidence |
| --- | --- | --- |
| `meadow-testnet4` | The whole testnet4 pool on one Compose host: `qbitd`, `ckpool`, `bitcoind`, `auxpow-stratum`, `prism-postgres`, `prism-coordinator`, `prism-public-api`, `prism-public-proxy`, `prism-metrics`, health sidecar on 9084. Pinned to `refs/heads/2.x.x`. No replica: the read tier is pinned onto the primary. PostgreSQL sized to a 22 GB host with about 20 GB free (`shared_buffers=4GB`, `work_mem=256MB`, shm 1 GB, read concurrency 6). | [V] `ansible/inventory/testnet4/mining_pool.ini`; `ansible/group_vars/testnet4/mining_pool.yml` lines 5, 140, 174-181, 291, 292-343, 367-375, 552-557 |
| `hashrouter-testnet4` | The private `qbit-hashrouter` bundle in Docker (hashrouter, Prometheus, Grafana, node-exporter) plus Holdings; routes owned and rented hashrate to PRISM on meadow port 3340 with the solo ckpool port 3333 as backup; unified Stratum listener on 3334; health sidecar on 9084; UFW managed by the role, admin ports on `tailscale0` only. | [V] `ansible/inventory/testnet4/hashrouter.ini`; `ansible/group_vars/testnet4/hashrouter.yml` lines 35-41, 76-82, 188-201; `ansible/roles/qbit_hashrouter/defaults/main.yml` lines 876-885, 909-935 |
| `fermion-testnet4`, `boson-testnet4` | Archive nodes (`prune=0`, `txindex=1`), PHOTON relay peers of each other, mempool.space origins; RPC bound to loopback plus a Tailscale bind allow-listed to monitoring, hashrouter and the mempool Docker subnet. | [V] `ansible/inventory/testnet4/archive_node.ini`; `ansible/group_vars/testnet4/archive_node.yml` lines 74-108 |
| `jarvis-testnet4` | Monitoring stack (Prometheus, Grafana, chain exporter, synthetics). PRISM alert rules are the 2.x.x set. | [V] `ansible/inventory/testnet4/monitoring.ini`; `ansible/group_vars/testnet4/monitoring.yml` lines 396-420 |
| `qbit-deploy-testnet4` | Deploy controller: `qbit-deploy` with `deploy_policy.yml`; testnet4 permits `--branch` on dry-run only; `mining-pool-prism` maps to scope `prism` on `meadow-testnet4`. | [V] `ansible/inventory/deploy_controller.ini`; `ansible/deploy_policy.yml` lines 20-29, 142-152 |
| `triplet-testnet4`, `coherence-testnet4` | DNS seeds. | [V] `ansible/inventory/testnet4/dns_seed.ini` |
| `union-mainnet` | The only PRISM mainnet host: 125 GiB ECC host, `shared_buffers=8GB`, `effective_cache_size=36GB`, `work_mem=256MB`, shm 2 GB, read concurrency 6, pgBackRest stanza `prism-mainnet-union-v1` to R2 with weekly full and daily differential, restic audit snapshots every 10 minutes, Tailnet read-only SQL role. | [V] `ansible/MAINNET_MINING_POOL_PROVISIONING.md` lines 7-9, 164-167, 196-215, 409-423; `ansible/group_vars/mainnet/mining_pool.yml` lines 411, 429-441, 444, 464-504 |
| `drift-mainnet` | ckpool only, PRISM-incapable; keeps restored PostgreSQL volumes as a cold spare. | [V] `ansible/inventory/mainnet/host_vars/drift-mainnet.yml` |
| `minidev-alex1` | Developer host, 22 cores, PostgreSQL 16.15, no Docker, no `qbitd`. Dry-runs only. | [V] #464 body ("Measured on minidev-alex1 (22 cores, PostgreSQL 16.15 ...)"); project memory |

### 1.2 The rehearsal topology

Three data domains are used and they cannot share a database, which is what
fixes the host count:

- **Domain P, the production copy.** A mainnet ledger signed with the mainnet
  keys. Any frontend that starts against it must carry the mainnet seeds and
  talk to a mainnet-chain node, because `configure` pins a fingerprint over
  the policy document that includes both public keys and the chain (`docs/prism-ha-reference-architecture.md:82-85`, `docs/prism-ledger-ops.md:2228-2231`). `migrate`, `import-audits`, `self-check` and the evidence export need only the trusted public key ([V] `docs/prism-rust-migration.md:1322-1326`). This domain serves exercise 1 only.
- **Domain T, the live testnet4 ledger.** Meadow's real database after its own
  3.x.x migration. It is the only domain where real frontends with real nodes,
  the operator TCP endpoint, real miners, the public API and the D3 alerts can
  be exercised together. This domain serves the overlay failover drill, the
  load-balancer exercise, the public-API criterion and the D4 rotation.
- **Domain H, the harness.** `qbit-prism-load` launches its own frontends and
  a fake node, seeds its own window and reconciles its own rows. With
  `--database-url` it owns the database it is pointed at; it cannot drive
  frontends it did not launch and it cannot share a cluster with Domain T
  frontends (different chain, different keys) ([V] `crates/qbit-prism-load/README.md:3-9`, `:96`). This domain serves the D1 matrix, the two-hour throughput soak, dense cadence, the sync flip, and a failover under 500 shares/s of load.

| # | Host | Exists? | Role in the rehearsal | Sizing and reasoning | Network placement | Managed by |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | `db-primary` (name to be chosen; suggested `lattice-testnet4`) | **New** | Domain P restore target and migration rehearsal (`restore_drill` group); then the PostgreSQL primary for Domains H and T; local writer-endpoint HAProxy; native tools container. | [E] **8 vCPU, 64 GiB RAM, 600 GB NVMe with power-loss protection** for data and a **separate NVMe device for WAL**. Assumptions: the restore drill's own example asks 100 GB database, 30 GB WAL, 100 GB audit, 100 GB staging plus a 25 percent margin (`ansible/PRISM_BACKUP_RESTORE.md` lines 96-99, 406); the production ledger's replaced indexes alone held 44 GB in September (`docs/prism-rust-migration.md:678-681`) and migration 013 builds two replacement indexes before dropping anything (same lines); union's settings need 8 GB `shared_buffers` and up to about 1.8 GB of `work_mem` across seven backends (`ansible/group_vars/mainnet/mining_pool.yml` lines 418-441). 64 GiB lets the rehearsal render union's settings unchanged; 32 GiB is the floor (section 6). Disk class matters more than size: a forced flush of 3.5 ms caps the append lane at about 117 shares/s, 34 µs lets it reach 500 (PR #473 doc lines 841-847, 1142-1146). | Tailscale for management and frontends; provider private VLAN to `db-standby` and `load` if available, same region and zone as every other new VM (section 1.4). Port 5432 reachable only from the writer-endpoint proxies and the exporter. No public ports. | `qbit_base_vm` playbook; `qbit_prism_restore` (as `restore_drill`); the drill PostgreSQL pair is **not** role-managed today (section 2.5) |
| 2 | `db-standby` (suggested `quark-testnet4`) | **New** | Dedicated asynchronous failover standby (`application_name=prism_standby_1`, slot `prism_ha_standby`); a second PostgreSQL instance on its own volume as the public-read replica, cascading from the standby; promotion target. | [E] **4 vCPU, 32 GiB, 2 × 600 GB NVMe with power-loss protection** (one per instance). Same PostgreSQL image digest as the primary. The disk must not be shared with the primary: #477's sync comparison is only meaningful on a separate standby disk (`#477` body, step 3), and `docs/prism-postgres-replica.md:312-315` requires separate storage. 32 GiB assumes `shared_buffers` 8 GB on the standby so it can become the writer without a settings change, plus the public replica at 4 GB. | As host 1. | `qbit_base_vm`; PostgreSQL by hand (section 2.5) |
| 3 | `load` (suggested `flux-testnet4`) | **New** | `qbit-prism-load` and the release binaries it launches (1, 2 or 4 harness frontends plus the client); local writer-endpoint HAProxy. Nothing else. | [E] **16 vCPU, 32 GiB, 200 GB SSD.** Assumptions: the harness floor is 6,144 MiB (`docs/prism-throughput-measurements.md:102-103`); one harness frontend held 1,075 MiB at 500k and up to 1,869 MiB at 500k with four frontends (PR #473 doc lines 1095-1099, 1637-1639); a frontend that lands its own block peaks at 1.9-2.5 GiB while publishing the bundle (#502 body, "Caveats") and the dense-cadence runs sampled 3,824 MiB (PR #473 doc line 1634); four frontends × 4 GiB worst case + floor + client is under 24 GiB. The #271 host (8 vCPU, 22.9 GiB) could not fit 100k at four frontends before #273 (`docs/prism-throughput-measurements.md:360-373`). CPU: frontends used at most 0.65 cores together at 20k (`:21-22`) but each refresh runs a builder pool of up to four threads per frontend since #502 (`.env.example:342-345`), so 16 vCPU keeps four refreshing frontends and the client off each other. | Tailscale plus the private VLAN to `db-primary`. No public ports. | `qbit_base_vm` only; the harness is run by hand from a release build of the pinned commit |
| 4 | `meadow-testnet4` | Exists | Frontend-1 (`prism-coordinator`) with its local `qbitd`; the qbit-tools #1107 deploy-path proof (its own one-way 3.x.x migration); after the drill copy is taken, its PRISM lane points at the writer endpoint (section 2.5). Keeps `ckpool` and the health sidecar. | [V] 22 GB host, about 20 GB free before PRISM sizing (`ansible/group_vars/testnet4/mining_pool.yml` lines 302-304). [E] Adequate for one native frontend: per-frontend RSS is 645-694 MiB at a 400k window in the two-frontend #502 pairs (#502 body, timing table) and the local PostgreSQL leaves the host once the lane is external. | Unchanged: public 3340 and 4334, Tailscale metrics 9828, public API 9085. | `qbit_mining_pool` via `qbit-deploy --service mining-pool-prism --scope meadow-testnet4` |
| 5 | `fermion-testnet4` | Exists | Frontend-2 (`prism-coordinator-2`) as a Compose service on the archive node, using fermion's own `qbitd` as its node (`PRISM_HA_RPC_HOST_2`), writer via the local endpoint proxy. | [E] Fits: the frontend adds under 1 GiB RSS at steady state; the node already runs there. Unknown: fermion's CPU and RAM are not in inventory or group vars; ask (section 7). | Stratum 3340 on `tailscale0` only (the LB reaches it over the tailnet); health 3341 on `tailscale0` only. RPC: add the coordinator's source to `qbit_archive_node_rpc_tailscale_allowips` or run the container on the host network (`ansible/group_vars/testnet4/archive_node.yml` lines 84-85, 105-108). | `qbit_archive_node` for the node; the frontend Compose project is **operator-managed drift** for the rehearsal (the mining-pool role cannot deploy a frontend-only host, section 2.5) |
| 6 | `hashrouter-testnet4` | Exists | The operator Stratum TCP endpoint: HAProxy (tcp mode) in front of frontends 1 and 2 with per-frontend HTTP `/healthz` checks; hashrouter's `QBT_CKPOOL_URL` repointed at it so owned hashrate flows through the LB. Optionally also the exporter host. | [V] The hashrouter role owns Docker and UFW here; adding HAProxy is drift that must be recorded and reverted (`ansible/roles/qbit_hashrouter/defaults/main.yml` lines 909-935). [E] A 2 vCPU, 4 GiB `lb` VM removes that drift and gives the LB its own failure domain; recommended, not required. | HAProxy Stratum 3340 bound to `tailscale0` (miners arrive through hashrouter, not directly); HAProxy admin socket local. | `qbit_hashrouter` (unchanged) plus a hand-installed HAProxy unit |
| 7 | `jarvis-testnet4` | Exists | `postgres_exporter` with the D3 query file scraping the primary every 1 s through its own writer-endpoint proxy; the two PostgreSQL HA alerts and the migrated native alert set for the rehearsal window. | [V] Required by D3 (`docs/prism-alert-migration.md:142-171`; `docs/prism-postgres-exporter-queries.yaml:1-7`). qbit-tools has no PostgreSQL exporter role and #1107 defers the alert migration (G6) (#1107 body; `#1106` G6). | Tailscale. | `qbit_monitoring_stack` for what exists; exporter and rules by hand for the window |
| 8 | `qbit-deploy-testnet4` | Exists | Runs `qbit-deploy`, the #1107 migration play, `prism_restore_drill.yml` and the D5 reconciliation play; holds the operator vars file for the restore drill. | [V] `ansible/deploy_policy.yml`; `ansible/PRISM_BACKUP_RESTORE.md` lines 133-138. | Tailscale. | `qbit_deploy_controller` |

Host name suggestions above are placeholders; the inventory owner picks names.

### 1.3 Which existing hosts cannot be reused, and why

- **`meadow-testnet4` as the database primary or the restore target.** The restore role refuses a target that is in `qbit_mining_pool` or resolves to a pool address ([V] `ansible/roles/qbit_prism_restore/tasks/main.yml`, assertion block; `ansible/PRISM_BACKUP_RESTORE.md` lines 13-16). Fencing a primary that shares a host with frontend-1 fences the frontend too, which is the "shares a Compose host" limitation the HA reference names ([V] `docs/prism-ha-reference-architecture.md:17-18`). The host has about 20 GB free, not 400 GB ([V] `ansible/group_vars/testnet4/mining_pool.yml` lines 303-304).
- **`hashrouter-testnet4` as frontend-2.** No node, and the role owns Docker and UFW.
- **`fermion`/`boson` as the database or the load host.** Their disks carry the chain and `txindex`; the harness memory floor and the D1 numbers must not compete with a node ([V] `crates/qbit-prism-load/README.md:964-978`).
- **`union-mainnet` and `drift-mainnet`.** Production. Only union's R2 backups and a read-only evidence export are used (section 4). Drift's cold-spare volumes are a mainnet pool asset, not a rehearsal input.
- **`minidev-alex1`.** No Docker, no `qbitd`, shared. Dry-runs only (section 5.1).
- **The laptop of #447.** Its commits do not force the drive cache; nothing measured there predicts the rehearsal host ([V] PR #473 doc lines 829-851).

### 1.4 Two measurements that gate the whole plan (day 1)

- **[V] WAL flush cost.** `pg_test_fsync` on the WAL volume of `db-primary` and `db-standby`, before any harness run, recorded beside `wal_sync_method` (#477 body, steps 1-2). On the laptop a forced flush of 3,517 µs took the same configuration from 500 to about 117 shares/s (PR #473 doc lines 1136-1146).
- **[E] Round-trip time from `load` to `db-primary`.** Every accepted share runs about eight statements while holding `ORDER_LOCK` (`crates/qbit-prism-load/README.md:510-512`), so an off-host database pays roughly eight round trips per serialised append. At 500 shares/s the whole append has 2 ms; at 0.25 ms RTT the round trips alone cost 2 ms. Assumption: statement count and serialisation as documented; no pipelining. Measure with `tailscale ping` (direct path, not DERP-relayed) and with the harness's own entry check, which records the observed `SELECT 1` median under `delay_proxy` (`crates/qbit-prism-load/README.md:683-693`). If the VMs cannot be placed in one zone with a private VLAN, the 500 shares/s row will read "not met" for a reason the go/no-go must attribute to placement, not to the server. A loopback control run (harness on `db-primary`, one configuration) separates the two effects (section 5.4).

## 2. Database layout

### 2.1 Members and hosts

| Member | Host | Settings | Evidence |
| --- | --- | --- | --- |
| Primary | `db-primary` | PostgreSQL 16.14 from the production pgBackRest image digest for Domain P (the restore role requires it: `ansible/PRISM_BACKUP_RESTORE.md` lines 24-27); the same image for the Domain H and T cluster. `fsync=on`, `full_page_writes=on`, `wal_level=replica`, `max_wal_senders=10`, `max_replication_slots=10`, `hot_standby=on`, `synchronous_standby_names=''`, `archive_mode=off` for the drill cluster, union's sizing lines rendered as `-c` flags, `wal_receiver_status_interval=1s` on every standby. Writer role `prism_writer` with `ALTER ROLE ... SET synchronous_commit='on'`. | [V] `docs/prism-ha-reference-architecture.md:275-297`; `ansible/roles/qbit_mining_pool/templates/prism-postgres.override.yml.j2` (how the role renders `-c` settings) |
| Dedicated failover standby | `db-standby`, instance A, own NVMe | `pg_basebackup --slot=prism_ha_standby --create-slot --write-recovery-conf` from the primary, then set `primary_conninfo` to carry `application_name=prism_standby_1`; `primary_slot_name='prism_ha_standby'`; `hot_standby=on`; no public reads. | [V] `docs/prism-ha-reference-architecture.md:267-304`; `docs/prism-postgres-replica.md:304-310` (the command shape); the shipped bootstrap does not set the application name (`docs/prism-ha-reference-architecture.md:299-301`) |
| Public-read replica | `db-standby`, instance B, own volume, port 5433 | Cascading standby streaming **from the dedicated standby** with slot `prism_public_replica` created on the standby, `application_name=prism_public_replica`, `recovery_target_timeline='latest'`. Reader role `prism_reader` with `pg_read_all_stats` created on the primary so it replicates. `prism-public-api` uses it with `PRISM_PUBLIC_REPLICA_MODE=require` and the default 60 s heartbeat bound. | [V] Reader grants and `require` semantics: `docs/prism-postgres-replica.md:118-158`, `:251-270`; refusal of a promoted writer: `:377-382`; `compose.yaml:745-757`. [E] Cascading is a plan choice: when the standby is promoted the public replica follows the new timeline without a re-basebackup, so the public API keeps answering through the promotion (criterion 10). Cost: one WAL sender on the standby, which is not a query. Record standby replay lag with the cascade attached; if it measurably delays replay, move the public replica to its own VM (section 6.2). |

The Domain P restore lives in the restore role's own Docker volumes and project
on `db-primary` and is never the drill's primary: the role starts PostgreSQL
with `listen_addresses=''` and no published port ([V] `ansible/roles/qbit_prism_restore/templates/prism-restore-compose.yml.j2`). The drill cluster is a separate Docker Compose project written for the rehearsal.

### 2.2 The writer endpoint

**[E] Choice: a local HAProxy on every host that opens writer connections**
(`load`, `meadow-testnet4`, `fermion-testnet4`, `db-primary` for tools,
`jarvis-testnet4` for the exporter), listening on the Docker bridge address and
loopback, with the backend switched by the operator. The DSN everywhere is the
stable name from the HA reference, `prism-writer.internal`, resolved to the
local proxy (`/etc/hosts` on the host; `extra_hosts: host-gateway` in the
coordinator override). Reasons:

- A DNS name that changes address does not retarget established sockets, which the promotion procedure already states ([V] `docs/prism-ha-reference-architecture.md:377-379`), and the harness's delay proxy resolves the database host **once** at entry and keeps the address for the run ([V] `crates/qbit-prism-load/README.md:96`). A name-based switch would leave the harness on the dead primary.
- A single central HAProxy adds one hop, and every one of the roughly eight statements per append would pay it twice; a local proxy adds tens of microseconds. Assumption: same statement count as above.
- A VIP needs layer-2 adjacency that Tailscale does not provide.

Shape (one file per host, identical):

```text
global
    stats socket /run/haproxy/admin.sock mode 600 level admin
defaults
    mode tcp
    timeout connect 3s
    timeout client 1h
    timeout server 1h
listen prism_writer
    bind <loopback-address>:5432
    bind <docker-bridge-address>:5432
    option tcp-check
    server primary <db-primary-tailnet-name>:5432 check inter 2s fall 3 rise 2
    server standby <db-standby-tailnet-name>:5432 check inter 2s fall 3 rise 2 disabled
```

The standby is `disabled`, never `backup`: HAProxy must not fail writes over
to a read-only standby on its own, and the promotion procedure fences first
and moves the endpoint last ([V] `docs/prism-ha-reference-architecture.md:364-381`). The operator's "move the writer endpoint" step is one script that
runs, on every proxy host in turn:

```text
set server prism_writer/primary state maint
shutdown sessions server prism_writer/primary
set server prism_writer/standby state ready
```

and records the wall-clock time each host was switched. PRISM's pools reconnect
through the same DSN; the HA functional example already proved a pool
reconnecting through an unchanged endpoint after promotion ([V] `docs/prism-ha-functional-qualification.md:122-125`).

### 2.3 Fencing, for real

**[E] Mechanism:** an `nftables` table on `db-primary`, installed at
provisioning and empty until the drill, whose fence rule drops all traffic on
5432 in both directions including established connections (conntrack state
is not consulted), plus `docker update --restart=no` on the primary container
before the window and `docker kill` at the fence for the unplanned case.
Assumptions: the drill's primary is a container whose restart policy would
otherwise resurrect it (`compose.yaml:421` sets `unless-stopped`); network
isolation that covers existing and new connections is what the HA reference
calls positive fencing ([V] `docs/prism-ha-reference-architecture.md:34`, `:366-368`).

- Planned switchover variant: fence writer traffic only, keep the replication port open, wait for the standby's replay LSN to reach the primary's flush LSN, then promote ([V] `:369-373`).
- Unplanned variant: fence and kill in the same second; the unreplicated gap is whatever `pg_stat_replication` last showed and what the exporter's 1 s series captured.
- Fencing authority: the named person in section 7, with root on `db-primary`. The fence stays until rejoin.

### 2.4 Promotion and rejoin, scripted

```text
-- on db-standby, instance A, after the fence is confirmed
SELECT pg_is_in_recovery(), pg_last_wal_receive_lsn(), pg_last_wal_replay_lsn();
SELECT pg_promote(wait => true, wait_seconds => 60);
SELECT pg_is_in_recovery(), pg_current_wal_lsn(), timeline_id FROM pg_control_checkpoint();
```

[V] Command and verification: `docs/prism-ha-reference-architecture.md:374-376`.
Then the endpoint script (2.2), then `self-check` on each frontend to confirm
one writer, matching fingerprint and preserved history ([V] `:379-381`), then
create the slot `prism_ha_standby` on the new primary for the rejoin. Rejoin
of the fenced host is a fresh `pg_basebackup` from the new primary ([V]
`:382-387`); `pg_rewind` is not assumed because the production image's
`wal_log_hints` setting is unknown. Record fence-to-promotion, promotion-to-endpoint-moved, and endpoint-moved-to-first-accepted-share on each frontend, from
the frontend logs and the harness side report.

### 2.5 What qbit-tools does not have, and who adds it

| Gap | Today | Needed for | Where it belongs |
| --- | --- | --- | --- |
| Off-host database for the PRISM lane | `qbit_mining_pool_prism_database_url` names the local `prism-postgres` container; the lane starts that container, lists it in expected services, and never applies `compose.prism-external-db.yaml` ([V] `ansible/roles/qbit_mining_pool/defaults/main.yml` line 1120; `ansible/group_vars/testnet4/mining_pool.yml` lines 291, 552-557; #1107 head `ansible/PRISM_3XX_ROLLOUT.md` lines 314-321) | Frontend-1 on meadow writing through the endpoint | Follow-up to #1107: a `qbit_mining_pool_prism_database_external` switch that adds the external-db overlay, drops `prism-postgres` from the lane and expected services, and renders `PRISM_PUBLIC_DATABASE_URL` plus `PRISM_PUBLIC_REPLICA_MODE=require` ([V] the overlay's shape: `compose.prism-external-db.yaml:1-19`) |
| Public replica in `require` mode | Pinned `off` on purpose, read tier on the primary ([V] `ansible/roles/qbit_mining_pool/defaults/main.yml` lines 1276-1286) | Criterion 10 | Same follow-up |
| A frontend-only host with its own node | `qbit_mining_pool_prism_frontends` places both frontends on one host sharing one `qbitd`; the HA derivation renders instance IDs and ports but no `PRISM_HA_RPC_HOST_2` ([V] #1107 head `ansible/roles/qbit_mining_pool/defaults/main.yml` lines 1959-1988; #1107 body, "Two frontends") | Frontend-2 on fermion (D3 requirements 1 and 3, `docs/prism-ha-reference-architecture.md:29,31`) | Operator-managed Compose project on fermion for the rehearsal; role support is a separate follow-up and is not needed to start |
| Primary/standby/public-replica provisioning | No PostgreSQL role for PRISM outside the pool Compose lane; `qbit_goalert_postgresql` provisions a bare-metal PostgreSQL 17 for GoAlert and is not reusable as-is ([V] `ansible/roles/qbit_goalert_postgresql/README.md`) | Hosts 1 and 2 | Hand-written Compose project plus the SQL above for the window; a `qbit_prism_postgres` role is post-cutover work |
| Writer endpoint | None | Every writer | The HAProxy unit in 2.2, hand-installed on five hosts for the window |
| Standby slot, `application_name`, cascade | The bundled replica bootstrap sets neither the standby name nor the HA slot ([V] `docs/prism-ha-reference-architecture.md:299-301`, `:306-310`) | D3 alerts, sync flip | Section 2.1 settings, by hand |
| D3 alerts and exporter | No PostgreSQL exporter role; alert migration deferred in #1107 (G6) | Criterion 4's "alert observations" and the stale-green class | Exporter container with `docs/prism-postgres-exporter-queries.yaml` and the two rules in `docs/prism-postgres-alert-rules.json` on jarvis, plus the native rule set from `docs/prism-native-alert-rules.json` via the generated patch (`docs/prism-alert-rules-qbit-tools.patch`) for the window |
| Backups on testnet4 | None; the #1107 migration play refuses without backup evidence ([V] #1107 head `ansible/PRISM_3XX_ROLLOUT.md` lines 107-130) | Deploy-path proof on meadow | An operator `pg_dump` plus audit-volume tarball before the play, with its timestamp given to the play's backup gate, or the stanza enabled on testnet4 as a reviewed change |

## 3. Stratum endpoint

### 3.1 Can the hashrouter be the D3 operator TCP endpoint? No.

- [V] Hashrouter routes to **one** upstream (`upstream.ckpool_url`) and tries `ckpool_backup_url` only when the primary cannot complete a Stratum session; it has no second equal PRISM origin, no HTTP readiness probe of a frontend and no hysteresis parameters (`ansible/roles/qbit_hashrouter/README.md` lines 216-220; `ansible/roles/qbit_hashrouter/defaults/main.yml` lines 876-885). Its health sidecar turns Stratum health into HTTP for Cloudflare monitors; it probes listeners, it does not route (`ansible/roles/qbit_stratum_health_sidecar/README.md`).
- [V] Hashbalancer, the shared AWS HAProxy in front of the public ports, checks origins in `tcp` mode on every PRISM route; its `http` mode probes the sidecar on one port and one path per route, and its defaults are `inter 5s fall 3 rise 2` (`services/hashbalancer/haproxy/entrypoint.sh` lines 25-41, 436-445, 537-545; `services/hashbalancer/terraform/terraform.tfvars`, `health_check_mode = "tcp"` on each route). #1107 states the consequence: frontend-2 receives no miner traffic until per-origin HTTP readiness lands ([V] #1107 head `ansible/PRISM_3XX_ROLLOUT.md` lines 513-533).
- The D3 contract needs, per frontend, `GET /healthz` on the frontend's own management port, HTTP 200 **and** JSON `ok: true`, every 2 s with a 1 s timeout, fall 6, rise 2, down for new sessions only ([V] `docs/prism-ha-reference-architecture.md:404-412`, `:433-439`). Neither existing component expresses that.

### 3.2 The HAProxy that does

Run on `hashrouter-testnet4` (or the optional `lb` VM), tcp mode, health in
http mode against the frontends' health ports:

```text
global
    stats socket /run/haproxy/admin.sock mode 600 level admin
    log stdout format raw local0 info
defaults
    mode tcp
    log global
    option tcplog
    timeout connect 3s
    timeout client 1h
    timeout server 1h
    timeout check 1s
frontend prism_stratum
    bind <tailnet-address-of-this-host>:3340
    default_backend prism_frontends
backend prism_frontends
    balance leastconn
    option httpchk GET /healthz
    http-check expect rstring "ok"[[:space:]]*:[[:space:]]*true
    default-server inter 2s fall 6 rise 2 on-error fastinter
    server frontend1 <meadow-tailnet-name>:3340 check port 3341
    server frontend2 <fermion-tailnet-name>:3340 check port 3341
```

Notes on the shape:

- `check port 3341` per server is the per-frontend health port the shipped Hashbalancer cannot do; the audit listener must be bound to a reachable interface, not loopback, on both frontends ([V] `docs/prism-ha-reference-architecture.md:109-118`; `compose.prism-ha.yaml:13-14`). Permit 3341 only from the LB host.
- No `on-marked-down shutdown-sessions`: an ordinary rebuild must not kill established sessions ([V] `:439`). Hashbalancer's active failback does exactly the opposite for backup routes and is not wanted here.
- `timeout check 1s` and `inter 2s` give the documented 13 s ejection bound and the 28 s bound with the stale-publication guard ([V] `:441-455`). Record the actual HAProxy log lines: each check result, each UP/DOWN transition, and each new connection with its chosen server; that log is the "probe and routing timestamps" evidence for criterion 5.
- The public route stays as it is: Hashbalancer keeps sending the public testnet PRISM port to meadow's 3340 directly, so public miners keep working while hashrouter's owned fleet is the population that goes through the rehearsal LB. Repoint `QBT_CKPOOL_URL` on hashrouter to the LB's tailnet address and port for the window ([V] the knob: `ansible/group_vars/testnet4/hashrouter.yml` lines 76-79).
- The readiness simulator's expected trace is the reference to compare the HAProxy log against ([V] `docs/prism-ha-readiness-probe-harness.md:1-16`; `docs/prism-ha-functional-qualification.md:151-161`).

## 4. Data: the production-sized copy

### 4.1 Source and mechanism

- [V] Mainnet backups exist: pgBackRest stanza `prism-mainnet-union-v1` (weekly full, daily differential, continuous WAL with `archive_timeout=300s`, three full sets retained) and restic audit snapshots every 10 minutes, both in one private R2 bucket under separate prefixes; the recovery objectives are a 15-minute RPO and a four-hour RTO (`ansible/PRISM_BACKUP_OPERATIONS.md` lines 3-25; `ansible/group_vars/mainnet/mining_pool.yml` lines 464-504).
- [V] `playbooks/prism_restore_drill.yml` runs role `qbit_prism_restore`, which: validates read-only credentials and forbidden addresses; lists restic snapshots without locking; reads pgBackRest metadata; selects a timestamp-aligned pair from the snapshot's recovery-watermark tag; refuses until the operator pins the full snapshot ID and supplies the exact confirmation string; checks free space against the four operator byte estimates plus 25 percent; creates epoch-labelled volumes and an internal network with outbound R2 only; restores pgBackRest with `--type=time --target-exclusive --target-action=promote --archive-mode=off`; restores restic into staging and finalises into an empty audit volume; starts only PostgreSQL with `listen_addresses=''`; waits up to the four-hour replay budget; validates schema, ledger, payout and manifest rows, body pointers, JSON, hashes and segments read-only; proves the snapshot metadata is unchanged; writes a non-secret JSON report with measured RPO and RTO (`ansible/roles/qbit_prism_restore/tasks/restore.yml`, task names; `ansible/PRISM_BACKUP_RESTORE.md` lines 151-186).
- **Verdict: pgBackRest restore through the existing drill role, not a logical dump.** A dump would need a superuser session on union during production hours and produces no WAL-coverage proof; the drill is the mechanism the D5 condition names and the monthly drill the backup runbook already requires ([V] `ansible/PRISM_BACKUP_OPERATIONS.md` lines 203-214).

### 4.2 What changes for a foreign target

- Nothing in the role: `db-primary` is exactly the "dedicated host in a `restore_drill` group" the role expects; the inventory for the drill must also contain the real `qbit_mining_pool` mainnet group so every production address is added to the forbidden set ([V] `ansible/PRISM_BACKUP_RESTORE.md` lines 32-47; `ansible/roles/qbit_prism_restore/tasks/main.yml`).
- The native steps after the restore are the D5 reconciliation play's skeleton, `prism_3xx_isolated_restore.yml` in #1107, which runs the restore role unmodified and then `migrate`, `import-audits`, `check-config`/`self-check` and the evidence export against the isolated copy ([V] #1107 head `ansible/playbooks/prism_3xx_isolated_restore.yml`, header comment; #1107 body). It has not been run against a real backup and says so; two reviewer findings on its reconcile tasks are open (#1107 comments, tenki-reviewer 2026-09-18). The rehearsal is its first real run.
- The restored PostgreSQL listens nowhere. To run the native tools, the D5 old-image start and the two public read tiers against it, the rehearsal attaches one-shot containers to the restore project's internal network (the role's compose already gives the `pgbackrest` and `restic` services that network; a tools container is added by the operator, outside the role).
- Partitioning: the restored database will be a 2.x.x schema; after `migrate` it is partitioned (016/017). The reconcile skeleton refuses a native database whose detached partitions are not in the operator's archive root ([V] #1107 head `ansible/playbooks/prism_3xx_isolated_restore.yml`, header comment). On a first migration no partition is detached, so this gate is trivially satisfied; record that.

### 4.3 The full exercise 1 procedure and the timings it produces

Steps and the doc that defines each; the report records every duration.

1. Discovery run of `prism_restore_drill.yml`, then the confirmed run ([V] `ansible/PRISM_BACKUP_RESTORE.md` lines 125-172). **Timings: pgBackRest restore, WAL replay to target, restic restore and finalise, validator, total RTO; RPO from the selected snapshot.**
2. Export the drained source baseline from the restored pre-migration copy with `scripts/prism-recovery-evidence.sql` and `scripts/prism-recovery-evidence.py`; export the same from union through the Tailnet read-only role at a matching boundary if the accounting owner agrees (the source export is what a production cutover compares against; on the rehearsal the restored copy stands in for it) ([V] `docs/prism-rust-migration.md:1281-1303`; `ansible/PRISM_POSTGRES_READONLY_ACCESS.md`).
3. **D5 old-image start.** Start the pinned 2.x.x image against the isolated copy with mining and broadcasting disabled, then stop it ([V] `docs/prism-rust-migration.md:1318-1321`). Needs the mainnet seeds and a mainnet node RPC (section 7, decision 3). If the decision is no, record the step as unexecuted with the reason.
4. `time qbit-prism-server migrate` and `time qbit-prism-server import-audits --root ...`, then the evidence export and `cmp` against the source summary ([V] `docs/prism-rust-migration.md:1327-1341`). **Timings: migrate (with 013's concurrent index builds and 017's validation scans separately, from the migrator's log), import-audits, evidence export.**
5. `self-check` in production mode with the intended services available; both completeness counts zero ([V] `docs/prism-rust-migration.md:1382-1388`).
6. Isolated public API on the migrated copy: fetch a sample of historical artifacts, require exact SHA-256 and ETag and no `x-prism-artifact-canonical-state: missing` ([V] `docs/prism-rust-migration.md:1342-1358`). This is the pre-cutover half of criterion 11.
7. The #282 comparison: run the pinned 2.x.x `prism-public-api` against the **pre-migration** restored copy and the 3.x.x `public-api` against the **migrated** copy, fetch the same `/public/v1` routes from both, and list every difference against the differences #282 recorded as intended ([V] #291 body, scope update). [E] The 2.x.x read tier is not documented to start on a native schema, which is why the comparison uses the pre-migration copy for the old tier.
8. D4 rotation on the migrated copy, if decision 3 is yes: `migrate` then `signing-transition --confirm` with the old-key environment, then start one new-key frontend and confirm the pin and the old-key refusal ([V] `docs/prism-ledger-ops.md:2245-2314`). **Timings: the command (63-67 ms on an empty fixture, #464 body), drain wait, stop, check, reset, first new-key configure.** Otherwise the rotation is rehearsed on Domain T on day 9.

### 4.4 How long it plausibly takes

All [E], with assumptions:

| Step | Estimate | Assumption |
| --- | --- | --- |
| pgBackRest restore | 30-90 min | 100 GB database class (section 1.2 row 1), zstd level 1, `process-max=2` (`ansible/roles/qbit_mining_pool/templates/prism-pgbackrest.conf.j2`), R2 throughput 30-100 MB/s from the provider region |
| WAL replay to target | 5-30 min | daily differential plus at most one day of WAL |
| restic audit restore and validation | 1-4 h | audit volume size unknown (ask); the validator streams every bundle body |
| `migrate` | 2-8 h | 013 builds two indexes concurrently on the whole ledger and is called "multi-hour" (`docs/prism-rust-migration.md:678-685`); 017's validation scans "run for hours on a production-sized ledger" (`:772-777`); both are resumable |
| `import-audits` | N × 1-3 s | one SHA-256 pass over about 235 MB per pre-#267 block (`docs/prism-storage-sizing.md:75-77`, `:130-133` for the per-pass cost class); N = mainnet blocks, unknown (ask) |
| Evidence export and summary, each | 10-40 min | the summariser hashes ordered share rows one at a time (`docs/prism-rust-migration.md:1284-1286`) |

Budget two working days plus one overnight for exercise 1.

### 4.5 Secrets and access the operator needs (names only)

- A bucket-scoped R2 **Object Read** credential for the backup bucket (never the production read-write token); the escrowed pgBackRest cipher passphrase and the restic repository password; confirmation that the credential is read-only ([V] `ansible/PRISM_BACKUP_RESTORE.md` lines 18-31).
- The Vault variables the drill vars file is shaped after: `qbit_prism_restore_r2_*`, `qbit_prism_restore_pgbackrest_cipher_pass`, `qbit_prism_restore_restic_password`, the pinned image digests and expected tool versions ([V] `ansible/PRISM_BACKUP_RESTORE.md` lines 52-100).
- The trusted ledger public key `PRISM_LEDGER_WRITER_PUBLIC_KEY_HEX` (not secret, Vault-held) for `import-audits` ([V] `docs/prism-rust-migration.md:1322-1326`).
- Only if decision 3 is yes: both mainnet signing seeds delivered as root-only files on `db-primary` for the window and destroyed after, and RPC access to a mainnet node with block submission and CTV broadcast disabled on the rehearsal frontends.
- Tailnet read-only SQL on union for the source evidence export, if the accounting owner wants a live-source comparison ([V] `ansible/PRISM_POSTGRES_READONLY_ACCESS.md`).

## 5. Sequence and durations

### 5.1 Before the VMs exist: dry-runs on minidev-alex1 and testnet4 data

- **Harness under an endpoint switch.** On minidev with a managed cluster, promote the harness's own standby and retarget a local proxy mid-`steady_state` to learn what the harness does: its post-load replication premise will observe no standby and exit 8 with the artifact withheld and the side report written ([V] `crates/qbit-prism-load/README.md:400`, `:850-866`), and its side pool's sampler may go blind ([V] `:352-361`). The drill script is adapted to what is observed; the side report, not the capacity artifact, is the drill's deliverable. Two hours.
- **B's functional exercise.** `scripts/prism_ha_qualification.py render` and `run` with the existing test binaries ([V] `docs/prism-ha-functional-qualification.md:55-78`); its evidence is the expected shape for the accounting section of the drill report. One hour after the build.
- **HAProxy config check.** `haproxy -c` on both files in sections 2.2 and 3.2; replay the readiness simulator's positive and negative controls to fix the expected trace ([V] `tests/test_prism_ha_readiness_probe.py`). One hour.
- **Alert rules.** `scripts/test_prism_postgres_alerts.py --promtool ...` against a disposable primary and standby ([V] `docs/prism-alert-migration.md:205-210`). One hour.
- **Rotation timing on a seeded fixture.** `signing-transition` on a 500k-share disposable ledger to see whether size moves the 63-67 ms figure. One hour.
- **Release builds** of `qbit-prism-server` and `qbit-prism-load` at the pinned 3.x.x commit in a clean worktree, with the dep-info files beside them so the harness accepts the binary as qualification evidence ([V] `crates/qbit-prism-load/README.md:24-33`, `:52-69`).
- **#1107 migration play under `--check`** against meadow from the controller with a `--branch` dry-run (allowed on testnet4, `ansible/deploy_policy.yml` line 29), to see every gate report ([V] #1107 head `ansible/PRISM_3XX_ROLLOUT.md` lines 235-262).

### 5.2 Who must be present

| Role | When | Why |
| --- | --- | --- |
| Rehearsal owner (C, Anatolie) | Every day; signs the go/no-go | #291 assignee |
| Fencing and promotion authority (root on both database hosts) | Days 7 and 8 | HA reference: an authority outside the pair (`docs/prism-ha-reference-architecture.md:360-363`) |
| Load-generator operator | Days 1, 4, 5, 7, 9 | Runs the harness, keeps the side reports |
| Accounting owner | Days 4-6, 8, 10 | Approves the evidence comparison and any loss reconciliation (`docs/prism-rust-migration.md:1378-1381`) |
| Key owner | Day 4 or 9 | D4 rotation; decision 3 |
| B contributor (djh58) | Days 7-8 | Offered to execute the B checks in an authorized environment (#477 comment 2026-09-22) |
| Monitoring owner | Days 1 and 8 | Exporter, D3 rules, alert observations |
| Deploy-controller operator (`deploy-operators` group) | Days 4, 6 | `qbit-deploy` and the plays (`ansible/group_vars/testnet4/deploy_controller.yml`) |

### 5.3 Day-by-day

Dates assume the section 7 asks are answered by 2026-10-01.

| Day | Date | What runs | Point of no return |
| --- | --- | --- | --- |
| 0 | 2026-09-25 to 10-02 | Section 5.1 dry-runs; VMs provisioned; `playbooks/base_vm.yml` on the new hosts; Tailscale; disks; `haproxy` units; exporter on jarvis; PostgreSQL image pulled by digest | none |
| 1 | Mon 2026-10-05 | `pg_test_fsync` on both WAL volumes; RTT measurements (section 1.4); bring up the drill cluster: primary, standby with `prism_standby_1`, cascading public replica; verify `pg_stat_replication` and slots; premise probes (`--frontends 2 --sessions 200 --plan short`) with `--database-url` at the endpoint ([V] argument lists: PR #473 doc lines 1893-1905) | none |
| 2 | Tue 10-06 | D1 matrix part 1 (#477 steps 2-3): 200k, 400k, 500k at one frontend; 400k at 1, 2, 4 frontends async, three repeats interleaved | none |
| 3 | Wed 10-07 | Matrix part 2: 500k at 1 (and 2, 4 if they fit), sync at 2 frontends × 3 with the flip applied on the primary and reverted after; the found-block run; dense cadence × 3; the loopback control (5.4) | none |
| 4 | Thu 10-08 | Exercise 1 steps 1-3: restore drill discovery and confirmed run; evidence baseline; D5 old-image start if approved | `migrate` on the copy is one-way for that copy; the copy is re-restorable |
| 5 | Fri 10-09 | Exercise 1 steps 4-8; long steps run overnight | same |
| 6 | Mon 10-12 | Deploy-path proof: #1107 on meadow (`qbit-deploy --service mining-pool-prism --scope meadow-testnet4 --dry-run`, then `--apply`, then the migration play under `--check`, then confirmed) with the legacy drain done first and the durations table filled ([V] #1107 head `ansible/PRISM_3XX_ROLLOUT.md` lines 132-170, 196-334, 383-410); copy the migrated testnet4 ledger to the drill primary; repoint meadow's lane at the endpoint; bring up frontend-2 on fermion; LB checks green on both | **First native ACK on meadow**: before it, restore-and-repin; after it, reconciliation only (`:336-381`). The testnet4 pool stays on the external database afterwards (decision 6) |
| 7 | Tue 10-13 | **Failover drill A (Domain H, 500 shares/s):** harness `--frontends 2 --window-shares 400000 --plan d1 --steady-state-seconds 900 --database-url ...`; at +300 s unplanned fence and kill of the primary; promote; move the endpoint; harness frontends reconnect; run to the end; reconcile; standby-down ACK behaviour through and beyond the 15 s commit timeout is observed in the same run ([V] required by `docs/prism-ha-reference-architecture.md:259-265`). Then rejoin the old primary as the new standby by `pg_basebackup`; re-establish the cascade; verify alerts fired and cleared | The fence: the old primary's timeline ends |
| 8 | Wed 10-14 | **Failover drill B and the LB exercise (Domain T):** owned hashrate through hashrouter to the LB to both frontends; planned switchover then an unplanned failover; endpoint move; frontend recovery times from logs; `prism-public-api` answers throughout with `qbit_prism_public_replica_*` recorded; D3 alerts observed. LB: new tips and revision bumps under load (readiness clears and returns without ejection), hold frontend-2's work unavailable (stop its node RPC) and confirm ejection within 13 s and re-entry after two successes, all-backends-down, recovery. The sync flip under load and back ([V] `docs/prism-ha-reference-architecture.md:200-219`). Rebuild the standby again | The fence, again |
| 9 | Thu 10-15 | **Soak:** harness two hours at 400k, 2,000 sessions, `--steady-state-seconds 7200` at the production-shaped rate with `--scheduled-blocks`, `--mid-flight-kill`, then `--cadence dense` (section 5.5); in parallel the Domain T overlay observed for two hours under real miners on both listeners (3340 and 4334) with restart counts, runtime lag and RSS from the native series. D4 rotation on Domain T after the soak: drain, stop both frontends, `signing-transition --confirm`, start new-key frontends, confirm #265's refusal by leaving one pending row in a copy | `signing-transition --confirm` commit: old-key starts re-pin and are recoverable (`docs/prism-ledger-ops.md:2306-2314`) |
| 10 | Fri 10-16 | Reconciliation of every export; rehearsal report; promotion plan; go/no-go record with the pinned toolchain and image digests; #477's per-configuration verdict filed; D1 disposition on #260 | none |

Slack: exercise 1's long steps can overrun into the weekend without moving
days 6-10, because they use the restore project's own volumes on `db-primary`
while the drill cluster idles. If `migrate` on the copy is still running on
day 8, pause it (it is resumable, `docs/prism-rust-migration.md:750-753`) for
the drill's duration so it does not share disk with the fenced primary's
replacement.

### 5.4 The loopback control

One run of `d1-400k-fe1-async` with the harness on `db-primary` and
`--database-url` at loopback, labelled a control, beside the same run from
`load` over the network. The difference attributes the 500 shares/s outcome
between placement and flush cost. It does not enter the D1 tables; the
document's rule is one host per table ([V] `crates/qbit-prism-load/README.md:972-973`).

### 5.5 Soak criteria mapped to native families

The soak's criteria come from the closed production incident and must be
re-expressed against #278's families ([V] #291 body, "There is also no soak").
Proposed mapping, each row to be confirmed by the A/B owners before day 9:

| Legacy criterion | Native evidence | Family or source |
| --- | --- | --- |
| Restart count | Zero unexpected frontend exits over two hours | qbit-tools coordinator restart collector on meadow (`ansible/roles/qbit_mining_pool/README.md`, "PRISM coordinator restart metrics"); harness `frontend_restarts` |
| Late wakes, pause distribution | `qbit_prism_runtime_lag_seconds`, `qbit_prism_runtime_poll_lag_seconds{task}`, `qbit_prism_runtime_task_stalled{task}` | `docs/prism-native-metrics.md:45-160` |
| Lag distribution | `qbit_prism_share_ack_seconds` p99 and the harness client ACK p99 | same; `crates/qbit-prism-load/README.md:668-679` |
| Memory bounds | `qbit_prism_process_resident_memory_bytes` under the provisional 4 GiB `PrismResidentMemoryHigh` bound, plus harness RSS and `VmHWM` | `docs/prism-alert-migration.md:382` |
| Both Stratum lanes | Sidecar checks on 3340 and 4334 stay green on meadow for two hours | `ansible/group_vars/testnet4/mining_pool.yml` lines 642-660 |
| Sustained accepted shares | `qbit_prism_accepted_shares_total` rate; harness `achieved` per phase | native inventory; harness |
| Live solves | `qbit_prism_blocks_total`, harness `scheduled_blocks` landed, `candidates list` empty after | `docs/prism-rust-migration.md:130-131` |
| Pending-candidate restart | `--mid-flight-kill` census with zero possible losses | `crates/qbit-prism-load/README.md:913-929` |
| Reject window per landing | Dense-cadence combined rebuild-pending window; the harness's per-landing budget reads `null` on this build (#480) so the phase is read from the other fields | PR #473 doc lines 782-789 |

### 5.6 The eleven acceptance criteria, mapped

| # | Criterion (#291) | Step that produces the evidence | Record |
| --- | --- | --- | --- |
| 1 | Signed-off rehearsal report with each phase's duration and reconciliation on a restored production-sized copy | Days 4-5, steps 1-6 of section 4.3 | Restore report JSON, evidence `cmp` results, timing table |
| 2 | Key rotation rehearsed, each step timed, #265's refusal confirmed | Day 5 step 8 or day 9 | `signing-transition` JSON, journal row, refusal transcript |
| 3 | Isolated restore rehearsed, `import-audits` duration on real history, point of no return stated | Days 4-5 steps 1-5 | Same as row 1 plus the statement of the boundary quoted from `docs/prism-rust-migration.md:1260-1267` |
| 4 | Failover drill under mining load under async D3, loss counted, WAL gap, times, alerts | Days 7 and 8 | Harness side report (reconciliation with `missing` counted, `premise` block), exporter series, promotion timestamps, alert history |
| 5 | Operator LB preserves routing through rebuilds and ejects prolonged unavailability, with timestamps | Day 8 | HAProxy log, frontend `/healthz` snapshots, simulator trace comparison |
| 6 | Two-hour soak within budget; throughput at 1, 2, 4 frontends meets D1 | Days 2-3 and 9 | #477 verdict tables, soak side report, `PrismResidentMemoryHigh` observation |
| 7 | Nine incident classes each demonstrated by a named test or step | Day 10, assembled from A/B evidence | Table in the report; proposal in 5.7 |
| 8 | Every P0/P1 closed with evidence; #288's capacity artifact validates | Day 10 | `capacity-evidence` validator output; the 2× gate is expected to fail at the D1 forecast and the suggested forecast is recorded (`crates/qbit-prism-load/README.md:509-541`) |
| 9 | Dated go/no-go record with release, toolchain, image digests, promotion and repair plans | Day 10 | The record; #1107's `PRISM_3XX_ROLLOUT.md` and `PRISM_3XX_REPAIR.md` as the deploy-side plans |
| 10 | `prism-public-api` keeps answering during the drill and a coordinator overload, freshness from `qbit_prism_public_replica_*` | Day 8 (drill) and day 9 (overload: the harness burst against the Domain T cluster is not possible; use the artifact-route load of `crates/qbit-prism-server/tests/artifact_admission.rs` against the public API while both coordinators rebuild) | Public API metrics and `X-Prism-Replica-Lag-Seconds` samples |
| 11 | Historical artifacts through the public edge hash to their path digest | Day 5 (origin, isolated) and after cutover (edge) | `curl`/`shasum`/ETag transcript per `docs/prism-rust-migration.md:1347-1353` |

### 5.7 The nine incident classes: a starting map

[E] Proposal for the owners to confirm; the rehearsal report cites the final
names.

| Class | Owner | Proposed evidence |
| --- | --- | --- |
| Lease loss from whole-window work | A | Native runtime has no writer lease; whole-window prepared work is compact since #273 and resumable (`compact_runtime_e2e.rs`, `docs/prism-ha-reference-architecture.md:556`) |
| A solve stalling every share acknowledgement | A/B | Found-block run at 400k: 399-415 of 500 shares/s acknowledged while blocks landed (PR #473 doc lines 782-785); repeated on the rehearsal host day 3 |
| Restart storms with pending large candidates | A | `--mid-flight-kill` census; `candidates list` empty after the soak; `PRISM_STRATUM_MAX_PENDING_INITIAL_JOBS` admission |
| Retained obsolete windows | B | `docs/window-reader-lifetime.md`; RSS converges across tips (#502 body, pool-size table) |
| Full rescans per fingerprint change | B | Refresh delta path retained across retargets (#501); `qbit_prism_refresh_window_acquisitions_total{outcome}` during the soak |
| Stale-green monitoring | C | `x-prism-metrics-state` freshness and the snapshot-age alerts (`docs/prism-capacity-readiness.md:409-447`; `docs/prism-native-alert-rules.json`) observed on jarvis day 8 |
| Per-block storage growth with window size | C | Body size table (`docs/prism-storage-sizing.md:67-72`); `sum(pg_column_size(audit_bundle))` before and after the soak's landed blocks |
| The 2026-07-16 reject outage | A | Dense-cadence reject window on the rehearsal host; `PrismRejectRatioByReasonHigh` stays silent during the soak |
| The soak | C | Day 9 |

## 6. Cost and alternatives

### 6.1 Cheaper variant

- Frontend-2 on fermion (already the base plan), HAProxy on `hashrouter-testnet4` (base plan), no `lb` VM.
- **Load host at laptop class (4 vCPU, 16 GiB).** [E] Forfeits: every four-frontend cell at 400k and 500k (four frontends reached about 8.2 GiB together before #273 and about 7.5 GiB at 500k after it; with the 6,144 MiB floor a 16 GiB host has no headroom for the landing peak), and the CPU columns become unreliable because the client and four refresh pools share four cores. Keeps: 200k/400k/500k at one frontend, the two-frontend rows, dense cadence at one frontend.
- **Database hosts at 16 GiB.** [E] Forfeits union's settings (`shared_buffers=8GB` leaves nothing for `work_mem` spills), so the rehearsal measures a different PostgreSQL than production; the numbers would need a second sizing footnote in every table.
- **Standby on the primary's host.** Forfeits the D3 requirement outright and the sync comparison (#477 step 3). Not acceptable.

### 6.2 A variant that mirrors mainnet sizing

Seven new VMs: `db-primary` and `db-standby` at 16 vCPU, 128 GiB, NVMe with
power-loss protection; a separate public-replica VM; two frontend VMs at 8 vCPU,
32 GiB each with their own `qbitd`; an `lb` VM; the `load` VM at 16 vCPU,
64 GiB. [E] What it buys: the drill's failure domains match production, the
public replica cannot compete with standby replay, and frontend hosts have no
archive-node neighbour. What it does not buy: any change to the two numbers in
section 1.4, which are properties of disk and placement, not host count.

### 6.3 The minimum to defend to the C owner

Three new VMs (section 1.2 rows 1-3, with the database hosts at the 32 GiB
floor if 64 GiB is refused), HAProxy on `hashrouter-testnet4`, frontend-2 on
`fermion-testnet4`, exporter and rules on `jarvis-testnet4`. With this set every
one of the eleven criteria has a producing step. The one thing this set can
still fail to deliver is a "met" verdict on the 500 shares/s row, and section
1.4 says why that is a measurement rather than a plan defect: if the provider
cannot give NVMe with power-loss protection in one zone with a private VLAN,
the record will say so with `pg_test_fsync` and the RTT beside it, which is
exactly what #477 and the D1 disposition on #260 ask for.

## 7. Blockers and asks

Paste-ready answer to #477, in the order the blockers bite:

1. **Name the rehearsal-host owner and executor.** Proposed: the C owner (Anatolie) owns the environment and the go/no-go; the load-generator operator and the fencing/promotion authority are two named people with root on `load` and on both database hosts respectively. djh58's offer to execute the B checks stands for days 7-8.
2. **Provision three VMs** in one provider region and zone with a private VLAN between them: `db-primary` (8 vCPU, 64 GiB, 600 GB NVMe with power-loss protection plus a separate NVMe for WAL), `db-standby` (4 vCPU, 32 GiB, two NVMe volumes of 600 GB), `load` (16 vCPU, 32 GiB, 200 GB). Optional fourth: `lb` (2 vCPU, 4 GiB). Run `playbooks/base_vm.yml`, enrol Tailscale, add them to a `restore_drill` inventory and a rehearsal inventory on the controller. Report fermion's CPU and RAM.
3. **Decide the mainnet-secret question for exercise 1.** Either provision both mainnet signing seeds as root-only files on `db-primary` for the window and grant RPC to a mainnet node with block submission and CTV broadcast disabled, so the D5 old-image start and the D4 rotation run on the production copy; or record those two steps as rehearsed on testnet4 only. The migration, import, evidence comparison and public-API sampling need only the public key either way.
4. **Grant backup access to the operator:** a bucket-scoped R2 Object Read credential, the escrowed pgBackRest cipher passphrase and restic password, the production image digests and tool versions, and confirmation of the current `pg_database_size`, audit volume size and block count on union (read-only), so the drill's four byte estimates are set from facts.
5. **Merge qbit-tools #1107** after review or repin of its bootstrap pin, and land the small follow-up: an external-database switch for the PRISM lane with the public replica in `require` mode. Without it frontend-1 cannot write through the endpoint and criterion 10 cannot be produced. Also land bootstrap **#473** so the go/no-go can cite the #447 record from `3.x.x`, and state where the native image is built and its digest recorded (#1106 G2).
6. **Decide testnet4's topology after the drill:** the pool keeps the external database pair as its ledger (recommended; switching back after the first native ACK would discard accepted shares), or the environment is torn down and testnet4 reverts to the meadow-local database with the copy discarded.
7. **Monitoring:** approve a PostgreSQL exporter with the D3 query file on `jarvis-testnet4` scraping the primary every second, the two HA rules and the migrated native rule set, for the window.
8. **Drift approval for the window:** HAProxy unit and UFW rule on `hashrouter-testnet4` (or the `lb` VM), the frontend-2 Compose project and RPC allow-list change on `fermion-testnet4`, the writer-endpoint proxies on five hosts; all recorded in the rehearsal report and reverted or adopted by a follow-up.
9. **D1 disposition on #260** before day 10, so the go/no-go record cites a decision rather than requesting one.

Answered together, these put day 1 on 2026-10-05 and the go/no-go on 2026-10-16.
