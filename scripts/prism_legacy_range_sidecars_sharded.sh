#!/bin/bash
# Run prism_legacy_range_sidecars.py --write over a rows CSV in N parallel, disjoint shards,
# publishing proven canonical sidecars into the import-audits --root directory.
#
# usage: prism_legacy_range_sidecars_sharded.sh ROWS.csv AUDIT_TREE IMPORT_ROOT CANONICALIZER TOOL OUT_DIR [N]
#   ROWS.csv       block_hash,body_uri,audit_bundle_sha256 (prism_legacy_parts_digest_check.py --affected-csv)
#   AUDIT_TREE     where the audit tree is mounted (the tool's --audit-root)
#   IMPORT_ROOT    the directory import-audits --root reads (the tool's --sidecar-dir)
#   CANONICALIZER  qbit-prism-audit-canonicalize built at the release pin; TOOL the sidecar tool
#   OUT_DIR        a new or empty directory outside IMPORT_ROOT for shard CSVs, work dirs and reports
#   N              shards, default 8
#
# Shards stay apart: each has its own CSV (a round-robin split that keeps the header), its own
# --work-dir and its own --report; the tool's report otherwise defaults to one file in the
# current directory. In IMPORT_ROOT the tool writes <sidecar>.tmp-<pid> and publishes it with a
# hard link that refuses an existing file, so no sidecar is overwritten or seen half-written,
# and a rerun verifies the sidecars already there (sidecar-ok). Exit 0 only when every shard
# exits 0 and no temporary file is left in IMPORT_ROOT.
set -euo pipefail
[ $# -ge 6 ] || { sed -n '5,11p' "$0" >&2; exit 2; }
rows=$(realpath "$1"); tree=$(realpath "$2"); root=$(realpath "$3")
canon=$(realpath "$4"); tool=$(realpath "$5"); out=$(realpath -m "$6"); n=${7:-8}
[[ $n =~ ^[1-9][0-9]?$ ]] || { echo "N must be 1 to 99" >&2; exit 2; }
case "$out/" in "$root"/*) echo "OUT_DIR must not be inside IMPORT_ROOT" >&2; exit 2;; esac
mkdir -p "$out"
[ -z "$(ls -A "$out")" ] || { echo "OUT_DIR $out is not empty" >&2; exit 2; }
cd "$out"
total=$(( $(wc -l < "$rows") - 1 ))
[ "$total" -gt 0 ] || { echo "no rows in $rows: nothing to do"; exit 0; }
tail -n +2 "$rows" | split -e -n r/"$n" -d -a 2 - shard-
for f in shard-??; do { head -1 "$rows"; cat "$f"; } > "$f.csv"; rm "$f"; done
shards=(shard-*.csv)
sharded=$(( $(cat "${shards[@]}" | wc -l) - ${#shards[@]} ))
[ "$sharded" -eq "$total" ] || { echo "shard rows $sharded != input rows $total" >&2; exit 3; }
echo "$(date -u +%H:%M:%SZ) $total rows in ${#shards[@]} shards; writing into $root"
for f in "${shards[@]}"; do
  i=${f#shard-}; i=${i%.csv}
  ( set +e
    python3 "$tool" --rows "$f" --audit-root "$tree" --canonicalizer "$canon" \
      --sidecar-dir "$root" --work-dir "work-$i" --report "report-$i.jsonl" --write \
      > "totals-$i.json" 2> "stderr-$i.log"
    echo $? > "exit-$i" ) &
done
wait
echo "$(date -u +%H:%M:%SZ) done; shard exit codes: $(cat exit-* | tr '\n' ' ')"
echo "statuses over all shards:"
cat report-*.jsonl | python3 -c '
import collections, json, sys
counts = collections.Counter(json.loads(line)["status"] for line in sys.stdin)
print(json.dumps(dict(sorted(counts.items())), indent=1))'
left=$(find "$root" -maxdepth 1 -name 'prism-audit-bundle-canonical-*.tmp-*' | wc -l)
echo "temporary files left in IMPORT_ROOT: $left"
exits=(exit-*)
[ "${#exits[@]}" -eq "${#shards[@]}" ] && ! grep -qv '^0$' "${exits[@]}" && [ "$left" -eq 0 ]
