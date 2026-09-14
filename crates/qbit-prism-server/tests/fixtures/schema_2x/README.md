# Frozen 2.x.x source schema

Byte-exact copies of the SQL the 2.x.x releases applied to their PostgreSQL
ledger. The native migrator accepts a database produced by these files and
nothing else; the upgrade tests build their 2.x.x sources from these copies,
never from the live in-tree files, and `tests/support/ledger_2x.rs` pins each
digest.

| Fixture | Source commit | Release | SHA-256 |
| --- | --- | --- | --- |
| `001_share_ledger.sql` | `504846cc0b72e8f86ed17f896d4ccbbe196a31dc` (v2.0.2, #258); byte-identical at `95ffe063846d51f83999a66cc654da5f7476fdef` (v2.0.1) and `f6854a0fde12b73407b19c2a3d3a6e974fcf89a7` (v2.0.0) | v2.0.0 to v2.0.2 | `9dfdad0651cb92d8a007fd62f50184d9bfdb9d41fda22f8be657ed6c2da92aca` |
| `002_candidate_bodies.sql` | `504846cc0b72e8f86ed17f896d4ccbbe196a31dc` (v2.0.2, #258) | v2.0.2 | `e36b2056a993543bb360c2a81ef961c96a6277a2c2b3c31a2723acafaab34b19` |

The release commits are the ones that set `VERSION` on `origin/2.x.x`; there
are no 2.x release tags. Never edit these files. If one is damaged, restore it
from the release commit:

```sh
git show 504846cc0b72e8f86ed17f896d4ccbbe196a31dc:crates/qbit-prism/sql/001_share_ledger.sql \
  > crates/qbit-prism-server/tests/fixtures/schema_2x/001_share_ledger.sql
git show 504846cc0b72e8f86ed17f896d4ccbbe196a31dc:crates/qbit-prism/sql/002_candidate_bodies.sql \
  > crates/qbit-prism-server/tests/fixtures/schema_2x/002_candidate_bodies.sql
```

The live `crates/qbit-prism/sql/001_share_ledger.sql` is what the migrator
applies to a 2.x.x database, so it must stay statement-for-statement identical
to the frozen copy; #244 changed only its comments. The live
`002_candidate_bodies.sql` is carried as a 2.x.x artifact and must stay
byte-identical. Both checks live in `tests/support/ledger_2x.rs`.
