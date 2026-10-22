"""Negative controls for the real production checkout census."""
from collections import Counter
import json
from pathlib import Path
import shutil
import tempfile
import unittest

from scripts import check_prism_pool_acquires as guard


class PoolAcquireGuardTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.inventory = json.loads((guard.ROOT / guard.INVENTORY).read_text())

    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        shutil.copytree(guard.ROOT / guard.SOURCE, self.root / guard.SOURCE)

    def append(self, path, text):
        target = self.root / guard.SOURCE / path
        target.write_text(target.read_text() + text)

    def errors(self):
        sites, _, _ = guard.census(self.root)
        return guard.check(sites, self.inventory)

    def test_existing_classifications_are_current(self):
        self.assertEqual(self.errors(), [])

    def test_new_direct_implicit_pool_executor_is_rejected(self):
        self.append("ledger/window.rs", '''
impl Ledger {
    async fn negative_implicit(&self) -> sqlx::Result<i32> {
        sqlx::query_scalar("SELECT 1").fetch_one(&self.pool).await
    }
}
''')
        self.assertTrue(any("unclassified" in e and "negative_implicit" in e for e in self.errors()))

    def test_new_explicit_pool_acquire_is_rejected(self):
        self.append("ledger/window.rs", '''
impl Ledger {
    async fn negative_explicit(&self) {
        let connection = self.pool.acquire().await;
    }
}
''')
        self.assertTrue(any("unclassified" in e and "negative_explicit" in e for e in self.errors()))

    def test_alias_and_multiline_raw_sql_executors_are_rejected(self):
        self.append("ledger/window.rs", '''
impl Ledger {
    async fn negative_alias(&self) {
        let database = self.pool.clone();
        sqlx::raw_sql(r#"SELECT '/* not a comment */'"#)
            .execute(
                &database
            ).await;
        database.begin().await;
        sqlx::Executor::fetch_one(&database, "SELECT 1").await;
    }
}
''')
        errors = [e for e in self.errors() if "negative_alias" in e]
        self.assertEqual(len(errors), 3, errors)

    def test_generic_helper_and_imported_alias_callers_are_rejected(self):
        self.append("ledger/window.rs", '''
use crate::ledger::audit_completeness as renamed_completeness;
impl Ledger {
    async fn negative_helper(&self) {
        let database = &self.pool;
        crate::ledger::audit_completeness(database).await;
        renamed_completeness(database).await;
    }
}
''')
        errors = [e for e in self.errors() if "negative_helper" in e]
        self.assertEqual(len(errors), 2, errors)

    def test_new_helper_module_is_scanned(self):
        self.append("lib.rs", "\nmod negative_module;\n")
        (self.root / guard.SOURCE / "negative_module.rs").write_text('''
async fn helper(database: &sqlx::PgPool) {
    database.acquire().await;
}
''')
        self.assertTrue(any("negative_module.rs" in e for e in self.errors()))

    def test_borrowed_connection_is_not_counted_as_checkout(self):
        before = guard.census(self.root)
        self.append("ledger/window.rs", '''
async fn negative_borrowed(connection: &mut sqlx::PgConnection) {
    sqlx::query("SELECT 1").fetch_one(&mut *connection).await;
}
''')
        after = guard.census(self.root)
        self.assertEqual(after[0], before[0])
        self.assertEqual(after[2], before[2] + 1)

    def test_reader_helper_without_metrics_cannot_be_added_silently(self):
        self.append("ledger/audit.rs", '''
async fn negative_reader(reader: &mut AuditReader<'_>) {
    sqlx::query("SELECT 1").fetch_one(&mut *reader.connection(None).await?).await;
}
''')
        self.assertTrue(any("unclassified" in e and "negative_reader" in e for e in self.errors()))

    def test_pool_type_alias_enrolls_helper_callers(self):
        self.append("ledger/window.rs", '''
use sqlx::PgPool as ImportedDatabase;
type DatabaseAlias = ImportedDatabase;
async fn aliased_helper(database: &DatabaseAlias) { database.acquire().await; }
async fn negative_type_alias(database: &DatabaseAlias) { aliased_helper(database).await; }
''')
        errors = self.errors()
        self.assertTrue(any("negative_type_alias" in e and "aliased_helper" in e for e in errors))

    def test_generated_rust_requires_explicit_scanner_support(self):
        self.append("lib.rs", '\ninclude!("generated.rs");\n')
        with self.assertRaisesRegex(ValueError, "include! needs explicit source-census support"):
            guard.census(self.root)

    def test_comments_strings_and_test_only_modules_are_opaque(self):
        before = guard.census(self.root)[0]
        self.append("ledger/window.rs", '''
// pool.acquire().await;
/* outer /* pool.begin().await */ nested */
const NEGATIVE_SQL: &str = r##"pool.acquire(); .fetch_one(&pool)"##;
#[cfg(test)]
mod negative_tests { async fn fixture() { pool.acquire().await; } }
#[cfg(test)]
#[path = "missing_test_file.rs"]
mod only_for_tests;
#[cfg(all(test, target_os = "linux"))]
mod platform_tests { async fn fixture() { pool.acquire().await; } }
''')
        self.assertEqual(guard.census(self.root)[0], before)
        self.append("ledger/window.rs", '''
#[cfg(all(unix, any(test, feature = "production")))]
mod negative_conditional {
    async fn production_path() { pool.acquire().await; }
}
''')
        self.assertTrue(any("unclassified" in e and "production_path" in e for e in self.errors()))

    def test_added_occurrence_and_stale_entries_are_rejected(self):
        sites = guard.census(self.root)[0]
        site = next(iter(sites))
        added = sites.copy()
        added[site] += 1
        self.assertTrue(any("changed count" in e and site in e for e in guard.check(added, self.inventory)))
        removed = sites.copy()
        del removed[site]
        self.assertTrue(any("stale classification" in e and site in e for e in guard.check(removed, self.inventory)))

    def test_removing_or_disabling_timer_changes_classification(self):
        target = self.root / guard.SOURCE / "ledger/connect/acquire.rs"
        original = target.read_text()
        for replacement in ["self.pool.acquire()", "time_pool_acquire(None, self.pool.acquire())"]:
            with self.subTest(replacement=replacement):
                target.write_text(original.replace("time_pool_acquire(self.metrics.as_deref(), self.pool.acquire())", replacement))
                errors = self.errors()
                self.assertTrue(any("unclassified" in e and "connect/acquire.rs" in e for e in errors))
                self.assertTrue(any("stale classification" in e and "connect/acquire.rs" in e for e in errors))

    def test_allowlist_requires_owner_reason_and_current_policy(self):
        site = "fixture::query::fetch_one(database)"
        sites = Counter({site: 1})
        inventory = {"sites": {site: [1, "excluded"]}, "policies": {"excluded": {"class": "operator"}}}
        self.assertTrue(any("owner and reason" in e for e in guard.check(sites, inventory)))
        inventory["policies"]["excluded"].update(owner="tools", reason="operator-only diagnostic")
        self.assertEqual(guard.check(sites, inventory), [])
        inventory["policies"]["unused"] = inventory["policies"]["excluded"].copy()
        self.assertIn("stale policy: unused", guard.check(sites, inventory))


if __name__ == "__main__":
    unittest.main()
