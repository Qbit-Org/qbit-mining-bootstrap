//! Shared discovery and before/after attribution for the JSONB ceiling gate.
//! Unchanged tuple rewrites retain the original value attribution; runtime
//! write probes separately observe every row write, including those rewrites.
use anyhow::{ensure, Context, Result};
use sqlx::{PgPool, Row};
use std::collections::{BTreeMap, BTreeSet, HashMap};

/// Floor after Ledger::connect has installed the full baseline schema.
/// Discover every newer JSONB column as well.
const EXPECTED_JSONB_COLUMNS: usize = 17;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct WriteKey {
    pub table: String,
    pub column: String,
    pub phase: &'static str,
}

/// One measured row of one JSONB column.
#[derive(Clone, Debug)]
struct RowMeasure {
    xmin: String,
    /// `pg_column_size(col)`: what the row occupies, TOAST-compressed.
    stored: i64,
    /// `octet_length(col::text)`: the JSON text form.
    text_len: i64,
    /// `pg_column_size(col::text::jsonb)`: the uncompressed JSONB container.
    uncompressed: i64,
    digest: String,
    /// The gate wrote this value itself in place of a rejected production write.
    substitute: bool,
}

/// Who owns the JSONB values `Inventory::observe` finds new or changed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Observe {
    /// Setup the gate did itself (schema, fixture, legacy reshaping): cached,
    /// never attributed to a phase.
    Baseline,
    /// The production write of this phase.
    Phase(&'static str),
    /// A row the gate wrote itself after PostgreSQL rejected this phase's
    /// production write. It is kept apart and never enters the ratchet.
    Substitute(&'static str),
}

impl Observe {
    fn phase(self) -> Option<&'static str> {
        match self {
            Self::Baseline => None,
            Self::Phase(phase) | Self::Substitute(phase) => Some(phase),
        }
    }

    fn label(self) -> String {
        match self {
            Self::Baseline => "baseline".into(),
            Self::Phase(phase) => phase.into(),
            Self::Substitute(phase) => format!("{phase} (gate-written substitute)"),
        }
    }
}

/// The largest value a phase wrote into one column.
#[derive(Clone, Debug)]
pub struct PhaseWrite {
    pub rows: usize,
    pub stored: i64,
    pub text_len: i64,
    pub uncompressed: i64,
}

pub struct Inventory {
    schema: String,
    pub columns: Vec<(String, String)>,
    row_key: HashMap<String, String>,
    pub keyless: BTreeSet<String>,
    cache: HashMap<(String, String), HashMap<String, RowMeasure>>,
    /// The attribution trap: tuples rewritten by a phase whose JSONB value did
    /// not change (the claim UPDATE carries the old TOAST pointer forward).
    pub rewritten_unchanged: Vec<String>,
}

impl Inventory {
    pub async fn discover(pool: &PgPool, schema: &str) -> Result<Self> {
        // Never hard-code the column list: a new table must be measured too.
        let columns: Vec<(String, String)> = sqlx::query(
            "SELECT c.table_name,c.column_name FROM information_schema.columns c \
             JOIN information_schema.tables t \
               ON t.table_schema=c.table_schema AND t.table_name=c.table_name \
             WHERE c.table_schema=$1 AND c.data_type='jsonb' AND t.table_type='BASE TABLE' \
             ORDER BY c.table_name,c.column_name",
        )
        .bind(schema)
        .fetch_all(pool)
        .await?
        .into_iter()
        .map(|row| -> Result<(String, String)> {
            Ok((row.try_get("table_name")?, row.try_get("column_name")?))
        })
        .collect::<Result<_>>()?;
        ensure!(
            columns.len() >= EXPECTED_JSONB_COLUMNS,
            "discovered only {} JSONB columns in {schema}; expected at least \
             {EXPECTED_JSONB_COLUMNS}. Was the schema initialized through Ledger::connect?",
            columns.len()
        );
        let mut row_key = HashMap::new();
        let mut keyless = BTreeSet::new();
        for (table, _) in &columns {
            if row_key.contains_key(table) {
                continue;
            }
            let pk: Vec<String> = sqlx::query(
                "SELECT a.attname FROM pg_index i \
                 JOIN pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=ANY(i.indkey) \
                 WHERE i.indrelid=format('%I.%I',$1,$2)::regclass AND i.indisprimary \
                 ORDER BY a.attnum",
            )
            .bind(schema)
            .bind(table)
            .fetch_all(pool)
            .await?
            .into_iter()
            .map(|row| row.try_get::<String, _>("attname"))
            .collect::<Result<_, _>>()?;
            let expression = if pk.is_empty() {
                keyless.insert(table.clone());
                "t.ctid::text".to_owned()
            } else {
                format!(
                    "concat_ws('|',{})",
                    pk.iter()
                        .map(|column| format!("t.\"{column}\"::text"))
                        .collect::<Vec<_>>()
                        .join(",")
                )
            };
            row_key.insert(table.clone(), expression);
        }
        Ok(Self {
            schema: schema.to_owned(),
            columns,
            row_key,
            keyless,
            cache: HashMap::new(),
            rewritten_unchanged: Vec::new(),
        })
    }

    /// Cheap pass: stored size and tuple version for every non-null value.
    /// `pg_column_size` on a TOAST pointer does not detoast, so this is safe to
    /// run after every phase even at 400,000 shares.
    async fn probe(
        &self,
        pool: &PgPool,
        table: &str,
        column: &str,
    ) -> Result<Vec<(String, String, i64)>> {
        let key = &self.row_key[table];
        let sql = format!(
            "SELECT {key} AS row_key,t.xmin::text AS xmin,\
             pg_column_size(t.\"{column}\") AS stored \
             FROM \"{}\".\"{table}\" t WHERE t.\"{column}\" IS NOT NULL",
            self.schema
        );
        sqlx::query(&sql)
            .fetch_all(pool)
            .await?
            .into_iter()
            .map(|row| -> Result<(String, String, i64)> {
                Ok((
                    row.try_get("row_key")?,
                    row.try_get("xmin")?,
                    i64::from(row.try_get::<i32, _>("stored")?),
                ))
            })
            .collect()
    }

    /// Expensive pass, only for rows the probe says may have changed.
    ///
    /// `pg_column_size(col)` reports the compressed on-disk size, which is not
    /// what PostgreSQL compares against its 268,435,455-byte ceiling. Casting
    /// through `text` back to `jsonb` builds a fresh in-memory datum, and a
    /// computed datum is never TOAST-compressed, so its `pg_column_size` is the
    /// uncompressed container size the limit actually applies to.
    async fn measure(
        &self,
        pool: &PgPool,
        table: &str,
        column: &str,
        rows: &[String],
    ) -> Result<HashMap<String, (i64, i64, String)>> {
        if rows.is_empty() {
            return Ok(HashMap::new());
        }
        let key = &self.row_key[table];
        let sql = format!(
            "SELECT {key} AS row_key,octet_length(t.\"{column}\"::text)::bigint AS text_len,\
             pg_column_size(t.\"{column}\"::text::jsonb) AS uncompressed,\
             md5(t.\"{column}\"::text) AS digest \
             FROM \"{}\".\"{table}\" t \
             WHERE t.\"{column}\" IS NOT NULL AND ({key})=ANY($1)",
            self.schema
        );
        sqlx::query(&sql)
            .bind(rows)
            .fetch_all(pool)
            .await?
            .into_iter()
            .map(|row| -> Result<(String, (i64, i64, String))> {
                Ok((
                    row.try_get("row_key")?,
                    (
                        row.try_get("text_len")?,
                        i64::from(row.try_get::<i32, _>("uncompressed")?),
                        row.try_get("digest")?,
                    ),
                ))
            })
            .collect()
    }

    /// Refresh the cache and return the values this phase actually wrote.
    ///
    /// A value belongs to a phase when its row is new, or when its value
    /// changed. Attributing by `xmin` alone would blame the claim UPDATE for
    /// the candidate written at enqueue, because that UPDATE touches only the
    /// lease columns and the new tuple version carries the same TOAST pointer.
    pub async fn observe(
        &mut self,
        pool: &PgPool,
        mode: Observe,
    ) -> Result<BTreeMap<WriteKey, PhaseWrite>> {
        let mut written = BTreeMap::new();
        for (table, column) in self.columns.clone() {
            let probed = self.probe(pool, &table, &column).await?;
            let cached = self
                .cache
                .entry((table.clone(), column.clone()))
                .or_default();
            let mut stale = Vec::new();
            for (row_key, xmin, stored) in &probed {
                match cached.get(row_key) {
                    Some(previous) if previous.stored == *stored && previous.xmin == *xmin => {}
                    _ => stale.push(row_key.clone()),
                }
            }
            let fresh = self.measure(pool, &table, &column, &stale).await?;
            let cached = self
                .cache
                .get_mut(&(table.clone(), column.clone()))
                .expect("cached");
            let mut attributed: Vec<RowMeasure> = Vec::new();
            let mut next: HashMap<String, RowMeasure> = HashMap::new();
            for (row_key, xmin, stored) in probed {
                let previous = cached.get(&row_key).cloned();
                let mut measure = match fresh.get(&row_key) {
                    Some((text_len, uncompressed, digest)) => RowMeasure {
                        xmin: xmin.clone(),
                        stored,
                        text_len: *text_len,
                        uncompressed: *uncompressed,
                        digest: digest.clone(),
                        substitute: false,
                    },
                    None => {
                        let previous = previous
                            .clone()
                            .context("probe reported an unchanged row that was never measured")?;
                        RowMeasure {
                            xmin: xmin.clone(),
                            stored,
                            ..previous
                        }
                    }
                };
                let changed = previous
                    .as_ref()
                    .is_none_or(|previous| previous.digest != measure.digest);
                // A value keeps its owner until a later write replaces it.
                measure.substitute = if changed {
                    matches!(mode, Observe::Substitute(_))
                } else {
                    previous
                        .as_ref()
                        .is_some_and(|previous| previous.substitute)
                };
                match &previous {
                    _ if changed => attributed.push(measure.clone()),
                    Some(previous) if previous.xmin != measure.xmin => {
                        self.rewritten_unchanged.push(format!(
                            "{table}.{column} row {row_key}{} rewritten by phase {} \
                             (xmin {} -> {}) with an unchanged {} B value",
                            if measure.substitute {
                                " (gate-written substitute, not a production write)"
                            } else {
                                ""
                            },
                            mode.label(),
                            previous.xmin,
                            measure.xmin,
                            measure.uncompressed
                        ));
                    }
                    _ => {}
                }
                next.insert(row_key, measure);
            }
            *cached = next;
            let (Some(phase), false) = (mode.phase(), attributed.is_empty()) else {
                continue;
            };
            written.insert(
                WriteKey {
                    table: table.clone(),
                    column: column.clone(),
                    phase,
                },
                PhaseWrite {
                    rows: attributed.len(),
                    stored: attributed.iter().map(|m| m.stored).max().unwrap_or(0),
                    text_len: attributed.iter().map(|m| m.text_len).max().unwrap_or(0),
                    uncompressed: attributed.iter().map(|m| m.uncompressed).max().unwrap_or(0),
                },
            );
        }
        Ok(written)
    }
}
