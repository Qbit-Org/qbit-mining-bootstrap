//! The rendered PRISM alert rules, evaluated on live `/metrics` scrapes
//! (#575). A scenario mirrors a rule's condition in Rust, pins the parts of
//! the rule's expression it mirrors (so a changed rule fails the scenario
//! instead of silently diverging), and asks whether the condition held on
//! every scrape for at least the rule's `for` duration: whether Prometheus,
//! scraping as often, would have fired it.
use super::*;
use std::collections::{BTreeMap, BTreeSet};

/// The rules the deployment renders.
const ALERT_RULES: &str = include_str!("../../../../docs/prism-native-alert-rules.json");

/// One scrape of a server's `/metrics`: every sample, keyed by its series as
/// rendered (`name` or `name{labels}`).
#[derive(Clone, Debug)]
pub(crate) struct Scrape {
    pub server: usize,
    pub at: Instant,
    series: BTreeMap<String, f64>,
}

impl Scrape {
    pub(crate) fn parse(server: usize, body: &str) -> Self {
        let series = body
            .lines()
            .filter(|line| !line.starts_with('#'))
            .filter_map(|line| {
                let (series, value) = line.rsplit_once(' ')?;
                Some((series.to_owned(), value.parse().ok()?))
            })
            .collect();
        Self {
            server,
            at: Instant::now(),
            series,
        }
    }

    /// Every series of family `name`, with its value.
    fn family<'a>(&'a self, name: &'a str) -> impl Iterator<Item = (&'a str, f64)> + 'a {
        self.series.iter().filter_map(move |(series, value)| {
            let rest = series.strip_prefix(name)?;
            (rest.is_empty() || rest.starts_with('{')).then_some((rest, *value))
        })
    }

    /// The first sample of family `name`: an unlabelled gauge's value.
    pub(crate) fn value(&self, name: &str) -> Option<f64> {
        self.family(name).next().map(|(_, value)| value)
    }

    /// The sample of family `name` whose `label` is `value`.
    pub(crate) fn labelled(&self, name: &str, label: &str, value: &str) -> Option<f64> {
        let needle = format!("{label}=\"{value}\"");
        self.family(name)
            .find(|(labels, _)| labels.contains(&needle))
            .map(|(_, sample)| sample)
    }

    /// Family `name` summed by `label`.
    pub(crate) fn by_label(&self, name: &str, label: &str) -> BTreeMap<String, f64> {
        let key = format!("{label}=\"");
        let mut sums = BTreeMap::new();
        for (labels, sample) in self.family(name) {
            let Some(value) = labels
                .split_once(&key)
                .and_then(|(_, rest)| rest.split_once('"'))
                .map(|(value, _)| value.to_owned())
            else {
                continue;
            };
            *sums.entry(value).or_default() += sample;
        }
        sums
    }

    /// Family `name` summed over every series.
    pub(crate) fn sum(&self, name: &str) -> f64 {
        self.family(name).map(|(_, value)| value).sum()
    }

    /// The snapshot gate every PRISM rule here is `and`-ed with: a complete
    /// metrics snapshot that is not stale.
    pub(crate) fn gate_open(&self) -> bool {
        self.value("qbit_prism_metrics_snapshot_available") == Some(1.0)
            && self.value("qbit_prism_metrics_snapshot_stale") == Some(0.0)
    }
}

pub(crate) async fn scrape(client: &reqwest::Client, server: usize, port: u16) -> Result<Scrape> {
    let body = client
        .get(format!("http://127.0.0.1:{port}/metrics"))
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?;
    Ok(Scrape::parse(server, &body))
}

/// A rendered rule: its expression and `for` duration.
pub(crate) struct Rule {
    pub title: &'static str,
    pub expr: String,
    pub hold: Duration,
}

/// The rule titled `title`, whose expression must still contain every one
/// of `fragments`: the parts a scenario's mirror of it depends on.
pub(crate) fn rule(title: &'static str, fragments: &[&str]) -> Result<Rule> {
    let rules: Value = serde_json::from_str(ALERT_RULES)?;
    let found = rules["rules"]
        .as_array()
        .context("alert rules missing")?
        .iter()
        .find(|rule| rule["title"] == title)
        .with_context(|| format!("{title} rule missing"))?;
    let expr = found["expr"]
        .as_str()
        .context("rule has no expr")?
        .to_owned();
    for fragment in fragments {
        ensure!(
            expr.contains(fragment),
            "{title} no longer contains {fragment:?}; update the scenario's mirror of it: {expr}"
        );
    }
    let text = found["for"].as_str().context("rule has no for")?;
    let (number, unit) = text.split_at(text.len() - 1);
    let number: u64 = number.parse()?;
    let hold = Duration::from_secs(match unit {
        "s" => number,
        "m" => number * 60,
        "h" => number * 3600,
        _ => bail!("{title}: unsupported for {text:?}"),
    });
    Ok(Rule { title, expr, hold })
}

impl Rule {
    /// The alternatives of the expression's `label=~"a|b"` matcher.
    pub(crate) fn label_alternatives(&self, label: &str) -> Result<BTreeSet<String>> {
        let matcher = format!("{label}=~\"");
        let (_, rest) = self
            .expr
            .split_once(&matcher)
            .with_context(|| format!("{} has no {label} matcher", self.title))?;
        let (alternatives, _) = rest.split_once('"').context("unterminated matcher")?;
        Ok(alternatives.split('|').map(str::to_owned).collect())
    }
}

/// The `[5m]` range of the rules' `increase`.
const INCREASE_WINDOW: Duration = Duration::from_secs(300);

/// A mirrored condition: `holds(base, scrape)`, where `base` is the scrape
/// an `increase(...[5m])` at `scrape` starts from.
type Condition = Box<dyn Fn(&Scrape, &Scrape) -> bool + Send + Sync>;

/// A rule and its condition mirrored in Rust.
pub(crate) struct Mirror {
    pub rule: Rule,
    holds: Condition,
}

impl Mirror {
    /// The rule titled `title`, pinned by `fragments` (see [`rule`]).
    pub(crate) fn new(
        title: &'static str,
        fragments: &[&str],
        holds: impl Fn(&Scrape, &Scrape) -> bool + Send + Sync + 'static,
    ) -> Result<Self> {
        Ok(Self {
            rule: rule(title, fragments)?,
            holds: Box::new(holds),
        })
    }

    /// The longest time the condition held on consecutive scrapes of one
    /// server's `series`, oldest first, from its first scrape to its last.
    /// An increase is measured from the last scrape at least five minutes
    /// older, or from the series' first while there is none.
    pub(crate) fn held(&self, series: &[&Scrape]) -> Duration {
        let Some(first) = series.first() else {
            return Duration::ZERO;
        };
        let base = |scrape: &Scrape| {
            series
                .iter()
                .take_while(|older| older.at + INCREASE_WINDOW <= scrape.at)
                .last()
                .unwrap_or(first)
        };
        let mut longest = Duration::ZERO;
        let mut since = None;
        for scrape in series {
            if (self.holds)(base(scrape), scrape) {
                let start = *since.get_or_insert(scrape.at);
                longest = longest.max(scrape.at - start);
            } else {
                since = None;
            }
        }
        longest
    }
}

/// `PrismMetricsSnapshotStale`, which every scenario here watches.
pub(crate) fn snapshot_stale() -> Result<Mirror> {
    Mirror::new(
        "PrismMetricsSnapshotStale",
        &["qbit_prism_metrics_snapshot_stale", "> bool 0"],
        |_, s| {
            s.value("qbit_prism_metrics_snapshot_stale")
                .is_some_and(|stale| stale > 0.0)
        },
    )
}

/// Which `mirrors` fire on each of `servers`, and a report of how long each
/// held: a rule fires when its condition held for at least its `for`.
pub(crate) struct Verdict {
    pub fired: BTreeMap<usize, BTreeSet<&'static str>>,
    pub report: String,
}

impl Verdict {
    pub(crate) fn of(scrapes: &[Scrape], servers: &[usize], mirrors: &[Mirror]) -> Self {
        let mut fired: BTreeMap<usize, BTreeSet<&'static str>> = servers
            .iter()
            .map(|server| (*server, BTreeSet::new()))
            .collect();
        let mut lines = Vec::new();
        for mirror in mirrors {
            let mut spans = Vec::new();
            for server in servers {
                let series: Vec<_> = scrapes.iter().filter(|s| s.server == *server).collect();
                let held = mirror.held(&series);
                let fires = held >= mirror.rule.hold;
                if fires {
                    fired.entry(*server).or_default().insert(mirror.rule.title);
                }
                spans.push(format!(
                    "server-{server} {:.0}s{}",
                    held.as_secs_f64(),
                    if fires { " FIRES" } else { "" }
                ));
            }
            lines.push(format!(
                "{} (for {:?}): {}",
                mirror.rule.title,
                mirror.rule.hold,
                spans.join(", ")
            ));
        }
        Self {
            fired,
            report: lines.join("\n"),
        }
    }
}

/// Scrape every `(server, port)` every quarter second for `duration`.
pub(crate) async fn sample(
    client: &reqwest::Client,
    ports: &[(usize, u16)],
    duration: Duration,
) -> Vec<Scrape> {
    let started = Instant::now();
    let mut scrapes = Vec::new();
    while started.elapsed() < duration {
        for (server, port) in ports {
            if let Ok(scrape) = scrape(client, *server, *port).await {
                scrapes.push(scrape);
            }
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    scrapes
}
