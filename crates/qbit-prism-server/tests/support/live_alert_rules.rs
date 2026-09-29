//! The rendered PRISM alert rules, evaluated on live `/metrics` scrapes
//! (#575). A scenario mirrors a rule's condition in Rust, pins the parts of
//! the rule's expression it mirrors (so a changed rule fails the scenario
//! instead of silently diverging), and asks whether the condition held on
//! every scrape for at least the rule's `for` duration: whether Prometheus,
//! scraping as often, would have fired it.
use super::*;
use std::collections::BTreeMap;

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

/// The longest run of consecutive scrapes on which `condition` held,
/// measured from its first scrape to its last.
pub(crate) fn longest<'a>(
    scrapes: impl IntoIterator<Item = &'a Scrape>,
    condition: impl Fn(&Scrape) -> bool,
) -> Duration {
    let mut longest = Duration::ZERO;
    let mut since = None;
    for scrape in scrapes {
        if condition(scrape) {
            let start = *since.get_or_insert(scrape.at);
            longest = longest.max(scrape.at - start);
        } else {
            since = None;
        }
    }
    longest
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
