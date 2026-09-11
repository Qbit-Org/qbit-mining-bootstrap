use super::registry::{assert_complete_registry, running_scrape, state};
use qbit_prism_server::{
    api::{public_service, router},
    metrics::{self, Metrics},
};
use std::{collections::BTreeSet, path::Path, process::Command, sync::Arc};

const INVENTORY: &str = include_str!("../../../../docs/prism-native-metrics.md");

fn rows() -> Vec<Vec<&'static str>> {
    INVENTORY
        .split("<!-- generated-inventory:start -->")
        .nth(1)
        .unwrap()
        .split("<!-- generated-inventory:end -->")
        .next()
        .unwrap()
        .lines()
        .filter(|line| line.starts_with("| `qbit_prism_"))
        .map(|line| line.trim_matches('|').split('|').map(str::trim).collect())
        .collect()
}

fn emitted_families(body: &str) -> BTreeSet<&str> {
    body.lines()
        .filter_map(|line| {
            let name = line
                .strip_prefix("# TYPE ")
                .or_else(|| line.strip_prefix("# HELP "))
                .unwrap_or(line)
                .split([' ', '{'])
                .next()?;
            if !name.starts_with("qbit_prism_") {
                return None;
            }
            // Normalize histogram samples only when TYPE declares the base.
            for suffix in ["_bucket", "_sum", "_count"] {
                if let Some(base) = name.strip_suffix(suffix) {
                    if body.contains(&format!("# TYPE {base} histogram\n")) {
                        return Some(base);
                    }
                }
            }
            Some(name)
        })
        .collect()
}

fn assert_inventory(body: &str, role: &str, replica: bool) {
    let expected: BTreeSet<_> = rows()
        .into_iter()
        .filter(|row| row[3].starts_with(role) && (replica || !row[3].contains("replica=require")))
        .map(|row| row[0].trim_matches('`'))
        .collect();
    let actual = emitted_families(body);
    assert_eq!(
        expected.difference(&actual).collect::<Vec<_>>(),
        Vec::<&&str>::new(),
        "documented but absent: {role}, replica={replica}"
    );
    assert_eq!(
        actual.difference(&expected).collect::<Vec<_>>(),
        Vec::<&&str>::new(),
        "emitted but undocumented: {role}, replica={replica}"
    );
    for row in rows()
        .into_iter()
        .filter(|row| expected.contains(row[0].trim_matches('`')))
    {
        let name = row[0].trim_matches('`');
        if role == "run" {
            assert!(body.contains(&format!("# TYPE {name} {}\n", row[1])));
        }
        for line in body.lines().filter(|line| !line.starts_with('#')) {
            let sample = line.split([' ', '{']).next().unwrap();
            if sample != name
                && sample != format!("{name}_bucket")
                && sample != format!("{name}_count")
                && sample != format!("{name}_sum")
            {
                continue;
            }
            if let Some((_, labels)) = line.split_once('{') {
                for label in labels.split('}').next().unwrap().split(',') {
                    let key = label.split('=').next().unwrap();
                    assert!(
                        key == "le"
                            || row[2].contains(&format!("`{key}="))
                            || row[2].contains(&format!("`{key}`")),
                        "undocumented label: {line}"
                    );
                }
            }
        }
    }
}

#[test]
fn inventory_is_generated_from_registry() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    assert!(Command::new("python3")
        .args(["scripts/generate_prism_metrics.py", "--check"])
        .current_dir(root)
        .status()
        .expect("Python 3 is required to verify the generated inventory")
        .success());
    for descriptor in metrics::descriptors() {
        let entries = rows();
        let row = entries
            .iter()
            .find(|row| row[0].trim_matches('`') == descriptor.name)
            .unwrap();
        assert!(
            row[4].starts_with(descriptor.help),
            "meaning drift: {}",
            descriptor.name
        );
    }
}

#[tokio::test]
async fn inventory_scrapes_both_roles_in_both_directions() {
    let metrics = Arc::new(Metrics::default());
    let state = state(metrics.clone());
    // A running role at startup still declares unwired families without data.
    for published in [false, true] {
        if published {
            state.publish_metrics(metrics.render()).unwrap();
        }
        let body = running_scrape(router(state.clone()), &[]).await;
        assert_complete_registry(&body);
        assert_inventory(&body, "run", false);
    }
    for replica_required in [false, true] {
        let (app, _) = public_service::router(
            state.clone(),
            public_service::ServiceConfig {
                replica_required,
                ..Default::default()
            },
        );
        let body = running_scrape(app, &["/healthz", "/public/v1/mining-configuration"]).await;
        assert_inventory(&body, "public-api", replica_required);
    }
}

fn metric_tokens(text: &str) -> BTreeSet<&str> {
    text.split(|c: char| !c.is_ascii_alphanumeric() && c != '_')
        .filter(|token| token.starts_with("qbit_prism_"))
        .collect()
}

#[test]
fn native_rules_reference_only_inventory_families_for_their_role() {
    let spec: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../docs/prism-native-alert-rules.json"
    ))
    .unwrap();
    let entries = rows();
    let mut titles = BTreeSet::new();
    let mut uids = BTreeSet::new();
    for rule in spec["rules"].as_array().unwrap() {
        assert!(titles.insert(rule["title"].as_str().unwrap()));
        assert!(uids.insert(rule["uid"].as_str().unwrap()));
        let role = rule["role"].as_str().unwrap();
        let allowed: BTreeSet<_> = entries
            .iter()
            .filter(|row| row[3].starts_with(role))
            .flat_map(|row| {
                let name = row[0].trim_matches('`');
                let mut names = vec![name.to_owned()];
                if row[1] == "histogram" {
                    names.extend(
                        ["_bucket", "_count", "_sum"].map(|suffix| format!("{name}{suffix}")),
                    );
                }
                names
            })
            .collect();
        let expression = rule["expr"].as_str().unwrap();
        for name in metric_tokens(expression) {
            assert!(
                allowed.contains(name),
                "rule {} references absent {name} in {role}",
                rule["title"]
            );
            assert!(!name.starts_with("qbit_prism_block_submit_seconds"));
            assert!(!name.starts_with("qbit_prism_database_advisory_lock_wait_seconds"));
            assert_ne!(
                name, "qbit_prism_public_requests_total",
                "routed probes are not public request rate"
            );
        }
        assert!(!rule["basis"].as_str().unwrap().is_empty());
        assert!(!rule["producer_refs"].as_array().unwrap().is_empty());
    }
}

#[test]
fn migration_covers_every_deployed_alert_and_all_46_historical_names() {
    let migration: serde_json::Value =
        serde_json::from_str(include_str!("../../../../docs/prism-alert-migration.json")).unwrap();
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../tests/fixtures/prism-deployed-alerts.json"
    ))
    .unwrap();
    let spec: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../docs/prism-native-alert-rules.json"
    ))
    .unwrap();
    let active: BTreeSet<_> = spec["rules"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["title"].as_str().unwrap())
        .collect();
    let deployed: BTreeSet<_> = fixture["alerts"]
        .as_array()
        .unwrap()
        .iter()
        .map(|a| (a["uid"].as_str().unwrap(), a["title"].as_str().unwrap()))
        .collect();
    assert_eq!(deployed.len(), 78);
    let mapped = migration["deployed_alerts"].as_array().unwrap();
    let actual: BTreeSet<_> = mapped
        .iter()
        .map(|a| (a["uid"].as_str().unwrap(), a["title"].as_str().unwrap()))
        .collect();
    assert_eq!(actual.len(), mapped.len(), "duplicate migration rows");
    assert_eq!(deployed, actual);
    let mut counts = [0; 3];
    for alert in mapped {
        let replacements = alert["replacement"].as_array().unwrap();
        assert!(!alert["reason"].as_str().unwrap().is_empty());
        match alert["disposition"].as_str().unwrap() {
            "migrated" => {
                counts[0] += 1;
                assert!(!replacements.is_empty());
                for replacement in replacements {
                    assert!(active.contains(replacement.as_str().unwrap()));
                }
            }
            "unchanged-external" => {
                counts[1] += 1;
                let original = fixture["alerts"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .find(|a| a["uid"] == alert["uid"])
                    .unwrap();
                assert_eq!(original["external"], true);
                assert_eq!(replacements, &vec![alert["title"].clone()]);
            }
            "no-replacement" => {
                counts[2] += 1;
                assert!(replacements.is_empty());
            }
            other => panic!("unrecognized disposition: {other}"),
        }
    }
    assert_eq!(counts, [22, 34, 22]);
    let historical = metric_tokens(include_str!("../../../../docs/prism-overload-alerts.md"));
    assert_eq!(historical.len(), 46);
    let retired = migration["retired_names"].as_array().unwrap();
    let mapped_names: BTreeSet<_> = retired
        .iter()
        .map(|r| r["name"].as_str().unwrap())
        .collect();
    assert_eq!(mapped_names.len(), retired.len());
    assert_eq!(historical, mapped_names);
    let inventory: BTreeSet<_> = rows().iter().map(|r| r[0].trim_matches('`')).collect();
    for retired in retired {
        assert!(!retired["reason"].as_str().unwrap().is_empty());
        for replacement in retired["replacement"].as_array().unwrap() {
            assert!(inventory.contains(replacement.as_str().unwrap()));
        }
    }
    assert!(Command::new("python3")
        .args(["scripts/generate_prism_alerts.py", "--check"])
        .current_dir(Path::new(env!("CARGO_MANIFEST_DIR")).join("../.."))
        .status()
        .unwrap()
        .success());
}
