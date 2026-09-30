//! `legacy-flags.json` (#511): what a build older than a flag ran instead.
//! scripts/prism_load_ab.py leaves a flag off an older build's command line
//! only when the table's rule holds, so an `equals` must be the value the
//! flag's own default keeps "as before"; a stale entry would let an older
//! build run a different workload under a preset's name.

use clap::CommandFactory;
use qbit_prism_load::{cli::Args, preset};
use serde_json::Value;

fn table() -> serde_json::Map<String, Value> {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("legacy-flags.json");
    let table: Value = serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
    assert_eq!(table["schema"], "qbit.prism.load-legacy-flags.v1");
    table["flags"].as_object().unwrap().clone()
}

/// The flag's clap default as the preset's JSON would pin it.
fn default_of(flag: &str) -> Value {
    let command = Args::command();
    let arg = command
        .get_arguments()
        .find(|arg| arg.get_long() == flag.strip_prefix("--"))
        .unwrap_or_else(|| panic!("{flag} is not a harness flag"));
    if !arg.get_action().takes_values() {
        // A switch: absent is false.
        return Value::Bool(false);
    }
    let defaults: Vec<String> = arg
        .get_default_values()
        .iter()
        .map(|v| v.to_string_lossy().into_owned())
        .collect();
    match defaults.as_slice() {
        [] => Value::Null,
        [one] if one == "false" || one == "true" => Value::Bool(one == "true"),
        [one] => one
            .parse::<f64>()
            .ok()
            .and_then(serde_json::Number::from_f64)
            .map_or_else(|| Value::String(one.clone()), Value::Number),
        many => panic!("{flag} has several defaults: {many:?}"),
    }
}

fn same(a: &Value, b: &Value) -> bool {
    match (a.as_f64(), b.as_f64()) {
        (Some(x), Some(y)) => x == y,
        _ => a == b,
    }
}

#[test]
fn every_rule_names_a_result_flag_and_states_why() {
    let results = preset::result_flags();
    for (flag, rule) in table() {
        assert!(results.contains(&flag), "{flag} is not a result flag");
        let rule = rule.as_object().unwrap();
        let kinds: Vec<&str> = ["equals", "equals_flag", "inert_when", "formula"]
            .into_iter()
            .filter(|kind| rule.contains_key(*kind))
            .collect();
        assert_eq!(kinds.len(), 1, "{flag}: {kinds:?}");
        assert!(
            rule["why"].as_str().is_some_and(|why| !why.is_empty()),
            "{flag}"
        );
        if let Some(other) = rule.get("equals_flag") {
            assert!(
                results.contains(&other.as_str().unwrap().to_owned()),
                "{flag}"
            );
        }
        if let Some(conditions) = rule.get("inert_when") {
            for key in conditions.as_object().unwrap().keys() {
                assert!(results.contains(key), "{flag}: {key}");
            }
        }
        if let Some(formula) = rule.get("formula") {
            assert_eq!(formula, "sessions-per-frontend-plus-16-min-128", "{flag}");
        }
    }
}

#[test]
fn every_equals_is_the_flags_own_as_before_default() {
    for (flag, rule) in table() {
        if let Some(value) = rule.get("equals") {
            let default = default_of(&flag);
            assert!(
                same(value, &default),
                "{flag}: table {value}, default {default}"
            );
        }
    }
}

#[test]
fn every_inert_condition_is_the_phases_off_default() {
    for (flag, rule) in table() {
        if let Some(conditions) = rule.get("inert_when") {
            for (key, value) in conditions.as_object().unwrap() {
                assert!(same(value, &default_of(key)), "{flag}: {key} {value}");
            }
        }
    }
}
