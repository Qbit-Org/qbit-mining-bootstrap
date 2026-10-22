"""Disposable PromQL regressions for #336; no database or deployment access."""

TITLE = "PrismDatabasePoolWaitHigh"
HISTOGRAM = "qbit_prism_database_pool_acquire_seconds"
BUCKETS = ("0.01", "0.025", "0.05", "0.1", "0.25", "0.5", "1", "2.5", "5", "10", "30", "+Inf")
IDENTITY = {"job": "qbit-prism", "instance": "one", "network": "mainnet"}
SCRAPE_GATE = (' and on(job, instance, network) '
               '(up{job="qbit-prism",network="mainnet"} == 1)')
SNAPSHOT_GATE = (' and on(job, instance, network) '
                 '((qbit_prism_metrics_snapshot_available{job="qbit-prism",network="mainnet"} == 1) '
                 'and on(job, instance, network) '
                 '(qbit_prism_metrics_snapshot_stale{job="qbit-prism",network="mainnet"} == 0))')
OLD_GATE = (' and on(job, instance, network) '
            '(qbit_prism_collector_available{job="qbit-prism",network="mainnet",'
            'collector="database"} == 1)')
NEGATIVE_CONTROL_SCENARIOS = (
    "pool wait: slow waits survive unavailable collector",
    "pool wait: publisher freshness cannot reset live pool dwell",
)


def old_pool_wait_expression(expression):
    """Restore both original guards, rather than adding a collector gate to up."""
    assert expression.endswith(SCRAPE_GATE), "pool wait rule must use current scrape availability"
    return expression[:-len(SCRAPE_GATE)] + SNAPSHOT_GATE + OLD_GATE


def series_name(metric, **labels):
    return metric + "{" + ",".join(f'{key}="{value}"' for key, value in sorted(labels.items())) + "}"


def target(instance="one", network="mainnet", job="qbit-prism", *,
           slow=True, count="0+1x900", collector=0, available=1, stale=0, up=1,
           outcomes=("failure",)):
    """One-second scrapes; bucket/count pairs describe real cumulative waits."""
    labels = dict(job=job, instance=instance, network=network)
    rows = {}
    for name, value in [("collector_available", collector),
                        ("metrics_snapshot_available", available),
                        ("metrics_snapshot_stale", stale), ("up", up)]:
        if value is not None:
            extra = {"collector": "database"} if name == "collector_available" else {}
            metric = name if name == "up" else "qbit_prism_" + name
            rows[series_name(metric, **labels, **extra)] = (
                value if isinstance(value, str) else f"{value}+0x900")
    if count is not None:
        for outcome in outcomes:
            result = dict(labels, result=outcome)
            # Match the producer buckets: slow waits are in (1, 2.5],
            # healthy waits in (0.05, 0.1].
            for le in BUCKETS:
                rows[series_name(HISTOGRAM + "_bucket", **result, le=le)] = (
                    "0+0x900" if float(le) < (2.5 if slow else 0.1) else count)
            rows[series_name(HISTOGRAM + "_count", **result)] = count
    return rows


def pool_wait_tests(rule, expression):
    labels = {"severity": rule["severity"], "service": rule["service"], **rule["labels"]}
    assert rule["evaluator"] == "gt" and rule["threshold"] == 0
    alert = {"alert": TITLE, "expr": f"({expression}) > 0", "for": rule["for"], "labels": labels}
    old_expression = old_pool_wait_expression(expression)
    tests = []

    def samples(values):
        return [{"labels": series_name("", **identity), "value": value} for identity, value in values]

    def expr_check(expr, values, at="6m"):
        return {"expr": expr, "eval_time": at, "exp_samples": samples(values)}

    def alert_check(identities, at="6m"):
        return {"alertname": TITLE, "eval_time": at,
                "exp_alerts": [{"exp_labels": {**identity, **labels}, "exp_annotations": {}}
                               for identity in identities]}

    def scenario(name, rows, values, firing=(), *, old_values=None):
        test = {"name": "pool wait: " + name, "interval": "1s",
                "input_series": [{"series": key, "values": value} for key, value in rows.items()],
                "promql_expr_test": [expr_check(expression, values)],
                "alert_rule_test": [alert_check([], "2m"), alert_check(firing)]}
        if old_values is not None:
            test["promql_expr_test"].append(expr_check(old_expression, old_values))
        tests.append(test)
        return test

    high, low = [(IDENTITY, 1)], [(IDENTITY, 0)]
    scenario("slow waits survive unavailable collector", target(), high, [IDENTITY], old_values=[])
    scenario("successful slow waits also survive collector failure",
             target(outcomes=("success",)), high, [IDENTITY], old_values=[])
    scenario("healthy low waits with unavailable collector", target(slow=False), low)
    scenario("healthy low waits with available collector", target(slow=False, collector=1), low)
    scenario("slow waits with available collector", target(collector=1), high, [IDENTITY], old_values=high)
    scenario("collector series absent", target(collector=None), high, [IDENTITY], old_values=[])
    for name, kwargs in [("stale snapshot does not hide live waits", {"stale": 1}),
                         ("unpublished snapshot does not hide live waits", {"available": 0, "stale": 1}),
                         ("missing body availability does not hide live waits", {"available": None}),
                         ("missing body freshness does not hide live waits", {"stale": None}),
                         ("no snapshot series does not hide live waits", {"available": None, "stale": None})]:
        scenario(name, target(collector=1, **kwargs), high, [IDENTITY], old_values=[])
    scenario("failed scrape suppresses pool waits", target(up=0), [])
    scenario("missing scrape availability suppresses pool waits", target(up=None), [])
    scenario("missing target", {}, [])
    scenario("no histogram series", target(count=None), [])
    scenario("zero observations cannot fabricate a percentile", target(count="0+0x900"), [])
    scenario("old observations outside window", target(count="100+0x900"), [])
    scenario("one isolated scrape cannot establish a rate",
             target(count=" ".join(["_"] * 360 + ["1", "stale"])), [])
    # Ten is a per-instance minimum; the two outcome series are combined.
    scenario("nine observations are insufficient", target(count="0+0x239 9+0x660"), [])
    scenario("ten observations are sufficient", target(count="0+0x239 10+0x660"), high)
    scenario("counts combine outcomes", target(count="0+0x239 5+0x660",
             outcomes=("success", "failure")), high)
    scenario("counts never combine instances", {
        **target("one", count="0+0x239 6+0x660"),
        **target("two", count="0+0x239 6+0x660"),
    }, [])
    two = dict(IDENTITY, instance="two")
    scenario("independent instances keep their labels", {
        **target("one"), **target("two", slow=False, collector=1),
    }, high + [(two, 0)], [IDENTITY], old_values=[(two, 0)])
    scenario("successful peer scrape cannot authorize failed instance", {
        **target("one", up=0), **target("two", slow=False),
    }, [(two, 0)])
    scenario("successful peer scrape cannot authorize missing instance", {
        **target("one", up=None), **target("two", slow=False),
    }, [(two, 0)])
    scenario("other job and network cannot supply sample count", {
        **target(count="0+0x239 9+0x660"),
        **target(network="signet"), **target(job="other-prism"),
    }, [])
    scenario("other job and network cannot supply scrape availability", {
        **target(up=None),
        **target(network="signet"), **target(job="other-prism"),
    }, [])
    # A blocked publisher can repeatedly age the cached body while successful
    # scrapes continue to include new live acquisition observations. Even with
    # a healthy collector, thirty-second body-freshness interruptions prevent
    # the old rule from ever earning its three-minute dwell.
    intermittent_stale = " ".join(f"{index % 2}+0x29" for index in range(31))
    for name, collector in [("sustained exhaustion survives periodic stale body", 0),
                            ("publisher freshness cannot reset live pool dwell", 1)]:
        stalled = scenario(name, target(stale=intermittent_stale, collector=collector),
                           high, [IDENTITY], old_values=high if collector else [])
        stalled["promql_expr_test"] += [expr_check(expression, high, "6m30s"),
                                        expr_check(old_expression, [], "6m30s")]
        stalled["alert_rule_test"] += [alert_check([IDENTITY], "6m30s"),
                                       alert_check([IDENTITY], "7m")]
    # The collector recovers at 6m01s, then only low waits are observed.
    # Existing slow observations keep the alert active until the window drains.
    rows = target(collector="0+0x360 1+0x539")
    for le in ("0.1", "0.25", "0.5", "1"):
        rows[series_name(HISTOGRAM + "_bucket", **IDENTITY, result="failure", le=le)] = (
            "0+0x360 1+1x539")
    recovery = scenario("recovery drains the five minute window", rows, high, [IDENTITY], old_values=[])
    recovery["promql_expr_test"] += [expr_check(expression, high, "6m1s"),
                                     expr_check(expression, low, "11m")]
    recovery["alert_rule_test"] += [alert_check([IDENTITY], "6m1s"), alert_check([], "11m")]
    # Repeated scrapes of frozen counters must resolve through the sample guard.
    frozen = scenario("frozen histogram ages out", target(count="0+1x360 360+0x539"), high, [IDENTITY])
    frozen["promql_expr_test"].append(expr_check(expression, [], "11m"))
    frozen["alert_rule_test"].append(alert_check([], "11m"))
    # A failed scrape clears firing state even while the preceding observations
    # remain inside the rate window. A successful scrape earns a new dwell.
    restored = scenario("scrape recovery restarts dwell", target(up="1+0x360 0+0x118 1+0x420",
                        count="0+1x360 360+0x118 361+1x420"), high, [IDENTITY])
    restored["promql_expr_test"] += [expr_check(expression, [], "7m"), expr_check(expression, high, "8m")]
    restored["alert_rule_test"] += [alert_check([], "7m"), alert_check([], "10m59s"),
                                    alert_check([IDENTITY], "11m")]
    return alert, tests
