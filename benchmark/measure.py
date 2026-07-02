#!/usr/bin/env python3
"""Measure benchmark results: ingest lag percentiles + dashboard query latency.

Run after run_benchmark.py. Prints a markdown results table for the submission.
Stdlib only.
"""

import time
import urllib.parse
import urllib.request

CH_URL = "http://localhost:8123/"


def ch_query(sql):
    q = urllib.parse.urlencode({"query": sql + " FORMAT TabSeparated"})
    with urllib.request.urlopen(CH_URL + "?" + q, timeout=60) as resp:
        return resp.read().decode().strip()


def timed_query(sql, runs=5):
    """Median client-observed latency over several runs (first run warms cache)."""
    times = []
    for _ in range(runs):
        t0 = time.time()
        result = ch_query(sql)
        times.append(time.time() - t0)
    return result, sorted(times)[len(times) // 2]


def main():
    total, tmin, tmax = ch_query(
        "SELECT count(), min(emit_ts), max(emit_ts) FROM analytics.events"
    ).split("\t")
    total = int(total)
    window = float(tmax) - float(tmin)
    achieved = total / window if window > 0 else 0

    lag = ch_query(
        "SELECT quantilesExact(0.5, 0.95, 0.99)"
        "(toUnixTimestamp64Milli(insert_ts) - toInt64(emit_ts * 1000)) "
        "FROM analytics.events"
    )
    p50, p95, p99 = [float(x) for x in lag.strip("[]").split(",")]

    # Dashboard query: live event breakdown for one tenant (hot tenant 1).
    dash_sql = (
        "SELECT event_type, count() FROM analytics.events "
        "WHERE tenant_id = 1 AND insert_ts > now() - INTERVAL 15 MINUTE "
        "GROUP BY event_type ORDER BY count() DESC"
    )
    _, dash_t = timed_query(dash_sql)

    # Segmentation query from the brief: visitors who viewed /pricing 3+ times.
    seg_sql = (
        "SELECT count() FROM ("
        "SELECT visitor_id FROM analytics.events "
        "WHERE tenant_id = 1 AND url = '/pricing' AND event_type = 'page_view' "
        "GROUP BY visitor_id HAVING count() >= 3)"
    )
    seg_matches, seg_t = timed_query(seg_sql)

    print("## Benchmark results (observed)\n")
    print("| Metric | Value |")
    print("|---|---|")
    print(f"| Events ingested | {total:,} |")
    print(f"| Sustained ingest rate | {achieved:,.0f} events/sec |")
    print(f"| Ingest lag (event created -> queryable), p50 | {p50:,.0f} ms |")
    print(f"| Ingest lag p95 | {p95:,.0f} ms |")
    print(f"| Ingest lag p99 | {p99:,.0f} ms |")
    print(f"| Dashboard query (per-tenant 15-min breakdown), median | {dash_t*1000:,.0f} ms |")
    print(f"| Segmentation query ('viewed /pricing 3+ times'), median | {seg_t*1000:,.0f} ms |")
    print(f"| Segment matches found | {seg_matches} |")


if __name__ == "__main__":
    main()
