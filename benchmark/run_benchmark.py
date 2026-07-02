#!/usr/bin/env python3
"""Load benchmark: synthetic multi-tenant event stream -> batched inserts -> ClickHouse.

Simulates the production ingest path at 10x average traffic (brief: 50M events/day
~= 580/sec average, so 10x spike ~= 5,800/sec). A generator thread paces event
creation; a writer thread batches events and flushes them to ClickHouse according
to should_flush(). Stdlib only — no pip dependencies.

Usage: python3 run_benchmark.py [--rate 5800] [--duration 60] [--tenants 500]
"""

import argparse
import json
import queue
import random
import threading
import time
import urllib.parse
import urllib.request
import uuid

CH_URL = "http://localhost:8123/"
TICK_S = 0.05  # generator pacing interval

EVENT_TYPES = ["page_view", "click", "form_submit", "custom"]
EVENT_WEIGHTS = [70, 20, 5, 5]
URLS = ["/", "/pricing", "/features", "/blog", "/docs", "/signup"]
URL_WEIGHTS = [30, 15, 20, 15, 12, 8]


def should_flush(batch_size: int, oldest_age_s: float) -> bool:
    """Decide when the writer flushes its in-memory batch to ClickHouse.

    Called roughly every 50ms while events accumulate. Return True to flush now.

    The trade-off: ClickHouse wants few, large inserts (each insert creates an
    on-disk "part"; thousands of tiny inserts per minute triggers TOO_MANY_PARTS
    errors and merge pressure). But the product SLA is <5s from event to
    dashboard, so events can't sit in the buffer too long. At ~5,800 events/sec,
    one second of buffering ~= 5,800 rows.

    Args:
        batch_size: number of events currently buffered.
        oldest_age_s: seconds since the oldest buffered event was created.
    """
    # Flush at ~1s of 10x traffic or when the oldest event hits 1s in the buffer:
    # ~1 insert/sec keeps ClickHouse part creation far below merge-pressure
    # territory, while spending only ~1s of the 5s end-to-end SLA in this stage.
    return batch_size >= 6000 or oldest_age_s >= 1.0


def ch_insert(rows):
    body = "\n".join(json.dumps(r) for r in rows).encode()
    q = urllib.parse.urlencode(
        {"query": "INSERT INTO analytics.events (tenant_id, visitor_id, event_type, url, emit_ts) FORMAT JSONEachRow"}
    )
    req = urllib.request.Request(CH_URL + "?" + q, data=body, method="POST")
    with urllib.request.urlopen(req, timeout=30) as resp:
        resp.read()


def make_event_factory(n_tenants):
    # Zipf-ish tenant weights: a few hot tenants, a long tail (matches 500+
    # customer multi-tenancy where traffic is never uniform).
    tenants = list(range(1, n_tenants + 1))
    tenant_weights = [1.0 / r for r in range(1, n_tenants + 1)]
    # Pool of returning visitors per tenant so segmentation queries ("viewed
    # /pricing 3x") have realistic repeat behavior.
    visitor_pools = {t: [uuid.uuid4().hex[:16] for _ in range(200)] for t in tenants}

    def make_batch(k):
        chosen_tenants = random.choices(tenants, weights=tenant_weights, k=k)
        chosen_types = random.choices(EVENT_TYPES, weights=EVENT_WEIGHTS, k=k)
        chosen_urls = random.choices(URLS, weights=URL_WEIGHTS, k=k)
        now = time.time()
        return [
            {
                "tenant_id": t,
                "visitor_id": random.choice(visitor_pools[t]),
                "event_type": et,
                "url": u,
                "emit_ts": now,
            }
            for t, et, u in zip(chosen_tenants, chosen_types, chosen_urls)
        ]

    return make_batch


def writer_loop(q_in, stats, stop_evt):
    batch = []
    oldest_added_at = None
    while True:
        try:
            batch.append(q_in.get(timeout=TICK_S))
            if oldest_added_at is None:
                oldest_added_at = time.time()
            while True:  # drain whatever else is queued without blocking
                try:
                    batch.append(q_in.get_nowait())
                except queue.Empty:
                    break
        except queue.Empty:
            pass

        drained = stop_evt.is_set() and q_in.empty()
        if batch and (drained or should_flush(len(batch), time.time() - oldest_added_at)):
            t0 = time.time()
            ch_insert(batch)
            stats["flushes"] += 1
            stats["rows_written"] += len(batch)
            stats["batch_sizes"].append(len(batch))
            stats["flush_secs"].append(time.time() - t0)
            batch = []
            oldest_added_at = None
        if drained and not batch:
            return


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rate", type=int, default=5800, help="target events/sec")
    ap.add_argument("--duration", type=int, default=60, help="seconds to run")
    ap.add_argument("--tenants", type=int, default=500)
    args = ap.parse_args()

    make_batch = make_event_factory(args.tenants)
    q_out = queue.Queue()
    stats = {"flushes": 0, "rows_written": 0, "batch_sizes": [], "flush_secs": []}
    stop_evt = threading.Event()
    writer = threading.Thread(target=writer_loop, args=(q_out, stats, stop_evt))
    writer.start()

    per_tick = max(1, round(args.rate * TICK_S))
    sent = 0
    started = time.time()
    next_tick = started
    print(f"Generating ~{args.rate} events/sec for {args.duration}s "
          f"({args.tenants} tenants, {per_tick} events per {int(TICK_S*1000)}ms tick)...")
    while time.time() - started < args.duration:
        for ev in make_batch(per_tick):
            q_out.put(ev)
        sent += per_tick
        next_tick += TICK_S
        delay = next_tick - time.time()
        if delay > 0:
            time.sleep(delay)

    gen_elapsed = time.time() - started
    stop_evt.set()
    writer.join()

    print(f"\nGenerated {sent:,} events in {gen_elapsed:.1f}s "
          f"(achieved generation rate: {sent/gen_elapsed:,.0f}/sec)")
    print(f"Writer: {stats['flushes']} flushes, {stats['rows_written']:,} rows written")
    if stats["batch_sizes"]:
        sizes = sorted(stats["batch_sizes"])
        print(f"Batch size p50={sizes[len(sizes)//2]:,}  max={sizes[-1]:,}")
        fl = sorted(stats["flush_secs"])
        print(f"Insert time p50={fl[len(fl)//2]*1000:.0f}ms  max={fl[-1]*1000:.0f}ms")
    print("\nNow run: python3 measure.py")


if __name__ == "__main__":
    main()
