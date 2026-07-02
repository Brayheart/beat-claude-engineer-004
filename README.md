# Engineer-004: Real-Time Analytics Pipeline — Submission Artifact

Operating artifact for the [beat-claude engineer-004 challenge](https://github.com/ericosiu/beat-claude/tree/main/challenges/engineer-004).
The written answer is in [SUBMISSION.md](SUBMISSION.md).

## What this artifact proves

The proposed architecture (Kinesis → batching consumers → ClickHouse) rests on one
claim that isn't answerable from vendor docs: **can ClickHouse make events queryable
in well under 5 seconds while sustaining 10x-peak write rates, using small inserts a
2-engineer team can operate?** This benchmark tests exactly that claim on commodity
hardware. The Kinesis leg is sized from AWS's published quotas instead (labeled
"benchmarked (vendor-published)" in the evidence log — see SUBMISSION.md).

The benchmark simulates the production ingest path:

```
synthetic SDK events (500 tenants, zipf traffic, ~5,800 events/sec = 10x average)
        │  generator thread, paced
        ▼
in-memory buffer  ──should_flush() policy──▶  batched HTTP inserts  ──▶  ClickHouse
```

and measures, per event, `insert_ts - emit_ts` = lag from "event created" to
"event queryable", plus median latency of the two dashboard queries from the brief
(live per-tenant breakdown; "visitors who viewed /pricing 3+ times" segmentation).

## Run it

Requires: macOS or Linux, Python 3.9+ (stdlib only, no pip installs), ~1GB disk.

```bash
cd benchmark
./run.sh                      # downloads ClickHouse binary on first run
./run.sh --rate 11600         # optional: push to 20x average
```

Results land in `benchmark/results/results.md`.

## Honest scope limits

- Local single node ≠ production cluster; numbers validate the *engine and the
  ingest pattern*, not the exact production deployment.
- The queue leg (Kinesis) is not simulated locally — a localhost Kafka would not
  be a faithful proxy anyway. Its capacity and durability numbers come from AWS
  published quotas and SLAs.
- See "What breaks it" in SUBMISSION.md for failure modes of both the benchmark
  and the design.
