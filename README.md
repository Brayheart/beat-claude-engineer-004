# Engineer-004: Real-Time Analytics Pipeline — Submission Artifact

Operating artifact for the [beat-claude engineer-004 challenge](https://github.com/ericosiu/beat-claude/tree/main/challenges/engineer-004).
The written answer is in [SUBMISSION.md](SUBMISSION.md). Standalone diagrams are
included in `diagrams/` as SVG files with Mermaid source:

- `diagrams/architecture.svg` / `diagrams/architecture.mmd`
- `diagrams/migration.svg` / `diagrams/migration.mmd`

Supporting evidence artifacts are in `evidence/`:

- `evidence/source-records.md` maps external claims to primary sources.
- `evidence/decision-log.md` records the stack trade-off decisions.

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
./run.sh                      # downloads ClickHouse v25.8-lts, then verifies version + SHA-256
./run.sh --rate 11600         # optional: push to 20x average
```

The runner uses its own pinned local ClickHouse binary, starts a throwaway local
server in a temporary data directory, applies the schema, drives the load, prints
results, and shuts the server down again on exit. If port 8123 is already
occupied, it exits instead of silently running against the wrong server.

Results land in `benchmark/results/results.md`; the committed observed run also has
a raw transcript at `benchmark/results/run-transcript.txt`.

## Honest scope limits

- Local single node ≠ production cluster; numbers validate the *engine and the
  ingest pattern*, not the exact production deployment.
- The benchmark schema is intentionally narrower than the production schema: it
  tests ingest/query latency, not identity stitching or duplicate-removal logic.
- The queue leg (Kinesis) is not simulated locally — a localhost Kafka would not
  be a faithful proxy anyway. Its capacity and durability numbers come from AWS
  published quotas and SLAs.
- See "What breaks it" in SUBMISSION.md for failure modes of both the benchmark
  and the design.
