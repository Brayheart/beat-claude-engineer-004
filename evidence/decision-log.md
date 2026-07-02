# Decision Log

## Chosen Path: Kinesis + ClickHouse

Chosen because the brief asks for real-time dashboards, behavioral segmentation, personalization triggers, warehouse export, AWS fit, and a system [Observed - brief] 2 senior engineers can operate.

What this optimizes for:

- Durable ingestion before processing, so peak traffic becomes lag instead of loss.
- One new analytical data system, not a broad streaming platform migration.
- Fast tenant-scoped aggregation and segmentation queries.
- Replay/backfill via S3 archive.
- A migration that does not require SDK updates.

What it sacrifices:

- No general stream-processing framework at MVP.
- At-least-once semantics, with dedupe strategy, instead of full exactly-once complexity.
- Some query-time cost for identity stitching and exact dedupe reads.

## Rejected Alternative: MSK/Kafka + Flink + Druid/ClickHouse

Why it was tempting:

- Strong streaming primitives.
- Better fit if the product needed complex multi-event joins or long-running stream state.

Why I rejected it:

- Too much operational surface for the brief's [Observed - brief] 2 dedicated senior engineers.
- Adds a stream-processing system before the core dashboard latency problem is proven.
- Harder migration and incident surface during the [Observed - brief] 3-month MVP window.

## Rejected Alternative: Firehose + Snowflake/BigQuery First

Why it was tempting:

- Great for warehouse export.
- Low operational burden.

Why I rejected it:

- Does not directly solve <5s dashboard/trigger latency.
- Pushes the product's real-time surface onto warehouse query latency and cost.
- Still needs a separate low-latency trigger path.

## Human Judgment Knobs

- Flush policy: 6,000 rows or 1s age [Assumed policy] in the benchmark. This spends about 1s [Estimated from policy] of the SLA budget to avoid many small ClickHouse inserts.
- Migration gates: the exact percentage steps and hold times are assumptions, but a human should decide whether reconciliation differences are bugs or legacy undercounting.
- ClickHouse Cloud vs. self-managed EC2: start managed if procurement allows; self-manage only if vendor/data-residency constraints force it.
