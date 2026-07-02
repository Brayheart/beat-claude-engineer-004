# Source Records

This file is the source trail behind the numbers and external claims in `SUBMISSION.md`.

## Challenge Inputs

- Engineer-004 brief: https://github.com/ericosiu/beat-claude/tree/main/challenges/engineer-004
  - [Observed - brief] 50M events/day, <5s dashboard target, 10x traffic spikes, ~3% peak loss, 500+ customers, $50K/month ceiling, MVP in 3 months/full system in 6 months, 2 dedicated senior engineers, AWS constraint, no SDK-breaking change.
- Public scoring guide: https://github.com/ericosiu/beat-claude/blob/main/SCORING.md
  - Used for evidence tiers, number source labels, AI disclosure, failure modes, and "what stays human."

## Vendor/Technical Claims

- Kinesis on-demand sizing/ramp behavior:
  - AWS docs: https://docs.aws.amazon.com/streams/latest/dev/how-do-i-size-a-stream.html
  - AWS FAQ: https://aws.amazon.com/kinesis/data-streams/faqs/
  - Used for: 4 MB/s / 4,000 records/sec starting write capacity, ~2x previous peak behavior, and possible throttling when traffic grows faster than that window.
- Kinesis retention:
  - AWS API docs: https://docs.aws.amazon.com/kinesis/latest/APIReference/API_IncreaseStreamRetentionPeriod.html
  - Used for: max retention up to 8,760 hours / 365 days; 7-day stream retention is my assumed configuration, not a default.
- ClickHouse insert batching:
  - ClickHouse docs: https://clickhouse.com/docs/best-practices/selecting-an-insert-strategy
  - Used for: larger batches reduce parts/merge pressure; benchmark flush policy deliberately trades ~1s buffer time for stable insert shape.
- ClickHouse ReplacingMergeTree correctness:
  - ClickHouse docs: https://clickhouse.com/docs/guides/replacing-merge-tree
  - Used for: dedupe is eventual; exact reads need `FINAL`/explicit grouping where correctness matters.
- ClickHouse insert dedupe:
  - ClickHouse docs: https://clickhouse.com/docs/guides/developer/deduplicating-inserts-on-retries
  - Used for: insert-deduplication tokens as one layer for retried batches.
- ClickHouse release asset:
  - Release: https://github.com/ClickHouse/ClickHouse/releases/tag/v25.8.25.37-lts
  - Used for: pinned benchmark binary and SHA-256 verification in `benchmark/run.sh`.

## Artifact Records

- Benchmark source: `benchmark/run_benchmark.py`
- Benchmark schema: `benchmark/schema.sql`
- Runner: `benchmark/run.sh`
- Observed benchmark result table: `benchmark/results/results.md`
- Raw benchmark transcript: `benchmark/results/run-transcript.txt`

