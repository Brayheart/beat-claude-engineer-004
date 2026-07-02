## Benchmark results (observed)

| Metric | Value |
|---|---|
| ClickHouse version | 25.8.25.37 |
| Events ingested | 348,000 |
| Sustained ingest rate | 5,805 events/sec |
| Ingest lag (event created -> queryable), p50 | 536 ms |
| Ingest lag p95 | 997 ms |
| Ingest lag p99 | 1,043 ms |
| Dashboard query (per-tenant 15-min breakdown), median | 4 ms |
| Segmentation query ('viewed /pricing 3+ times'), median | 6 ms |
| Segment matches found | 200 |
