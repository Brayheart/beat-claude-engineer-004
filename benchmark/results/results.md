## Benchmark results (observed)

| Metric | Value |
|---|---|
| ClickHouse version | 25.8.25.37 |
| Events ingested | 348,000 |
| Sustained ingest rate | 5,804 events/sec |
| Ingest lag (event created -> queryable), p50 | 553 ms |
| Ingest lag p95 | 1,033 ms |
| Ingest lag p99 | 1,058 ms |
| Dashboard query (per-tenant 15-min breakdown), median | 4 ms |
| Segmentation query ('viewed /pricing 3+ times'), median | 5 ms |
| Segment matches found | 200 |
