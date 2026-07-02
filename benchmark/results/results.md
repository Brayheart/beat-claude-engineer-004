## Benchmark results (observed)

| Metric | Value |
|---|---|
| ClickHouse version | 25.8.25.37 |
| Events ingested | 348,000 |
| Sustained ingest rate | 5,804 events/sec |
| Ingest lag (event created -> queryable), p50 | 546 ms |
| Ingest lag p95 | 1,013 ms |
| Ingest lag p99 | 1,053 ms |
| Dashboard query (per-tenant 15-min breakdown), median | 5 ms |
| Segmentation query ('viewed /pricing 3+ times'), median | 6 ms |
| Segment matches found | 200 |
