-- Local proxy for the production events table (see SUBMISSION.md §1 for the full production schema).
-- emit_ts is stamped client-side by the generator (stands in for the SDK timestamp);
-- insert_ts is stamped by ClickHouse at insert time, so (insert_ts - emit_ts) measures
-- end-to-end pipeline lag from "event created" to "event queryable".

CREATE DATABASE IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.events
(
    tenant_id   UInt32,
    visitor_id  String,
    event_type  LowCardinality(String),
    url         String,
    emit_ts     Float64,                       -- unix seconds, client-stamped
    insert_ts   DateTime64(3) DEFAULT now64(3) -- server-stamped at insert
)
ENGINE = MergeTree
PARTITION BY toDate(insert_ts)
ORDER BY (tenant_id, event_type, insert_ts);
