---
sidebar_position: 17
---

# Flink Multi-Source SQL Join POC

The `codex/flink-multi-source-join-workbench` branch provides a minimal SQL join validation for Flink 1.20.5: BATCH mode, two sources, and a single-field equi `INNER JOIN`. The SQL query determines how many input tables are used and produces one output table.

The execution order is:

```text
Source → Flink SQL → Python Transform → Sink
```

The SQL stage reuses the same `StreamExecutionEnvironment`, submits one Flink job, and keeps existing single-input `SeaTunnelTransform` plugins unchanged. The example configuration is `python_transform_two_source_inner_join.conf` in `seatunnel-flink-20-example`.

The finite FakeSource validation uses append-only BATCH inputs. STREAMING mode uses changelog inputs and preserves RowKind. The local run produced four source rows and two SQL output rows. PostgreSQL CDC, late events, time windows, state TTL, and recovery semantics for multi-level joins remain outside this POC.
