---
sidebar_position: 17
---

# Flink 多 Source SQL Join POC

`codex/flink-multi-source-join-workbench` 提供一个最小 SQL Join 验证：Flink 1.20.5、BATCH、两路 Source、单字段等值 `INNER JOIN`。输入表数量由 SQL 查询决定，输出固定为一个表。

执行顺序是：

```text
Source → Flink SQL → Python Transform → Sink
```

SQL 阶段复用同一个 `StreamExecutionEnvironment`，不会额外提交 Flink Job，也不改变现有单输入 `SeaTunnelTransform` 接口。配置示例位于 `seatunnel-flink-20-example` 的 `python_transform_two_source_inner_join.conf`。

有限 FakeSource 使用 BATCH append-only 输入；STREAMING 模式使用 changelog 输入并保留 RowKind。运行验证得到两路 Source 共 4 行输入、SQL 后 2 行输出。当前 POC 尚未覆盖 PostgreSQL CDC、迟到数据、时间窗口、状态 TTL 和多级 Join 的恢复语义。
