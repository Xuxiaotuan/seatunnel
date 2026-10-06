---
sidebar_position: 17
---

# Flink 多 Source SQL Join POC

`codex/flink-multi-source-join-workbench` 提供一个最小 SQL Join 验证：Flink 1.20.5、BATCH、两路 Source、单字段等值 `INNER JOIN`。输入表数量由 SQL 查询决定，输出固定为一个表。

执行顺序是：

```text
Source → pre_transform SQL → Python Transform → post_transform SQL → Sink
```

两个 SQL 阶段复用同一个 `StreamExecutionEnvironment`，不会额外提交 Flink Job，也不改变现有单输入 `SeaTunnelTransform` 接口。嵌套 SQL 阶段必须用 `plugin_input` 显式声明可见输入；`pre_transform` 处理多 Source JOIN，`post_transform` 可以读取 Python 新增的字段并继续过滤或计算。旧的顶层 `sql { query, plugin_output }` 仍按 pre 阶段处理，并兼容读取全部上游输入。配置示例位于 `seatunnel-flink-20-example` 的 `python_transform_two_source_inner_join.conf`。

当前 POC 只接受有限 FakeSource 的 BATCH 输入；配置 SQL 阶段时使用 STREAMING 会直接失败，避免在没有 watermark、状态 TTL 和恢复策略的情况下误用无界 JOIN。运行验证得到两路 Source 共 4 行输入、pre_transform SQL 后 2 行、post_transform SQL 过滤后 1 行。当前 POC 尚未覆盖 PostgreSQL CDC、迟到数据、时间窗口、状态 TTL 和多级 Join 的恢复语义。
