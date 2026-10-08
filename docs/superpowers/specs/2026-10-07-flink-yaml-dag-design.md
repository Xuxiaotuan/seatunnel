# Flink YAML DAG Runtime Design

**Status:** draft

**Target branch:** `dev`

**Target runtime:** local Flink 1.20.5, with SeaTunnel's existing Flink 1.20 starter and translation layer.

## Goal

Allow a user to submit one YAML file whose `nodes` and `inputs` describe an arbitrary execution graph. SeaTunnel resolves the dependencies, validates plugin capabilities, builds one Flink job, and submits it to Flink. `source`, `transform`, `sink`, `sql`, and custom actions are node implementations rather than fixed top-level stages.

The first implementation is bounded Batch. A node may consume data, control, or completion values. Source and sink are still connector capabilities, but the graph has no fixed source-transform-sink ordering.

## Configuration contract

The user-facing format contains `runtime` and a `nodes` mapping. Each node has a plugin identifier, an operation, optional inputs, and plugin configuration. Explicit `edges`, `input_ports`, and `output_ports` are not required in the first format.

```yaml
apiVersion: seatunnel.apache.org/v1alpha1
kind: FlinkGraphJob
runtime:
  engine: flink
  version: "1.20.5"
  mode: batch
nodes:
  orders:
    plugin: jdbc
    operation: read
    inputs: []
    config: {}
  customers:
    plugin: jdbc
    operation: read
    inputs: []
    config: {}
  normalize:
    plugin: Python
    operation: map
    inputs: [orders]
    config: {}
  enrich:
    plugin: sql
    operation: query
    inputs:
      orders: normalize
      customers: customers
    config:
      query: |
        SELECT o.order_id, c.customer_name
        FROM orders o JOIN customers c
          ON o.customer_id = c.customer_id
  output:
    plugin: jdbc
    operation: write
    inputs: [enrich]
    config: {}
```

`inputs` accepts a sequence for ordinary positional inputs or a mapping from SQL/input alias to upstream node ID. The parser normalizes both shapes into `InputBinding(alias, upstreamNodeId)`.

## Graph and execution semantics

The resolver derives internal edges from `inputs`, detects cycles, and computes a topological order. A node referenced by multiple nodes is a fan-out. A node with no downstream reference is a terminal. The internal graph may contain data, control, and completion values; these are inferred from the plugin capability descriptor.

Data nodes use `DataStreamTableInfo` as the initial compatibility carrier so that `DataStream<SeaTunnelRow>`, `CatalogTable`, and logical table name remain together. Runtime-only control values carry readiness or completion signals and are not converted into `SeaTunnelRow` streams.

Control-only action nodes are visible in YAML but are not ordinary Flink data operators. For the first Batch slice, a root action runs before Flink submission and a terminal action runs after the Flink job completes. An action placed in the middle of a data path, or a streaming action with no completion boundary, is rejected until a Flink coordinator protocol is implemented.

SQL is one node executor. It receives named input values, registers them with a Flink `StreamTableEnvironment`, executes one query, and produces one output value. The first implementation requires bounded, insert-only inputs and uses Batch mode. Streaming interval joins, watermarks, and state TTL are subsequent work.

## Code boundaries

Add the version-neutral model and SPI under `seatunnel-core/seatunnel-flink-starter/seatunnel-flink-graph-api`. Put the Flink 1.20 implementation under the existing `seatunnel-flink-20-starter` so Flink 1.13 and 1.15 artifacts are not forced to load Flink 1.20 classes.

Reuse the following existing code:

- `FactoryUtil.createAndPrepareSource` and `createAndPrepareSink`;
- `SeaTunnelFactoryDiscovery` and existing table factory SPIs;
- `FlinkSource` from `seatunnel-translation-flink-common`;
- Flink 1.20 `FlinkSink` from `seatunnel-translation-flink-20`;
- `DataStreamTableInfo` and the existing Python `TableTransformFactory`.

The existing `FlinkExecution` and old `source`/`transform`/`sink` configuration remain unchanged. A separate `GraphFlinkExecution` and an explicit graph YAML entry path are added for the new format.

## YAML loading

Use the existing shaded `YAMLMapper` dependency pattern already used by `AgentYamlLoader`, with strict graph DTO binding so misspelled graph fields fail validation. Command-line variables must be applied before plugin option validation. No credentials are written into examples or logs.

## Acceptance boundary

The first verified path is:

```text
FakeSource + FakeSource → Python Transform → Batch SQL multi-input join → Console/JDBC Sink
```

The second path is:

```text
JDBC preparation action → PostgreSQL Source → Python Transform → PostgreSQL Sink → JDBC recon action
```

Local unit and MiniCluster evidence, targeted Maven build evidence, and PostgreSQL runtime evidence are reported separately. A successful compile is not presented as Flink cluster or database E2E proof.

## Non-goals

- Do not remove or alter the old SeaTunnel pipeline configuration.
- Do not add Flink 2.x support in this slice.
- Do not add arbitrary streaming interval joins in the Batch slice.
- Do not require users to declare explicit edges or port lists.
- Do not compile all connectors for the private artifact.
