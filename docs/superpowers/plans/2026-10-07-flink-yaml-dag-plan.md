# Flink YAML DAG Runtime Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a Flink 1.20 graph execution path where a YAML file defines nodes and `inputs`, SeaTunnel infers the DAG, and one Flink job is built and submitted.

**Architecture:** Add a version-neutral graph model/SPI module and a Flink-1.20 implementation inside the existing Flink 1.20 starter. The resolver converts each node's `inputs` into internal dependency edges; plugin capability descriptors distinguish data, control, and completion values. The legacy `FlinkExecution` path remains unchanged.

**Tech Stack:** Java 8 source compatibility, Maven, Jackson YAML through `org.apache.seatunnel.shade.com.fasterxml.jackson.dataformat.yaml.YAMLMapper`, SeaTunnel table factory SPI, Flink 1.20 DataStream/Table API, JUnit 5, existing Python transform worker.

**Spec:** `docs/superpowers/specs/2026-10-07-flink-yaml-dag-design.md`

## Global Constraints

- Work from the clean `dev` branch at the current repository revision; do not create a worktree or change unrelated modules.
- Target the local Flink runtime `/Users/xujiawei/software/flink/flink-1.20.5`; do not claim that the repository-wide `1.20.1` Maven property has become `1.20.5` until dependency convergence is verified.
- Preserve all existing HOCON/JSON `source`/`transform`/`sink` configuration and the current `FlinkExecution` behavior.
- YAML graph configuration contains `nodes` and `inputs`; explicit `edges`, `input_ports`, and `output_ports` are not required in the first version.
- The first graph execution mode is bounded Batch with insert-only SQL results; streaming interval joins are out of scope.
- Reuse the existing Python transform under `seatunnel-transforms-v2`; do not create a second Python protocol.
- Do not include PostgreSQL credentials in source, examples, logs, or generated artifacts.
- Build and package only the graph path plus Fake, Console, JDBC, Python Transform, and their transitive dependencies.

## Review Focus

- A YAML node that references a missing node, duplicates an ID, or creates a cycle must fail before any connector is initialized.
- Mapping-form inputs must preserve SQL aliases, while list-form inputs must receive deterministic aliases.
- A control-only action before a Source or after a Sink must be ordered outside the DataStream graph; a middle-of-stream control dependency must be rejected rather than silently ignored.
- Batch SQL output must reject update/delete changelog results until the graph runtime has a defined downstream changelog contract.
- Existing Python options (`source_code` or `source_code_path`, non-empty `columns`, and `process(row, context)`) must continue to be validated by the existing factory.

---

### Task 1: Add the graph API module and immutable definitions

**Files:**
- Create: `seatunnel-core/seatunnel-flink-starter/seatunnel-flink-graph-api/pom.xml`
- Create: `seatunnel-core/seatunnel-flink-starter/seatunnel-flink-graph-api/src/main/java/org/apache/seatunnel/core/starter/flink/graph/api/GraphJobDefinition.java`
- Create: `seatunnel-core/seatunnel-flink-starter/seatunnel-flink-graph-api/src/main/java/org/apache/seatunnel/core/starter/flink/graph/api/GraphNodeDefinition.java`
- Create: `seatunnel-core/seatunnel-flink-starter/seatunnel-flink-graph-api/src/main/java/org/apache/seatunnel/core/starter/flink/graph/api/InputBinding.java`
- Create: `seatunnel-core/seatunnel-flink-starter/seatunnel-flink-graph-api/src/main/java/org/apache/seatunnel/core/starter/flink/graph/api/NodeCapability.java`
- Create: `seatunnel-core/seatunnel-flink-starter/seatunnel-flink-graph-api/src/main/java/org/apache/seatunnel/core/starter/flink/graph/api/GraphNodeFactory.java`
- Modify: `seatunnel-core/seatunnel-flink-starter/pom.xml` to include the new module.
- Test: `seatunnel-core/seatunnel-flink-starter/seatunnel-flink-graph-api/src/test/java/org/apache/seatunnel/core/starter/flink/graph/api/GraphDefinitionTest.java`

**Interfaces:**
- `GraphJobDefinition` exposes runtime settings and an insertion-ordered `Map<String, GraphNodeDefinition>`.
- `GraphNodeDefinition` exposes `id`, `plugin`, `operation`, raw `inputs`, and plugin `config`.
- `InputBinding` exposes `alias()` and `upstreamNodeId()`.
- `NodeCapability` exposes accepted input kinds, output kinds, minimum/maximum input count, bounded Batch support, and whether a completion signal is produced.
- `GraphNodeFactory` exposes `factoryIdentifier()`, `operation()`, and `capability()`; Flink-20 runtime adapters create the version-specific executor.

- [ ] **Step 1: Write tests for list and mapping input shapes and immutable node identity.** Assert that `[orders]` yields an `orders` binding and `{customers: customers_node}` preserves the `customers` alias.
- [ ] **Step 2: Run the API test.** Run `./mvnw -pl seatunnel-core/seatunnel-flink-starter/seatunnel-flink-graph-api -am -DskipITs test`. Expected: the new test fails because the model/module does not exist.
- [ ] **Step 3: Implement the DTOs, capability value objects, and SPI with Java 8-compatible classes.** Keep Flink classes out of the API module.
- [ ] **Step 4: Run the API test and Spotless for the new module.** Expected: tests pass and formatting succeeds.

### Task 2: Add strict YAML loading and input normalization

**Files:**
- Modify: `seatunnel-core/seatunnel-flink-starter/seatunnel-flink-20-starter/pom.xml` to depend on `seatunnel-flink-graph-api` and the existing shaded YAML mapper.
- Create: `seatunnel-core/seatunnel-flink-starter/seatunnel-flink-20-starter/src/main/java/org/apache/seatunnel/core/starter/flink/graph/config/GraphYamlParser.java`
- Create: `seatunnel-core/seatunnel-flink-starter/seatunnel-flink-20-starter/src/main/java/org/apache/seatunnel/core/starter/flink/graph/config/GraphVariableResolver.java`
- Test: `seatunnel-core/seatunnel-flink-starter/seatunnel-flink-20-starter/src/test/java/org/apache/seatunnel/core/starter/flink/graph/config/GraphYamlParserTest.java`
- Test resources: `seatunnel-core/seatunnel-flink-starter/seatunnel-flink-20-starter/src/test/resources/graph/*.yaml`

**Interfaces:**
- `GraphYamlParser.parse(Path, Map<String, String>) -> GraphJobDefinition`.
- `GraphVariableResolver.resolve(Object, Map<String, String>) -> Object` for scalar config values and variable substitution before plugin validation.

- [ ] **Step 1: Add parser tests for the canonical YAML, unknown graph fields, missing `nodes`, list inputs, mapping inputs, and variable substitution.** Unknown graph fields must fail; unknown plugin-specific fields remain inside plugin config for the plugin validator.
- [ ] **Step 2: Run the parser tests and confirm the expected failures.** Run the targeted test class with Maven.
- [ ] **Step 3: Implement `YAMLMapper` loading using the existing shaded dependency pattern from `AgentYamlLoader`, strict graph DTO binding, and normalization into `InputBinding` values.**
- [ ] **Step 4: Run the parser tests and `git diff --check`.** Expected: all parser tests pass and no whitespace errors remain.

### Task 3: Resolve dependencies, validate graph structure, and produce a plan

**Files:**
- Create: `seatunnel-core/seatunnel-flink-starter/seatunnel-flink-20-starter/src/main/java/org/apache/seatunnel/core/starter/flink/graph/resolve/InputDependencyResolver.java`
- Create: `seatunnel-core/seatunnel-flink-starter/seatunnel-flink-20-starter/src/main/java/org/apache/seatunnel/core/starter/flink/graph/resolve/ResolvedGraph.java`
- Create: `seatunnel-core/seatunnel-flink-starter/seatunnel-flink-20-starter/src/main/java/org/apache/seatunnel/core/starter/flink/graph/resolve/ResolvedNode.java`
- Create: `seatunnel-core/seatunnel-flink-starter/seatunnel-flink-20-starter/src/main/java/org/apache/seatunnel/core/starter/flink/graph/validate/GraphValidator.java`
- Create: `seatunnel-core/seatunnel-flink-starter/seatunnel-flink-20-starter/src/main/java/org/apache/seatunnel/core/starter/flink/graph/validate/GraphValidationException.java`
- Create: `seatunnel-core/seatunnel-flink-starter/seatunnel-flink-20-starter/src/main/java/org/apache/seatunnel/core/starter/flink/graph/plan/TopologicalPlanner.java`
- Test: `seatunnel-core/seatunnel-flink-starter/seatunnel-flink-20-starter/src/test/java/org/apache/seatunnel/core/starter/flink/graph/resolve/InputDependencyResolverTest.java`
- Test: `seatunnel-core/seatunnel-flink-starter/seatunnel-flink-20-starter/src/test/java/org/apache/seatunnel/core/starter/flink/graph/validate/GraphValidatorTest.java`

**Interfaces:**
- `InputDependencyResolver.resolve(GraphJobDefinition) -> ResolvedGraph`.
- `GraphValidator.validate(ResolvedGraph, Map<String, GraphNodeFactory>) -> void`.
- `TopologicalPlanner.order(ResolvedGraph) -> List<ResolvedNode>`.

- [ ] **Step 1: Write tests for missing references, duplicate IDs, self-cycles, multi-node cycles, fan-out, list input aliases, mapping input aliases, zero-input roots, and terminal discovery.**
- [ ] **Step 2: Run the resolver/validator tests and verify failures identify the missing rule.**
- [ ] **Step 3: Implement normalization, cycle detection, deterministic topological ordering, root/terminal discovery, and capability-aware input checks.** Do not initialize a connector while validating structure.
- [ ] **Step 4: Run the targeted tests.** Expected: all structural tests pass, including deterministic order for the same YAML.

### Task 4: Implement runtime values and node executor SPI adapters

**Files:**
- Create: `seatunnel-core/seatunnel-flink-starter/seatunnel-flink-20-starter/src/main/java/org/apache/seatunnel/core/starter/flink/graph/runtime/GraphRuntimeValue.java`
- Create: `seatunnel-core/seatunnel-flink-starter/seatunnel-flink-20-starter/src/main/java/org/apache/seatunnel/core/starter/flink/graph/runtime/DataRuntimeValue.java`
- Create: `seatunnel-core/seatunnel-flink-starter/seatunnel-flink-20-starter/src/main/java/org/apache/seatunnel/core/starter/flink/graph/runtime/ControlRuntimeValue.java`
- Create: `seatunnel-core/seatunnel-flink-starter/seatunnel-flink-20-starter/src/main/java/org/apache/seatunnel/core/starter/flink/graph/runtime/CompletionRuntimeValue.java`
- Create: `seatunnel-core/seatunnel-flink-starter/seatunnel-flink-20-starter/src/main/java/org/apache/seatunnel/core/starter/flink/graph/runtime/GraphNodeExecutor.java`
- Create: `seatunnel-core/seatunnel-flink-starter/seatunnel-flink-20-starter/src/main/java/org/apache/seatunnel/core/starter/flink/graph/runtime/NodeExecutionContext.java`
- Create: `seatunnel-core/seatunnel-flink-starter/seatunnel-flink-20-starter/src/main/java/org/apache/seatunnel/core/starter/flink/graph/node/ConnectorNodeFactory.java`
- Create: `seatunnel-core/seatunnel-flink-starter/seatunnel-flink-20-starter/src/main/java/org/apache/seatunnel/core/starter/flink/graph/node/TransformNodeFactory.java`
- Create: `seatunnel-core/seatunnel-flink-starter/seatunnel-flink-20-starter/src/main/java/org/apache/seatunnel/core/starter/flink/graph/node/ActionNodeFactory.java`
- Test: `seatunnel-core/seatunnel-flink-starter/seatunnel-flink-20-starter/src/test/java/org/apache/seatunnel/core/starter/flink/graph/runtime/GraphRuntimeValueTest.java`

**Interfaces:**
- `GraphNodeExecutor.execute(NodeExecutionContext, Map<String, GraphRuntimeValue>) -> GraphRuntimeValue`.
- `DataRuntimeValue` wraps the existing `DataStreamTableInfo` and boundedness/schema metadata.
- `ControlRuntimeValue` represents a completed client-side action.
- `CompletionRuntimeValue` represents a terminal operator completion boundary.

- [ ] **Step 1: Write tests that reject connecting a data consumer to a control-only output and accept a Sink completion value for a terminal action.**
- [ ] **Step 2: Run the runtime-value tests and verify the type checks fail before implementation.**
- [ ] **Step 3: Implement value classes and context objects without changing `DataStreamTableInfo`.**
- [ ] **Step 4: Implement factory adapters that resolve existing SeaTunnel table factories by plugin identifier and expose capability metadata.**
- [ ] **Step 5: Run the targeted tests and check classloader/error messages include node ID and plugin ID.**

### Task 5: Build Source, Transform, Sink, and Action node executors

**Files:**
- Create: `seatunnel-core/seatunnel-flink-starter/seatunnel-flink-20-starter/src/main/java/org/apache/seatunnel/core/starter/flink/graph/node/SourceNodeExecutor.java`
- Create: `seatunnel-core/seatunnel-flink-starter/seatunnel-flink-20-starter/src/main/java/org/apache/seatunnel/core/starter/flink/graph/node/TransformNodeExecutor.java`
- Create: `seatunnel-core/seatunnel-flink-starter/seatunnel-flink-20-starter/src/main/java/org/apache/seatunnel/core/starter/flink/graph/node/SinkNodeExecutor.java`
- Create: `seatunnel-core/seatunnel-flink-starter/seatunnel-flink-20-starter/src/main/java/org/apache/seatunnel/core/starter/flink/graph/node/JdbcActionExecutor.java`
- Modify: `seatunnel-core/seatunnel-flink-starter/seatunnel-flink-20-starter/pom.xml` only for required graph/runtime dependencies.
- Test: `seatunnel-core/seatunnel-flink-starter/seatunnel-flink-20-starter/src/test/java/org/apache/seatunnel/core/starter/flink/graph/node/ConnectorNodeExecutorTest.java`
- Test: `seatunnel-core/seatunnel-flink-starter/seatunnel-flink-20-starter/src/test/java/org/apache/seatunnel/core/starter/flink/graph/node/ActionNodeExecutorTest.java`

**Interfaces and reuse points:**
- Source executor uses `FactoryUtil.createAndPrepareSource`, existing `SeaTunnelFactoryDiscovery`, and `FlinkSource`.
- Transform executor uses `TableTransformFactory` and `FactoryUtil.createAndPrepareMultiTableTransform`; the existing `PythonTransformFactory` remains the implementation for `plugin: Python`.
- Sink executor uses `FactoryUtil.createAndPrepareSink` and Flink 1.20 `FlinkSink`.
- JDBC action accepts a connection/query configuration, never logs passwords, and returns `ControlRuntimeValue` or fails with node context.

- [ ] **Step 1: Add adapter tests for FakeSource, Console Sink, and Python option validation using existing factories.**
- [ ] **Step 2: Add action tests for readiness/recon scalar result rules and masked error messages.**
- [ ] **Step 3: Implement Source/Transform/Sink adapters by extracting only the reusable construction logic from the existing processors; do not call `FlinkExecution` from the graph path.**
- [ ] **Step 4: Implement JDBC action execution for Batch control nodes.** Root control nodes run before Flink submission; terminal control nodes run after the Flink job completes. Reject middle control dependencies in the first version.
- [ ] **Step 5: Run targeted adapter/action tests.** Expected: Fake, Console, Python validation, and action failure modes pass without starting a cluster.

### Task 6: Implement the multi-input Batch SQL node

**Files:**
- Create: `seatunnel-core/seatunnel-flink-starter/seatunnel-flink-20-starter/src/main/java/org/apache/seatunnel/core/starter/flink/graph/node/SqlNodeExecutor.java`
- Create: `seatunnel-core/seatunnel-flink-starter/seatunnel-flink-20-starter/src/main/java/org/apache/seatunnel/core/starter/flink/graph/sql/SeaTunnelTableBridge.java`
- Create: `seatunnel-core/seatunnel-flink-starter/seatunnel-flink-20-starter/src/main/java/org/apache/seatunnel/core/starter/flink/graph/sql/SqlOutputSchemaResolver.java`
- Test: `seatunnel-core/seatunnel-flink-starter/seatunnel-flink-20-starter/src/test/java/org/apache/seatunnel/core/starter/flink/graph/node/SqlNodeExecutorTest.java`
- Test resources: `seatunnel-core/seatunnel-flink-starter/seatunnel-flink-20-starter/src/test/resources/graph/sql/*.yaml`

**Interfaces:**
- `SqlNodeExecutor.execute(...)` registers each named `DataRuntimeValue` as a table alias, executes one query, and returns one `DataRuntimeValue`.
- `SeaTunnelTableBridge` converts between `SeaTunnelRow`/`CatalogTable` and Flink Table API values while preserving field names and types.

- [ ] **Step 1: Write a bounded FakeSource + FakeSource + SQL INNER JOIN test with mapping-form inputs and one output.** Assert output rows and output field names.
- [ ] **Step 2: Run the test and verify it fails before the Table bridge exists.**
- [ ] **Step 3: Create one `StreamTableEnvironment` from the graph's existing execution environment using Batch settings; register named inputs, execute the SQL, and convert an insert-only result back to a SeaTunnel data value.**
- [ ] **Step 4: Reject update/delete changelog output in this first Batch implementation with an explicit error naming the SQL node.**
- [ ] **Step 5: Run the SQL test with Flink 1.20 dependencies and verify the result.**

The Flink 1.20 DataStream/Table bridge supports using `StreamTableEnvironment` with Batch settings and materializing the Table result back into the DataStream pipeline; the implementation must still verify bounded and insert-only source/result conditions. See the official [DataStream API integration](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/table/data_stream_api/) documentation.

### Task 7: Compile and execute one complete graph

**Files:**
- Create: `seatunnel-core/seatunnel-flink-starter/seatunnel-flink-20-starter/src/main/java/org/apache/seatunnel/core/starter/flink/graph/execution/GraphFlinkExecution.java`
- Create: `seatunnel-core/seatunnel-flink-starter/seatunnel-flink-20-starter/src/main/java/org/apache/seatunnel/core/starter/flink/graph/execution/GraphExecutionBootstrap.java`
- Create: `seatunnel-core/seatunnel-flink-starter/seatunnel-flink-20-starter/src/main/java/org/apache/seatunnel/core/starter/flink/FlinkGraphMain.java`
- Modify: `seatunnel-core/seatunnel-flink-starter/seatunnel-flink-20-starter/pom.xml` to include the graph API and graph runtime classes in the Flink 1.20 artifact.
- Test: `seatunnel-core/seatunnel-flink-starter/seatunnel-flink-20-starter/src/test/java/org/apache/seatunnel/core/starter/flink/graph/execution/GraphFlinkExecutionTest.java`

**Interfaces:**
- `GraphFlinkExecution.execute(Path yaml, Map<String, String> variables) -> GraphExecutionResult`.
- `GraphExecutionBootstrap` creates exactly one `StreamExecutionEnvironment`, plugin classloader, graph planner, and job context.

- [ ] **Step 1: Write an execution test asserting that two sources, Python Transform, SQL, and Console Sink are registered into one environment and only one `execute(jobName)` call occurs.**
- [ ] **Step 2: Implement graph execution in topological order; retain all intermediate values by node ID; register terminal sinks; execute once after all nodes are built.**
- [ ] **Step 3: Run root actions, submit the Flink job, await Batch completion, and run terminal actions with separate error reporting.**
- [ ] **Step 4: Run the local execution test with FakeSource and Console.** Expected: one successful Flink job and visible output.
- [ ] **Step 5: Verify the legacy `FlinkExecution` targeted tests remain unchanged and passing.**

### Task 8: Add YAML examples, user entrypoint, and focused documentation

**Files:**
- Create: `seatunnel-core/seatunnel-flink-starter/seatunnel-flink-20-starter/src/test/resources/graph/fake_python_sql_console.yaml`
- Create: `seatunnel-core/seatunnel-flink-starter/seatunnel-flink-20-starter/src/test/resources/graph/postgres_python_jdbc_recon.yaml`
- Modify: `/Users/xujiawei/magic/workbench/seatunnel/README.md` with the experimental YAML DAG section, Mermaid graph, command, and verified result.
- Create: `docs/zh/architecture/flink-yaml-dag.md`
- Create: `docs/en/architecture/flink-yaml-dag.md`

- [ ] **Step 1: Add the Fake + Python + multi-input SQL + Console example without secrets.**
- [ ] **Step 2: Add the PostgreSQL example using environment-variable placeholders and document the required local table/schema setup.**
- [ ] **Step 3: Document that `inputs` infer dependencies, SQL mapping keys become aliases, and explicit edges are not required in the first format.**
- [ ] **Step 4: Document the evidence boundary: unit tests, local Flink run, and PostgreSQL E2E are separate claims.**
- [ ] **Step 5: Run Markdown link/config example checks and `git diff --check`.**

### Task 9: Targeted build and private artifact verification

**Files:**
- Modify: only the graph module/starter packaging configuration required to include the graph entrypoint and selected plugins.
- Create: a non-secret packaging manifest under `seatunnel-core/seatunnel-flink-starter/seatunnel-flink-20-starter/src/assembly/` if the existing assembly requires one.

- [ ] **Step 1: Run the targeted graph/API/Flink20 reactor tests.**

```bash
./mvnw \
  -pl \
  seatunnel-core/seatunnel-flink-starter/seatunnel-flink-graph-api,\
seatunnel-core/seatunnel-flink-starter/seatunnel-flink-20-starter \
  -am -DskipITs test
```

- [ ] **Step 2: Run Spotless on touched Java/POM files and repeat the targeted tests.**
- [ ] **Step 3: Build the selected Flink 1.20 artifact and inspect its dependency tree for mixed Flink versions.**
- [ ] **Step 4: Run the Fake graph locally with `/Users/xujiawei/software/flink/flink-1.20.5/bin/flink --version` evidence and save the job output.**
- [ ] **Step 5: Run PostgreSQL E2E only with externally supplied credentials and record it separately from local build evidence.**
- [ ] **Step 6: Produce the private archive containing only the graph starter, selected connectors, Python Transform, examples, and required libraries; inspect the archive contents before any release submission.**

## Execution handoff

This plan is intended for the current `dev` branch and does not authorize a commit, push, merge, or release. After review, choose whether to implement it natively in this session or with task-by-task subagents. The first implementation gate should be Task 1–3: if YAML normalization or capability inference is wrong, do not proceed to connector execution.
