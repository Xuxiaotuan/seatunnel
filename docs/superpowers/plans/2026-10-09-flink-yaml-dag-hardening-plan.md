# Flink YAML DAG Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans. Steps use checkbox syntax for tracking.

**Goal:** Make the existing Flink 1.20 YAML DAG runner correctly support typed data/control/completion dependencies, task-level pre/post actions, strict graph validation, and an extensible Flink20 node-provider registry without changing legacy SeaTunnel execution.

**Architecture:** Keep `nodes` plus `inputs` as the only user-facing edge declaration. The planner will classify edges by the upstream value kind, build one Flink data graph, and execute control nodes around that graph. Built-in adapters remain Fake, Console, JDBC, Python, SQL, and JDBC action; additional Flink20 graph nodes may be supplied through a version-specific `ServiceLoader` provider.

**Tech Stack:** Java 8-compatible source, Maven, Flink 1.20.5, existing SeaTunnel factory discovery, ServiceLoader, JUnit 5, Spotless.

**Spec:** `docs/superpowers/specs/2026-10-07-flink-yaml-dag-design.md`

## Global Constraints

- Work directly on the current `dev` checkout; preserve unrelated files and legacy `FlinkExecution` behavior.
- Target bounded Batch on Flink 1.20.x; streaming joins and checkpoint-coordinated actions remain out of scope.
- Do not add explicit `edges`, ports, or `before`/`after` fields.
- Keep the selected validation scope: Fake, Console, JDBC, Python Transform, SQL, and JDBC actions.
- Do not log credentials or place PostgreSQL credentials in committed files or artifacts.
- Do not commit or push unless separately requested after verification.

## Review Focus

- `prepare -> source` must be a control dependency and must run before Flink submission.
- `sink -> recon` must consume completion, run only after successful job completion, and fail the job result if reconciliation fails.
- Duplicate inputs must not corrupt topological indegrees.
- Invalid engine, version, mode, plugin, operation, or value-kind combinations must fail before connector initialization.
- A provider loaded from a graph-node ServiceLoader must not override a built-in `(plugin, operation)` key silently.

### Task 1: Harden YAML and graph validation

**Files:** Modify `InputBinding`, `InputDependencyResolver`, `GraphValidator`; extend existing parser/resolver/validator tests.

- [x] Add failing tests for duplicate list inputs, unsupported engine/version/mode, and invalid graph envelope.
- [x] Add duplicate-input detection and exact validation for `seatunnel.apache.org/v1alpha1`, `FlinkGraphJob`, `flink`, `1.20.x`, and `batch`.
- [x] Keep deterministic list aliases and mapping aliases unchanged.
- [x] Run the focused parser/resolver/validator tests and `git diff --check`.

### Task 2: Implement typed pre/post dependencies

**Files:** Modify `GraphExecutionBootstrap`, `GraphFlinkExecution`, `SourceNodeExecutor`, `JdbcActionExecutor`; extend `GraphValidatorTest`, `ActionNodeExecutorTest`, and `GraphFlinkExecutionTest`.

- [x] Add failing tests proving a control action may feed a source and a sink completion may feed a post action.
- [x] Make source capabilities accept control-only dependencies while rejecting data dependencies.
- [x] Make JDBC actions accept zero inputs for pre-checks or one completion input for reconciliation; reject data/control inputs.
- [x] Defer nodes consuming `COMPLETION` until after the single Flink `execute()` call instead of identifying actions by a plugin-name suffix.
- [x] Preserve failure semantics: pre-action failure prevents submission; data failure prevents post actions; post-action failure propagates with node context.
- [x] Run graph execution and action tests, including a one-job execution assertion.

### Task 3: Add Flink20 graph-node provider discovery

**Files:** Create a Flink20 `GraphNodeProvider` interface and discovery utility; modify `GraphExecutionBootstrap`; add provider discovery tests/resources.

- [x] Add a failing ServiceLoader test for a provider exposing `(plugin, operation)`, capability metadata, and executor.
- [x] Load providers with the graph classloader and reject duplicate keys with a clear error.
- [x] Register built-ins through the same registry path while keeping the existing constructor used by tests.
- [x] Do not change the version-neutral graph API to depend on Flink classes.
- [x] Run discovery tests and targeted module verification.

### Task 4: Harden data and SQL boundaries

**Files:** Modify `SqlNodeExecutor`, `SeaTunnelTableBridge`, `TransformNodeExecutor`, and focused SQL/connector tests.

- [x] Add tests for unsupported changelog kinds, missing catalog metadata, and Python output schema failures.
- [x] Keep SQL restricted to bounded insert-only inputs/results and report the SQL node ID on failures.
- [x] Preserve multiple source registration, SQL alias mapping, Python row-level transforms, and sink fan-out.
- [x] Verify connector initialization happens only after graph validation succeeds.

### Task 5: Documentation, examples, build, and evidence

**Files:** Modify root README and bilingual DAG docs; add pre/post example; update private packaging only if required.

- [x] Document logical DAG versus physical Flink data graph and task-level action semantics.
- [x] Add a non-secret YAML example for `prepare -> Jdbc -> Python -> Jdbc -> recon`.
- [x] Run targeted Maven verify for graph API and Flink20 starter with Flink 1.20.5 dependencies.
- [x] Run the local Fake/Python/SQL/Console graph and separately run PostgreSQL connectivity or E2E evidence when available.
- [x] Inspect the private archive contents and report unverified boundaries.
- [x] Review the final diff; commit and push only after a separate user request.
