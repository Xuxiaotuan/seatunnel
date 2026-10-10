/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.core.starter.flink.graph.execution;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.core.starter.flink.graph.api.GraphNodeFactory;
import org.apache.seatunnel.core.starter.flink.graph.api.NodeCapability;
import org.apache.seatunnel.core.starter.flink.graph.config.GraphYamlParser;
import org.apache.seatunnel.core.starter.flink.graph.node.ActionNodeFactory;
import org.apache.seatunnel.core.starter.flink.graph.node.ConnectorNodeFactory;
import org.apache.seatunnel.core.starter.flink.graph.node.JdbcActionExecutor;
import org.apache.seatunnel.core.starter.flink.graph.node.SinkNodeExecutor;
import org.apache.seatunnel.core.starter.flink.graph.node.SourceNodeExecutor;
import org.apache.seatunnel.core.starter.flink.graph.node.SqlNodeExecutor;
import org.apache.seatunnel.core.starter.flink.graph.node.TransformNodeExecutor;
import org.apache.seatunnel.core.starter.flink.graph.plan.TopologicalPlanner;
import org.apache.seatunnel.core.starter.flink.graph.resolve.InputDependencyResolver;
import org.apache.seatunnel.core.starter.flink.graph.resolve.ResolvedGraph;
import org.apache.seatunnel.core.starter.flink.graph.resolve.ResolvedNode;
import org.apache.seatunnel.core.starter.flink.graph.runtime.GraphNodeExecutor;
import org.apache.seatunnel.core.starter.flink.graph.spi.GraphNodeProviderDiscovery;
import org.apache.seatunnel.core.starter.flink.graph.validate.GraphValidator;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.nio.file.Path;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/** Builds one shared Flink runtime for a parsed graph. */
public final class GraphExecutionBootstrap {

    private final Map<String, GraphNodeFactory> factories;
    private final Map<String, GraphNodeExecutor> executors;

    public GraphExecutionBootstrap() {
        Map<String, GraphNodeFactory> factoryMap = new LinkedHashMap<>();
        Map<String, GraphNodeExecutor> executorMap = new LinkedHashMap<>();
        register(
                factoryMap,
                executorMap,
                "FakeSource",
                "read",
                sourceFactory("FakeSource"),
                new SourceNodeExecutor());
        register(
                factoryMap,
                executorMap,
                "Jdbc",
                "read",
                sourceFactory("Jdbc"),
                new SourceNodeExecutor());
        register(
                factoryMap,
                executorMap,
                "Python",
                "map",
                transformFactory("Python"),
                new TransformNodeExecutor());
        register(
                factoryMap,
                executorMap,
                "sql",
                "query",
                transformFactory("sql"),
                new SqlNodeExecutor());
        register(
                factoryMap,
                executorMap,
                "Console",
                "write",
                sinkFactory("Console"),
                new SinkNodeExecutor());
        register(
                factoryMap,
                executorMap,
                "Jdbc",
                "write",
                sinkFactory("Jdbc"),
                new SinkNodeExecutor());
        register(
                factoryMap,
                executorMap,
                "jdbc_action",
                "check",
                actionFactory(),
                new JdbcActionExecutor());
        new GraphNodeProviderDiscovery()
                .load(Thread.currentThread().getContextClassLoader(), factoryMap, executorMap);
        this.factories = Collections.unmodifiableMap(factoryMap);
        this.executors = Collections.unmodifiableMap(executorMap);
    }

    public GraphExecutionBootstrap(
            Map<String, GraphNodeFactory> factories, Map<String, GraphNodeExecutor> executors) {
        this.factories = Collections.unmodifiableMap(new LinkedHashMap<>(factories));
        this.executors = Collections.unmodifiableMap(new LinkedHashMap<>(executors));
    }

    public PreparedGraph prepare(Path yaml, Map<String, String> variables) throws Exception {
        GraphYamlParser parser = new GraphYamlParser();
        ResolvedGraph graph =
                new InputDependencyResolver()
                        .resolve(
                                parser.parse(
                                        yaml,
                                        variables == null ? Collections.emptyMap() : variables));
        new GraphValidator().validate(graph, factories);
        java.util.List<ResolvedNode> order = new TopologicalPlanner().order(graph);
        StreamExecutionEnvironment environment =
                StreamExecutionEnvironment.getExecutionEnvironment();
        if (graph.definition().runtime().parallelism() != null) {
            environment.setParallelism(graph.definition().runtime().parallelism());
        }
        StreamTableEnvironment tableEnvironment =
                StreamTableEnvironment.create(
                        environment, EnvironmentSettings.newInstance().inBatchMode().build());
        JobContext jobContext = new JobContext();
        jobContext.setJobMode(JobMode.BATCH);
        return new PreparedGraph(graph, order, environment, tableEnvironment, jobContext);
    }

    public static String key(String plugin, String operation) {
        return plugin + ":" + operation;
    }

    private static void register(
            Map<String, GraphNodeFactory> factoryMap,
            Map<String, GraphNodeExecutor> executorMap,
            String plugin,
            String operation,
            GraphNodeFactory factory,
            GraphNodeExecutor executor) {
        factoryMap.put(key(plugin, operation), factory);
        executorMap.put(key(plugin, operation), executor);
    }

    private static GraphNodeFactory sourceFactory(String plugin) {
        return new ConnectorNodeFactory(
                plugin,
                "read",
                NodeCapability.builder()
                        .inputCount(0, Integer.MAX_VALUE)
                        .acceptsInput(NodeCapability.ValueKind.CONTROL)
                        .produces(NodeCapability.ValueKind.DATA)
                        .supportsBatch(true)
                        .build()) {
            @Override
            public GraphNodeExecutor createExecutor(ResolvedNode node) {
                return new SourceNodeExecutor();
            }
        };
    }

    private static GraphNodeFactory transformFactory(String plugin) {
        return new ConnectorNodeFactory(
                plugin,
                "sql".equals(plugin) ? "query" : "map",
                NodeCapability.builder()
                        .inputCount(1, Integer.MAX_VALUE)
                        .acceptsInput(NodeCapability.ValueKind.DATA)
                        .produces(NodeCapability.ValueKind.DATA)
                        .supportsBatch(true)
                        .build()) {
            @Override
            public GraphNodeExecutor createExecutor(ResolvedNode node) {
                return "sql".equals(plugin) ? new SqlNodeExecutor() : new TransformNodeExecutor();
            }
        };
    }

    private static GraphNodeFactory sinkFactory(String plugin) {
        return new ConnectorNodeFactory(
                plugin,
                "write",
                NodeCapability.builder()
                        .inputCount(1, 1)
                        .acceptsInput(NodeCapability.ValueKind.DATA)
                        .produces(NodeCapability.ValueKind.COMPLETION)
                        .producesCompletion(true)
                        .supportsBatch(true)
                        .build()) {
            @Override
            public GraphNodeExecutor createExecutor(ResolvedNode node) {
                return new SinkNodeExecutor();
            }
        };
    }

    private static GraphNodeFactory actionFactory() {
        return new ActionNodeFactory(
                "jdbc_action",
                "check",
                NodeCapability.builder()
                        .inputCount(0, Integer.MAX_VALUE)
                        .acceptsInput(NodeCapability.ValueKind.CONTROL)
                        .acceptsInput(NodeCapability.ValueKind.COMPLETION)
                        .produces(NodeCapability.ValueKind.CONTROL)
                        .executionScope(NodeCapability.ExecutionScope.CLIENT_CONTROL)
                        .supportsBatch(true)
                        .build()) {
            @Override
            public GraphNodeExecutor createExecutor(ResolvedNode node) {
                return new JdbcActionExecutor();
            }
        };
    }

    public final class PreparedGraph {
        private final ResolvedGraph graph;
        private final java.util.List<ResolvedNode> order;
        private final StreamExecutionEnvironment environment;
        private final StreamTableEnvironment tableEnvironment;
        private final JobContext jobContext;

        private PreparedGraph(
                ResolvedGraph graph,
                java.util.List<ResolvedNode> order,
                StreamExecutionEnvironment environment,
                StreamTableEnvironment tableEnvironment,
                JobContext jobContext) {
            this.graph = graph;
            this.order = Collections.unmodifiableList(new java.util.ArrayList<>(order));
            this.environment = environment;
            this.tableEnvironment = tableEnvironment;
            this.jobContext = jobContext;
        }

        public ResolvedGraph graph() {
            return graph;
        }

        public java.util.List<ResolvedNode> order() {
            return order;
        }

        public StreamExecutionEnvironment environment() {
            return environment;
        }

        public StreamTableEnvironment tableEnvironment() {
            return tableEnvironment;
        }

        public JobContext jobContext() {
            return jobContext;
        }

        public GraphNodeExecutor executor(ResolvedNode node) {
            GraphNodeExecutor executor =
                    executors.get(key(node.definition().plugin(), node.definition().operation()));
            if (executor == null) {
                executor = executors.get(node.definition().plugin());
            }
            if (executor == null) {
                throw new IllegalArgumentException(
                        "No executor registered for "
                                + node.definition().plugin()
                                + ":"
                                + node.definition().operation());
            }
            return executor;
        }
    }
}
