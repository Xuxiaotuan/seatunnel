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

import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.core.starter.flink.execution.DataStreamTableInfo;
import org.apache.seatunnel.core.starter.flink.graph.api.GraphNodeFactory;
import org.apache.seatunnel.core.starter.flink.graph.api.NodeCapability;
import org.apache.seatunnel.core.starter.flink.graph.runtime.DataRuntimeValue;
import org.apache.seatunnel.core.starter.flink.graph.runtime.GraphNodeExecutor;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

class GraphFlinkExecutionTest {

    @TempDir Path tempDir;

    @Test
    void buildsPythonAndSqlNodesIntoOneFlinkSubmission() throws Exception {
        Path yaml = tempDir.resolve("graph.yaml");
        Files.write(yaml, yaml().getBytes(StandardCharsets.UTF_8));

        Map<String, GraphNodeFactory> factories = new LinkedHashMap<>();
        Map<String, GraphNodeExecutor> executors = new LinkedHashMap<>();
        register(factories, executors, "fake_source", "read", sourceExecutor());
        register(factories, executors, "Python", "map", passthroughExecutor());
        register(factories, executors, "sql", "query", passthroughExecutor());
        register(factories, executors, "console", "write", sinkExecutor());

        GraphExecutionBootstrap bootstrap = new GraphExecutionBootstrap(factories, executors);
        GraphExecutionResult result =
                new GraphFlinkExecution(bootstrap).execute(yaml, Collections.emptyMap());

        Assertions.assertEquals(
                Arrays.asList("orders", "customers", "normalize", "enrich", "output"),
                result.orderedNodeIds());
        Assertions.assertEquals(1, result.executeCallCount());
    }

    private static void register(
            Map<String, GraphNodeFactory> factories,
            Map<String, GraphNodeExecutor> executors,
            String plugin,
            String operation,
            GraphNodeExecutor executor) {
        factories.put(plugin + ":" + operation, factory(plugin, operation));
        executors.put(plugin + ":" + operation, executor);
    }

    private static GraphNodeFactory factory(String plugin, String operation) {
        NodeCapability.Builder capability = NodeCapability.builder().supportsBatch(true);
        if ("read".equals(operation)) {
            capability.inputCount(0, 0).produces(NodeCapability.ValueKind.DATA);
        } else if ("write".equals(operation)) {
            capability
                    .inputCount(1, 1)
                    .acceptsInput(NodeCapability.ValueKind.DATA)
                    .produces(NodeCapability.ValueKind.COMPLETION)
                    .producesCompletion(true);
        } else {
            capability
                    .inputCount(1, Integer.MAX_VALUE)
                    .acceptsInput(NodeCapability.ValueKind.DATA)
                    .produces(NodeCapability.ValueKind.DATA);
        }
        return new GraphNodeFactory() {
            @Override
            public String factoryIdentifier() {
                return plugin;
            }

            @Override
            public String operation() {
                return operation;
            }

            @Override
            public NodeCapability capability() {
                return capability.build();
            }
        };
    }

    private static GraphNodeExecutor sourceExecutor() {
        return (context, values) -> {
            DataStream<SeaTunnelRow> stream =
                    context.executionEnvironment()
                            .fromCollection(
                                    Collections.singletonList(new SeaTunnelRow(new Object[] {1})),
                                    TypeInformation.of(SeaTunnelRow.class));
            SeaTunnelRowType rowType =
                    new SeaTunnelRowType(
                            new String[] {"id"}, new SeaTunnelDataType<?>[] {BasicType.INT_TYPE});
            return new DataRuntimeValue(
                    new DataStreamTableInfo(
                            stream,
                            Collections.singletonList(
                                    CatalogTableUtil.getCatalogTable(context.node().id(), rowType)),
                            context.node().id()),
                    true,
                    true);
        };
    }

    private static GraphNodeExecutor passthroughExecutor() {
        return (context, values) -> values.values().iterator().next();
    }

    private static GraphNodeExecutor sinkExecutor() {
        return (context, values) -> {
            DataRuntimeValue data = (DataRuntimeValue) values.values().iterator().next();
            data.tableInfo().getDataStream().print(context.node().id());
            return new org.apache.seatunnel.core.starter.flink.graph.runtime.CompletionRuntimeValue(
                    context.node().id());
        };
    }

    private static String yaml() {
        return "apiVersion: seatunnel.apache.org/v1alpha1\n"
                + "kind: FlinkGraphJob\n"
                + "runtime:\n"
                + "  engine: flink\n"
                + "  version: \"1.20.5\"\n"
                + "  mode: batch\n"
                + "nodes:\n"
                + "  orders:\n"
                + "    plugin: fake_source\n"
                + "    operation: read\n"
                + "  customers:\n"
                + "    plugin: fake_source\n"
                + "    operation: read\n"
                + "  normalize:\n"
                + "    plugin: Python\n"
                + "    operation: map\n"
                + "    inputs: [orders]\n"
                + "  enrich:\n"
                + "    plugin: sql\n"
                + "    operation: query\n"
                + "    inputs: [normalize, customers]\n"
                + "  output:\n"
                + "    plugin: console\n"
                + "    operation: write\n"
                + "    inputs: [enrich]\n";
    }
}
