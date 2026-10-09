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

package org.apache.seatunnel.core.starter.flink.graph.validate;

import org.apache.seatunnel.core.starter.flink.graph.api.GraphJobDefinition;
import org.apache.seatunnel.core.starter.flink.graph.api.GraphNodeDefinition;
import org.apache.seatunnel.core.starter.flink.graph.api.GraphNodeFactory;
import org.apache.seatunnel.core.starter.flink.graph.api.GraphRuntimeDefinition;
import org.apache.seatunnel.core.starter.flink.graph.api.NodeCapability;
import org.apache.seatunnel.core.starter.flink.graph.resolve.InputDependencyResolver;
import org.apache.seatunnel.core.starter.flink.graph.resolve.ResolvedGraph;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

class GraphValidatorTest {

    private final InputDependencyResolver resolver = new InputDependencyResolver();
    private final GraphValidator validator = new GraphValidator();

    @Test
    void acceptsADataGraphWithZeroInputRoot() {
        Map<String, GraphNodeDefinition> nodes = new LinkedHashMap<>();
        nodes.put("source", node("source", Collections.emptyList(), "source"));
        nodes.put("sink", node("sink", Collections.singletonList("source"), "sink"));
        ResolvedGraph graph = resolver.resolve(job(nodes));

        Assertions.assertDoesNotThrow(
                () ->
                        validator.validate(
                                graph, factories(dataFactory("source"), sinkFactory("sink"))));
    }

    @Test
    void rejectsUnknownPluginAndWrongOperation() {
        Map<String, GraphNodeDefinition> nodes = new LinkedHashMap<>();
        nodes.put("source", node("source", Collections.emptyList(), "unknown"));
        ResolvedGraph graph = resolver.resolve(job(nodes));

        GraphValidationException unknown =
                Assertions.assertThrows(
                        GraphValidationException.class,
                        () -> validator.validate(graph, factories(dataFactory("source"))));
        Assertions.assertTrue(unknown.getMessage().contains("source"));
    }

    @Test
    void rejectsControlOnlyOutputConnectedToDataConsumer() {
        Map<String, GraphNodeDefinition> nodes = new LinkedHashMap<>();
        nodes.put("prepare", node("prepare", Collections.emptyList(), "action"));
        nodes.put("source", node("source", Collections.singletonList("prepare"), "source"));
        ResolvedGraph graph = resolver.resolve(job(nodes));

        GraphValidationException error =
                Assertions.assertThrows(
                        GraphValidationException.class,
                        () ->
                                validator.validate(
                                        graph, factories(actionFactory(), dataFactory("source"))));
        Assertions.assertTrue(error.getMessage().contains("control"));
    }

    @Test
    void acceptsControlDependencyBeforeSource() {
        Map<String, GraphNodeDefinition> nodes = new LinkedHashMap<>();
        nodes.put("prepare", node("prepare", Collections.emptyList(), "action"));
        nodes.put("source", node("source", Collections.singletonList("prepare"), "source"));
        ResolvedGraph graph = resolver.resolve(job(nodes));

        Assertions.assertDoesNotThrow(
                () ->
                        validator.validate(
                                graph, factories(controlFactory(), controlSourceFactory())));
    }

    @Test
    void acceptsCompletionDependencyForTerminalAction() {
        Map<String, GraphNodeDefinition> nodes = new LinkedHashMap<>();
        nodes.put("sink", node("sink", Collections.emptyList(), "sink"));
        nodes.put("recon", node("recon", Collections.singletonList("sink"), "action"));
        ResolvedGraph graph = resolver.resolve(job(nodes));

        Assertions.assertDoesNotThrow(
                () ->
                        validator.validate(
                                graph,
                                factories(completionSinkFactory(), completionActionFactory())));
    }

    @Test
    void rejectsMismatchedNodeMapIdentity() {
        Map<String, GraphNodeDefinition> nodes = new LinkedHashMap<>();
        nodes.put("map_key", node("actual_id", Collections.emptyList(), "source"));

        Assertions.assertThrows(IllegalArgumentException.class, () -> job(nodes));
    }

    @Test
    void rejectsUnsupportedGraphEnvelope() {
        Map<String, GraphNodeDefinition> nodes = new LinkedHashMap<>();
        nodes.put("source", node("source", Collections.emptyList(), "source"));
        GraphJobDefinition invalid =
                new GraphJobDefinition(
                        "seatunnel.apache.org/v2",
                        "OtherJob",
                        new GraphRuntimeDefinition("spark", "3.5.0", "streaming"),
                        nodes);

        GraphValidationException error =
                Assertions.assertThrows(
                        GraphValidationException.class,
                        () ->
                                validator.validate(
                                        invalidGraph(invalid), factories(dataFactory("source"))));

        Assertions.assertTrue(error.getMessage().contains("apiVersion"));
    }

    private static ResolvedGraph invalidGraph(GraphJobDefinition definition) {
        return new InputDependencyResolver().resolve(definition);
    }

    private static Map<String, GraphNodeFactory> factories(GraphNodeFactory... values) {
        Map<String, GraphNodeFactory> result = new LinkedHashMap<>();
        for (GraphNodeFactory value : values) {
            result.put(value.factoryIdentifier(), value);
        }
        return result;
    }

    private static GraphNodeFactory dataFactory(String identifier) {
        return factory(
                identifier,
                NodeCapability.builder()
                        .inputCount(0, Integer.MAX_VALUE)
                        .acceptsInput(NodeCapability.ValueKind.DATA)
                        .produces(NodeCapability.ValueKind.DATA)
                        .supportsBatch(true)
                        .build());
    }

    private static GraphNodeFactory sinkFactory(String identifier) {
        return factory(
                identifier,
                NodeCapability.builder()
                        .inputCount(1, Integer.MAX_VALUE)
                        .acceptsInput(NodeCapability.ValueKind.DATA)
                        .produces(NodeCapability.ValueKind.COMPLETION)
                        .supportsBatch(true)
                        .producesCompletion(true)
                        .build());
    }

    private static GraphNodeFactory actionFactory() {
        return factory(
                "action",
                NodeCapability.builder()
                        .inputCount(0, 0)
                        .produces(NodeCapability.ValueKind.CONTROL)
                        .supportsBatch(true)
                        .build());
    }

    private static GraphNodeFactory controlFactory() {
        return factory(
                "action",
                NodeCapability.builder()
                        .inputCount(0, 0)
                        .produces(NodeCapability.ValueKind.CONTROL)
                        .supportsBatch(true)
                        .build());
    }

    private static GraphNodeFactory controlSourceFactory() {
        return factory(
                "source",
                NodeCapability.builder()
                        .inputCount(0, Integer.MAX_VALUE)
                        .acceptsInput(NodeCapability.ValueKind.CONTROL)
                        .produces(NodeCapability.ValueKind.DATA)
                        .supportsBatch(true)
                        .build());
    }

    private static GraphNodeFactory completionSinkFactory() {
        return factory(
                "sink",
                NodeCapability.builder()
                        .inputCount(0, 0)
                        .produces(NodeCapability.ValueKind.COMPLETION)
                        .supportsBatch(true)
                        .producesCompletion(true)
                        .build());
    }

    private static GraphNodeFactory completionActionFactory() {
        return factory(
                "action",
                NodeCapability.builder()
                        .inputCount(1, 1)
                        .acceptsInput(NodeCapability.ValueKind.COMPLETION)
                        .produces(NodeCapability.ValueKind.CONTROL)
                        .supportsBatch(true)
                        .build());
    }

    private static GraphNodeFactory factory(String identifier, NodeCapability capability) {
        return new GraphNodeFactory() {
            @Override
            public String factoryIdentifier() {
                return identifier;
            }

            @Override
            public String operation() {
                return "op";
            }

            @Override
            public NodeCapability capability() {
                return capability;
            }
        };
    }

    private static GraphJobDefinition job(Map<String, GraphNodeDefinition> nodes) {
        return new GraphJobDefinition(
                "seatunnel.apache.org/v1alpha1",
                "FlinkGraphJob",
                new GraphRuntimeDefinition("flink", "1.20.5", "batch"),
                nodes);
    }

    private static GraphNodeDefinition node(String id, Object inputs, String plugin) {
        return new GraphNodeDefinition(id, plugin, "op", inputs, Collections.emptyMap());
    }
}
