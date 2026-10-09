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

package org.apache.seatunnel.core.starter.flink.graph.resolve;

import org.apache.seatunnel.core.starter.flink.graph.api.GraphJobDefinition;
import org.apache.seatunnel.core.starter.flink.graph.api.GraphNodeDefinition;
import org.apache.seatunnel.core.starter.flink.graph.api.GraphRuntimeDefinition;
import org.apache.seatunnel.core.starter.flink.graph.api.InputBinding;
import org.apache.seatunnel.core.starter.flink.graph.plan.TopologicalPlanner;
import org.apache.seatunnel.core.starter.flink.graph.validate.GraphValidationException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

class InputDependencyResolverTest {

    private final InputDependencyResolver resolver = new InputDependencyResolver();

    @Test
    void resolvesListAndMappingInputsAndDiscoversRootsAndTerminals() {
        Map<String, GraphNodeDefinition> nodes = new LinkedHashMap<>();
        nodes.put("orders", node("orders", Collections.emptyList()));
        nodes.put("customers", node("customers", Collections.emptyList()));
        nodes.put("normalize", node("normalize", Collections.singletonList("orders")));
        Map<String, String> inputs = new LinkedHashMap<>();
        inputs.put("orders", "normalize");
        inputs.put("customers", "customers");
        nodes.put("join", node("join", inputs));

        ResolvedGraph graph = resolver.resolve(job(nodes));

        Assertions.assertEquals(Arrays.asList("orders", "customers"), graph.rootNodeIds());
        Assertions.assertEquals(Collections.singletonList("join"), graph.terminalNodeIds());
        Assertions.assertEquals(
                Arrays.asList(
                        new InputBinding("orders", "normalize"),
                        new InputBinding("customers", "customers")),
                graph.node("join").inputBindings());
        Assertions.assertEquals(
                Collections.singleton("join"), graph.node("normalize").downstreamNodeIds());
    }

    @Test
    void rejectsMissingUpstreamReferencesBeforePlanning() {
        Map<String, GraphNodeDefinition> nodes = new LinkedHashMap<>();
        nodes.put("sink", node("sink", Collections.singletonList("missing")));

        GraphValidationException error =
                Assertions.assertThrows(
                        GraphValidationException.class, () -> resolver.resolve(job(nodes)));

        Assertions.assertTrue(error.getMessage().contains("missing"));
    }

    @Test
    void rejectsSelfAndMultiNodeCycles() {
        Map<String, GraphNodeDefinition> self = new LinkedHashMap<>();
        self.put("a", node("a", Collections.singletonList("a")));
        Assertions.assertThrows(
                GraphValidationException.class,
                () -> new TopologicalPlanner().order(resolver.resolve(job(self))));

        Map<String, GraphNodeDefinition> cycle = new LinkedHashMap<>();
        cycle.put("a", node("a", Collections.singletonList("b")));
        cycle.put("b", node("b", Collections.singletonList("a")));
        Assertions.assertThrows(
                GraphValidationException.class,
                () -> new TopologicalPlanner().order(resolver.resolve(job(cycle))));
    }

    @Test
    void producesStableTopologicalOrderForFanOut() {
        Map<String, GraphNodeDefinition> nodes = new LinkedHashMap<>();
        nodes.put("source", node("source", Collections.emptyList()));
        nodes.put("left", node("left", Collections.singletonList("source")));
        nodes.put("right", node("right", Collections.singletonList("source")));
        nodes.put("sink", node("sink", Arrays.asList("left", "right")));
        ResolvedGraph graph = resolver.resolve(job(nodes));

        List<String> first = ids(new TopologicalPlanner().order(graph));
        List<String> second = ids(new TopologicalPlanner().order(graph));

        Assertions.assertEquals(Arrays.asList("source", "left", "right", "sink"), first);
        Assertions.assertEquals(first, second);
    }

    @Test
    void rejectsDuplicateListInputs() {
        Map<String, GraphNodeDefinition> nodes = new LinkedHashMap<>();
        nodes.put("source", node("source", Collections.emptyList()));
        nodes.put("join", node("join", Arrays.asList("source", "source")));

        GraphValidationException error =
                Assertions.assertThrows(
                        GraphValidationException.class, () -> resolver.resolve(job(nodes)));

        Assertions.assertTrue(error.getMessage().contains("duplicate input"));
    }

    private static List<String> ids(List<ResolvedNode> nodes) {
        java.util.ArrayList<String> result = new java.util.ArrayList<>();
        for (ResolvedNode node : nodes) {
            result.add(node.id());
        }
        return result;
    }

    private static GraphJobDefinition job(Map<String, GraphNodeDefinition> nodes) {
        return new GraphJobDefinition(
                "seatunnel.apache.org/v1alpha1",
                "FlinkGraphJob",
                new GraphRuntimeDefinition("flink", "1.20.5", "batch"),
                nodes);
    }

    private static GraphNodeDefinition node(String id, Object inputs) {
        return new GraphNodeDefinition(id, "test", "op", inputs, Collections.emptyMap());
    }
}
