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

package org.apache.seatunnel.core.starter.flink.graph.config;

import org.apache.seatunnel.core.starter.flink.graph.api.GraphJobDefinition;
import org.apache.seatunnel.core.starter.flink.graph.api.GraphNodeDefinition;
import org.apache.seatunnel.core.starter.flink.graph.api.InputBinding;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;

class GraphYamlParserTest {

    private final GraphYamlParser parser = new GraphYamlParser();

    @Test
    void parsesCanonicalGraphAndPreservesPluginConfig() throws Exception {
        GraphJobDefinition graph = parser.parse(resource("canonical.yaml"), Collections.emptyMap());

        Assertions.assertEquals("flink", graph.runtime().engine());
        Assertions.assertEquals(1, graph.runtime().parallelism());
        Assertions.assertEquals(4, graph.nodes().size());
        GraphNodeDefinition join = graph.nodes().get("enrich");
        Assertions.assertEquals(
                java.util.Arrays.asList(
                        new InputBinding("orders", "normalize"),
                        new InputBinding("customers", "customers")),
                join.inputBindings());
        Assertions.assertEquals("SELECT * FROM orders", join.config().get("query"));
        Assertions.assertEquals("kept", join.config().get("plugin_specific_option"));
    }

    @Test
    void substitutesVariablesBeforePluginValidation() throws Exception {
        GraphJobDefinition graph =
                parser.parse(
                        resource("variables.yaml"),
                        Collections.singletonMap("PG_HOST", "100.82.226.63"));

        Assertions.assertEquals(
                "jdbc:postgresql://100.82.226.63:30660/xxt",
                graph.nodes().get("source").config().get("url"));
    }

    @Test
    void rejectsUnknownGraphFields() {
        IllegalArgumentException error =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                parser.parse(
                                        resource("unknown_graph_field.yaml"),
                                        Collections.emptyMap()));

        Assertions.assertTrue(error.getMessage().contains("unexpected"));
    }

    @Test
    void rejectsMissingNodes() {
        IllegalArgumentException error =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () -> parser.parse(resource("missing_nodes.yaml"), Collections.emptyMap()));

        Assertions.assertTrue(error.getMessage().contains("nodes"));
    }

    private static Path resource(String name) {
        try {
            return Paths.get(GraphYamlParserTest.class.getResource("/graph/" + name).toURI());
        } catch (URISyntaxException | NullPointerException e) {
            throw new IllegalStateException("Missing graph parser test resource: " + name, e);
        }
    }
}
