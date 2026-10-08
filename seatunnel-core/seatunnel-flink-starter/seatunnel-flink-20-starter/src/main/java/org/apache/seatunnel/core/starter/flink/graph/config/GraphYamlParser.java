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

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.dataformat.yaml.YAMLMapper;

import org.apache.seatunnel.core.starter.flink.graph.api.GraphJobDefinition;
import org.apache.seatunnel.core.starter.flink.graph.api.GraphNodeDefinition;
import org.apache.seatunnel.core.starter.flink.graph.api.GraphRuntimeDefinition;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/** Loads and strictly validates the graph envelope before plugin validation. */
public final class GraphYamlParser {

    private static final YAMLMapper YAML =
            YAMLMapper.builder().enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES).build();

    private static final Set<String> GRAPH_FIELDS =
            Collections.unmodifiableSet(
                    new LinkedHashSet<>(Arrays.asList("apiVersion", "kind", "runtime", "nodes")));
    private static final Set<String> RUNTIME_FIELDS =
            Collections.unmodifiableSet(
                    new LinkedHashSet<>(Arrays.asList("engine", "version", "mode", "parallelism")));
    private static final Set<String> NODE_FIELDS =
            Collections.unmodifiableSet(
                    new LinkedHashSet<>(Arrays.asList("plugin", "operation", "inputs", "config")));

    public GraphJobDefinition parse(Path path, Map<String, String> variables) throws IOException {
        if (path == null || !Files.isRegularFile(path)) {
            throw new IllegalArgumentException("Graph YAML is not a readable file: " + path);
        }
        Map<String, Object> document = readMap(path);
        rejectUnknown(document, GRAPH_FIELDS, "graph");

        String apiVersion = requiredText(document, "apiVersion", "graph");
        String kind = requiredText(document, "kind", "graph");
        Map<String, Object> runtime = requiredMap(document, "runtime", "graph");
        rejectUnknown(runtime, RUNTIME_FIELDS, "runtime");
        GraphRuntimeDefinition runtimeDefinition =
                new GraphRuntimeDefinition(
                        requiredText(runtime, "engine", "runtime"),
                        requiredText(runtime, "version", "runtime"),
                        requiredText(runtime, "mode", "runtime"),
                        optionalPositiveInt(runtime, "parallelism", "runtime"));

        Map<String, Object> rawNodes = requiredMap(document, "nodes", "graph");
        if (rawNodes.isEmpty()) {
            throw new IllegalArgumentException("graph.nodes must not be empty");
        }
        Map<String, GraphNodeDefinition> nodes = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : rawNodes.entrySet()) {
            if (entry.getValue() == null || !(entry.getValue() instanceof Map)) {
                throw new IllegalArgumentException("node " + entry.getKey() + " must be a mapping");
            }
            Map<String, Object> rawNode = castMap(entry.getValue(), "node " + entry.getKey());
            rejectUnknown(rawNode, NODE_FIELDS, "node " + entry.getKey());
            String plugin = requiredText(rawNode, "plugin", "node " + entry.getKey());
            String operation = requiredText(rawNode, "operation", "node " + entry.getKey());
            Object inputs = rawNode.get("inputs");
            Object configValue = GraphVariableResolver.resolve(rawNode.get("config"), variables);
            Map<String, Object> config =
                    configValue == null
                            ? Collections.emptyMap()
                            : castMap(configValue, "node " + entry.getKey() + ".config");
            nodes.put(
                    entry.getKey(),
                    new GraphNodeDefinition(entry.getKey(), plugin, operation, inputs, config));
        }
        return new GraphJobDefinition(apiVersion, kind, runtimeDefinition, nodes);
    }

    private static Map<String, Object> readMap(Path path) throws IOException {
        Object value = YAML.readValue(path.toFile(), Object.class);
        return castMap(value, "graph document");
    }

    private static void rejectUnknown(
            Map<String, Object> values, Set<String> allowed, String scope) {
        for (String field : values.keySet()) {
            if (!allowed.contains(field)) {
                throw new IllegalArgumentException("Unknown " + scope + " field: " + field);
            }
        }
    }

    private static String requiredText(Map<String, Object> values, String field, String scope) {
        Object value = values.get(field);
        if (!(value instanceof String) || ((String) value).trim().isEmpty()) {
            throw new IllegalArgumentException(scope + "." + field + " must be a non-empty string");
        }
        return (String) value;
    }

    private static Integer optionalPositiveInt(
            Map<String, Object> values, String field, String scope) {
        if (!values.containsKey(field)) {
            return null;
        }
        Object value = values.get(field);
        if (!(value instanceof Number) || ((Number) value).intValue() < 1) {
            throw new IllegalArgumentException(scope + "." + field + " must be a positive integer");
        }
        return ((Number) value).intValue();
    }

    private static Map<String, Object> requiredMap(
            Map<String, Object> values, String field, String scope) {
        if (!values.containsKey(field)) {
            throw new IllegalArgumentException(scope + "." + field + " is required");
        }
        return castMap(values.get(field), scope + "." + field);
    }

    private static Map<String, Object> castMap(Object value, String scope) {
        if (!(value instanceof Map)) {
            throw new IllegalArgumentException(scope + " must be a mapping");
        }
        Map<String, Object> result = new LinkedHashMap<>();
        for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
            if (!(entry.getKey() instanceof String)) {
                throw new IllegalArgumentException(scope + " keys must be strings");
            }
            result.put((String) entry.getKey(), entry.getValue());
        }
        return result;
    }
}
