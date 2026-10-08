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

package org.apache.seatunnel.core.starter.flink.graph.api;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/** Immutable graph job definition. */
public final class GraphJobDefinition {

    private final String apiVersion;
    private final String kind;
    private final GraphRuntimeDefinition runtime;
    private final Map<String, GraphNodeDefinition> nodes;

    public GraphJobDefinition(
            String apiVersion,
            String kind,
            GraphRuntimeDefinition runtime,
            Map<String, GraphNodeDefinition> nodes) {
        this.apiVersion = requireText(apiVersion, "apiVersion");
        this.kind = requireText(kind, "kind");
        if (runtime == null) {
            throw new IllegalArgumentException("runtime must not be null");
        }
        this.runtime = runtime;
        if (nodes == null || nodes.isEmpty()) {
            throw new IllegalArgumentException("nodes must not be empty");
        }
        Map<String, GraphNodeDefinition> copy = new LinkedHashMap<>();
        for (Map.Entry<String, GraphNodeDefinition> entry : nodes.entrySet()) {
            if (entry.getValue() == null) {
                throw new IllegalArgumentException("node " + entry.getKey() + " must not be null");
            }
            if (!entry.getKey().equals(entry.getValue().id())) {
                throw new IllegalArgumentException(
                        "node map key "
                                + entry.getKey()
                                + " does not match node ID "
                                + entry.getValue().id());
            }
            if (copy.put(entry.getKey(), entry.getValue()) != null) {
                throw new IllegalArgumentException("duplicate node ID: " + entry.getKey());
            }
        }
        this.nodes = Collections.unmodifiableMap(copy);
    }

    public String apiVersion() {
        return apiVersion;
    }

    public String kind() {
        return kind;
    }

    public GraphRuntimeDefinition runtime() {
        return runtime;
    }

    public Map<String, GraphNodeDefinition> nodes() {
        return nodes;
    }

    private static String requireText(String value, String name) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(name + " must not be empty");
        }
        return value;
    }
}
