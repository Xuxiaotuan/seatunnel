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

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Immutable user-defined graph node. */
public final class GraphNodeDefinition {

    private final String id;
    private final String plugin;
    private final String operation;
    private final Object rawInputs;
    private final Map<String, Object> config;

    public GraphNodeDefinition(
            String id,
            String plugin,
            String operation,
            Object rawInputs,
            Map<String, Object> config) {
        this.id = requireText(id, "id");
        this.plugin = requireText(plugin, "plugin");
        this.operation = requireText(operation, "operation");
        this.rawInputs = immutableValue(rawInputs);
        this.config = immutableMap(config);
    }

    public String id() {
        return id;
    }

    public String plugin() {
        return plugin;
    }

    public String operation() {
        return operation;
    }

    public Object rawInputs() {
        return rawInputs;
    }

    public Map<String, Object> config() {
        return config;
    }

    public List<InputBinding> inputBindings() {
        return InputBinding.normalize(rawInputs);
    }

    private static Map<String, Object> immutableMap(Map<String, Object> values) {
        if (values == null || values.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<String, Object> copy = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : values.entrySet()) {
            copy.put(entry.getKey(), immutableValue(entry.getValue()));
        }
        return Collections.unmodifiableMap(copy);
    }

    @SuppressWarnings("unchecked")
    private static Object immutableValue(Object value) {
        if (value instanceof Map) {
            Map<Object, Object> copy = new LinkedHashMap<>();
            for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
                copy.put(entry.getKey(), immutableValue(entry.getValue()));
            }
            return Collections.unmodifiableMap(copy);
        }
        if (value instanceof Iterable) {
            List<Object> copy = new ArrayList<>();
            for (Object item : (Iterable<?>) value) {
                copy.add(immutableValue(item));
            }
            return Collections.unmodifiableList(copy);
        }
        return value;
    }

    private static String requireText(String value, String name) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(name + " must not be empty");
        }
        return value;
    }
}
