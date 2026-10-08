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
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** A named input of a graph node. */
public final class InputBinding {

    private final String alias;
    private final String upstreamNodeId;

    public InputBinding(String alias, String upstreamNodeId) {
        this.alias = requireText(alias, "alias");
        this.upstreamNodeId = requireText(upstreamNodeId, "upstreamNodeId");
    }

    public String alias() {
        return alias;
    }

    public String upstreamNodeId() {
        return upstreamNodeId;
    }

    /**
     * Normalizes the two supported YAML shapes: a list of node IDs and a map of input aliases to
     * node IDs.
     */
    public static List<InputBinding> normalize(Object rawInputs) {
        if (rawInputs == null) {
            return Collections.emptyList();
        }
        if (rawInputs instanceof Iterable) {
            List<InputBinding> result = new ArrayList<>();
            for (Object value : (Iterable<?>) rawInputs) {
                String upstream = asText(value, "list input");
                result.add(new InputBinding(upstream, upstream));
            }
            return Collections.unmodifiableList(result);
        }
        if (rawInputs instanceof Map) {
            List<InputBinding> result = new ArrayList<>();
            for (Map.Entry<?, ?> entry : ((Map<?, ?>) rawInputs).entrySet()) {
                result.add(
                        new InputBinding(
                                asText(entry.getKey(), "input alias"),
                                asText(entry.getValue(), "upstream node ID")));
            }
            return Collections.unmodifiableList(result);
        }
        throw new IllegalArgumentException("inputs must be a list or mapping");
    }

    private static String asText(Object value, String description) {
        if (!(value instanceof String)) {
            throw new IllegalArgumentException(description + " must be a string");
        }
        return (String) value;
    }

    private static String requireText(String value, String name) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(name + " must not be empty");
        }
        return value;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof InputBinding)) {
            return false;
        }
        InputBinding that = (InputBinding) other;
        return alias.equals(that.alias) && upstreamNodeId.equals(that.upstreamNodeId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(alias, upstreamNodeId);
    }

    @Override
    public String toString() {
        return alias + " <- " + upstreamNodeId;
    }
}
