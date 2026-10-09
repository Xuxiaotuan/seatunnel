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
import org.apache.seatunnel.core.starter.flink.graph.api.InputBinding;
import org.apache.seatunnel.core.starter.flink.graph.validate.GraphValidationException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Converts user-facing input references into a navigable dependency graph. */
public final class InputDependencyResolver {

    public ResolvedGraph resolve(GraphJobDefinition definition) {
        Map<String, ResolvedNode> resolved = new LinkedHashMap<>();
        for (Map.Entry<String, GraphNodeDefinition> entry : definition.nodes().entrySet()) {
            if (!entry.getKey().equals(entry.getValue().id())) {
                throw new GraphValidationException(
                        "Node map key "
                                + entry.getKey()
                                + " does not match node ID "
                                + entry.getValue().id());
            }
            if (resolved.put(entry.getKey(), new ResolvedNode(entry.getValue())) != null) {
                throw new GraphValidationException("Duplicate node ID: " + entry.getKey());
            }
        }

        for (ResolvedNode node : resolved.values()) {
            Set<String> aliases = new HashSet<>();
            for (InputBinding binding : node.inputBindings()) {
                if (!aliases.add(binding.alias())) {
                    throw new GraphValidationException(
                            "Node "
                                    + node.id()
                                    + " has a duplicate input alias "
                                    + binding.alias());
                }
                ResolvedNode upstream = resolved.get(binding.upstreamNodeId());
                if (upstream == null) {
                    throw new GraphValidationException(
                            "Node "
                                    + node.id()
                                    + " references missing upstream node "
                                    + binding.upstreamNodeId());
                }
                upstream.addDownstreamNode(node.id());
            }
        }

        List<String> roots = new ArrayList<>();
        List<String> terminals = new ArrayList<>();
        for (ResolvedNode node : resolved.values()) {
            if (node.inputBindings().isEmpty()) {
                roots.add(node.id());
            }
            if (node.downstreamNodeIds().isEmpty()) {
                terminals.add(node.id());
            }
        }
        return new ResolvedGraph(
                definition,
                resolved,
                Collections.unmodifiableList(roots),
                Collections.unmodifiableList(terminals));
    }
}
