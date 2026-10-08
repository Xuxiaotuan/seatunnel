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

import org.apache.seatunnel.core.starter.flink.graph.api.GraphNodeFactory;
import org.apache.seatunnel.core.starter.flink.graph.api.NodeCapability;
import org.apache.seatunnel.core.starter.flink.graph.resolve.ResolvedGraph;
import org.apache.seatunnel.core.starter.flink.graph.resolve.ResolvedNode;

import java.util.Map;

/** Validates plugin capabilities and value-kind compatibility before execution. */
public final class GraphValidator {

    public void validate(ResolvedGraph graph, Map<String, GraphNodeFactory> factories) {
        if (factories == null) {
            throw new GraphValidationException("Graph factories must not be null");
        }
        for (ResolvedNode node : graph.nodes().values()) {
            GraphNodeFactory factory = factoryFor(node, factories);
            if (factory == null) {
                throw new GraphValidationException(
                        "Node " + node.id() + " uses unknown plugin " + node.definition().plugin());
            }
            if (!factory.operation().equals(node.definition().operation())) {
                throw new GraphValidationException(
                        "Node "
                                + node.id()
                                + " operation "
                                + node.definition().operation()
                                + " is not supported by plugin "
                                + node.definition().plugin());
            }
            NodeCapability capability = factory.capability();
            int inputCount = node.inputBindings().size();
            if (inputCount < capability.minimumInputCount()
                    || inputCount > capability.maximumInputCount()) {
                throw new GraphValidationException(
                        "Node "
                                + node.id()
                                + " expects between "
                                + capability.minimumInputCount()
                                + " and "
                                + capability.maximumInputCount()
                                + " inputs but received "
                                + inputCount);
            }
            if ("batch".equalsIgnoreCase(graph.definition().runtime().mode())
                    && !capability.supportsBatch()) {
                throw new GraphValidationException(
                        "Node " + node.id() + " does not support Batch mode");
            }
            for (org.apache.seatunnel.core.starter.flink.graph.api.InputBinding binding :
                    node.inputBindings()) {
                ResolvedNode upstream = graph.node(binding.upstreamNodeId());
                boolean compatible = false;
                for (NodeCapability.ValueKind outputKind : outputKinds(upstream, factories)) {
                    if (capability.acceptedInputKinds().contains(outputKind)) {
                        compatible = true;
                        break;
                    }
                }
                if (!compatible) {
                    throw new GraphValidationException(
                            "Node "
                                    + node.id()
                                    + " cannot consume the "
                                    + outputDescription(upstream, factories)
                                    + " output of node "
                                    + upstream.id());
                }
            }
        }
    }

    private static java.util.Set<NodeCapability.ValueKind> outputKinds(
            ResolvedNode node, Map<String, GraphNodeFactory> factories) {
        return factoryFor(node, factories).capability().outputKinds();
    }

    private static String outputDescription(
            ResolvedNode node, Map<String, GraphNodeFactory> factories) {
        if (outputKinds(node, factories).contains(NodeCapability.ValueKind.CONTROL)) {
            return "control";
        }
        if (outputKinds(node, factories).contains(NodeCapability.ValueKind.COMPLETION)) {
            return "completion";
        }
        return "data";
    }

    private static GraphNodeFactory factoryFor(
            ResolvedNode node, Map<String, GraphNodeFactory> factories) {
        GraphNodeFactory factory =
                factories.get(node.definition().plugin() + ":" + node.definition().operation());
        if (factory == null) {
            factory = factories.get(node.definition().plugin());
        }
        return factory;
    }
}
