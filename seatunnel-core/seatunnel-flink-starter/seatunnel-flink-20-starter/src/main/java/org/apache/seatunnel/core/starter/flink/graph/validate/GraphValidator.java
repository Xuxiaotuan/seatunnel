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

import java.util.Collections;
import java.util.Map;
import java.util.Set;

/** Validates plugin capabilities and value-kind compatibility before execution. */
public final class GraphValidator {

    private static final String API_VERSION = "seatunnel.apache.org/v1alpha1";
    private static final String KIND = "FlinkGraphJob";
    private static final String ENGINE = "flink";
    private static final String MODE = "batch";
    private static final java.util.regex.Pattern FLINK_20 =
            java.util.regex.Pattern.compile("1\\.20(?:\\.\\d+)?");

    public void validate(ResolvedGraph graph, Map<String, GraphNodeFactory> factories) {
        validateEnvelope(graph);
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
            validateExecutionScope(graph, node, capability, factories);
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

    private static void validateExecutionScope(
            ResolvedGraph graph,
            ResolvedNode node,
            NodeCapability capability,
            Map<String, GraphNodeFactory> factories) {
        if (capability.executionScope() == NodeCapability.ExecutionScope.CLIENT_CONTROL) {
            if (capability.acceptedInputKinds().contains(NodeCapability.ValueKind.DATA)) {
                throw new GraphValidationException(
                        "Client-control node " + node.id() + " cannot consume data values");
            }
            if (!Collections.singleton(NodeCapability.ValueKind.CONTROL)
                    .equals(capability.outputKinds())) {
                throw new GraphValidationException(
                        "Client-control node " + node.id() + " must produce only control values");
            }
        } else if (capability.acceptedInputKinds().contains(NodeCapability.ValueKind.COMPLETION)) {
            throw new GraphValidationException(
                    "Flink data node " + node.id() + " cannot consume completion values");
        }

        boolean hasControlDependency = false;
        boolean hasCompletionDependency = false;
        for (org.apache.seatunnel.core.starter.flink.graph.api.InputBinding binding :
                node.inputBindings()) {
            Set<NodeCapability.ValueKind> upstreamKinds =
                    outputKinds(graph.node(binding.upstreamNodeId()), factories);
            hasControlDependency |= upstreamKinds.contains(NodeCapability.ValueKind.CONTROL);
            hasCompletionDependency |= upstreamKinds.contains(NodeCapability.ValueKind.COMPLETION);
        }
        if (hasControlDependency && hasCompletionDependency) {
            throw new GraphValidationException(
                    "Node " + node.id() + " cannot mix control and completion dependencies");
        }
        if (hasCompletionDependency) {
            if (capability.executionScope() != NodeCapability.ExecutionScope.CLIENT_CONTROL) {
                throw new GraphValidationException(
                        "Completion dependency "
                                + node.id()
                                + " must be handled by a client-control node");
            }
            if (!node.downstreamNodeIds().isEmpty()) {
                throw new GraphValidationException(
                        "Completion action " + node.id() + " must be terminal");
            }
        }
    }

    private static void validateEnvelope(ResolvedGraph graph) {
        if (!API_VERSION.equals(graph.definition().apiVersion())) {
            throw new GraphValidationException(
                    "Unsupported graph apiVersion: " + graph.definition().apiVersion());
        }
        if (!KIND.equals(graph.definition().kind())) {
            throw new GraphValidationException(
                    "Unsupported graph kind: " + graph.definition().kind());
        }
        if (!ENGINE.equalsIgnoreCase(graph.definition().runtime().engine())) {
            throw new GraphValidationException(
                    "Graph runtime.engine must be flink but was "
                            + graph.definition().runtime().engine());
        }
        if (!FLINK_20.matcher(graph.definition().runtime().version()).matches()) {
            throw new GraphValidationException(
                    "Graph runtime.version must be Flink 1.20.x but was "
                            + graph.definition().runtime().version());
        }
        if (!MODE.equalsIgnoreCase(graph.definition().runtime().mode())) {
            throw new GraphValidationException(
                    "Graph runtime.mode must be batch but was "
                            + graph.definition().runtime().mode());
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
