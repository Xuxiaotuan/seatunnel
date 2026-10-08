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

import org.apache.seatunnel.core.starter.flink.graph.api.GraphNodeDefinition;
import org.apache.seatunnel.core.starter.flink.graph.api.InputBinding;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/** A graph node with normalized upstream bindings. */
public final class ResolvedNode {

    private final GraphNodeDefinition definition;
    private final List<InputBinding> inputBindings;
    private final Set<String> downstreamNodeIds = new LinkedHashSet<>();

    public ResolvedNode(GraphNodeDefinition definition) {
        this.definition = definition;
        this.inputBindings =
                Collections.unmodifiableList(new ArrayList<>(definition.inputBindings()));
    }

    public String id() {
        return definition.id();
    }

    public GraphNodeDefinition definition() {
        return definition;
    }

    public List<InputBinding> inputBindings() {
        return inputBindings;
    }

    public Set<String> downstreamNodeIds() {
        return Collections.unmodifiableSet(downstreamNodeIds);
    }

    void addDownstreamNode(String nodeId) {
        downstreamNodeIds.add(nodeId);
    }
}
