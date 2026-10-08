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

package org.apache.seatunnel.core.starter.flink.graph.plan;

import org.apache.seatunnel.core.starter.flink.graph.resolve.ResolvedGraph;
import org.apache.seatunnel.core.starter.flink.graph.resolve.ResolvedNode;
import org.apache.seatunnel.core.starter.flink.graph.validate.GraphValidationException;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

/** Produces a stable topological execution order. */
public final class TopologicalPlanner {

    public List<ResolvedNode> order(ResolvedGraph graph) {
        Map<String, Integer> insertionOrder = new HashMap<>();
        int index = 0;
        for (String id : graph.nodes().keySet()) {
            insertionOrder.put(id, index++);
        }

        Map<String, Integer> indegree = new HashMap<>();
        for (ResolvedNode node : graph.nodes().values()) {
            indegree.put(node.id(), node.inputBindings().size());
        }
        PriorityQueue<String> ready =
                new PriorityQueue<>(Comparator.comparingInt(insertionOrder::get));
        for (Map.Entry<String, Integer> entry : indegree.entrySet()) {
            if (entry.getValue() == 0) {
                ready.add(entry.getKey());
            }
        }

        List<ResolvedNode> ordered = new ArrayList<>();
        while (!ready.isEmpty()) {
            String id = ready.remove();
            ResolvedNode node = graph.node(id);
            ordered.add(node);
            for (String downstream : node.downstreamNodeIds()) {
                int next = indegree.get(downstream) - 1;
                indegree.put(downstream, next);
                if (next == 0) {
                    ready.add(downstream);
                }
            }
        }
        if (ordered.size() != graph.nodes().size()) {
            List<String> cyclic = new ArrayList<>();
            for (Map.Entry<String, Integer> entry : indegree.entrySet()) {
                if (entry.getValue() > 0) {
                    cyclic.add(entry.getKey());
                }
            }
            cyclic.sort(Comparator.comparingInt(insertionOrder::get));
            throw new GraphValidationException("Graph contains a cycle involving: " + cyclic);
        }
        return ordered;
    }
}
