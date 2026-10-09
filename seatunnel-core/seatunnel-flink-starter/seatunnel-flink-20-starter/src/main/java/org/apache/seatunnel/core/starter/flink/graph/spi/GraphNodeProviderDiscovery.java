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

package org.apache.seatunnel.core.starter.flink.graph.spi;

import org.apache.seatunnel.core.starter.flink.graph.api.GraphNodeFactory;
import org.apache.seatunnel.core.starter.flink.graph.runtime.GraphNodeExecutor;

import java.util.Map;
import java.util.ServiceLoader;

/** Discovers Flink 1.20 graph node providers without coupling the graph API to Flink. */
public final class GraphNodeProviderDiscovery {

    public void load(
            ClassLoader classLoader,
            Map<String, GraphNodeFactory> factories,
            Map<String, GraphNodeExecutor> executors) {
        for (GraphNodeProvider provider :
                ServiceLoader.load(GraphNodeProvider.class, classLoader)) {
            register(factories, executors, provider);
        }
    }

    public static void register(
            Map<String, GraphNodeFactory> factories,
            Map<String, GraphNodeExecutor> executors,
            GraphNodeProvider provider) {
        if (provider == null) {
            throw new IllegalArgumentException(
                    "Graph node provider, factory, and executor are required");
        }
        GraphNodeFactory factory = provider.factory();
        GraphNodeExecutor executor = provider.executor();
        if (factory == null || executor == null) {
            throw new IllegalArgumentException(
                    "Graph node provider, factory, and executor are required");
        }
        String key = factory.factoryIdentifier() + ":" + factory.operation();
        if (factories.containsKey(key) || executors.containsKey(key)) {
            throw new IllegalArgumentException("Duplicate graph node provider: " + key);
        }
        factories.put(key, factory);
        executors.put(key, executor);
    }
}
