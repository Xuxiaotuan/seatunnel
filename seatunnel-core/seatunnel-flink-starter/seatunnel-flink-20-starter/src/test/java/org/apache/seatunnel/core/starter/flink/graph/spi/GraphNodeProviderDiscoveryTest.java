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
import org.apache.seatunnel.core.starter.flink.graph.api.NodeCapability;
import org.apache.seatunnel.core.starter.flink.graph.runtime.GraphNodeExecutor;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

public class GraphNodeProviderDiscoveryTest {

    @Test
    void discoversProviderFromServiceLoader() {
        GraphNodeProviderDiscovery discovery = new GraphNodeProviderDiscovery();

        Map<String, GraphNodeFactory> factories = new LinkedHashMap<>();
        Map<String, GraphNodeExecutor> executors = new LinkedHashMap<>();
        discovery.load(Thread.currentThread().getContextClassLoader(), factories, executors);

        Assertions.assertTrue(factories.containsKey("test_provider:map"));
        Assertions.assertNotNull(executors.get("test_provider:map"));
    }

    @Test
    void rejectsDuplicateProviderKeys() {
        Map<String, GraphNodeFactory> factories = new LinkedHashMap<>();
        Map<String, GraphNodeExecutor> executors = new LinkedHashMap<>();
        GraphNodeProvider provider = new TestGraphNodeProvider();
        GraphNodeProviderDiscovery.register(factories, executors, provider);

        IllegalArgumentException error =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () -> GraphNodeProviderDiscovery.register(factories, executors, provider));

        Assertions.assertTrue(error.getMessage().contains("test_provider:map"));
    }

    public static final class TestGraphNodeProvider implements GraphNodeProvider {
        @Override
        public GraphNodeFactory factory() {
            return new GraphNodeFactory() {
                @Override
                public String factoryIdentifier() {
                    return "test_provider";
                }

                @Override
                public String operation() {
                    return "map";
                }

                @Override
                public NodeCapability capability() {
                    return NodeCapability.builder()
                            .inputCount(0, 0)
                            .produces(NodeCapability.ValueKind.DATA)
                            .supportsBatch(true)
                            .build();
                }
            };
        }

        @Override
        public GraphNodeExecutor executor() {
            return (context, inputs) -> null;
        }
    }
}
