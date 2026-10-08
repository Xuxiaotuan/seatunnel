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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

class GraphDefinitionTest {

    @Test
    void listInputsUseTheUpstreamIdAsDeterministicAlias() {
        GraphNodeDefinition node =
                new GraphNodeDefinition(
                        "normalize",
                        "Python",
                        "map",
                        Arrays.asList("orders"),
                        Collections.emptyMap());

        Assertions.assertEquals(
                Collections.singletonList(new InputBinding("orders", "orders")),
                node.inputBindings());
    }

    @Test
    void mappingInputsPreserveTheSqlAlias() {
        Map<String, String> inputs = new LinkedHashMap<>();
        inputs.put("customers", "customer_source");
        GraphNodeDefinition node =
                new GraphNodeDefinition("join", "sql", "query", inputs, Collections.emptyMap());

        Assertions.assertEquals(
                Collections.singletonList(new InputBinding("customers", "customer_source")),
                node.inputBindings());
    }

    @Test
    void nodeIdentityAndCollectionsAreImmutable() {
        Map<String, Object> config = new LinkedHashMap<>();
        config.put("query", "SELECT 1");
        GraphNodeDefinition node =
                new GraphNodeDefinition("join", "sql", "query", Collections.emptyList(), config);

        config.put("query", "mutated");
        Assertions.assertEquals("SELECT 1", node.config().get("query"));
        Assertions.assertThrows(
                UnsupportedOperationException.class, () -> node.config().put("new", "value"));
    }
}
