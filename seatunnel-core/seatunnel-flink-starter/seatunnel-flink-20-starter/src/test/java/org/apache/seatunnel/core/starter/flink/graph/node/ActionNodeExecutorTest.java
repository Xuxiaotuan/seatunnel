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

package org.apache.seatunnel.core.starter.flink.graph.node;

import org.apache.seatunnel.core.starter.flink.graph.api.GraphNodeDefinition;
import org.apache.seatunnel.core.starter.flink.graph.resolve.ResolvedNode;
import org.apache.seatunnel.core.starter.flink.graph.runtime.CompletionRuntimeValue;
import org.apache.seatunnel.core.starter.flink.graph.runtime.ControlRuntimeValue;
import org.apache.seatunnel.core.starter.flink.graph.runtime.NodeExecutionContext;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

class ActionNodeExecutorTest {

    @Test
    void rejectsReadinessActionWithoutAQuery() {
        GraphNodeDefinition definition =
                new GraphNodeDefinition(
                        "prepare",
                        "jdbc_action",
                        "check",
                        Collections.emptyList(),
                        Collections.emptyMap());
        ResolvedNode node = new ResolvedNode(definition);

        IllegalArgumentException error =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                new JdbcActionExecutor()
                                        .execute(context(node), Collections.emptyMap()));
        Assertions.assertTrue(error.getMessage().contains("config.url"));
    }

    @Test
    void masksPasswordWhenJdbcConnectionFails() {
        Map<String, Object> config = new LinkedHashMap<>();
        config.put("url", "jdbc:unknown:graph");
        config.put("query", "SELECT 1");
        config.put("password", "secret-password");
        GraphNodeDefinition definition =
                new GraphNodeDefinition(
                        "recon", "jdbc_action", "check", Collections.emptyList(), config);
        ResolvedNode node = new ResolvedNode(definition);

        IllegalStateException error =
                Assertions.assertThrows(
                        IllegalStateException.class,
                        () ->
                                new JdbcActionExecutor()
                                        .execute(context(node), Collections.emptyMap()));
        Assertions.assertFalse(error.getMessage().contains("secret-password"));
    }

    @Test
    void acceptsCompletionInputForReconciliation() {
        GraphNodeDefinition definition =
                new GraphNodeDefinition(
                        "recon",
                        "jdbc_action",
                        "check",
                        Collections.emptyList(),
                        Collections.emptyMap());
        ResolvedNode node = new ResolvedNode(definition);
        Map<String, org.apache.seatunnel.core.starter.flink.graph.runtime.GraphRuntimeValue>
                inputs = Collections.singletonMap("output", new CompletionRuntimeValue("output"));

        IllegalArgumentException error =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () -> new JdbcActionExecutor().execute(context(node), inputs));

        Assertions.assertTrue(error.getMessage().contains("config.url"));
    }

    @Test
    void acceptsMultipleCompletionInputsForReconciliation() {
        GraphNodeDefinition definition =
                new GraphNodeDefinition(
                        "recon",
                        "jdbc_action",
                        "check",
                        Collections.emptyList(),
                        Collections.emptyMap());
        ResolvedNode node = new ResolvedNode(definition);
        Map<String, org.apache.seatunnel.core.starter.flink.graph.runtime.GraphRuntimeValue>
                inputs = new LinkedHashMap<>();
        inputs.put("orders_sink", new CompletionRuntimeValue("orders_sink"));
        inputs.put("customers_sink", new CompletionRuntimeValue("customers_sink"));

        IllegalArgumentException error =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () -> new JdbcActionExecutor().execute(context(node), inputs));

        Assertions.assertTrue(error.getMessage().contains("config.url"));
    }

    @Test
    void acceptsControlInputForChainedPreparationAction() {
        GraphNodeDefinition definition =
                new GraphNodeDefinition(
                        "prepare_second",
                        "jdbc_action",
                        "check",
                        Collections.emptyList(),
                        Collections.emptyMap());
        ResolvedNode node = new ResolvedNode(definition);
        Map<String, org.apache.seatunnel.core.starter.flink.graph.runtime.GraphRuntimeValue>
                inputs = Collections.singletonMap("prepare", new ControlRuntimeValue("prepare"));

        IllegalArgumentException error =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () -> new JdbcActionExecutor().execute(context(node), inputs));

        Assertions.assertTrue(error.getMessage().contains("config.url"));
    }

    @Test
    void rejectsMixedControlAndCompletionInputs() {
        GraphNodeDefinition definition =
                new GraphNodeDefinition(
                        "recon",
                        "jdbc_action",
                        "check",
                        Collections.emptyList(),
                        Collections.emptyMap());
        ResolvedNode node = new ResolvedNode(definition);
        Map<String, org.apache.seatunnel.core.starter.flink.graph.runtime.GraphRuntimeValue>
                inputs = new LinkedHashMap<>();
        inputs.put("prepare", new ControlRuntimeValue("prepare"));
        inputs.put("sink", new CompletionRuntimeValue("sink"));

        IllegalArgumentException error =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () -> new JdbcActionExecutor().execute(context(node), inputs));

        Assertions.assertTrue(error.getMessage().contains("same lifecycle phase"));
    }

    private static NodeExecutionContext context(ResolvedNode node) {
        return new NodeExecutionContext(node, null, null, Collections.emptyMap());
    }
}
