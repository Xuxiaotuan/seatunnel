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

import org.apache.seatunnel.core.starter.flink.graph.resolve.ResolvedNode;
import org.apache.seatunnel.core.starter.flink.graph.runtime.CompletionRuntimeValue;
import org.apache.seatunnel.core.starter.flink.graph.runtime.ControlRuntimeValue;
import org.apache.seatunnel.core.starter.flink.graph.runtime.GraphNodeExecutor;
import org.apache.seatunnel.core.starter.flink.graph.runtime.GraphRuntimeValue;
import org.apache.seatunnel.core.starter.flink.graph.runtime.NodeExecutionContext;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Map;
import java.util.Objects;

/** Executes a bounded JDBC readiness or reconciliation action without logging credentials. */
public final class JdbcActionExecutor implements GraphNodeExecutor {

    @Override
    public GraphRuntimeValue execute(
            NodeExecutionContext context, Map<String, GraphRuntimeValue> inputValues)
            throws Exception {
        ResolvedNode node = context.node();
        org.apache.seatunnel.core.starter.flink.graph.api.NodeCapability.ValueKind inputKind = null;
        for (GraphRuntimeValue input : inputValues.values()) {
            org.apache.seatunnel.core.starter.flink.graph.api.NodeCapability.ValueKind currentKind;
            if (input instanceof CompletionRuntimeValue) {
                currentKind =
                        org.apache.seatunnel.core.starter.flink.graph.api.NodeCapability.ValueKind
                                .COMPLETION;
            } else if (input instanceof ControlRuntimeValue) {
                currentKind =
                        org.apache.seatunnel.core.starter.flink.graph.api.NodeCapability.ValueKind
                                .CONTROL;
            } else {
                throw new IllegalArgumentException(
                        "JDBC action " + node.id() + " only accepts control or completion inputs");
            }
            if (inputKind != null && !Objects.equals(inputKind, currentKind)) {
                throw new IllegalArgumentException(
                        "JDBC action "
                                + node.id()
                                + " requires inputs from the same lifecycle phase");
            }
            inputKind = currentKind;
        }
        String url = required(node, "url");
        String query = required(node, "query");
        String user = optional(node, "user");
        String password = optional(node, "password");
        try (Connection connection = DriverManager.getConnection(url, user, password);
                Statement statement = connection.createStatement()) {
            boolean hasResult = statement.execute(query);
            if (hasResult) {
                try (ResultSet ignored = statement.getResultSet()) {
                    while (ignored.next()) {
                        // A readiness/reconciliation action only needs successful execution.
                    }
                }
            }
            return new ControlRuntimeValue(node.id());
        } catch (Exception error) {
            throw new IllegalStateException(
                    "JDBC action " + node.id() + " failed for configured URL and query", error);
        }
    }

    private static String required(ResolvedNode node, String key) {
        String value = optional(node, key);
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(
                    "JDBC action " + node.id() + " requires config." + key);
        }
        return value;
    }

    private static String optional(ResolvedNode node, String key) {
        Object value = node.definition().config().get(key);
        return value == null ? null : String.valueOf(value);
    }
}
