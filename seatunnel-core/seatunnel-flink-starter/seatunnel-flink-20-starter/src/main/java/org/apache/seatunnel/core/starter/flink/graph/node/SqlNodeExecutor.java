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

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.core.starter.flink.execution.DataStreamTableInfo;
import org.apache.seatunnel.core.starter.flink.graph.resolve.ResolvedNode;
import org.apache.seatunnel.core.starter.flink.graph.runtime.DataRuntimeValue;
import org.apache.seatunnel.core.starter.flink.graph.runtime.GraphNodeExecutor;
import org.apache.seatunnel.core.starter.flink.graph.runtime.GraphRuntimeValue;
import org.apache.seatunnel.core.starter.flink.graph.runtime.NodeExecutionContext;
import org.apache.seatunnel.core.starter.flink.graph.sql.SeaTunnelTableBridge;
import org.apache.seatunnel.core.starter.flink.graph.sql.SqlOutputSchemaResolver;
import org.apache.seatunnel.core.starter.flink.utils.TableUtil;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.LinkedHashMap;
import java.util.Map;

/** Executes one bounded Batch SQL query over named graph inputs. */
public final class SqlNodeExecutor implements GraphNodeExecutor {

    @Override
    public GraphRuntimeValue execute(
            NodeExecutionContext context, Map<String, GraphRuntimeValue> inputValues) {
        ResolvedNode node = context.node();
        if (inputValues.isEmpty()) {
            throw new IllegalArgumentException(
                    "SQL node " + node.id() + " requires at least one input");
        }
        StreamTableEnvironment tableEnvironment = context.tableEnvironment();
        if (tableEnvironment == null) {
            throw new IllegalArgumentException(
                    "SQL node " + node.id() + " requires a TableEnvironment");
        }
        Map<String, DataRuntimeValue> inputs = new LinkedHashMap<>();
        for (Map.Entry<String, GraphRuntimeValue> entry : inputValues.entrySet()) {
            DataRuntimeValue value = DataRuntimeValue.require(entry.getValue(), node.id());
            SeaTunnelTableBridge.registerInput(tableEnvironment, entry.getKey(), value);
            inputs.put(entry.getKey(), value);
        }
        String query = query(node);
        Table result;
        try {
            result = tableEnvironment.sqlQuery(query);
        } catch (RuntimeException error) {
            throw new IllegalArgumentException(
                    "SQL node " + node.id() + " failed to plan config.query", error);
        }
        DataStream<Row> rows;
        try {
            rows = TableUtil.tableToInsertOnlyDataStream(tableEnvironment, result);
        } catch (RuntimeException error) {
            throw new IllegalArgumentException(
                    "SQL node " + node.id() + " requires an insert-only query output", error);
        }
        DataStream<org.apache.seatunnel.api.table.type.SeaTunnelRow> output =
                rows.map(new ChangelogRowMapper(node.id()))
                        .returns(
                                TypeInformation.of(
                                        org.apache.seatunnel.api.table.type.SeaTunnelRow.class));
        java.util.List<CatalogTable> catalogTables =
                SqlOutputSchemaResolver.catalogTables(result, node.id());
        return new DataRuntimeValue(
                new DataStreamTableInfo(output, catalogTables, node.id()), true, true);
    }

    private static String query(ResolvedNode node) {
        Object query = node.definition().config().get("query");
        if (!(query instanceof String) || ((String) query).trim().isEmpty()) {
            throw new IllegalArgumentException("SQL node " + node.id() + " requires config.query");
        }
        return (String) query;
    }

    private static final class ChangelogRowMapper
            implements MapFunction<Row, org.apache.seatunnel.api.table.type.SeaTunnelRow> {
        private final String nodeId;

        private ChangelogRowMapper(String nodeId) {
            this.nodeId = nodeId;
        }

        @Override
        public org.apache.seatunnel.api.table.type.SeaTunnelRow map(Row row) {
            return SeaTunnelTableBridge.toSeaTunnelRow(row, nodeId);
        }
    }
}
