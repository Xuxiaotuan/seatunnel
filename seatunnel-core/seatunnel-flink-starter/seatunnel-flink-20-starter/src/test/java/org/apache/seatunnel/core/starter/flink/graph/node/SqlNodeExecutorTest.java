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
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.core.starter.flink.execution.DataStreamTableInfo;
import org.apache.seatunnel.core.starter.flink.graph.api.GraphNodeDefinition;
import org.apache.seatunnel.core.starter.flink.graph.resolve.ResolvedNode;
import org.apache.seatunnel.core.starter.flink.graph.runtime.DataRuntimeValue;
import org.apache.seatunnel.core.starter.flink.graph.runtime.GraphRuntimeValue;
import org.apache.seatunnel.core.starter.flink.graph.runtime.NodeExecutionContext;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

class SqlNodeExecutorTest {

    @Test
    void joinsTwoBoundedInputsWithMappingAliasesAndReturnsOneOutput() throws Exception {
        StreamExecutionEnvironment environment =
                StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment =
                StreamTableEnvironment.create(environment, EnvironmentSettings.inBatchMode());

        DataRuntimeValue orders =
                data(
                        environment,
                        "orders",
                        new SeaTunnelRowType(
                                new String[] {"order_id", "customer_id"},
                                new SeaTunnelDataType<?>[] {
                                    BasicType.INT_TYPE, BasicType.INT_TYPE
                                }),
                        new SeaTunnelRow(new Object[] {1, 7}));
        DataRuntimeValue customers =
                data(
                        environment,
                        "customers",
                        new SeaTunnelRowType(
                                new String[] {"customer_id", "customer_name"},
                                new SeaTunnelDataType<?>[] {
                                    BasicType.INT_TYPE, BasicType.STRING_TYPE
                                }),
                        new SeaTunnelRow(new Object[] {7, "Alice"}));

        Map<String, String> inputs = new LinkedHashMap<>();
        inputs.put("orders", "orders");
        inputs.put("customers", "customers");
        Map<String, Object> config = new LinkedHashMap<>();
        config.put(
                "query",
                "SELECT o.order_id, c.customer_name FROM orders o JOIN customers c "
                        + "ON o.customer_id = c.customer_id");
        GraphNodeDefinition definition =
                new GraphNodeDefinition("enrich", "sql", "query", inputs, config);
        ResolvedNode node = new ResolvedNode(definition);
        Map<String, GraphRuntimeValue> values = new LinkedHashMap<>();
        values.put("orders", orders);
        values.put("customers", customers);

        DataRuntimeValue result =
                (DataRuntimeValue)
                        new SqlNodeExecutor()
                                .execute(
                                        new NodeExecutionContext(
                                                node,
                                                environment,
                                                tableEnvironment,
                                                Collections.emptyMap()),
                                        values);

        Assertions.assertArrayEquals(
                new String[] {"order_id", "customer_name"},
                result.catalogTables().get(0).getTableSchema().getFieldNames());
        Assertions.assertEquals("enrich", result.tableInfo().getTableName());
        java.util.List<SeaTunnelRow> rows = result.tableInfo().getDataStream().executeAndCollect(1);
        Assertions.assertEquals(1, rows.size());
        Assertions.assertEquals(1, rows.get(0).getField(0));
        Assertions.assertEquals("Alice", rows.get(0).getField(1));
    }

    private static DataRuntimeValue data(
            StreamExecutionEnvironment environment,
            String name,
            SeaTunnelRowType rowType,
            SeaTunnelRow row) {
        CatalogTable catalogTable = CatalogTableUtil.getCatalogTable(name, rowType);
        org.apache.flink.streaming.api.datastream.DataStream<SeaTunnelRow> stream =
                environment.fromCollection(
                        Collections.singletonList(row), TypeInformation.of(SeaTunnelRow.class));
        return new DataRuntimeValue(
                new DataStreamTableInfo(stream, Collections.singletonList(catalogTable), name),
                true,
                true);
    }
}
