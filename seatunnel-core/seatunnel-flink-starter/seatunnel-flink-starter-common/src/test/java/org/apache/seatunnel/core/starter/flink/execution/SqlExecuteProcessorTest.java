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

package org.apache.seatunnel.core.starter.flink.execution;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class SqlExecuteProcessorTest {

    @Test
    void supportsExplicitPreAndPostTransformSqlStages() {
        Config config =
                ConfigFactory.parseString(
                        "sql {\n"
                                + " pre_transform {\n"
                                + "  plugin_output = joined\n"
                                + "  query = \"SELECT 1\"\n"
                                + " }\n"
                                + " post_transform {\n"
                                + "  plugin_output = final\n"
                                + "  query = \"SELECT 2\"\n"
                                + " }\n"
                                + "}\n");

        Assertions.assertTrue(
                new SqlExecuteProcessor(config, SqlExecuteProcessor.PRE_TRANSFORM).isConfigured());
        Assertions.assertTrue(
                new SqlExecuteProcessor(config, SqlExecuteProcessor.POST_TRANSFORM).isConfigured());
    }

    @Test
    void keepsLegacySqlSyntaxAsPreTransformOnly() {
        Config config =
                ConfigFactory.parseString(
                        "sql {\n"
                                + " plugin_output = joined\n"
                                + " query = \"SELECT 1\"\n"
                                + "}\n");

        Assertions.assertTrue(
                new SqlExecuteProcessor(config, SqlExecuteProcessor.PRE_TRANSFORM).isConfigured());
        Assertions.assertFalse(
                new SqlExecuteProcessor(config, SqlExecuteProcessor.POST_TRANSFORM).isConfigured());
    }

    @Test
    void mapsSeaTunnelTypesToFlinkTypes() {
        Assertions.assertEquals(
                DataTypes.INT().getLogicalType(),
                SqlExecuteProcessor.toFlinkDataType(BasicType.INT_TYPE).getLogicalType());
        Assertions.assertEquals(
                DataTypes.STRING().getLogicalType(),
                SqlExecuteProcessor.toFlinkDataType(BasicType.STRING_TYPE).getLogicalType());
        Assertions.assertEquals(
                DataTypes.DECIMAL(12, 2).getLogicalType(),
                SqlExecuteProcessor.toFlinkDataType(new DecimalType(12, 2)).getLogicalType());
    }

    @Test
    void convertsFlinkChangelogRowToSeaTunnelRow() {
        Row flinkRow = Row.of(7, "joined");
        flinkRow.setKind(org.apache.flink.types.RowKind.UPDATE_AFTER);

        SeaTunnelRow row = SqlExecuteProcessor.toSeaTunnelRow(flinkRow, "sql_result");

        Assertions.assertArrayEquals(new Object[] {7, "joined"}, row.getFields());
        Assertions.assertEquals("sql_result", row.getTableId());
        Assertions.assertEquals(RowKind.UPDATE_AFTER, row.getRowKind());
    }

    @Test
    void buildsSchemaWithSeaTunnelFieldNames() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id", "name"},
                        new org.apache.seatunnel.api.table.type.SeaTunnelDataType<?>[] {
                            BasicType.INT_TYPE, BasicType.STRING_TYPE
                        });

        Assertions.assertEquals(
                java.util.Arrays.asList("id", "name"),
                SqlExecuteProcessor.buildSchema(rowType).getColumns().stream()
                        .map(column -> column.getName())
                        .collect(java.util.stream.Collectors.toList()));
    }

    @Test
    void rejectsUnsupportedTypeInsteadOfSilentlyChangingIt() {
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> SqlExecuteProcessor.toFlinkDataType(BasicType.VOID_TYPE));
    }
}
