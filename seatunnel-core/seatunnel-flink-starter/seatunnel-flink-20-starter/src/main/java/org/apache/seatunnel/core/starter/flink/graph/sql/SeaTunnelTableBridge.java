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

package org.apache.seatunnel.core.starter.flink.graph.sql;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.core.starter.flink.graph.runtime.DataRuntimeValue;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.List;

/** Bridges SeaTunnel rows and catalog metadata to Flink's Table API. */
public final class SeaTunnelTableBridge {

    private SeaTunnelTableBridge() {}

    public static void registerInput(
            StreamTableEnvironment tableEnvironment, String alias, DataRuntimeValue value) {
        if (!value.bounded() || !value.insertOnly()) {
            throw new IllegalArgumentException(
                    "SQL input " + alias + " must be bounded and insert-only in Batch mode");
        }
        if (value.catalogTables().isEmpty()) {
            throw new IllegalArgumentException("SQL input " + alias + " has no catalog table");
        }
        CatalogTable catalogTable = value.catalogTables().get(0);
        SeaTunnelRowType rowType = catalogTable.getSeaTunnelRowType();
        DataStream<Row> rows =
                value.tableInfo()
                        .getDataStream()
                        .map(SeaTunnelTableBridge::toFlinkRow)
                        .returns(rowTypeInfo(rowType));
        Schema.Builder schema = Schema.newBuilder();
        for (int i = 0; i < rowType.getTotalFields(); i++) {
            schema.column(rowType.getFieldName(i), toFlinkDataType(rowType.getFieldType(i)));
        }
        tableEnvironment.createTemporaryView(alias, rows, schema.build());
    }

    public static SeaTunnelRow toSeaTunnelRow(Row row, String nodeId) {
        if (row.getKind() != org.apache.flink.types.RowKind.INSERT) {
            throw new IllegalStateException(
                    "SQL node " + nodeId + " produced unsupported changelog kind " + row.getKind());
        }
        Object[] fields = new Object[row.getArity()];
        for (int i = 0; i < row.getArity(); i++) {
            fields[i] = row.getField(i);
        }
        return new SeaTunnelRow(fields);
    }

    public static Row toFlinkRow(SeaTunnelRow row) {
        Row result = Row.withPositions(row.getFields().length);
        for (int i = 0; i < row.getFields().length; i++) {
            result.setField(i, row.getField(i));
        }
        return result;
    }

    private static RowTypeInfo rowTypeInfo(SeaTunnelRowType rowType) {
        TypeInformation<?>[] types = new TypeInformation<?>[rowType.getTotalFields()];
        for (int i = 0; i < rowType.getTotalFields(); i++) {
            types[i] = TypeInformation.of(rowType.getFieldType(i).getTypeClass());
        }
        return new RowTypeInfo(types, rowType.getFieldNames());
    }

    public static SeaTunnelRowType toSeaTunnelRowType(
            org.apache.flink.table.catalog.ResolvedSchema schema) {
        List<String> names = schema.getColumnNames();
        List<org.apache.flink.table.types.DataType> dataTypes = schema.getColumnDataTypes();
        SeaTunnelDataType<?>[] types = new SeaTunnelDataType<?>[dataTypes.size()];
        for (int i = 0; i < dataTypes.size(); i++) {
            types[i] = toSeaTunnelDataType(dataTypes.get(i));
        }
        return new SeaTunnelRowType(names.toArray(new String[0]), types);
    }

    private static org.apache.flink.table.types.DataType toFlinkDataType(
            SeaTunnelDataType<?> type) {
        switch (type.getSqlType()) {
            case BOOLEAN:
                return DataTypes.BOOLEAN();
            case TINYINT:
                return DataTypes.TINYINT();
            case SMALLINT:
                return DataTypes.SMALLINT();
            case INT:
                return DataTypes.INT();
            case BIGINT:
                return DataTypes.BIGINT();
            case FLOAT:
                return DataTypes.FLOAT();
            case DOUBLE:
                return DataTypes.DOUBLE();
            case BYTES:
                return DataTypes.BYTES();
            case DATE:
                return DataTypes.DATE();
            case TIME:
                return DataTypes.TIME();
            case TIMESTAMP:
                return DataTypes.TIMESTAMP();
            case TIMESTAMP_TZ:
                return DataTypes.TIMESTAMP_WITH_TIME_ZONE();
            case DECIMAL:
                return DataTypes.DECIMAL(38, 18);
            default:
                return DataTypes.STRING();
        }
    }

    private static SeaTunnelDataType<?> toSeaTunnelDataType(
            org.apache.flink.table.types.DataType type) {
        switch (type.getLogicalType().getTypeRoot()) {
            case BOOLEAN:
                return BasicType.BOOLEAN_TYPE;
            case TINYINT:
                return BasicType.BYTE_TYPE;
            case SMALLINT:
                return BasicType.SHORT_TYPE;
            case INTEGER:
                return BasicType.INT_TYPE;
            case BIGINT:
                return BasicType.LONG_TYPE;
            case FLOAT:
                return BasicType.FLOAT_TYPE;
            case DOUBLE:
                return BasicType.DOUBLE_TYPE;
            case BINARY:
            case VARBINARY:
                return org.apache.seatunnel.api.table.type.PrimitiveByteArrayType.INSTANCE;
            default:
                return BasicType.STRING_TYPE;
        }
    }
}
