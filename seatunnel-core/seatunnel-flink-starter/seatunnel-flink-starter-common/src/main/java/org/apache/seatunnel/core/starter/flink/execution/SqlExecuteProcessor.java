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

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.core.starter.exception.TaskExecuteException;
import org.apache.seatunnel.core.starter.execution.PluginExecuteProcessor;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.types.Row;

import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Executes one Flink SQL query over all source tables and exposes its single result to downstream
 * SeaTunnel transforms and sinks.
 *
 * <p>The table environment is created from the existing StreamExecutionEnvironment, so this stage
 * contributes operators to the same Flink job rather than submitting a second job.
 */
@Slf4j
public class SqlExecuteProcessor
        implements PluginExecuteProcessor<DataStreamTableInfo, FlinkRuntimeEnvironment> {

    public static final String PRE_TRANSFORM = "pre_transform";
    public static final String POST_TRANSFORM = "post_transform";

    private final Config sqlConfig;
    private final String stageName;
    private FlinkRuntimeEnvironment runtimeEnvironment;

    public SqlExecuteProcessor(Config config) {
        this(config, PRE_TRANSFORM);
    }

    public SqlExecuteProcessor(Config config, String stageName) {
        this.stageName = stageName;
        this.sqlConfig = resolveStageConfig(config, stageName);
    }

    static Config resolveStageConfig(Config rootConfig, String stageName) {
        if (!rootConfig.hasPath("sql")) {
            return null;
        }
        Config sqlRoot = rootConfig.getConfig("sql");
        if (sqlRoot.hasPath(stageName)) {
            return sqlRoot.getConfig(stageName);
        }
        // Keep the original `sql { query = ... }` syntax as a pre-transform SQL stage.
        if (PRE_TRANSFORM.equals(stageName)
                && (sqlRoot.hasPath("query") || sqlRoot.hasPath("plugin_output"))) {
            return sqlRoot;
        }
        return null;
    }

    boolean isConfigured() {
        return sqlConfig != null;
    }

    @Override
    public List<DataStreamTableInfo> execute(List<DataStreamTableInfo> upstreamDataStreams)
            throws TaskExecuteException {
        if (sqlConfig == null) {
            return upstreamDataStreams;
        }
        if (runtimeEnvironment == null) {
            throw new TaskExecuteException("Flink runtime environment is required for SQL stage");
        }

        try {
            String query = requiredString("query");
            String outputName = requiredString("plugin_output");
            StreamTableEnvironment tableEnvironment =
                    StreamTableEnvironment.create(
                            runtimeEnvironment.getStreamExecutionEnvironment());
            Map<String, DataStreamTableInfo> inputs = indexInputs(upstreamDataStreams);
            for (DataStreamTableInfo input : inputs.values()) {
                registerInput(tableEnvironment, input, runtimeEnvironment.getJobMode());
            }

            Table result = tableEnvironment.sqlQuery(query);
            SeaTunnelRowType resultType = toSeaTunnelRowType(result.getResolvedSchema());
            DataStream<Row> changelog = tableEnvironment.toChangelogStream(result);
            DataStream<SeaTunnelRow> output =
                    changelog
                            .map(
                                    (MapFunction<Row, SeaTunnelRow>)
                                            row -> toSeaTunnelRow(row, outputName))
                            .returns(TypeInformation.of(SeaTunnelRow.class));
            CatalogTable outputTable =
                    CatalogTableUtil.getCatalogTable(
                            "schema", "default", null, outputName, resultType);
            log.info("Created Flink SQL {} output {} from query: {}", stageName, outputName, query);
            return Collections.singletonList(
                    new DataStreamTableInfo(
                            output, Collections.singletonList(outputTable), outputName));
        } catch (Exception e) {
            throw new TaskExecuteException("SeaTunnel Flink SQL execute error", e);
        }
    }

    private Map<String, DataStreamTableInfo> indexInputs(
            List<DataStreamTableInfo> upstreamDataStreams) {
        Map<String, DataStreamTableInfo> inputs = new LinkedHashMap<>();
        for (DataStreamTableInfo input : upstreamDataStreams) {
            if (input.getTableName() == null || input.getTableName().trim().isEmpty()) {
                throw new IllegalArgumentException("SQL input table name must not be blank");
            }
            if (inputs.put(input.getTableName(), input) != null) {
                throw new IllegalArgumentException(
                        "Duplicate SQL input table name: " + input.getTableName());
            }
        }
        if (inputs.isEmpty()) {
            throw new IllegalArgumentException("SQL stage requires at least one source table");
        }
        return inputs;
    }

    static void registerInput(
            StreamTableEnvironment tableEnvironment, DataStreamTableInfo input, JobMode jobMode) {
        if (input.getCatalogTables().size() != 1) {
            throw new IllegalArgumentException(
                    "SQL stage requires exactly one catalog table for input: "
                            + input.getTableName());
        }
        SeaTunnelRowType rowType = input.getCatalogTables().get(0).getSeaTunnelRowType();
        Schema schema = buildSchema(rowType);
        DataStream<Row> rows =
                input.getDataStream()
                        .map((MapFunction<SeaTunnelRow, Row>) SqlExecuteProcessor::toFlinkRow)
                        .returns(
                                Types.ROW_NAMED(
                                        rowType.getFieldNames(), toFlinkTypeInformation(rowType)));
        Table inputTable;
        if (jobMode == JobMode.BATCH) {
            inputTable = tableEnvironment.fromDataStream(rows, schema);
        } else {
            inputTable = tableEnvironment.fromChangelogStream(rows, schema);
        }
        tableEnvironment.createTemporaryView(input.getTableName(), inputTable);
    }

    private static TypeInformation<?>[] toFlinkTypeInformation(SeaTunnelRowType rowType) {
        TypeInformation<?>[] fieldTypes = new TypeInformation<?>[rowType.getTotalFields()];
        for (int i = 0; i < rowType.getTotalFields(); i++) {
            fieldTypes[i] = toFlinkTypeInformation(rowType.getFieldType(i));
        }
        return fieldTypes;
    }

    private static TypeInformation<?> toFlinkTypeInformation(SeaTunnelDataType<?> dataType) {
        switch (dataType.getSqlType()) {
            case STRING:
                return Types.STRING;
            case BOOLEAN:
                return Types.BOOLEAN;
            case TINYINT:
                return Types.BYTE;
            case SMALLINT:
                return Types.SHORT;
            case INT:
                return Types.INT;
            case BIGINT:
                return Types.LONG;
            case FLOAT:
                return Types.FLOAT;
            case DOUBLE:
                return Types.DOUBLE;
            case DECIMAL:
                return Types.BIG_DEC;
            case BYTES:
                return Types.PRIMITIVE_ARRAY(Types.BYTE);
            case DATE:
                return Types.LOCAL_DATE;
            case TIME:
                return Types.LOCAL_TIME;
            case TIMESTAMP:
                return Types.LOCAL_DATE_TIME;
            case TIMESTAMP_TZ:
                return TypeInformation.of(dataType.getTypeClass());
            case ARRAY:
                return Types.OBJECT_ARRAY(
                        toFlinkTypeInformation(((ArrayType<?, ?>) dataType).getElementType()));
            case MAP:
                MapType<?, ?> mapType = (MapType<?, ?>) dataType;
                return Types.MAP(
                        toFlinkTypeInformation(mapType.getKeyType()),
                        toFlinkTypeInformation(mapType.getValueType()));
            case ROW:
                SeaTunnelRowType rowType = (SeaTunnelRowType) dataType;
                return Types.ROW_NAMED(rowType.getFieldNames(), toFlinkTypeInformation(rowType));
            default:
                throw new IllegalArgumentException(
                        "Unsupported SeaTunnel SQL type for Flink row type information: "
                                + dataType.getSqlType());
        }
    }

    static Schema buildSchema(SeaTunnelRowType rowType) {
        Schema.Builder schema = Schema.newBuilder();
        for (int i = 0; i < rowType.getTotalFields(); i++) {
            schema.column(rowType.getFieldName(i), toFlinkDataType(rowType.getFieldType(i)));
        }
        return schema.build();
    }

    private String requiredString(String key) {
        if (!sqlConfig.hasPath(key) || sqlConfig.getString(key).trim().isEmpty()) {
            throw new IllegalArgumentException(
                    "sql." + stageName + "." + key + " must not be blank");
        }
        return sqlConfig.getString(key);
    }

    static DataType toFlinkDataType(SeaTunnelDataType<?> dataType) {
        switch (dataType.getSqlType()) {
            case STRING:
                return DataTypes.STRING();
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
            case DECIMAL:
                DecimalType decimalType = (DecimalType) dataType;
                return DataTypes.DECIMAL(decimalType.getPrecision(), decimalType.getScale());
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
            case ARRAY:
                return DataTypes.ARRAY(
                        toFlinkDataType(((ArrayType<?, ?>) dataType).getElementType()));
            case MAP:
                MapType<?, ?> mapType = (MapType<?, ?>) dataType;
                return DataTypes.MAP(
                        toFlinkDataType(mapType.getKeyType()),
                        toFlinkDataType(mapType.getValueType()));
            case ROW:
                SeaTunnelRowType rowType = (SeaTunnelRowType) dataType;
                DataTypes.Field[] fields = new DataTypes.Field[rowType.getTotalFields()];
                for (int i = 0; i < rowType.getTotalFields(); i++) {
                    fields[i] =
                            DataTypes.FIELD(
                                    rowType.getFieldName(i),
                                    toFlinkDataType(rowType.getFieldType(i)));
                }
                return DataTypes.ROW(fields);
            default:
                throw new IllegalArgumentException(
                        "Unsupported SeaTunnel SQL type for Flink SQL: " + dataType.getSqlType());
        }
    }

    private SeaTunnelRowType toSeaTunnelRowType(
            org.apache.flink.table.catalog.ResolvedSchema resolvedSchema) {
        List<String> names = resolvedSchema.getColumnNames();
        List<DataType> types = resolvedSchema.getColumnDataTypes();
        SeaTunnelDataType<?>[] seaTunnelTypes = new SeaTunnelDataType<?>[types.size()];
        for (int i = 0; i < types.size(); i++) {
            seaTunnelTypes[i] = toSeaTunnelDataType(types.get(i).getLogicalType());
        }
        return new SeaTunnelRowType(names.toArray(new String[0]), seaTunnelTypes);
    }

    private SeaTunnelDataType<?> toSeaTunnelDataType(LogicalType type) {
        LogicalTypeRoot root = type.getTypeRoot();
        switch (root) {
            case CHAR:
            case VARCHAR:
                return BasicType.STRING_TYPE;
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
            case DECIMAL:
                org.apache.flink.table.types.logical.DecimalType decimal =
                        (org.apache.flink.table.types.logical.DecimalType) type;
                return new DecimalType(decimal.getPrecision(), decimal.getScale());
            case BINARY:
            case VARBINARY:
                return org.apache.seatunnel.api.table.type.PrimitiveByteArrayType.INSTANCE;
            case DATE:
                return LocalTimeType.LOCAL_DATE_TYPE;
            case TIME_WITHOUT_TIME_ZONE:
                return LocalTimeType.LOCAL_TIME_TYPE;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return LocalTimeType.LOCAL_DATE_TIME_TYPE;
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return LocalTimeType.OFFSET_DATE_TIME_TYPE;
            case ARRAY:
                org.apache.flink.table.types.logical.ArrayType array =
                        (org.apache.flink.table.types.logical.ArrayType) type;
                return ArrayType.of(toSeaTunnelDataType(array.getElementType()));
            case MAP:
                org.apache.flink.table.types.logical.MapType map =
                        (org.apache.flink.table.types.logical.MapType) type;
                return new MapType<>(
                        toSeaTunnelDataType(map.getKeyType()),
                        toSeaTunnelDataType(map.getValueType()));
            case ROW:
                org.apache.flink.table.types.logical.RowType row =
                        (org.apache.flink.table.types.logical.RowType) type;
                String[] fieldNames = new String[row.getFieldCount()];
                SeaTunnelDataType<?>[] fieldTypes = new SeaTunnelDataType<?>[row.getFieldCount()];
                for (int i = 0; i < row.getFieldCount(); i++) {
                    fieldNames[i] = row.getFieldNames().get(i);
                    fieldTypes[i] = toSeaTunnelDataType(row.getTypeAt(i));
                }
                return new SeaTunnelRowType(fieldNames, fieldTypes);
            case NULL:
                return BasicType.VOID_TYPE;
            default:
                throw new IllegalArgumentException(
                        "Unsupported Flink SQL result type: " + type.asSummaryString());
        }
    }

    private static Row toFlinkRow(SeaTunnelRow row) {
        Row result = Row.withPositions(toFlinkRowKind(row.getRowKind()), row.getArity());
        for (int i = 0; i < row.getArity(); i++) {
            result.setField(i, row.getField(i));
        }
        return result;
    }

    static SeaTunnelRow toSeaTunnelRow(Row row, String tableName) {
        SeaTunnelRow result = new SeaTunnelRow(row.getArity());
        for (int i = 0; i < row.getArity(); i++) {
            result.setField(i, row.getField(i));
        }
        result.setTableId(tableName);
        result.setRowKind(toSeaTunnelRowKind(row.getKind()));
        return result;
    }

    private static org.apache.flink.types.RowKind toFlinkRowKind(RowKind rowKind) {
        switch (rowKind) {
            case INSERT:
                return org.apache.flink.types.RowKind.INSERT;
            case UPDATE_BEFORE:
                return org.apache.flink.types.RowKind.UPDATE_BEFORE;
            case UPDATE_AFTER:
                return org.apache.flink.types.RowKind.UPDATE_AFTER;
            case DELETE:
                return org.apache.flink.types.RowKind.DELETE;
            default:
                throw new IllegalArgumentException("Unsupported SeaTunnel row kind: " + rowKind);
        }
    }

    private static RowKind toSeaTunnelRowKind(org.apache.flink.types.RowKind rowKind) {
        switch (rowKind) {
            case INSERT:
                return RowKind.INSERT;
            case UPDATE_BEFORE:
                return RowKind.UPDATE_BEFORE;
            case UPDATE_AFTER:
                return RowKind.UPDATE_AFTER;
            case DELETE:
                return RowKind.DELETE;
            default:
                throw new IllegalArgumentException("Unsupported Flink row kind: " + rowKind);
        }
    }

    @Override
    public void setRuntimeEnvironment(FlinkRuntimeEnvironment runtimeEnvironment) {
        this.runtimeEnvironment = runtimeEnvironment;
    }
}
