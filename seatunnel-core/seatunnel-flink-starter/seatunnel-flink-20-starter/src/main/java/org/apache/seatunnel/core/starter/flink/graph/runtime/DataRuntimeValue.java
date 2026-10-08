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

package org.apache.seatunnel.core.starter.flink.graph.runtime;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.core.starter.flink.execution.DataStreamTableInfo;
import org.apache.seatunnel.core.starter.flink.graph.api.NodeCapability;
import org.apache.seatunnel.core.starter.flink.graph.validate.GraphValidationException;

import java.util.List;

/** A data stream plus the schema and boundedness information needed by SQL nodes. */
public final class DataRuntimeValue implements GraphRuntimeValue {

    private final DataStreamTableInfo tableInfo;
    private final boolean bounded;
    private final boolean insertOnly;

    public DataRuntimeValue(DataStreamTableInfo tableInfo, boolean bounded, boolean insertOnly) {
        if (tableInfo == null) {
            throw new IllegalArgumentException("tableInfo must not be null");
        }
        this.tableInfo = tableInfo;
        this.bounded = bounded;
        this.insertOnly = insertOnly;
    }

    @Override
    public NodeCapability.ValueKind kind() {
        return NodeCapability.ValueKind.DATA;
    }

    public DataStreamTableInfo tableInfo() {
        return tableInfo;
    }

    public boolean bounded() {
        return bounded;
    }

    public boolean insertOnly() {
        return insertOnly;
    }

    public List<CatalogTable> catalogTables() {
        return tableInfo.getCatalogTables();
    }

    public static DataRuntimeValue require(GraphRuntimeValue value, String nodeId) {
        if (!(value instanceof DataRuntimeValue)) {
            String kind = value == null ? "null" : value.kind().name().toLowerCase();
            throw new GraphValidationException(
                    "Node " + nodeId + " requires a data value but received " + kind + " output");
        }
        return (DataRuntimeValue) value;
    }
}
