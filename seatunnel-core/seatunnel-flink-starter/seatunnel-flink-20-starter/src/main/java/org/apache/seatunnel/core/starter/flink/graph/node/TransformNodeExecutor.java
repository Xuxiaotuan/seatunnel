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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.factory.FactoryUtil;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.transform.SeaTunnelMapTransform;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;
import org.apache.seatunnel.core.starter.flink.execution.DataStreamTableInfo;
import org.apache.seatunnel.core.starter.flink.graph.resolve.ResolvedNode;
import org.apache.seatunnel.core.starter.flink.graph.runtime.DataRuntimeValue;
import org.apache.seatunnel.core.starter.flink.graph.runtime.GraphNodeExecutor;
import org.apache.seatunnel.core.starter.flink.graph.runtime.GraphRuntimeValue;
import org.apache.seatunnel.core.starter.flink.graph.runtime.NodeExecutionContext;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.util.Map;
import java.util.Objects;

/** Applies an existing SeaTunnel table transform to one graph data value. */
@SuppressWarnings({"unchecked", "rawtypes"})
public final class TransformNodeExecutor implements GraphNodeExecutor {

    @Override
    public GraphRuntimeValue execute(
            NodeExecutionContext context, Map<String, GraphRuntimeValue> inputValues) {
        ResolvedNode node = context.node();
        if (inputValues.size() != 1) {
            throw new IllegalArgumentException(
                    "Transform node " + node.id() + " requires exactly one data input");
        }
        DataRuntimeValue input =
                DataRuntimeValue.require(inputValues.values().iterator().next(), node.id());
        SeaTunnelTransform<SeaTunnelRow> transform =
                (SeaTunnelTransform<SeaTunnelRow>)
                        FactoryUtil.createAndPrepareMultiTableTransform(
                                input.catalogTables(),
                                ReadonlyConfig.fromConfig(
                                        GraphNodeConfig.toPluginConfig(node.definition())),
                                Thread.currentThread().getContextClassLoader(),
                                node.definition().plugin());
        transform.setJobContext(context.jobContext());
        org.apache.flink.streaming.api.datastream.DataStream<SeaTunnelRow> output =
                input.tableInfo()
                        .getDataStream()
                        .map(new TransformMapFunction(transform))
                        .returns(TypeInformation.of(SeaTunnelRow.class))
                        .filter(Objects::nonNull);
        DataStreamTableInfo tableInfo =
                new DataStreamTableInfo(output, transform.getProducedCatalogTables(), node.id());
        return new DataRuntimeValue(tableInfo, input.bounded(), input.insertOnly());
    }

    private static final class TransformMapFunction
            extends RichMapFunction<SeaTunnelRow, SeaTunnelRow> {
        private final SeaTunnelTransform<SeaTunnelRow> transform;

        private TransformMapFunction(SeaTunnelTransform<SeaTunnelRow> transform) {
            this.transform = transform;
        }

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) {
            transform.open();
        }

        @Override
        public SeaTunnelRow map(SeaTunnelRow value) {
            return ((SeaTunnelMapTransform<SeaTunnelRow>) transform).map(value);
        }

        @Override
        public void close() {
            transform.close();
        }
    }
}
