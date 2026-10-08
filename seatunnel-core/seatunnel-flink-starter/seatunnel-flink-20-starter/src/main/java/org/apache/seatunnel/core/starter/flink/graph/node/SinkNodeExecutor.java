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

import org.apache.seatunnel.api.common.PluginIdentifier;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.table.factory.FactoryUtil;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.constants.EngineType;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.core.starter.flink.graph.resolve.ResolvedNode;
import org.apache.seatunnel.core.starter.flink.graph.runtime.CompletionRuntimeValue;
import org.apache.seatunnel.core.starter.flink.graph.runtime.DataRuntimeValue;
import org.apache.seatunnel.core.starter.flink.graph.runtime.GraphNodeExecutor;
import org.apache.seatunnel.core.starter.flink.graph.runtime.GraphRuntimeValue;
import org.apache.seatunnel.core.starter.flink.graph.runtime.NodeExecutionContext;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelFactoryDiscovery;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelSinkPluginDiscovery;

import org.apache.flink.streaming.api.datastream.DataStream;

import java.util.Map;

/** Registers a SeaTunnel sink in the shared Flink dataflow. */
@SuppressWarnings({"unchecked", "rawtypes"})
public final class SinkNodeExecutor implements GraphNodeExecutor {

    @Override
    public GraphRuntimeValue execute(
            NodeExecutionContext context, Map<String, GraphRuntimeValue> inputValues) {
        ResolvedNode node = context.node();
        if (inputValues.size() != 1) {
            throw new IllegalArgumentException(
                    "Sink node " + node.id() + " requires exactly one data input");
        }
        DataRuntimeValue input =
                DataRuntimeValue.require(inputValues.values().iterator().next(), node.id());
        if (input.catalogTables().isEmpty()) {
            throw new IllegalArgumentException("Sink node " + node.id() + " has no catalog table");
        }
        org.apache.seatunnel.shade.com.typesafe.config.Config config =
                GraphNodeConfig.toPluginConfig(node.definition());
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        PluginIdentifier identifier =
                PluginIdentifier.of(
                        EngineType.SEATUNNEL.getEngine(),
                        PluginType.SINK.getType(),
                        node.definition().plugin());
        SeaTunnelFactoryDiscovery discovery = new SeaTunnelFactoryDiscovery(TableSinkFactory.class);
        SeaTunnelSinkPluginDiscovery fallback = new SeaTunnelSinkPluginDiscovery();
        SeaTunnelSink sink =
                FactoryUtil.createAndPrepareSink(
                        input.catalogTables().get(0),
                        ReadonlyConfig.fromConfig(config),
                        classLoader,
                        node.definition().plugin(),
                        fallback::createPluginInstance,
                        (TableSinkFactory)
                                discovery.createOptionalPluginInstance(identifier).orElse(null));
        sink.setJobContext(context.jobContext());
        DataStream<SeaTunnelRow> stream = input.tableInfo().getDataStream();
        stream.sinkTo(
                        new org.apache.seatunnel.translation.flink.sink.FlinkSink<>(
                                sink, input.catalogTables(), 1))
                .name(node.id() + "-Sink");
        return new CompletionRuntimeValue(node.id());
    }
}
