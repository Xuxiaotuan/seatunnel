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

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.apache.seatunnel.api.common.PluginIdentifier;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.factory.FactoryUtil;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.constants.EngineType;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.core.starter.flink.execution.DataStreamTableInfo;
import org.apache.seatunnel.core.starter.flink.graph.resolve.ResolvedNode;
import org.apache.seatunnel.core.starter.flink.graph.runtime.DataRuntimeValue;
import org.apache.seatunnel.core.starter.flink.graph.runtime.GraphNodeExecutor;
import org.apache.seatunnel.core.starter.flink.graph.runtime.GraphRuntimeValue;
import org.apache.seatunnel.core.starter.flink.graph.runtime.NodeExecutionContext;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelFactoryDiscovery;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelSourcePluginDiscovery;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;

import scala.Tuple2;

import java.io.Serializable;
import java.util.Map;

import static org.apache.seatunnel.api.table.factory.FactoryUtil.ensureJobModeMatch;

/** Builds one SeaTunnel source inside the graph's shared Flink environment. */
@SuppressWarnings({"unchecked", "rawtypes"})
public final class SourceNodeExecutor implements GraphNodeExecutor {

    @Override
    public GraphRuntimeValue execute(
            NodeExecutionContext context, Map<String, GraphRuntimeValue> inputValues) {
        ResolvedNode node = context.node();
        if (!inputValues.isEmpty()) {
            throw new IllegalArgumentException("Source node " + node.id() + " cannot have inputs");
        }
        Config config = GraphNodeConfig.toPluginConfig(node.definition());
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        PluginIdentifier identifier =
                PluginIdentifier.of(
                        EngineType.SEATUNNEL.getEngine(),
                        PluginType.SOURCE.getType(),
                        node.definition().plugin());
        SeaTunnelSourcePluginDiscovery sourceDiscovery = new SeaTunnelSourcePluginDiscovery();
        SeaTunnelFactoryDiscovery factoryDiscovery =
                new SeaTunnelFactoryDiscovery(TableSourceFactory.class);
        Tuple2<SeaTunnelSource<Object, SourceSplit, Serializable>, java.util.List<CatalogTable>>
                prepared =
                        FactoryUtil.createAndPrepareSource(
                                ReadonlyConfig.fromConfig(config),
                                classLoader,
                                node.definition().plugin(),
                                sourceDiscovery::createPluginInstance,
                                (TableSourceFactory)
                                        factoryDiscovery
                                                .createOptionalPluginInstance(identifier)
                                                .orElse(null),
                                null);
        SeaTunnelSource<SeaTunnelRow, SourceSplit, Serializable> source =
                (SeaTunnelSource) prepared._1();
        source.setJobContext(context.jobContext());
        ensureJobModeMatch(context.jobContext(), source);
        DataStreamSource<SeaTunnelRow> stream =
                context.executionEnvironment()
                        .fromSource(
                                new org.apache.seatunnel.translation.flink.source.FlinkSource<>(
                                        source, ConfigFactory.empty()),
                                WatermarkStrategy.noWatermarks(),
                                node.id() + "-Source");
        String tableName =
                ReadonlyConfig.fromConfig(config)
                        .get(org.apache.seatunnel.api.options.ConnectorCommonOptions.PLUGIN_OUTPUT);
        DataStreamTableInfo tableInfo = new DataStreamTableInfo(stream, prepared._2(), tableName);
        return new DataRuntimeValue(
                tableInfo,
                source.getBoundedness() == org.apache.seatunnel.api.source.Boundedness.BOUNDED,
                true);
    }
}
