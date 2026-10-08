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
import org.apache.seatunnel.shade.com.typesafe.config.ConfigValueFactory;

import org.apache.seatunnel.core.starter.flink.graph.api.GraphNodeDefinition;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.apache.seatunnel.api.options.ConnectorCommonOptions.PLUGIN_NAME;
import static org.apache.seatunnel.api.options.ConnectorCommonOptions.PLUGIN_OUTPUT;

/** Converts graph node configuration into the existing SeaTunnel plugin config contract. */
final class GraphNodeConfig {

    private GraphNodeConfig() {}

    static Config toPluginConfig(GraphNodeDefinition node) {
        Map<String, Object> values = new LinkedHashMap<>(node.config());
        Config config = ConfigFactory.parseMap(values);
        config = config.withValue(PLUGIN_NAME.key(), ConfigValueFactory.fromAnyRef(node.plugin()));
        if (!config.hasPath(PLUGIN_OUTPUT.key())) {
            config =
                    config.withValue(PLUGIN_OUTPUT.key(), ConfigValueFactory.fromAnyRef(node.id()));
        }
        return config;
    }
}
