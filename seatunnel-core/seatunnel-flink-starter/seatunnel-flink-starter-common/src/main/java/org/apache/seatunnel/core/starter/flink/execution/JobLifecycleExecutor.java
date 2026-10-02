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
import org.apache.seatunnel.shade.com.typesafe.config.ConfigUtil;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigValueType;

import org.apache.seatunnel.core.starter.exception.TaskExecuteException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/** Executes job-level preconditions and post-job checks once per logical plugin. */
public class JobLifecycleExecutor {

    private static final Logger LOGGER = LoggerFactory.getLogger(JobLifecycleExecutor.class);
    private static final String LIFECYCLE = "lifecycle";

    private final Config rootConfig;

    public JobLifecycleExecutor(Config rootConfig) {
        this.rootConfig = rootConfig;
    }

    public void executePre(
            List<? extends Config> sourceConfigs, List<? extends Config> sinkConfigs) {
        executeForPlugins("source", sourceConfigs, "pre");
        executeForPlugins("sink", sinkConfigs, "pre");
    }

    public void executePost(
            List<? extends Config> sourceConfigs, List<? extends Config> sinkConfigs) {
        executeForPlugins("sink", sinkConfigs, "post");
        executeForPlugins("source", sourceConfigs, "post");
    }

    private void executeForPlugins(
            String pluginType, List<? extends Config> pluginConfigs, String phase) {
        if (!rootConfig.hasPath(LIFECYCLE) || pluginConfigs == null) {
            return;
        }

        for (Config pluginConfig : pluginConfigs) {
            for (String target : lifecycleTargets(pluginType, pluginConfig)) {
                String actionPath = ConfigUtil.joinPath(LIFECYCLE, pluginType, target, phase);
                if (rootConfig.hasPath(actionPath)) {
                    executeAction(target, phase, rootConfig.getConfig(actionPath));
                }
            }
        }
    }

    private List<String> lifecycleTargets(String pluginType, Config pluginConfig) {
        if ("source".equals(pluginType)) {
            if (!pluginConfig.hasPath("plugin_output")) {
                return Collections.emptyList();
            }
            return Collections.singletonList(pluginConfig.getString("plugin_output"));
        }
        if (!pluginConfig.hasPath("plugin_input")) {
            return Collections.emptyList();
        }
        if (pluginConfig.getValue("plugin_input").valueType() == ConfigValueType.LIST) {
            return pluginConfig.getStringList("plugin_input");
        }
        return Collections.singletonList(pluginConfig.getString("plugin_input"));
    }

    private void executeAction(String target, String phase, Config actionConfig) {
        String type = actionConfig.getString("type");
        try {
            switch (type) {
                case "jdbc_ready":
                    executeJdbcReady(target, phase, actionConfig);
                    break;
                case "jdbc_sql":
                    executeJdbcSql(target, phase, actionConfig);
                    break;
                case "jdbc_recon":
                    executeJdbcRecon(target, phase, actionConfig);
                    break;
                default:
                    throw new TaskExecuteException(
                            "Unsupported lifecycle action type '" + type + "' for " + target);
            }
        } catch (SQLException | ClassNotFoundException e) {
            throw new TaskExecuteException(
                    "Lifecycle " + phase + " failed for target '" + target + "'", e);
        }
    }

    private void executeJdbcReady(String target, String phase, Config config)
            throws SQLException, ClassNotFoundException {
        try (Connection connection = openConnection(config);
                PreparedStatement statement =
                        connection.prepareStatement(config.getString("query"));
                ResultSet resultSet = statement.executeQuery()) {
            if (!resultSet.next()) {
                throw new SQLException("readiness query returned no rows");
            }
            String actual = resultSet.getString(1);
            String expected = config.hasPath("expect") ? config.getString("expect") : "1";
            if (!expected.equals(actual)) {
                throw new SQLException("expected '" + expected + "' but received '" + actual + "'");
            }
            LOGGER.info("Lifecycle {} readiness check passed for target '{}'.", phase, target);
        }
    }

    private void executeJdbcSql(String target, String phase, Config config)
            throws SQLException, ClassNotFoundException {
        try (Connection connection = openConnection(config);
                PreparedStatement statement =
                        connection.prepareStatement(config.getString("query"))) {
            statement.execute();
            LOGGER.info("Lifecycle {} SQL action passed for target '{}'.", phase, target);
        }
    }

    private void executeJdbcRecon(String target, String phase, Config config)
            throws SQLException, ClassNotFoundException {
        if (!"post".equals(phase)) {
            throw new SQLException("jdbc_recon is only supported in the post phase");
        }
        List<String> sourceResult = executeScalarQuery(config, "source_query", "source_url");
        List<String> sinkResult = executeScalarQuery(config, "sink_query", "sink_url");
        if (!sourceResult.equals(sinkResult)) {
            throw new SQLException(
                    "source result " + sourceResult + " does not match sink result " + sinkResult);
        }
        LOGGER.info("Lifecycle post recon passed for target '{}': {}", target, sourceResult);
    }

    private List<String> executeScalarQuery(Config config, String queryKey, String urlKey)
            throws SQLException, ClassNotFoundException {
        String url = config.hasPath(urlKey) ? config.getString(urlKey) : config.getString("url");
        loadDriver(config);
        try (Connection connection = openConnection(config, url);
                PreparedStatement statement =
                        connection.prepareStatement(config.getString(queryKey));
                ResultSet resultSet = statement.executeQuery()) {
            if (!resultSet.next()) {
                throw new SQLException(queryKey + " returned no rows");
            }
            ResultSetMetaData metadata = resultSet.getMetaData();
            List<String> values = new ArrayList<>(metadata.getColumnCount());
            for (int i = 1; i <= metadata.getColumnCount(); i++) {
                values.add(String.valueOf(resultSet.getObject(i)));
            }
            if (resultSet.next()) {
                throw new SQLException(queryKey + " must return exactly one row");
            }
            return values;
        }
    }

    private Connection openConnection(Config config) throws SQLException, ClassNotFoundException {
        loadDriver(config);
        return openConnection(config, config.getString("url"));
    }

    private Connection openConnection(Config config, String url) throws SQLException {
        String username = config.hasPath("username") ? config.getString("username") : "";
        String password = config.hasPath("password") ? config.getString("password") : "";
        if (config.hasPath("driver")) {
            try {
                Class<?> driverClass =
                        Class.forName(
                                config.getString("driver"),
                                true,
                                Thread.currentThread().getContextClassLoader());
                java.sql.Driver driver =
                        (java.sql.Driver) driverClass.getDeclaredConstructor().newInstance();
                Properties properties = new Properties();
                properties.setProperty("user", username);
                properties.setProperty("password", password);
                Connection connection = driver.connect(url, properties);
                if (connection == null) {
                    throw new SQLException("JDBC driver rejected URL: " + url);
                }
                return connection;
            } catch (ReflectiveOperationException e) {
                throw new SQLException("Unable to instantiate JDBC driver", e);
            }
        }
        return DriverManager.getConnection(url, username, password);
    }

    private void loadDriver(Config config) throws ClassNotFoundException {
        if (config.hasPath("driver")) {
            Class.forName(config.getString("driver"));
        }
    }
}
