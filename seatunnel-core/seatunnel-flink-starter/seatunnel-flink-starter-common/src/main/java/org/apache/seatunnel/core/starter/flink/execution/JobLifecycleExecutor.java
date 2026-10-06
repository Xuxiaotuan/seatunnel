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
        validateConfiguration(sourceConfigs, sinkConfigs);
        executeForPlugins("source", sourceConfigs, "pre");
        executeForPlugins("sink", sinkConfigs, "pre");
    }

    public void executePost(
            List<? extends Config> sourceConfigs, List<? extends Config> sinkConfigs) {
        validateConfiguration(sourceConfigs, sinkConfigs);
        executeForPlugins("sink", sinkConfigs, "post");
        executeForPlugins("source", sourceConfigs, "post");
    }

    void validateConfiguration(
            List<? extends Config> sourceConfigs, List<? extends Config> sinkConfigs) {
        if (!rootConfig.hasPath(LIFECYCLE)) {
            return;
        }
        validatePluginLifecycle("source", sourceConfigs);
        validatePluginLifecycle("sink", sinkConfigs);
    }

    private void validatePluginLifecycle(String pluginType, List<? extends Config> pluginConfigs) {
        Config lifecycle = rootConfig.getConfig(LIFECYCLE);
        if (!lifecycle.hasPath(pluginType)) {
            return;
        }
        java.util.Set<String> configuredTargets = new java.util.HashSet<>();
        if (pluginConfigs != null) {
            for (Config pluginConfig : pluginConfigs) {
                configuredTargets.addAll(lifecycleTargets(pluginType, pluginConfig));
            }
        }
        Config pluginLifecycle = lifecycle.getConfig(pluginType);
        for (String target : pluginLifecycle.root().keySet()) {
            if (!configuredTargets.contains(target)) {
                throw new TaskExecuteException(
                        "Lifecycle target '"
                                + target
                                + "' is not present in the configured "
                                + pluginType
                                + " plugins");
            }
            Config targetConfig = pluginLifecycle.getConfig(target);
            for (String phase : targetConfig.root().keySet()) {
                if (!"pre".equals(phase) && !"post".equals(phase)) {
                    throw new TaskExecuteException(
                            "Unsupported lifecycle phase '"
                                    + phase
                                    + "' for target '"
                                    + target
                                    + "'");
                }
                validateAction(target, phase, targetConfig.getConfig(phase));
            }
        }
    }

    private void validateAction(String target, String phase, Config actionConfig) {
        if (!actionConfig.hasPath("type")) {
            throw new TaskExecuteException(
                    "Lifecycle action for target '" + target + "' must define type");
        }
        String type = actionConfig.getString("type");
        if (actionConfig.hasPath("query_timeout_seconds")
                && actionConfig.getInt("query_timeout_seconds") <= 0) {
            throw new TaskExecuteException(
                    "Lifecycle action for target '"
                            + target
                            + "' must use a positive query_timeout_seconds");
        }
        switch (type) {
            case "jdbc_ready":
            case "jdbc_sql":
                requireString(actionConfig, "url", target, type);
                requireString(actionConfig, "query", target, type);
                break;
            case "jdbc_recon":
                if (!"post".equals(phase)) {
                    throw new TaskExecuteException(
                            "jdbc_recon is only supported in the post phase for target '"
                                    + target
                                    + "'");
                }
                requireString(actionConfig, "source_query", target, type);
                requireString(actionConfig, "sink_query", target, type);
                requireConnectionValue(actionConfig, "source_url", target, type);
                requireConnectionValue(actionConfig, "sink_url", target, type);
                break;
            default:
                throw new TaskExecuteException(
                        "Unsupported lifecycle action type '" + type + "' for " + target);
        }
    }

    private void requireString(Config config, String key, String target, String type) {
        if (!config.hasPath(key) || config.getString(key).trim().isEmpty()) {
            throw new TaskExecuteException(
                    "Lifecycle action '"
                            + type
                            + "' for target '"
                            + target
                            + "' must define non-blank "
                            + key);
        }
    }

    private void requireConnectionValue(Config config, String key, String target, String type) {
        boolean preferredPresent = config.hasPath(key) && !config.getString(key).trim().isEmpty();
        boolean fallbackPresent =
                config.hasPath("url") && !config.getString("url").trim().isEmpty();
        if (!preferredPresent && !fallbackPresent) {
            throw new TaskExecuteException(
                    "Lifecycle action '"
                            + type
                            + "' for target '"
                            + target
                            + "' must define "
                            + key
                            + " or url");
        }
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
                        connection.prepareStatement(config.getString("query"))) {
            configureQueryTimeout(statement, config);
            try (ResultSet resultSet = statement.executeQuery()) {
                if (!resultSet.next()) {
                    throw new SQLException("readiness query returned no rows");
                }
                String actual = resultSet.getString(1);
                String expected = config.hasPath("expect") ? config.getString("expect") : "1";
                if (!expected.equals(actual)) {
                    throw new SQLException(
                            "expected '" + expected + "' but received '" + actual + "'");
                }
            }
            LOGGER.info("Lifecycle {} readiness check passed for target '{}'.", phase, target);
        }
    }

    private void executeJdbcSql(String target, String phase, Config config)
            throws SQLException, ClassNotFoundException {
        try (Connection connection = openConnection(config);
                PreparedStatement statement =
                        connection.prepareStatement(config.getString("query"))) {
            configureQueryTimeout(statement, config);
            statement.execute();
            LOGGER.info("Lifecycle {} SQL action passed for target '{}'.", phase, target);
        }
    }

    private void executeJdbcRecon(String target, String phase, Config config)
            throws SQLException, ClassNotFoundException {
        if (!"post".equals(phase)) {
            throw new SQLException("jdbc_recon is only supported in the post phase");
        }
        List<String> sourceResult =
                executeScalarQuery(
                        config,
                        "source_query",
                        "source_url",
                        "source_driver",
                        "source_username",
                        "source_password");
        List<String> sinkResult =
                executeScalarQuery(
                        config,
                        "sink_query",
                        "sink_url",
                        "sink_driver",
                        "sink_username",
                        "sink_password");
        if (!sourceResult.equals(sinkResult)) {
            throw new SQLException(
                    "source result " + sourceResult + " does not match sink result " + sinkResult);
        }
        LOGGER.info("Lifecycle post recon passed for target '{}': {}", target, sourceResult);
    }

    private List<String> executeScalarQuery(
            Config config,
            String queryKey,
            String urlKey,
            String driverKey,
            String usernameKey,
            String passwordKey)
            throws SQLException, ClassNotFoundException {
        String url = config.hasPath(urlKey) ? config.getString(urlKey) : config.getString("url");
        try (Connection connection =
                        openConnection(config, url, driverKey, usernameKey, passwordKey);
                PreparedStatement statement =
                        connection.prepareStatement(config.getString(queryKey))) {
            configureQueryTimeout(statement, config);
            try (ResultSet resultSet = statement.executeQuery()) {
                if (!resultSet.next()) {
                    throw new SQLException(queryKey + " returned no rows");
                }
                ResultSetMetaData metadata = resultSet.getMetaData();
                List<String> values = new ArrayList<>(metadata.getColumnCount());
                for (int i = 1; i <= metadata.getColumnCount(); i++) {
                    Object value = resultSet.getObject(i);
                    values.add(resultSet.wasNull() ? "<NULL>" : String.valueOf(value));
                }
                if (resultSet.next()) {
                    throw new SQLException(queryKey + " must return exactly one row");
                }
                return values;
            }
        }
    }

    private Connection openConnection(Config config) throws SQLException, ClassNotFoundException {
        return openConnection(config, config.getString("url"), "driver", "username", "password");
    }

    private Connection openConnection(
            Config config, String url, String driverKey, String usernameKey, String passwordKey)
            throws SQLException, ClassNotFoundException {
        String username = connectionValue(config, usernameKey, "username");
        String password = connectionValue(config, passwordKey, "password");
        String driverClassName = connectionValue(config, driverKey, "driver");
        if (!driverClassName.isEmpty()) {
            try {
                Class<?> driverClass =
                        Class.forName(
                                driverClassName,
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

    private String connectionValue(Config config, String preferredKey, String fallbackKey) {
        if (config.hasPath(preferredKey)) {
            return config.getString(preferredKey);
        }
        return config.hasPath(fallbackKey) ? config.getString(fallbackKey) : "";
    }

    private void configureQueryTimeout(PreparedStatement statement, Config config)
            throws SQLException {
        if (config.hasPath("query_timeout_seconds")) {
            int timeoutSeconds = config.getInt("query_timeout_seconds");
            if (timeoutSeconds <= 0) {
                throw new SQLException("query_timeout_seconds must be positive");
            }
            statement.setQueryTimeout(timeoutSeconds);
        }
    }
}
