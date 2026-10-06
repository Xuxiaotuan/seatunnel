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

import org.apache.seatunnel.core.starter.exception.TaskExecuteException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;

class JobLifecycleExecutorTest {

    @Test
    void rejectsLifecycleTargetThatIsNotPresentInTheJobGraph() {
        Config config =
                ConfigFactory.parseString(
                        "lifecycle {\n"
                                + " source {\n"
                                + "  missing {\n"
                                + "   pre { type = jdbc_sql, url = \"jdbc:test\", query = \"select 1\" }\n"
                                + "  }\n"
                                + " }\n"
                                + "}\n");
        Config source = ConfigFactory.parseString("plugin_output = input");

        Assertions.assertThrows(
                TaskExecuteException.class,
                () ->
                        new JobLifecycleExecutor(config)
                                .validateConfiguration(
                                        Collections.singletonList(source),
                                        Collections.emptyList()));
    }

    @Test
    void rejectsJdbcReadyActionWithoutQuery() {
        Config config =
                ConfigFactory.parseString(
                        "lifecycle {\n"
                                + " source {\n"
                                + "  input {\n"
                                + "   pre { type = jdbc_ready, url = \"jdbc:test\" }\n"
                                + "  }\n"
                                + " }\n"
                                + "}\n");
        Config source = ConfigFactory.parseString("plugin_output = input");

        Assertions.assertThrows(
                TaskExecuteException.class,
                () ->
                        new JobLifecycleExecutor(config)
                                .validateConfiguration(
                                        Collections.singletonList(source),
                                        Collections.emptyList()));
    }

    @Test
    void rejectsReconConfiguredInPrePhase() {
        Config config =
                ConfigFactory.parseString(
                        "lifecycle {\n"
                                + " source {\n"
                                + "  input {\n"
                                + "   pre { type = jdbc_recon, source_query = \"select 1\", sink_query = \"select 1\", url = \"jdbc:test\" }\n"
                                + "  }\n"
                                + " }\n"
                                + "}\n");
        Config source = ConfigFactory.parseString("plugin_output = input");

        Assertions.assertThrows(
                TaskExecuteException.class,
                () ->
                        new JobLifecycleExecutor(config)
                                .validateConfiguration(
                                        Collections.singletonList(source),
                                        Collections.emptyList()));
    }

    @Test
    void fallsBackWhenPreferredConnectionValueIsBlank() {
        Config config = ConfigFactory.parseString("url = \"jdbc:test\"\n" + "source_url = \"\"\n");

        Assertions.assertEquals(
                "jdbc:test",
                JobLifecycleExecutor.resolveConnectionValue(config, "source_url", "url"));
    }

    @Test
    void wrapsInvalidLifecycleValueTypesAsTaskExecuteException() {
        Config config =
                ConfigFactory.parseString(
                        "lifecycle {\n"
                                + " source {\n"
                                + "  input {\n"
                                + "   pre { type = jdbc_ready, url = \"jdbc:test\", query = \"select 1\", query_timeout_seconds = \"bad\" }\n"
                                + "  }\n"
                                + " }\n"
                                + "}\n");
        Config source = ConfigFactory.parseString("plugin_output = input");

        Assertions.assertThrows(
                TaskExecuteException.class,
                () ->
                        new JobLifecycleExecutor(config)
                                .validateConfiguration(
                                        Collections.singletonList(source),
                                        Collections.emptyList()));
    }
}
