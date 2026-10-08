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

package org.apache.seatunnel.core.starter.flink.graph.execution;

import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.Map;

/** Command-line entry point for the experimental Flink YAML graph runner. */
public final class FlinkGraphMain {

    private FlinkGraphMain() {}

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            throw new IllegalArgumentException(
                    "Usage: FlinkGraphMain <graph.yaml> [NAME=value ...]");
        }
        Map<String, String> variables = new LinkedHashMap<>();
        for (int index = 1; index < args.length; index++) {
            String[] pair = args[index].split("=", 2);
            if (pair.length != 2 || pair[0].trim().isEmpty()) {
                throw new IllegalArgumentException("Variables must use NAME=value syntax");
            }
            variables.put(pair[0], pair[1]);
        }
        new GraphFlinkExecution().execute(Paths.get(args[0]), variables);
    }
}
