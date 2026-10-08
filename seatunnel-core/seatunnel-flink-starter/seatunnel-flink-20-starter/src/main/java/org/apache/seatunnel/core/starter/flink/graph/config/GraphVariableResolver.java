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

package org.apache.seatunnel.core.starter.flink.graph.config;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Resolves ${NAME} placeholders without exposing the supplied values in logs. */
public final class GraphVariableResolver {

    private static final Pattern VARIABLE = Pattern.compile("\\$\\{([^}]+)}");

    private GraphVariableResolver() {}

    public static Object resolve(Object value, Map<String, String> variables) {
        if (value instanceof String) {
            return resolveString((String) value, variables);
        }
        if (value instanceof Map) {
            Map<Object, Object> result = new LinkedHashMap<>();
            for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
                result.put(entry.getKey(), resolve(entry.getValue(), variables));
            }
            return result;
        }
        if (value instanceof Iterable) {
            List<Object> result = new ArrayList<>();
            for (Object item : (Iterable<?>) value) {
                result.add(resolve(item, variables));
            }
            return result;
        }
        return value;
    }

    private static String resolveString(String value, Map<String, String> variables) {
        Matcher matcher = VARIABLE.matcher(value);
        StringBuffer result = new StringBuffer();
        while (matcher.find()) {
            String name = matcher.group(1);
            if (variables == null || !variables.containsKey(name)) {
                throw new IllegalArgumentException("Missing graph variable: " + name);
            }
            String replacement = variables.get(name);
            if (replacement == null) {
                throw new IllegalArgumentException("Graph variable must not be null: " + name);
            }
            matcher.appendReplacement(result, Matcher.quoteReplacement(replacement));
        }
        matcher.appendTail(result);
        return result.toString();
    }
}
