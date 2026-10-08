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

import org.apache.flink.api.common.JobExecutionResult;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Result of one graph compilation and Flink submission. */
public final class GraphExecutionResult {

    private final String jobName;
    private final List<String> orderedNodeIds;
    private final JobExecutionResult jobExecutionResult;
    private final int executeCallCount;

    public GraphExecutionResult(
            String jobName,
            List<String> orderedNodeIds,
            JobExecutionResult jobExecutionResult,
            int executeCallCount) {
        this.jobName = jobName;
        this.orderedNodeIds = Collections.unmodifiableList(new ArrayList<>(orderedNodeIds));
        this.jobExecutionResult = jobExecutionResult;
        this.executeCallCount = executeCallCount;
    }

    public String jobName() {
        return jobName;
    }

    public List<String> orderedNodeIds() {
        return orderedNodeIds;
    }

    public JobExecutionResult jobExecutionResult() {
        return jobExecutionResult;
    }

    public int executeCallCount() {
        return executeCallCount;
    }
}
