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

package org.apache.seatunnel.core.starter.flink.graph.runtime;

import org.apache.seatunnel.core.starter.flink.graph.api.NodeCapability;

/** A completion boundary emitted by a terminal data operator. */
public final class CompletionRuntimeValue implements GraphRuntimeValue {

    private final String nodeId;

    public CompletionRuntimeValue(String nodeId) {
        if (nodeId == null || nodeId.trim().isEmpty()) {
            throw new IllegalArgumentException("nodeId must not be empty");
        }
        this.nodeId = nodeId;
    }

    @Override
    public NodeCapability.ValueKind kind() {
        return NodeCapability.ValueKind.COMPLETION;
    }

    public String nodeId() {
        return nodeId;
    }
}
